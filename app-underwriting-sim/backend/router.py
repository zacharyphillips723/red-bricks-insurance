"""FastAPI routes for the Underwriting Simulation Portal."""

import asyncio
import json
from typing import Optional

from databricks.sdk import WorkspaceClient
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse

from .agent import query_underwriting_agent, stream_underwriting_agent, _execute_sql
from .data_loader import data_cache
from .database import db
from .genie import ask_genie
from .env_config import (
    UW_AGENT_ENDPOINT, LLM_ENDPOINT, UC_CATALOG,
    UC_TRACE_SCHEMA, UC_TRACE_TABLE_PREFIX,
)

# Models this app invokes — used to scope the observability cost query.
OBSERVED_MODELS = [UW_AGENT_ENDPOINT, LLM_ENDPOINT]
from .models import (
    AgentChatIn,
    AgentChatOut,
    BaselineSummaryOut,
    BookOfBusinessSummaryOut,
    ComparisonIn,
    ComparisonOut,
    FactorTablesOut,
    GenieQuestionIn,
    GenieResponseOut,
    RateBuildupIn,
    RateBuildupOut,
    RiskPoolOut,
    SimulateIn,
    SimulateOut,
    SimulationDetailOut,
    SimulationListOut,
    SimulationUpdateIn,
    AuditLogEntry,
    ScenarioPackageIn,
    ScenarioPackageOut,
    FundingArrangementInfo,
    FundingQuoteIn,
    AuthorityTier,
    ApprovalIn,
    ApprovalDecisionIn,
    ApprovalOut,
    FactorVersionCreateIn,
    FactorVersionOut,
    IntakeExtractIn,
    IntakeParseIn,
    RerateIn,
    PreflightIn,
    GuardrailIn,
    OutcomeIn,
)
from .scenarios import (
    create_comparison,
    delete_simulation,
    get_audit_log,
    get_comparison,
    get_simulation,
    list_comparisons,
    list_simulations,
    save_simulation,
    update_simulation,
)
from .pricing_engine import compute_rate_buildup, compute_risk_pool, get_book_of_business_summary, get_factor_tables
from .simulation_engine import run_simulation
from .scenario_packager import package_scenarios
from .funding_arrangements import FUNDING_ARRANGEMENTS, price_funding_arrangement
from .funding_store import (
    save_funding_quote,
    list_funding_quotes,
    get_funding_quote,
    update_funding_quote_status,
    save_quote_revision,
    list_quote_revisions,
)
from .intake import extract_submission, parse_intake
from .negotiation import rerate_quote
from .ops_analytics import operational_analytics, reconciliation
from .preflight import preflight_check
from .twin_memory import (
    list_guardrails,
    create_guardrail,
    recent_memory,
    record_outcome,
    get_profile,
)
from .approvals import (
    AUTHORITY_MATRIX,
    create_approval,
    list_approvals,
    get_approval,
    decide_approval,
    get_approval_audit,
)
from .factor_governance import (
    create_factor_version,
    list_factor_versions,
    get_factor_version,
    approve_factor_version,
    publish_factor_version,
)

api = APIRouter(prefix="/api")


def _actor(request: Request) -> str:
    """Resolve the acting user from Databricks Apps forwarded identity headers.

    Databricks Apps inject the end user's identity on every request; prefer the
    email, then the preferred username, falling back to a generic label when the
    app is run outside the Apps proxy (e.g. local dev).
    """
    hdr = request.headers
    return (
        hdr.get("X-Forwarded-Email")
        or hdr.get("X-Forwarded-Preferred-Username")
        or hdr.get("X-Forwarded-User")
        or "underwriter"
    )


# ===================================================================
# Health
# ===================================================================

@api.get("/health")
async def health():
    import os
    return {
        "status": "ok",
        "db_initialized": db._initialized,
        "app_state_schema": os.environ.get("APP_STATE_SCHEMA", "app_state"),
    }


# ===================================================================
# Baseline data
# ===================================================================

@api.get("/baseline", response_model=BaselineSummaryOut)
async def get_baseline(lob: Optional[str] = None):
    """Current book-level financials from cached gold tables."""
    summary = await asyncio.to_thread(data_cache.get_baseline_summary, lob)
    return BaselineSummaryOut(**summary)


@api.post("/baseline/refresh")
async def refresh_baseline():
    """Force refresh cached gold table data."""
    data_cache.invalidate()
    return {"status": "cache_invalidated"}


# ===================================================================
# Simulations — run
# ===================================================================

@api.post("/simulate", response_model=SimulateOut)
async def simulate(body: SimulateIn, request: Request):
    """Run a what-if simulation and optionally save to Lakebase."""
    result = await asyncio.to_thread(
        run_simulation,
        data_cache,
        body.simulation_type.value,
        body.parameters,
    )

    sim_id = None

    if body.save:
        if not body.name:
            raise HTTPException(400, "name is required when save=True")
        if not db._initialized:
            raise HTTPException(503, "Database not initialized — cannot save simulation")

        async with db.session() as session:
            saved = await save_simulation(
                session,
                simulation_name=body.name,
                simulation_type=body.simulation_type.value,
                parameters=body.parameters,
                results=result,
                baseline_snapshot=result.get("baseline", {}),
                created_by=_actor(request),
                scope_lob=body.parameters.get("lob"),
                scope_group_id=body.parameters.get("group_id"),
            )
            sim_id = saved["simulation_id"]

    return SimulateOut(
        simulation_id=sim_id,
        simulation_type=body.simulation_type,
        baseline=result["baseline"],
        projected=result["projected"],
        delta=result["delta"],
        delta_pct=result["delta_pct"],
        narrative=result["narrative"],
        warnings=result.get("warnings", []),
    )


# ===================================================================
# Packaged Scenarios (Standard / Competitive / Retention / Custom)
# ===================================================================

@api.post("/scenarios/package", response_model=ScenarioPackageOut)
async def scenarios_package(body: ScenarioPackageIn):
    """Generate Standard/Competitive/Retention/Custom renewal pricing scenarios,
    each with projected margin, MLR, and modeled retention, plus an explainable
    recommendation that maximizes expected retained margin."""
    result = await asyncio.to_thread(package_scenarios, data_cache, body.model_dump())
    return ScenarioPackageOut(**result)


# ===================================================================
# Simulations — CRUD
# ===================================================================

@api.get("/simulations", response_model=list[SimulationListOut])
async def list_sims(
    simulation_type: Optional[str] = None,
    status: Optional[str] = None,
    lob: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
):
    if not db._initialized:
        return []
    async with db.session() as session:
        rows = await list_simulations(
            session,
            simulation_type=simulation_type,
            status=status,
            lob=lob,
            limit=limit,
            offset=offset,
        )
        return [SimulationListOut(**r) for r in rows]


@api.get("/simulations/{sim_id}", response_model=SimulationDetailOut)
async def get_sim(sim_id: str):
    if not db._initialized:
        raise HTTPException(503, "Database not initialized")
    async with db.session() as session:
        row = await get_simulation(session, sim_id)
        if not row:
            raise HTTPException(404, "Simulation not found")
        return SimulationDetailOut(**row)


@api.patch("/simulations/{sim_id}", response_model=SimulationDetailOut)
async def update_sim(sim_id: str, body: SimulationUpdateIn, request: Request):
    if not db._initialized:
        raise HTTPException(503, "Database not initialized")
    async with db.session() as session:
        row = await update_simulation(
            session,
            sim_id,
            actor=_actor(request),
            status=body.status.value if body.status else None,
            notes=body.notes,
        )
        if not row:
            raise HTTPException(404, "Simulation not found")
        return SimulationDetailOut(**row)


@api.delete("/simulations/{sim_id}")
async def delete_sim(sim_id: str):
    if not db._initialized:
        raise HTTPException(503, "Database not initialized")
    async with db.session() as session:
        deleted = await delete_simulation(session, sim_id)
        if not deleted:
            raise HTTPException(404, "Simulation not found")
        return {"status": "deleted"}


@api.get("/simulations/{sim_id}/audit", response_model=list[AuditLogEntry])
async def get_sim_audit(sim_id: str):
    if not db._initialized:
        return []
    async with db.session() as session:
        rows = await get_audit_log(session, sim_id)
        return [AuditLogEntry(**r) for r in rows]


# ===================================================================
# Comparisons
# ===================================================================

@api.post("/comparisons", response_model=ComparisonOut)
async def create_comp(body: ComparisonIn, request: Request):
    if not db._initialized:
        raise HTTPException(503, "Database not initialized")
    async with db.session() as session:
        comp = await create_comparison(
            session,
            comparison_name=body.comparison_name,
            simulation_ids=[str(s) for s in body.simulation_ids],
            created_by=_actor(request),
            notes=body.notes,
        )
        return ComparisonOut(**comp)


@api.get("/comparisons", response_model=list[ComparisonOut])
async def list_comps(limit: int = 20, offset: int = 0):
    if not db._initialized:
        return []
    async with db.session() as session:
        rows = await list_comparisons(session, limit=limit, offset=offset)
        return [ComparisonOut(**r) for r in rows]


@api.get("/comparisons/{comp_id}", response_model=ComparisonOut)
async def get_comp(comp_id: str):
    if not db._initialized:
        raise HTTPException(503, "Database not initialized")
    async with db.session() as session:
        comp = await get_comparison(session, comp_id)
        if not comp:
            raise HTTPException(404, "Comparison not found")
        return ComparisonOut(**comp)


# ===================================================================
# Actuarial Pricing — Rate Build-Up
# ===================================================================

@api.post("/pricing/rate-buildup", response_model=RateBuildupOut)
async def rate_buildup(body: RateBuildupIn):
    """Compute community-rated actuarial pricing with step-by-step factors."""
    result = await asyncio.to_thread(
        compute_rate_buildup,
        data_cache,
        avg_age_band=body.avg_age_band,
        county_type=body.county_type,
        sic_code=body.sic_code,
        loss_ratio=body.loss_ratio,
        credibility_factor=body.credibility_factor,
        trend_pct=body.trend_pct,
        lob=body.lob or "Commercial",
        group_id=body.group_id,
    )
    return RateBuildupOut(**result)


@api.get("/pricing/factor-tables", response_model=FactorTablesOut)
async def factor_tables():
    """Return all actuarial rating factor reference tables."""
    tables = get_factor_tables()
    return FactorTablesOut(**tables)


# ===================================================================
# Risk Pool Analysis
# ===================================================================

@api.get("/groups/{group_id}/risk-pool", response_model=RiskPoolOut)
async def group_risk_pool(group_id: str):
    """Compare a group's risk profile against the book of business."""
    result = await asyncio.to_thread(compute_risk_pool, data_cache, group_id)
    return RiskPoolOut(**result)


@api.get("/book-of-business/risk-summary", response_model=BookOfBusinessSummaryOut)
async def book_risk_summary():
    """Return aggregate book-of-business risk statistics."""
    return BookOfBusinessSummaryOut(**get_book_of_business_summary())


# ===================================================================
# Agent chat
# ===================================================================

@api.post("/agent/chat", response_model=AgentChatOut)
async def agent_chat(body: AgentChatIn):
    result = await asyncio.to_thread(
        query_underwriting_agent,
        body.message,
        body.conversation_history or None,
    )
    return AgentChatOut(**result)


@api.post("/agent/chat/stream")
async def agent_chat_stream(body: AgentChatIn):
    """SSE variant of /agent/chat — streams tool-progress milestones then the answer."""
    message = body.message
    history = body.conversation_history or None

    async def event_source():
        loop = asyncio.get_running_loop()
        queue: asyncio.Queue = asyncio.Queue()
        _SENTINEL = object()

        def _produce():
            try:
                for event_type, payload in stream_underwriting_agent(message, history):
                    loop.call_soon_threadsafe(queue.put_nowait, (event_type, payload))
            except Exception as e:  # pragma: no cover - defensive
                loop.call_soon_threadsafe(queue.put_nowait, ("error", {"message": str(e)}))
            finally:
                loop.call_soon_threadsafe(queue.put_nowait, _SENTINEL)

        producer = loop.run_in_executor(None, _produce)
        try:
            while True:
                item = await queue.get()
                if item is _SENTINEL:
                    break
                event_type, payload = item
                yield f"event: {event_type}\ndata: {json.dumps(payload, default=str)}\n\n"
        finally:
            await producer

    return StreamingResponse(
        event_source(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ===================================================================
# Observability — traces + model cost/usage
# ===================================================================

@api.get("/observability/traces")
async def observability_traces():
    """Recent agent + simulation traces from the UC OTel span tables."""
    spans_table = f"`{UC_CATALOG}`.`{UC_TRACE_SCHEMA}`.`{UC_TRACE_TABLE_PREFIX}_otel_spans`"
    sql = f"""
        SELECT trace_id,
               MIN(start_time_unix_nano) AS trace_start_ns,
               MAX(end_time_unix_nano) AS trace_end_ns,
               COUNT(*) AS span_count,
               CASE WHEN SUM(CASE WHEN status.code = 'STATUS_CODE_ERROR' THEN 1 ELSE 0 END) > 0
                    THEN 'ERROR' ELSE 'OK' END AS trace_status
        FROM {spans_table}
        GROUP BY trace_id
        ORDER BY trace_start_ns DESC
        LIMIT 25
    """
    try:
        rows = await asyncio.to_thread(_execute_sql, sql)
        records = []
        for d in rows:
            start_ns = int(d.get("trace_start_ns") or 0)
            end_ns = int(d.get("trace_end_ns") or 0)
            records.append({
                "request_id": d.get("trace_id", ""),
                "timestamp_ms": start_ns // 1_000_000 if start_ns else 0,
                "execution_time_ms": (end_ns - start_ns) // 1_000_000 if start_ns and end_ns else 0,
                "status": d.get("trace_status", "UNKNOWN"),
                "span_count": int(d.get("span_count") or 0),
            })
        return {"traces": records}
    except Exception as e:
        print(f"[observability] Trace fetch error: {e}")
        return {"traces": [], "error": str(e)}


@api.get("/observability/costs")
async def observability_costs():
    """Token usage + estimated cost per model, scoped to this workspace."""
    endpoints = ", ".join(f"'{m}'" for m in OBSERVED_MODELS)
    try:
        try:
            workspace_id = WorkspaceClient().get_workspace_id()
            workspace_filter = f"AND eu.workspace_id = '{workspace_id}'" if workspace_id else ""
        except Exception:
            workspace_filter = ""
        rows = await asyncio.to_thread(_execute_sql, f"""
            SELECT
                se.endpoint_name AS endpoint,
                COUNT(*) AS request_count,
                COALESCE(SUM(eu.input_token_count), 0) AS total_input_tokens,
                COALESCE(SUM(eu.output_token_count), 0) AS total_output_tokens,
                CASE se.endpoint_name
                  WHEN 'databricks-llama-4-maverick'
                    THEN ROUND(SUM(eu.input_token_count) * 0.40 / 1000000
                             + SUM(eu.output_token_count) * 1.60 / 1000000, 4)
                  WHEN 'databricks-claude-haiku-4-5'
                    THEN ROUND(SUM(eu.input_token_count) * 1.00 / 1000000
                             + SUM(eu.output_token_count) * 5.00 / 1000000, 4)
                  ELSE 0
                END AS estimated_cost_usd
            FROM system.serving.endpoint_usage eu
            JOIN system.serving.served_entities se
              ON eu.served_entity_id = se.served_entity_id
            WHERE se.endpoint_name IN ({endpoints})
              AND eu.request_time >= DATE_SUB(CURRENT_TIMESTAMP(), 30)
              {workspace_filter}
            GROUP BY se.endpoint_name
            ORDER BY request_count DESC
        """)
        return {"costs": rows}
    except Exception as e:
        print(f"[observability] Cost query error: {e}")
        return {"costs": [], "error": str(e)}


# ===================================================================
# Phase 2 — Funding arrangements
# ===================================================================

@api.get("/funding/arrangements", response_model=list[FundingArrangementInfo])
async def funding_arrangements():
    """List the supported funding arrangements and who bears the risk."""
    return [FundingArrangementInfo(**a) for a in FUNDING_ARRANGEMENTS]


@api.post("/funding/quote")
async def funding_quote(body: FundingQuoteIn, request: Request):
    """Price a quote under a funding arrangement; optionally persist it."""
    try:
        result = await asyncio.to_thread(
            price_funding_arrangement, data_cache, body.arrangement, body.parameters
        )
    except ValueError as e:
        raise HTTPException(400, str(e))

    quote_id = None
    if body.save:
        if not db._initialized:
            raise HTTPException(503, "Database not initialized — cannot save quote")
        params = body.parameters or {}
        group_name = body.group_name or params.get("group_name") or params.get("group_id") or "Unnamed group"
        async with db.session() as session:
            saved = await save_funding_quote(
                session,
                group_name=group_name,
                funding_arrangement=body.arrangement,
                inputs=params,
                result=result,
                created_by=_actor(request),
                lob=params.get("lob"),
                scope_group_id=params.get("group_id"),
            )
            quote_id = saved["quote_id"]
    return {**result, "quote_id": quote_id}


@api.get("/funding/quotes")
async def funding_quotes(status: Optional[str] = None, limit: int = 50, offset: int = 0):
    if not db._initialized:
        return []
    async with db.session() as session:
        return await list_funding_quotes(session, status=status, limit=limit, offset=offset)


@api.get("/funding/quotes/{quote_id}")
async def funding_quote_detail(quote_id: str):
    if not db._initialized:
        raise HTTPException(503, "Database not initialized")
    async with db.session() as session:
        row = await get_funding_quote(session, quote_id)
        if not row:
            raise HTTPException(404, "Quote not found")
        return row


@api.patch("/funding/quotes/{quote_id}/status")
async def funding_quote_status(quote_id: str, status: str):
    if not db._initialized:
        raise HTTPException(503, "Database not initialized")
    async with db.session() as session:
        try:
            row = await update_funding_quote_status(session, quote_id, status=status)
        except ValueError as e:
            raise HTTPException(400, str(e))
        if not row:
            raise HTTPException(404, "Quote not found")
        return row


# ===================================================================
# Phase 2 — Approval routing
# ===================================================================

@api.get("/approvals/authority-matrix", response_model=list[AuthorityTier])
async def approvals_authority_matrix():
    """Return the deterministic approval authority matrix (tiers + limits)."""
    return [AuthorityTier(**t) for t in AUTHORITY_MATRIX]


@api.post("/approvals", response_model=ApprovalOut)
async def approvals_create(body: ApprovalIn, request: Request):
    if not db._initialized:
        raise HTTPException(503, "Database not initialized")
    async with db.session() as session:
        row = await create_approval(
            session,
            subject=body.subject,
            decision_type=body.decision_type,
            dollar_impact=body.dollar_impact,
            rate_change_pct=body.rate_change_pct,
            requested_by=_actor(request),
            group_id=body.group_id,
            lob=body.lob,
            context=body.context,
        )
        return ApprovalOut(**row)


@api.get("/approvals", response_model=list[ApprovalOut])
async def approvals_list(status: Optional[str] = None, limit: int = 50, offset: int = 0):
    if not db._initialized:
        return []
    async with db.session() as session:
        rows = await list_approvals(session, status=status, limit=limit, offset=offset)
        return [ApprovalOut(**r) for r in rows]


@api.get("/approvals/{approval_id}", response_model=ApprovalOut)
async def approvals_get(approval_id: str):
    if not db._initialized:
        raise HTTPException(503, "Database not initialized")
    async with db.session() as session:
        row = await get_approval(session, approval_id)
        if not row:
            raise HTTPException(404, "Approval not found")
        return ApprovalOut(**row)


@api.post("/approvals/{approval_id}/decide", response_model=ApprovalOut)
async def approvals_decide(approval_id: str, body: ApprovalDecisionIn, request: Request):
    if not db._initialized:
        raise HTTPException(503, "Database not initialized")
    async with db.session() as session:
        try:
            row = await decide_approval(
                session, approval_id, decision=body.decision,
                decided_by=_actor(request), notes=body.notes,
            )
        except ValueError as e:
            raise HTTPException(400, str(e))
        if not row:
            raise HTTPException(404, "Approval not found")
        return ApprovalOut(**row)


@api.get("/approvals/{approval_id}/audit")
async def approvals_audit(approval_id: str):
    if not db._initialized:
        return []
    async with db.session() as session:
        return await get_approval_audit(session, approval_id)


# ===================================================================
# Phase 2 — Factor governance
# ===================================================================

@api.get("/factors/versions", response_model=list[FactorVersionOut])
async def factor_versions_list(limit: int = 50):
    if not db._initialized:
        return []
    async with db.session() as session:
        rows = await list_factor_versions(session, limit=limit)
        return [FactorVersionOut(**r) for r in rows]


@api.post("/factors/versions", response_model=FactorVersionOut)
async def factor_versions_create(body: FactorVersionCreateIn, request: Request):
    if not db._initialized:
        raise HTTPException(503, "Database not initialized")
    factors = [f.model_dump() for f in body.factors] if body.factors is not None else None
    async with db.session() as session:
        try:
            row = await create_factor_version(
                session, created_by=_actor(request),
                factors=factors, notes=body.notes, source=body.source,
            )
        except ValueError as e:
            raise HTTPException(400, str(e))
        return FactorVersionOut(**row)


@api.get("/factors/versions/{version_id}", response_model=FactorVersionOut)
async def factor_versions_get(version_id: str):
    if not db._initialized:
        raise HTTPException(503, "Database not initialized")
    async with db.session() as session:
        row = await get_factor_version(session, version_id)
        if not row:
            raise HTTPException(404, "Factor version not found")
        return FactorVersionOut(**row)


@api.post("/factors/versions/{version_id}/approve", response_model=FactorVersionOut)
async def factor_versions_approve(version_id: str, request: Request):
    if not db._initialized:
        raise HTTPException(503, "Database not initialized")
    async with db.session() as session:
        try:
            row = await approve_factor_version(session, version_id, approved_by=_actor(request))
        except ValueError as e:
            raise HTTPException(400, str(e))
        if not row:
            raise HTTPException(404, "Factor version not found")
        return FactorVersionOut(**row)


@api.post("/factors/versions/{version_id}/publish")
async def factor_versions_publish(version_id: str, request: Request):
    if not db._initialized:
        raise HTTPException(503, "Database not initialized")
    async with db.session() as session:
        try:
            return await publish_factor_version(session, version_id, published_by=_actor(request))
        except ValueError as e:
            raise HTTPException(400, str(e))


# ===================================================================
# Phase 3 — Intake & document intelligence
# ===================================================================

@api.post("/intake/extract")
async def intake_extract(body: IntakeExtractIn):
    """Extract a structured submission from pasted census/SBC/competitor text."""
    return await asyncio.to_thread(extract_submission, body.text, body.doc_type)


@api.post("/intake/parse")
async def intake_parse(body: IntakeParseIn):
    """Parse a free-text submission into structured fields + a completeness gate + memo."""
    return await asyncio.to_thread(parse_intake, body.text, body.strategy_memo)


# ===================================================================
# Phase 3 — Negotiation / re-rate loop
# ===================================================================

@api.post("/funding/quotes/{quote_id}/rerate")
async def funding_quote_rerate(quote_id: str, body: RerateIn, request: Request):
    """Apply a natural-language change to a saved quote, re-price, and record the revision."""
    if not db._initialized:
        raise HTTPException(503, "Database not initialized")
    async with db.session() as session:
        quote = await get_funding_quote(session, quote_id)
        if not quote:
            raise HTTPException(404, "Quote not found")
        try:
            rerate = await asyncio.to_thread(rerate_quote, data_cache, quote, body.instruction)
        except ValueError as e:
            raise HTTPException(400, str(e))
        await save_quote_revision(
            session,
            quote_id=quote_id,
            instruction=body.instruction,
            param_changes=rerate["param_changes"],
            result=rerate["result"],
            created_by=_actor(request),
        )
        revisions = await list_quote_revisions(session, quote_id)
    return {
        "quote_id": quote_id,
        "param_changes": rerate["param_changes"],
        "result": rerate["result"],
        "revisions": revisions,
    }


@api.get("/funding/quotes/{quote_id}/revisions")
async def funding_quote_revisions(quote_id: str):
    if not db._initialized:
        return []
    async with db.session() as session:
        return await list_quote_revisions(session, quote_id)


# ===================================================================
# Phase 3 — Operational analytics & reconciliation
# ===================================================================

@api.get("/ops/analytics")
async def ops_analytics():
    """Funnel conversion, approval velocity, factor drift, and cycle time."""
    if not db._initialized:
        return {}
    async with db.session() as session:
        return await operational_analytics(session)


@api.get("/ops/reconciliation")
async def ops_reconciliation():
    """Rated → Sold → Implemented lineage with per-stage totals."""
    if not db._initialized:
        return {"stage_summary": {}, "quotes": []}
    async with db.session() as session:
        return await reconciliation(session)


# ===================================================================
# Phase 4 (optional) — Underwriting Digital Twin
# ===================================================================

@api.post("/policy/preflight-check")
async def policy_preflight_check(body: PreflightIn, request: Request):
    """Advisory pre-implementation check: recall precedent, simulate impact, evaluate
    guardrails, critique (grounded), return a GREEN/AMBER/RED verdict, write candidate."""
    if not db._initialized:
        raise HTTPException(503, "Database not initialized")
    policy = body.model_dump()
    policy["analyst_id"] = policy.get("analyst_id") or _actor(request)
    async with db.session() as session:
        return await preflight_check(session, data_cache, policy)


@api.get("/twin/guardrails")
async def twin_guardrails_list(active_only: bool = True):
    if not db._initialized:
        return []
    async with db.session() as session:
        return await list_guardrails(session, active_only=active_only)


@api.post("/twin/guardrails")
async def twin_guardrails_create(body: GuardrailIn, request: Request):
    if not db._initialized:
        raise HTTPException(503, "Database not initialized")
    async with db.session() as session:
        return await create_guardrail(
            session,
            rule_type=body.rule_type,
            threshold=body.threshold,
            severity=body.severity,
            scope=body.scope,
            approved_by=_actor(request),
        )


@api.get("/twin/memory")
async def twin_memory_recent(limit: int = 25):
    if not db._initialized:
        return []
    async with db.session() as session:
        return await recent_memory(session, limit=limit)


@api.post("/twin/outcome")
async def twin_outcome(body: OutcomeIn):
    if not db._initialized:
        raise HTTPException(503, "Database not initialized")
    async with db.session() as session:
        return await record_outcome(
            session,
            decision_id=body.decision_id,
            actual_mlr=body.actual_mlr,
            actual_margin=body.actual_margin,
            retained=body.retained,
            note=body.note,
        )


@api.get("/twin/profile/{analyst_id}")
async def twin_profile(analyst_id: str):
    if not db._initialized:
        return None
    async with db.session() as session:
        return await get_profile(session, analyst_id)


# ===================================================================
# Genie
# ===================================================================

@api.post("/genie/ask", response_model=GenieResponseOut)
async def genie_ask(body: GenieQuestionIn):
    result = await asyncio.to_thread(ask_genie, body)
    return result
