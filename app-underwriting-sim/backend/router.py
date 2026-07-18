"""FastAPI routes for the Underwriting Simulation Portal."""

import asyncio
import json
from typing import Optional

from databricks.sdk import WorkspaceClient
from fastapi import APIRouter, HTTPException
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

api = APIRouter(prefix="/api")


# ===================================================================
# Health
# ===================================================================

@api.get("/health")
async def health():
    import os
    return {
        "status": "ok",
        "db_initialized": db._initialized,
        "lakebase_project": os.environ.get("LAKEBASE_PROJECT_ID", "not set"),
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
async def simulate(body: SimulateIn):
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
                created_by="underwriter",
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
async def update_sim(sim_id: str, body: SimulationUpdateIn):
    if not db._initialized:
        raise HTTPException(503, "Database not initialized")
    async with db.session() as session:
        row = await update_simulation(
            session,
            sim_id,
            actor="underwriter",
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
async def create_comp(body: ComparisonIn):
    if not db._initialized:
        raise HTTPException(503, "Database not initialized")
    async with db.session() as session:
        comp = await create_comparison(
            session,
            comparison_name=body.comparison_name,
            simulation_ids=[str(s) for s in body.simulation_ids],
            created_by="underwriter",
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
# Genie
# ===================================================================

@api.post("/genie/ask", response_model=GenieResponseOut)
async def genie_ask(body: GenieQuestionIn):
    result = await asyncio.to_thread(ask_genie, body)
    return result
