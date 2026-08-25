"""FastAPI routes for the Claim Scrubber / Denial Risk Predictor."""

import asyncio
import json
import uuid
from decimal import Decimal
from typing import Optional

import mlflow
from databricks.sdk import WorkspaceClient
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from mlflow.entities import AssessmentSource
from sqlalchemy import text

from .database import db
from .denial_agent import _execute_sql
from . import scrub_engine as engine
from .sample_records import list_sample_scenarios, build_sample_draft
from .env_config import (
    SCRUB_AGENT_ENDPOINT, LLM_ENDPOINT, UC_CATALOG,
    UC_TRACE_SCHEMA, UC_TRACE_TABLE_PREFIX,
)
from .models import (
    CarcReferenceOut,
    DraftClaimIn,
    FeedbackIn,
    FeedbackRow,
    MemberSearchOut,
    ScrubResultOut,
    ScrubSessionSummary,
)

# Models this app invokes — used to scope the cost query.
OBSERVED_MODELS = [SCRUB_AGENT_ENDPOINT, LLM_ENDPOINT]

api = APIRouter(prefix="/api")


# ===================================================================
# Health
# ===================================================================

@api.get("/health", operation_id="healthCheck")
async def health_check():
    import os
    return {
        "status": "ok",
        "db_initialized": db._initialized,
        "lakebase_project": os.environ.get("LAKEBASE_PROJECT_ID", "not set"),
    }


# ===================================================================
# Members + reference
# ===================================================================

@api.get("/members/search", response_model=list[MemberSearchOut], operation_id="searchMembers")
async def members_search(q: str = ""):
    if not q or len(q.strip()) < 2:
        return []
    rows = await asyncio.to_thread(engine.search_members, q.strip(), 20)
    return [MemberSearchOut(**_coerce_row(r)) for r in rows]


@api.get("/reference/carc", response_model=list[CarcReferenceOut], operation_id="getCarcReference")
async def carc_reference():
    rows = await asyncio.to_thread(engine.get_carc_reference)
    return [CarcReferenceOut(**{k: r.get(k) for k in CarcReferenceOut.model_fields}) for r in rows]


# ===================================================================
# Denial Intelligence — propensity, drivers, correlations
# ===================================================================

@api.get("/analytics/propensity", operation_id="getPropensityDistribution")
async def analytics_propensity():
    return await asyncio.to_thread(engine.get_propensity_distribution)


@api.get("/analytics/drivers", operation_id="getDenialDrivers")
async def analytics_drivers():
    return {"drivers": await asyncio.to_thread(engine.get_denial_drivers)}


@api.get("/analytics/correlations", operation_id="getDenialCorrelations")
async def analytics_correlations(dimension: str = "procedure"):
    rows = await asyncio.to_thread(engine.get_denial_correlations, dimension, 12)
    return {"dimension": dimension, "rows": rows}


@api.get("/analytics/forecast", operation_id="getDenialForecast")
async def analytics_forecast():
    return {"series": await asyncio.to_thread(engine.get_denial_forecast)}


# ===================================================================
# Sample drafts
# ===================================================================

@api.get("/scrub/samples", operation_id="listScrubSamples")
async def scrub_samples():
    """List sample scenarios, each with a runnable draft bound to a real member.

    The eligibility scenario is bound to any member; others are bound to an
    active member so the non-eligibility rules fire cleanly.
    """
    scenarios = list_sample_scenarios()

    def _pick_member(active: bool) -> dict:
        rows = engine.search_members("MBR", 25)
        for r in rows:
            if bool(r.get("is_active")) == active:
                return r
        return rows[0] if rows else {"member_id": "MBR000001"}

    active_member = await asyncio.to_thread(_pick_member, True)
    out = []
    for s in scenarios:
        member = active_member
        draft = build_sample_draft(s["scenario"], member.get("member_id"),
                                   member.get("line_of_business"))
        out.append({**s, "draft": draft})
    return {"samples": out}


# ===================================================================
# Scrub — run (sync + streaming) with Lakebase persistence
# ===================================================================

def _new_session_id() -> str:
    return f"SCR-{uuid.uuid4().hex[:12].upper()}"


async def _persist_result(result: dict, draft: dict) -> str:
    """Insert the scrub session + findings + remediations; return session_id."""
    session_id = _new_session_id()
    async with db.session() as session:
        await session.execute(
            text("""
                INSERT INTO scrub_sessions (
                    session_id, member_id, member_name, provider_npi, date_of_service,
                    request_type, risk_score, decision, ml_denial_prob, line_count,
                    dx_codes, clinical_notes, resubmitted_from, mlflow_trace_id
                ) VALUES (
                    :sid, :member_id, :member_name, :npi, CAST(:dos AS DATE),
                    CAST(:req_type AS scrub_request_type), :risk, CAST(:decision AS scrub_decision),
                    :ml_prob, :line_count, :dx, :notes, :resub_from, :trace_id
                )
            """),
            {
                "sid": session_id,
                "member_id": result["member_id"],
                "member_name": result.get("member_name"),
                "npi": result.get("provider_npi"),
                "dos": result.get("date_of_service"),
                "req_type": result.get("request_type") or "claim",
                "risk": result.get("risk_score"),
                "decision": result.get("decision"),
                "ml_prob": result.get("ml_denial_prob"),
                "line_count": len(draft.get("lines", [])),
                "dx": "|".join(draft.get("dx_codes", [])) or None,
                "notes": draft.get("clinical_notes"),
                "resub_from": result.get("resubmitted_from"),
                "trace_id": result.get("trace_id"),
            },
        )
        for card in result.get("reason_cards", []):
            await session.execute(
                text("""
                    INSERT INTO scrub_line_findings
                        (session_id, carc_code, reason_category, reason_label, likelihood, layer, evidence)
                    VALUES (:sid, :carc, :cat, :label, :lik, CAST(:layer AS scrub_reason_layer), :ev)
                """),
                {
                    "sid": session_id, "carc": card["carc_code"],
                    "cat": card.get("reason_category"), "label": card.get("reason_label"),
                    "lik": card.get("likelihood"), "layer": card.get("layer"),
                    "ev": card.get("evidence"),
                },
            )
            if card.get("remediation") or card.get("required_action"):
                await session.execute(
                    text("""
                        INSERT INTO scrub_remediations
                            (session_id, carc_code, remediation_text, required_action, doc_needed)
                        VALUES (:sid, :carc, :rem, :action, :doc)
                    """),
                    {
                        "sid": session_id, "carc": card["carc_code"],
                        "rem": card.get("remediation"), "action": card.get("required_action"),
                        "doc": card.get("doc_needed"),
                    },
                )
        await session.commit()
    return session_id


@api.post("/scrub/run", response_model=ScrubResultOut, operation_id="runScrub")
async def run_scrub(draft_in: DraftClaimIn):
    draft = draft_in.model_dump(mode="json")
    try:
        result = await asyncio.to_thread(engine.run_scrub, draft)
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Scrub engine error: {e}")
    try:
        result["session_id"] = await _persist_result(result, draft)
    except Exception as e:
        print(f"[Router] scrub persist failed: {e}")
        result["session_id"] = _new_session_id()
    return ScrubResultOut(**result)


@api.post("/scrub/run/stream", operation_id="runScrubStream")
async def run_scrub_stream(draft_in: DraftClaimIn):
    """SSE variant — streams per-layer milestones, then the persisted result."""
    draft = draft_in.model_dump(mode="json")

    async def event_source():
        loop = asyncio.get_running_loop()
        queue: asyncio.Queue = asyncio.Queue()
        _SENTINEL = object()

        def _produce():
            try:
                for event_type, payload in engine.run_scrub_stream(draft):
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
                if event_type == "result":
                    try:
                        payload["session_id"] = await _persist_result(payload, draft)
                    except Exception as e:
                        print(f"[Router] scrub persist failed: {e}")
                        payload["session_id"] = _new_session_id()
                yield f"event: {event_type}\ndata: {json.dumps(payload, default=str)}\n\n"
        finally:
            await producer

    return StreamingResponse(
        event_source(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@api.post("/scrub/{session_id}/resubmit", response_model=ScrubResultOut, operation_id="resubmitScrub")
async def resubmit_scrub(session_id: str, draft_in: DraftClaimIn):
    """Re-scrub an amended draft, linking the new session to the original."""
    draft = draft_in.model_dump(mode="json")
    draft["resubmitted_from"] = session_id
    result = await asyncio.to_thread(engine.run_scrub, draft)
    result["resubmitted_from"] = session_id
    try:
        result["session_id"] = await _persist_result(result, draft)
    except Exception as e:
        print(f"[Router] resubmit persist failed: {e}")
        result["session_id"] = _new_session_id()
    return ScrubResultOut(**result)


# ===================================================================
# Scrub history
# ===================================================================

@api.get("/scrub/history", response_model=list[ScrubSessionSummary], operation_id="getScrubHistory")
async def scrub_history():
    async with db.session() as session:
        result = await session.execute(text("""
            SELECT s.session_id, s.member_id, s.member_name, s.provider_npi,
                   CAST(s.date_of_service AS TEXT) AS date_of_service,
                   s.request_type::text, s.risk_score, s.decision::text,
                   s.resubmitted_from, s.created_at,
                   COUNT(f.finding_id) AS finding_count
            FROM scrub_sessions s
            LEFT JOIN scrub_line_findings f ON s.session_id = f.session_id
            GROUP BY s.session_id, s.member_id, s.member_name, s.provider_npi,
                     s.date_of_service, s.request_type, s.risk_score, s.decision,
                     s.resubmitted_from, s.created_at
            ORDER BY s.created_at DESC
            LIMIT 100
        """))
        return [ScrubSessionSummary(**_coerce_row(r)) for r in result.mappings().all()]


# ===================================================================
# User feedback (MLflow 3 assessments on the agent trace)
# ===================================================================

def _feedback_user(request: Request) -> str:
    """Resolve the reviewer identity from Databricks Apps auth headers."""
    for h in ("X-Forwarded-Email", "X-Forwarded-Preferred-Username", "X-Forwarded-User"):
        v = request.headers.get(h)
        if v:
            return v
    return "app-user"


def _log_mlflow_feedback(trace_id: str, name: str, value: bool, user: str, rationale: str | None):
    """Attach a human Feedback assessment to the agent trace (best-effort)."""
    mlflow.log_feedback(
        trace_id=trace_id,
        name=name,
        value=value,
        source=AssessmentSource(source_type="HUMAN", source_id=user),
        rationale=rationale,
    )


@api.post("/scrub/feedback", operation_id="submitScrubFeedback")
async def submit_scrub_feedback(feedback: FeedbackIn, request: Request):
    """Log user feedback as an MLflow assessment on the trace + persist to Lakebase."""
    user = _feedback_user(request)
    name = "overall_useful" if feedback.target == "overall" else f"reason_correct:{feedback.target}"

    # 1. MLflow assessment on the trace (governed artifact). Best-effort — a trace
    #    export lag or MLflow hiccup must not lose the Lakebase record.
    mlflow_ok = False
    if feedback.trace_id:
        try:
            await asyncio.to_thread(
                _log_mlflow_feedback, feedback.trace_id, name, feedback.value, user, feedback.rationale
            )
            mlflow_ok = True
        except Exception as e:
            print(f"[Feedback] mlflow.log_feedback failed: {e}")

    # 2. Durable in-app copy.
    try:
        async with db.session() as session:
            await session.execute(
                text("""
                    INSERT INTO scrub_feedback
                        (session_id, trace_id, target, value, rationale, source_id)
                    VALUES (:sid, :tid, :target, :value, :rationale, :src)
                """),
                {
                    "sid": feedback.session_id, "tid": feedback.trace_id,
                    "target": feedback.target, "value": 1 if feedback.value else -1,
                    "rationale": feedback.rationale, "src": user,
                },
            )
            await session.commit()
    except Exception as e:
        print(f"[Feedback] Lakebase persist failed: {e}")
        raise HTTPException(status_code=500, detail=f"Feedback persist failed: {e}")

    return {"status": "recorded", "trace_id": feedback.trace_id, "mlflow_logged": mlflow_ok}


@api.get("/scrub/feedback/recent", response_model=list[FeedbackRow], operation_id="getRecentFeedback")
async def get_recent_feedback():
    async with db.session() as session:
        result = await session.execute(text("""
            SELECT session_id, trace_id, target, value, rationale, source_id, created_at
            FROM scrub_feedback ORDER BY created_at DESC LIMIT 50
        """))
        return [FeedbackRow(**_coerce_row(r)) for r in result.mappings().all()]


@api.get("/scrub/{session_id}", operation_id="getScrubSession")
async def get_scrub_session(session_id: str):
    async with db.session() as session:
        head = await session.execute(
            text("""
                SELECT session_id, member_id, member_name, provider_npi,
                       CAST(date_of_service AS TEXT) AS date_of_service,
                       request_type::text, risk_score, decision::text,
                       ml_denial_prob, dx_codes, clinical_notes,
                       resubmitted_from, created_at
                FROM scrub_sessions WHERE session_id = :sid
            """),
            {"sid": session_id},
        )
        row = head.mappings().one_or_none()
        if not row:
            raise HTTPException(status_code=404, detail="Scrub session not found")

        findings = await session.execute(
            text("""
                SELECT f.carc_code, f.reason_category, f.reason_label, f.likelihood,
                       f.layer::text, f.evidence,
                       r.remediation_text, r.required_action, r.doc_needed
                FROM scrub_line_findings f
                LEFT JOIN scrub_remediations r
                    ON f.session_id = r.session_id AND f.carc_code = r.carc_code
                WHERE f.session_id = :sid
                ORDER BY f.likelihood DESC
            """),
            {"sid": session_id},
        )
        cards = [_coerce_row(r) for r in findings.mappings().all()]
        session_data = _coerce_row(row)
        session_data["reason_cards"] = cards
        return session_data


# ===================================================================
# Observability — traces + model cost/usage
# ===================================================================

@api.get("/observability/traces", operation_id="getObservabilityTraces")
async def get_observability_traces():
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


@api.get("/observability/costs", operation_id="getObservabilityCosts")
async def get_observability_costs():
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
# Helpers
# ===================================================================

def _coerce_row(row) -> dict:
    """Convert Decimal values to float for Pydantic compatibility."""
    return {k: float(v) if isinstance(v, Decimal) else v for k, v in dict(row).items()}
