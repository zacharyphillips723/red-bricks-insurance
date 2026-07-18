"""FastAPI routes for the FWA Investigation Portal."""

import asyncio
import json
import logging
import os
from typing import Optional

from databricks.sdk import WorkspaceClient
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy import text

from .database import db
from .env_config import (
    UC_CATALOG, SQL_WAREHOUSE_ID, GATEWAY_MODELS, GENIE_SPACE_ID, GEMINI_ENDPOINT,
    UC_TRACE_SCHEMA, UC_TRACE_TABLE_PREFIX,
)
from .agent import (
    query_fwa_agent,
    query_fwa_agent_via_endpoint,
    stream_fwa_agent,
    get_provider_risk_profile,
    get_provider_flagged_claims,
    get_provider_ml_scores,
    get_provider_shap_values,
    get_dashboard_analytics,
)
from .models import (
    AddNoteIn,
    AgentQueryIn,
    AgentQueryOut,
    AssignInvestigatorIn,
    AuditLogOut,
    DashboardStats,
    EvidenceOut,
    InvestigationDetailOut,
    InvestigationListOut,
    InvestigatorCaseload,
    InvestigatorOut,
    ProviderRiskOut,
    RecordRecoveryIn,
    UpdateStatusIn,
)

api = APIRouter(prefix="/api")
log = logging.getLogger(__name__)

_CAT = f"`{UC_CATALOG}`"


def _uc_query(sql: str) -> list[dict]:
    """Execute a read-only SQL query against Unity Catalog via Statement Execution."""
    w = WorkspaceClient()
    stmt = w.statement_execution.execute_statement(
        warehouse_id=SQL_WAREHOUSE_ID,
        statement=sql,
        wait_timeout="30s",
    )
    if not stmt.result or not stmt.result.data_array:
        return []
    cols = [c.name for c in stmt.manifest.schema.columns] if stmt.manifest and stmt.manifest.schema else []
    if not cols:
        return []
    return [dict(zip(cols, row)) for row in stmt.result.data_array]


# ===================================================================
# Health check
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
# Dashboard
# ===================================================================

@api.get("/dashboard/stats", response_model=DashboardStats, operation_id="getDashboardStats")
async def get_dashboard_stats():
    """Get aggregate dashboard statistics."""
    async with db.session() as session:
        result = await session.execute(text("""
            SELECT
                COUNT(*) AS total_investigations,
                COUNT(*) FILTER (WHERE status = 'Open') AS open_count,
                COUNT(*) FILTER (WHERE severity = 'Critical') AS critical_count,
                COUNT(*) FILTER (WHERE severity = 'High') AS high_count,
                COALESCE(SUM(estimated_overpayment), 0) AS total_estimated_overpayment,
                COALESCE(SUM(recovered_amount), 0) AS total_recovered,
                COUNT(*) FILTER (WHERE status IN (
                    'Closed — Confirmed Fraud', 'Closed — No Fraud', 'Closed — Insufficient Evidence')
                    AND closed_at >= date_trunc('month', now())
                ) AS closed_this_month
            FROM fwa_investigations
        """))
        row = result.mappings().one()

        # By status
        status_result = await session.execute(text("""
            SELECT status::text, COUNT(*) AS cnt
            FROM fwa_investigations GROUP BY status
        """))
        by_status = {r["status"]: r["cnt"] for r in status_result.mappings()}

        # By type
        type_result = await session.execute(text("""
            SELECT investigation_type::text, COUNT(*) AS cnt
            FROM fwa_investigations GROUP BY investigation_type
        """))
        by_type = {r["investigation_type"]: r["cnt"] for r in type_result.mappings()}

        # By severity
        sev_result = await session.execute(text("""
            SELECT severity::text, COUNT(*) AS cnt
            FROM fwa_investigations GROUP BY severity
        """))
        by_severity = {r["severity"]: r["cnt"] for r in sev_result.mappings()}

        total_est = float(row["total_estimated_overpayment"])
        total_rec = float(row["total_recovered"])

        return DashboardStats(
            total_investigations=row["total_investigations"],
            open_count=row["open_count"],
            critical_count=row["critical_count"],
            high_count=row["high_count"],
            total_estimated_overpayment=total_est,
            total_recovered=total_rec,
            recovery_rate=round(total_rec / total_est, 4) if total_est > 0 else 0,
            closed_this_month=row["closed_this_month"],
            investigations_by_status=by_status,
            investigations_by_type=by_type,
            investigations_by_severity=by_severity,
        )


# ===================================================================
# Investigations
# ===================================================================

@api.get("/investigations", response_model=list[InvestigationListOut], operation_id="listInvestigations")
async def list_investigations(
    status: Optional[str] = None,
    severity: Optional[str] = None,
    investigation_type: Optional[str] = None,
    investigator_id: Optional[str] = None,
):
    """List investigations with optional filters."""
    query = """
        SELECT
            i.investigation_id,
            i.investigation_type::text,
            i.target_type,
            i.target_id,
            i.target_name,
            i.fraud_types,
            i.severity::text,
            i.status::text,
            i.source::text,
            i.estimated_overpayment,
            i.claims_involved_count,
            i.composite_risk_score,
            i.rules_risk_score,
            i.ml_risk_score,
            inv.display_name AS investigator_name,
            inv.role AS investigator_role,
            i.assigned_at,
            i.created_at,
            to_char(now() - i.created_at, 'DD "d" HH24 "h"') AS time_open
        FROM fwa_investigations i
        LEFT JOIN fraud_investigators inv ON i.assigned_investigator_id = inv.investigator_id
        WHERE 1=1
    """
    params: dict = {}

    if status:
        query += " AND i.status = CAST(:status AS investigation_status)"
        params["status"] = status
    if severity:
        query += " AND i.severity = CAST(:severity AS fraud_severity)"
        params["severity"] = severity
    if investigation_type:
        query += " AND i.investigation_type = CAST(:inv_type AS investigation_type)"
        params["inv_type"] = investigation_type
    if investigator_id:
        query += " AND i.assigned_investigator_id = CAST(:inv_id AS uuid)"
        params["inv_id"] = investigator_id

    query += """
        ORDER BY
            CASE i.severity WHEN 'Critical' THEN 1 WHEN 'High' THEN 2
                WHEN 'Medium' THEN 3 WHEN 'Low' THEN 4 END,
            i.composite_risk_score DESC NULLS LAST,
            i.created_at ASC
    """

    async with db.session() as session:
        result = await session.execute(text(query), params)
        return [InvestigationListOut(**dict(r)) for r in result.mappings().all()]


@api.get("/investigations/{inv_id}", response_model=InvestigationDetailOut, operation_id="getInvestigation")
async def get_investigation(inv_id: str):
    """Get full investigation detail including audit log and evidence."""
    async with db.session() as session:
        result = await session.execute(
            text("""
                SELECT
                    i.investigation_id,
                    i.investigation_type::text,
                    i.target_type,
                    i.target_id,
                    i.target_name,
                    i.fraud_types,
                    i.severity::text,
                    i.status::text,
                    i.source::text,
                    i.assigned_investigator_id::text,
                    inv.display_name AS investigator_name,
                    inv.role AS investigator_role,
                    i.assigned_at,
                    i.estimated_overpayment,
                    i.confirmed_overpayment,
                    i.recovered_amount,
                    i.claims_involved_count,
                    i.investigation_summary,
                    i.evidence_summary,
                    i.recommendation,
                    i.rules_risk_score,
                    i.ml_risk_score,
                    i.composite_risk_score,
                    i.created_at,
                    i.updated_at,
                    i.closed_at
                FROM fwa_investigations i
                LEFT JOIN fraud_investigators inv ON i.assigned_investigator_id = inv.investigator_id
                WHERE i.investigation_id = :inv_id
            """),
            {"inv_id": inv_id},
        )
        row = result.mappings().one_or_none()
        if not row:
            raise HTTPException(status_code=404, detail="Investigation not found")

        # Audit log
        audit_result = await session.execute(
            text("""
                SELECT l.audit_id::text, l.investigation_id, inv.display_name AS investigator_name,
                       l.action_type, l.previous_status::text, l.new_status::text, l.note, l.created_at
                FROM investigation_audit_log l
                LEFT JOIN fraud_investigators inv ON l.investigator_id = inv.investigator_id
                WHERE l.investigation_id = :inv_id
                ORDER BY l.created_at DESC
            """),
            {"inv_id": inv_id},
        )
        audit_log = [AuditLogOut(**dict(r)) for r in audit_result.mappings().all()]

        # Evidence
        evidence_result = await session.execute(
            text("""
                SELECT e.evidence_id::text, e.investigation_id, e.evidence_type,
                       e.reference_id, e.description, inv.display_name AS added_by_name, e.created_at
                FROM investigation_evidence e
                LEFT JOIN fraud_investigators inv ON e.added_by = inv.investigator_id
                WHERE e.investigation_id = :inv_id
                ORDER BY e.created_at DESC
            """),
            {"inv_id": inv_id},
        )
        evidence = [EvidenceOut(**dict(r)) for r in evidence_result.mappings().all()]

        inv_data = dict(row)
        inv_data["audit_log"] = audit_log
        inv_data["evidence"] = evidence
        return InvestigationDetailOut(**inv_data)


@api.post("/investigations/{inv_id}/assign", response_model=InvestigationDetailOut, operation_id="assignInvestigation")
async def assign_investigation(inv_id: str, assign_in: AssignInvestigatorIn):
    """Assign an investigator to a case."""
    async with db.session() as session:
        check = await session.execute(
            text("SELECT status::text FROM fwa_investigations WHERE investigation_id = :inv_id"),
            {"inv_id": inv_id},
        )
        row = check.mappings().one_or_none()
        if not row:
            raise HTTPException(status_code=404, detail="Investigation not found")

        old_status = row["status"]

        await session.execute(
            text("""
                UPDATE fwa_investigations
                SET assigned_investigator_id = CAST(:inv_id2 AS uuid),
                    status = 'Under Review'::investigation_status
                WHERE investigation_id = :inv_id
            """),
            {"inv_id": inv_id, "inv_id2": assign_in.investigator_id},
        )

        await session.execute(
            text("""
                INSERT INTO investigation_audit_log
                    (investigation_id, investigator_id, action_type, previous_status, new_status)
                VALUES (:inv_id, CAST(:inv_id2 AS uuid), 'assignment',
                    CAST(:old AS investigation_status), 'Under Review'::investigation_status)
            """),
            {"inv_id": inv_id, "inv_id2": assign_in.investigator_id, "old": old_status},
        )
        await session.commit()

    return await get_investigation(inv_id)


@api.post("/investigations/{inv_id}/status", response_model=InvestigationDetailOut, operation_id="updateInvestigationStatus")
async def update_status(inv_id: str, status_in: UpdateStatusIn):
    """Update investigation status."""
    async with db.session() as session:
        check = await session.execute(
            text("SELECT status::text, assigned_investigator_id::text FROM fwa_investigations WHERE investigation_id = :inv_id"),
            {"inv_id": inv_id},
        )
        row = check.mappings().one_or_none()
        if not row:
            raise HTTPException(status_code=404, detail="Investigation not found")

        await session.execute(
            text("""
                UPDATE fwa_investigations
                SET status = CAST(:new_status AS investigation_status),
                    assigned_investigator_id = CASE
                        WHEN CAST(:new_status AS text) != 'Open'
                             AND assigned_investigator_id IS NULL
                        THEN (SELECT investigator_id FROM fraud_investigators
                              WHERE is_active = TRUE ORDER BY random() LIMIT 1)
                        ELSE assigned_investigator_id
                    END,
                    assigned_at = CASE
                        WHEN CAST(:new_status AS text) != 'Open'
                             AND assigned_investigator_id IS NULL
                        THEN now()
                        ELSE assigned_at
                    END
                WHERE investigation_id = :inv_id
            """),
            {"inv_id": inv_id, "new_status": status_in.status.value},
        )

        # Re-fetch investigator ID in case one was auto-assigned above
        updated = await session.execute(
            text("SELECT assigned_investigator_id::text FROM fwa_investigations WHERE investigation_id = :inv_id"),
            {"inv_id": inv_id},
        )
        current_investigator_id = updated.mappings().one()["assigned_investigator_id"]

        await session.execute(
            text("""
                INSERT INTO investigation_audit_log
                    (investigation_id, investigator_id, action_type, previous_status, new_status, note)
                VALUES (:inv_id, CAST(:cm_id AS uuid), 'status_change',
                    CAST(:old AS investigation_status), CAST(:new AS investigation_status), :note)
            """),
            {
                "inv_id": inv_id, "cm_id": current_investigator_id,
                "old": row["status"], "new": status_in.status.value, "note": status_in.note,
            },
        )
        await session.commit()

    return await get_investigation(inv_id)


@api.post("/investigations/{inv_id}/notes", response_model=InvestigationDetailOut, operation_id="addInvestigationNote")
async def add_note(inv_id: str, note_in: AddNoteIn):
    """Add a note to an investigation."""
    async with db.session() as session:
        check = await session.execute(
            text("SELECT assigned_investigator_id::text FROM fwa_investigations WHERE investigation_id = :inv_id"),
            {"inv_id": inv_id},
        )
        row = check.mappings().one_or_none()
        if not row:
            raise HTTPException(status_code=404, detail="Investigation not found")

        await session.execute(
            text("""
                INSERT INTO investigation_audit_log
                    (investigation_id, investigator_id, action_type, note)
                VALUES (:inv_id, CAST(:cm_id AS uuid), 'note_added', :note)
            """),
            {"inv_id": inv_id, "cm_id": row["assigned_investigator_id"], "note": note_in.note},
        )
        await session.commit()

    return await get_investigation(inv_id)


@api.post("/investigations/{inv_id}/recovery", response_model=InvestigationDetailOut, operation_id="recordRecovery")
async def record_recovery(inv_id: str, recovery_in: RecordRecoveryIn):
    """Record a recovery amount for an investigation."""
    async with db.session() as session:
        check = await session.execute(
            text("SELECT assigned_investigator_id::text, recovered_amount FROM fwa_investigations WHERE investigation_id = :inv_id"),
            {"inv_id": inv_id},
        )
        row = check.mappings().one_or_none()
        if not row:
            raise HTTPException(status_code=404, detail="Investigation not found")

        new_total = float(row["recovered_amount"] or 0) + recovery_in.recovered_amount

        await session.execute(
            text("UPDATE fwa_investigations SET recovered_amount = :amt WHERE investigation_id = :inv_id"),
            {"inv_id": inv_id, "amt": new_total},
        )

        await session.execute(
            text("""
                INSERT INTO investigation_audit_log
                    (investigation_id, investigator_id, action_type, note, metadata_json)
                VALUES (:inv_id, CAST(:cm_id AS uuid), 'recovery_recorded', :note,
                    CAST(:meta AS jsonb))
            """),
            {
                "inv_id": inv_id,
                "cm_id": row["assigned_investigator_id"],
                "note": recovery_in.note or f"Recovery of ${recovery_in.recovered_amount:,.2f} recorded.",
                "meta": f'{{"amount": {recovery_in.recovered_amount}, "new_total": {new_total}}}',
            },
        )
        await session.commit()

    return await get_investigation(inv_id)


# ===================================================================
# Investigators
# ===================================================================

@api.get("/investigators", response_model=list[InvestigatorOut], operation_id="listInvestigators")
async def list_investigators():
    async with db.session() as session:
        result = await session.execute(text("""
            SELECT investigator_id::text, email, display_name, role::text, department, max_caseload, is_active
            FROM fraud_investigators WHERE is_active = TRUE ORDER BY display_name
        """))
        return [InvestigatorOut(**dict(r)) for r in result.mappings().all()]


@api.get("/investigators/caseload", response_model=list[InvestigatorCaseload], operation_id="getInvestigatorCaseload")
async def get_investigator_caseload():
    async with db.session() as session:
        result = await session.execute(text("""
            SELECT investigator_id::text, display_name, role::text, max_caseload,
                   active_cases, critical_cases, evidence_gathering, recovery_in_progress,
                   total_active_overpayment, total_recovered, available_capacity
            FROM v_investigator_caseload ORDER BY active_cases DESC
        """))
        return [InvestigatorCaseload(**dict(r)) for r in result.mappings().all()]


# ===================================================================
# Provider Analysis (from gold tables via Statement Execution)
# ===================================================================

@api.get("/providers/{npi}/risk-profile", response_model=ProviderRiskOut, operation_id="getProviderRisk")
async def get_provider_risk(npi: str):
    profile = await asyncio.to_thread(get_provider_risk_profile, npi)
    if not profile:
        raise HTTPException(status_code=404, detail=f"Provider {npi} not found")
    return ProviderRiskOut(**profile)


@api.get("/providers/{npi}/claims", operation_id="getProviderClaims")
async def get_provider_claims(npi: str):
    claims = await asyncio.to_thread(get_provider_flagged_claims, npi)
    return claims


@api.get("/providers/{npi}/ml-scores", operation_id="getProviderMLScores")
async def get_provider_ml(npi: str):
    scores = await asyncio.to_thread(get_provider_ml_scores, npi)
    return scores


@api.get("/providers/{npi}/shap-values", operation_id="getProviderShapValues")
async def get_provider_shap(npi: str):
    """Get SHAP-like feature attributions for a provider's risk score."""
    values = await asyncio.to_thread(get_provider_shap_values, npi)
    if not values:
        raise HTTPException(status_code=404, detail=f"Provider {npi} not found")
    return values


# ===================================================================
# Network Graph
# ===================================================================

@api.get("/network-graph", operation_id="getNetworkGraph")
async def get_network_graph():
    """Build a provider-member-claim fraud network from investigations + UC claim flags."""

    # --- 1. Load provider investigations from Lakebase ---
    async with db.session() as session:
        inv_result = await session.execute(text("""
            SELECT
                i.investigation_id,
                i.target_type,
                i.target_id,
                i.target_name,
                i.composite_risk_score,
                i.estimated_overpayment,
                i.claims_involved_count
            FROM fwa_investigations i
            WHERE i.target_type = 'provider'
            ORDER BY i.composite_risk_score DESC NULLS LAST
            LIMIT 60
        """))
        investigations = [dict(r) for r in inv_result.mappings().all()]

    # --- 2. Query UC gold_fwa_claim_flags for provider→member edges ---
    provider_npis = [inv["target_id"] for inv in investigations if inv.get("target_id")]
    claim_edges: list[dict] = []
    if provider_npis:
        npi_list = ", ".join(f"'{npi}'" for npi in provider_npis)
        sql = f"""
            SELECT
                provider_npi,
                member_id,
                COUNT(*) AS claim_count,
                AVG(CAST(fraud_score AS DOUBLE)) AS avg_fraud_score,
                SUM(CAST(estimated_overpayment AS DOUBLE)) AS total_overpayment
            FROM {_CAT}.fwa.gold_fwa_claim_flags
            WHERE provider_npi IN ({npi_list})
            GROUP BY provider_npi, member_id
            ORDER BY avg_fraud_score DESC
            LIMIT 300
        """
        try:
            claim_edges = await asyncio.to_thread(_uc_query, sql)
        except Exception as exc:
            log.warning("UC query for claim edges failed, falling back to Lakebase-only: %s", exc)

    # --- 3. Build nodes and edges ---
    nodes: list[dict] = []
    edges: list[dict] = []
    node_ids: set[str] = set()
    edge_set: set[str] = set()

    total_providers = 0
    total_members = 0
    total_claims = 0
    total_overpayment = 0.0

    # Provider nodes from investigations
    for inv in investigations:
        target_id = inv["target_id"]
        if not target_id:
            continue
        node_key = f"provider_{target_id}"
        risk_score = float(inv["composite_risk_score"] or 0)
        overpayment = float(inv["estimated_overpayment"] or 0)
        claim_count = int(inv["claims_involved_count"] or 0)

        total_overpayment += overpayment
        total_claims += claim_count

        if node_key not in node_ids:
            node_ids.add(node_key)
            nodes.append({
                "id": node_key,
                "type": "provider",
                "name": inv["target_name"] or target_id,
                "risk_score": risk_score,
                "investigation_count": 1,
                "claim_count": claim_count,
                "estimated_overpayment": overpayment,
            })
            total_providers += 1
        else:
            for n in nodes:
                if n["id"] == node_key:
                    n["investigation_count"] = n.get("investigation_count", 0) + 1
                    n["risk_score"] = max(n["risk_score"], risk_score)
                    break

    # Member nodes and provider→member edges from UC claim flags
    provider_lookup = {n["id"]: n for n in nodes}
    for row in claim_edges:
        npi = row.get("provider_npi")
        member_id = row.get("member_id")
        if not npi or not member_id:
            continue

        provider_key = f"provider_{npi}"
        member_key = f"member_{member_id}"

        # Only add edges for providers we have nodes for
        if provider_key not in node_ids:
            continue

        avg_fraud = float(row.get("avg_fraud_score") or 0)
        edge_claims = int(row.get("claim_count") or 1)
        edge_overpay = float(row.get("total_overpayment") or 0)

        # Create member node if new
        if member_key not in node_ids:
            node_ids.add(member_key)
            nodes.append({
                "id": member_key,
                "type": "member",
                "name": f"Member {member_id[:8]}…" if len(member_id) > 8 else f"Member {member_id}",
                "risk_score": avg_fraud * 0.6,
                "investigation_count": 0,
                "claim_count": edge_claims,
                "estimated_overpayment": edge_overpay,
            })
            total_members += 1

        edge_key = f"{provider_key}->{member_key}"
        if edge_key not in edge_set:
            edge_set.add(edge_key)
            edges.append({
                "source": provider_key,
                "target": member_key,
                "weight": min(edge_claims, 10),
                "fraud_score": avg_fraud,
                "claim_count": edge_claims,
            })

    # If a member is linked to multiple providers, add provider→provider edges
    # through shared members for a richer network
    member_providers: dict[str, list[str]] = {}
    for e in edges:
        src = e["source"]
        tgt = e["target"]
        if isinstance(tgt, str) and tgt.startswith("member_"):
            member_providers.setdefault(tgt, []).append(src)

    for member_key, providers in member_providers.items():
        if len(providers) < 2:
            continue
        for i in range(len(providers)):
            for j in range(i + 1, len(providers)):
                pp_key = f"{providers[i]}->{providers[j]}"
                pp_key_r = f"{providers[j]}->{providers[i]}"
                if pp_key not in edge_set and pp_key_r not in edge_set:
                    edge_set.add(pp_key)
                    p1 = provider_lookup.get(providers[i], {})
                    p2 = provider_lookup.get(providers[j], {})
                    edges.append({
                        "source": providers[i],
                        "target": providers[j],
                        "weight": 2,
                        "fraud_score": (float(p1.get("risk_score", 0)) + float(p2.get("risk_score", 0))) / 2,
                        "claim_count": 0,
                    })

    return {
        "nodes": nodes[:120],
        "edges": edges[:400],
        "stats": {
            "total_providers": total_providers,
            "total_members": total_members,
            "total_claims": total_claims,
            "total_overpayment": round(total_overpayment, 2),
        },
    }


# ===================================================================
# Agent
# ===================================================================

@api.post("/agent/query", response_model=AgentQueryOut, operation_id="queryFWAAgent")
async def query_agent(query_in: AgentQueryIn):
    agent_mode = os.environ.get("AGENT_MODE", "local")
    if agent_mode == "endpoint":
        result = await asyncio.to_thread(
            query_fwa_agent_via_endpoint,
            query_in.target_id or "",
            query_in.target_type or "investigation",
            query_in.question,
            query_in.model_endpoint,
        )
    else:
        result = await asyncio.to_thread(
            query_fwa_agent,
            query_in.target_id or "",
            query_in.target_type or "investigation",
            query_in.question,
            query_in.model_endpoint,
        )
    return AgentQueryOut(**result)


@api.post("/agent/query/stream", operation_id="queryFWAAgentStream")
async def query_agent_stream(query_in: AgentQueryIn):
    """Server-Sent Events variant of /agent/query.

    Emits milestone events as each sub-agent finishes so the UI can render the
    early Gemini clinical analysis (~18s) while the slower Genie claims query
    (~40s) is still running, then the final synthesized briefing.
    """
    target_id = query_in.target_id or ""
    target_type = query_in.target_type or "investigation"
    question = query_in.question

    async def event_source():
        # Bridge the blocking sync generator onto a worker thread so each
        # yielded event reaches the client as soon as it is produced without
        # blocking the event loop.
        loop = asyncio.get_running_loop()
        queue: asyncio.Queue = asyncio.Queue()
        _SENTINEL = object()

        def _produce():
            try:
                for event_type, payload in stream_fwa_agent(target_id, target_type, question):
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


@api.get("/agent/models", operation_id="getAvailableModels")
async def get_available_models():
    return {"models": GATEWAY_MODELS}


# ===================================================================
# Observability
# ===================================================================

@api.get("/observability/traces", operation_id="getObservabilityTraces")
async def get_observability_traces():
    """Get recent agent traces from UC OTel span tables (real-time via MLflow tracing)."""
    try:
        spans_table = (
            f"`{UC_CATALOG}`.`{UC_TRACE_SCHEMA}`.`{UC_TRACE_TABLE_PREFIX}_otel_spans`"
        )
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
        rows = await asyncio.to_thread(_uc_query, sql)
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
                "tags": {},
            })
        return {"traces": records}
    except Exception as e:
        print(f"[observability] Trace fetch error: {e}")
        return {"traces": [], "error": str(e)}


@api.get("/observability/costs", operation_id="getObservabilityCosts")
async def get_observability_costs():
    """Get token usage and cost summary from system tables — scoped to this app's service principal."""
    try:
        endpoints = ", ".join(f"'{m}'" for m in GATEWAY_MODELS)
        # Auto-detect workspace ID to scope queries (avoids account-wide totals)
        try:
            w = WorkspaceClient()
            workspace_id = w.get_workspace_id()
            workspace_filter = f"AND eu.workspace_id = '{workspace_id}'" if workspace_id else ""
        except Exception:
            workspace_filter = ""
        rows = _uc_query(f"""
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
        log.warning("Cost query error: %s", e)
        return {"costs": [], "error": str(e)}


# ---------------------------------------------------------------------------
# Genie Search
# ---------------------------------------------------------------------------

@api.post("/genie/ask", operation_id="askGenie")
async def ask_genie_route(payload: dict):
    """Send a natural language question to the Genie Space."""
    from .genie import ask_genie

    question = payload.get("question", "").strip()
    if not question:
        raise HTTPException(status_code=400, detail="question is required")
    conversation_id = payload.get("conversation_id", "")
    result = await asyncio.to_thread(
        ask_genie,
        question_text=question,
        conversation_id=conversation_id,
        space_id=GENIE_SPACE_ID,
        warehouse_id=SQL_WAREHOUSE_ID,
    )
    return result
