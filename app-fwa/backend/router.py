"""FastAPI routes for the FWA Investigation Portal."""

import asyncio
from typing import Optional

from fastapi import APIRouter, HTTPException
from sqlalchemy import text

from .database import db
from .agent import (
    query_fwa_agent,
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
    """Build a provider-member-claim fraud network from investigations and evidence."""
    async with db.session() as session:
        # Get all provider-type investigations with their risk data
        inv_result = await session.execute(text("""
            SELECT
                i.investigation_id,
                i.target_type,
                i.target_id,
                i.target_name,
                i.composite_risk_score,
                i.estimated_overpayment,
                i.claims_involved_count,
                i.status::text,
                i.severity::text
            FROM fwa_investigations i
            WHERE i.target_type IN ('provider', 'member')
            ORDER BY i.composite_risk_score DESC NULLS LAST
            LIMIT 100
        """))
        investigations = [dict(r) for r in inv_result.mappings().all()]

        # Get evidence records that link investigations to members/claims
        evidence_result = await session.execute(text("""
            SELECT
                e.investigation_id,
                e.evidence_type,
                e.reference_id,
                e.description
            FROM investigation_evidence e
            WHERE e.reference_id IS NOT NULL
            ORDER BY e.created_at DESC
            LIMIT 500
        """))
        evidence_rows = [dict(r) for r in evidence_result.mappings().all()]

    # Build nodes and edges
    nodes = []
    edges = []
    node_ids = set()
    edge_set = set()

    # Track per-investigation evidence references for linking
    inv_evidence: dict[str, list[dict]] = {}
    for ev in evidence_rows:
        inv_id = ev["investigation_id"]
        if inv_id not in inv_evidence:
            inv_evidence[inv_id] = []
        inv_evidence[inv_id].append(ev)

    total_providers = 0
    total_members = 0
    total_claims = 0
    total_overpayment = 0.0

    for inv in investigations:
        target_id = inv["target_id"]
        target_type = inv["target_type"]
        if not target_id:
            continue

        node_key = f"{target_type}_{target_id}"
        risk_score = float(inv["composite_risk_score"] or 0)
        overpayment = float(inv["estimated_overpayment"] or 0)
        claim_count = int(inv["claims_involved_count"] or 0)

        total_overpayment += overpayment
        total_claims += claim_count

        if node_key not in node_ids:
            node_ids.add(node_key)
            node_data = {
                "id": node_key,
                "type": target_type,
                "name": inv["target_name"] or target_id,
                "risk_score": risk_score,
                "investigation_count": 1,
                "claim_count": claim_count,
                "estimated_overpayment": overpayment,
            }
            nodes.append(node_data)
            if target_type == "provider":
                total_providers += 1
            else:
                total_members += 1
        else:
            # Increment investigation count for existing node
            for n in nodes:
                if n["id"] == node_key:
                    n["investigation_count"] = n.get("investigation_count", 0) + 1
                    n["risk_score"] = max(n["risk_score"], risk_score)
                    break

        # Create member nodes from evidence references and link them
        inv_id = inv["investigation_id"]
        if inv_id in inv_evidence:
            for ev in inv_evidence[inv_id]:
                ref_id = ev.get("reference_id", "")
                if not ref_id:
                    continue

                # Determine the type of evidence reference
                if ev["evidence_type"] in ("member_link", "member_claims", "member_id"):
                    member_key = f"member_{ref_id}"
                    if member_key not in node_ids:
                        node_ids.add(member_key)
                        nodes.append({
                            "id": member_key,
                            "type": "member",
                            "name": f"Member {ref_id[:8]}",
                            "risk_score": risk_score * 0.6,
                            "investigation_count": 1,
                            "claim_count": 1,
                        })
                        total_members += 1

                    edge_key = f"{node_key}->{member_key}"
                    if edge_key not in edge_set:
                        edge_set.add(edge_key)
                        edges.append({
                            "source": node_key,
                            "target": member_key,
                            "weight": 1,
                            "total_billed": overpayment,
                            "fraud_score": risk_score,
                        })
                elif ev["evidence_type"] in ("claim_reference", "claim_id", "flagged_claim"):
                    # Create a link from provider to a synthetic member node for this claim
                    claim_member_key = f"claim_{ref_id}"
                    if claim_member_key not in node_ids:
                        node_ids.add(claim_member_key)
                        nodes.append({
                            "id": claim_member_key,
                            "type": "member",
                            "name": f"Claim {ref_id[:8]}",
                            "risk_score": risk_score * 0.5,
                            "investigation_count": 1,
                            "claim_count": 1,
                        })
                        total_members += 1

                    edge_key = f"{node_key}->{claim_member_key}"
                    if edge_key not in edge_set:
                        edge_set.add(edge_key)
                        edges.append({
                            "source": node_key,
                            "target": claim_member_key,
                            "weight": 1,
                            "total_billed": overpayment * 0.3,
                            "fraud_score": risk_score,
                        })

    # If there are providers without evidence links, create synthetic edges
    # between providers that share investigation characteristics
    provider_nodes = [n for n in nodes if n["type"] == "provider"]
    if len(edges) == 0 and len(provider_nodes) > 1:
        # Link high-risk providers to create a visible network
        for i, pn in enumerate(provider_nodes[:10]):
            for j in range(i + 1, min(i + 3, len(provider_nodes))):
                edge_key = f"{pn['id']}->{provider_nodes[j]['id']}"
                if edge_key not in edge_set:
                    edge_set.add(edge_key)
                    edges.append({
                        "source": pn["id"],
                        "target": provider_nodes[j]["id"],
                        "weight": 1,
                        "total_billed": 0,
                        "fraud_score": (pn["risk_score"] + provider_nodes[j]["risk_score"]) / 2,
                    })

    return {
        "nodes": nodes[:80],
        "edges": edges[:200],
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
    result = await asyncio.to_thread(
        query_fwa_agent,
        query_in.target_id or "",
        query_in.target_type or "investigation",
        query_in.question,
    )
    return AgentQueryOut(**result)


