"""FastAPI routes for the PA Review Portal."""

import asyncio
from decimal import Decimal
from typing import Optional

from fastapi import APIRouter, HTTPException
from sqlalchemy import text

from .database import db
from .agent import query_pa_agent, get_pa_analytics, get_policy_rules, get_ml_prediction
from .models import (
    ActionLogOut,
    AddNoteIn,
    AgentQueryIn,
    AgentQueryOut,
    AssignReviewerIn,
    DashboardStats,
    PARequestDetailOut,
    PARequestListOut,
    ReviewerCaseload,
    ReviewerOut,
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
    async with db.session() as session:
        result = await session.execute(text("""
            SELECT
                COUNT(*) AS total_requests,
                COUNT(*) FILTER (WHERE status = 'Pending Review') AS pending_count,
                COUNT(*) FILTER (WHERE status = 'In Review') AS in_review_count,
                COUNT(*) FILTER (WHERE urgency = 'expedited'
                    AND status IN ('Pending Review', 'In Review')) AS expedited_pending,
                COUNT(*) FILTER (WHERE status = 'Approved') AS approved_count,
                COUNT(*) FILTER (WHERE status = 'Denied') AS denied_count,
                ROUND(AVG(turnaround_hours) FILTER (WHERE turnaround_hours IS NOT NULL), 1)
                    AS avg_turnaround_hours,
                ROUND(
                    SUM(CASE WHEN cms_compliant THEN 1 ELSE 0 END) * 100.0
                    / NULLIF(SUM(CASE WHEN status IN ('Approved', 'Denied', 'Partially Approved')
                        THEN 1 ELSE 0 END), 0), 2
                ) AS cms_compliance_rate,
                COUNT(*) FILTER (WHERE status IN ('Pending Review', 'In Review', 'Additional Info Requested')
                    AND cms_deadline < now()) AS overdue_count,
                COUNT(*) FILTER (WHERE determination_tier = 'tier_1_auto') AS auto_adjudicated_count
            FROM pa_review_queue
        """))
        row = result.mappings().one()

        # By status
        status_result = await session.execute(text("""
            SELECT status::text, COUNT(*) AS cnt
            FROM pa_review_queue GROUP BY status
        """))
        by_status = {r["status"]: r["cnt"] for r in status_result.mappings()}

        # By service type
        svc_result = await session.execute(text("""
            SELECT service_type, COUNT(*) AS cnt
            FROM pa_review_queue GROUP BY service_type ORDER BY cnt DESC
        """))
        by_service = {r["service_type"]: r["cnt"] for r in svc_result.mappings()}

        # By urgency
        urg_result = await session.execute(text("""
            SELECT urgency::text, COUNT(*) AS cnt
            FROM pa_review_queue GROUP BY urgency
        """))
        by_urgency = {r["urgency"]: r["cnt"] for r in urg_result.mappings()}

        total = row["total_requests"]
        approved = row["approved_count"]
        denied = row["denied_count"]
        determined = approved + denied

        return DashboardStats(
            total_requests=total,
            pending_count=row["pending_count"],
            in_review_count=row["in_review_count"],
            expedited_pending=row["expedited_pending"],
            approved_count=approved,
            denied_count=denied,
            approval_rate=round(approved / determined, 4) if determined > 0 else None,
            avg_turnaround_hours=float(row["avg_turnaround_hours"]) if row["avg_turnaround_hours"] else None,
            cms_compliance_rate=float(row["cms_compliance_rate"]) if row["cms_compliance_rate"] else None,
            overdue_count=row["overdue_count"],
            auto_adjudicated_count=row["auto_adjudicated_count"],
            requests_by_status=by_status,
            requests_by_service_type=by_service,
            requests_by_urgency=by_urgency,
        )


# ===================================================================
# PA Requests
# ===================================================================

@api.get("/requests", response_model=list[PARequestListOut], operation_id="listPARequests")
async def list_requests(
    status: Optional[str] = None,
    urgency: Optional[str] = None,
    service_type: Optional[str] = None,
    reviewer_id: Optional[str] = None,
):
    query = """
        SELECT
            q.auth_request_id,
            q.member_id,
            q.member_name,
            q.requesting_provider_npi,
            q.provider_name,
            q.service_type,
            q.procedure_code,
            q.procedure_description,
            q.diagnosis_codes,
            q.policy_name,
            q.line_of_business,
            q.urgency::text,
            q.estimated_cost,
            q.status::text,
            q.determination_tier::text,
            q.ai_recommendation,
            q.ai_confidence,
            q.tier1_auto_eligible,
            r.display_name AS reviewer_name,
            r.role::text AS reviewer_role,
            q.assigned_at,
            q.request_date,
            q.cms_deadline,
            q.cms_compliant,
            to_char(now() - q.request_date, 'DD "d" HH24 "h"') AS time_open,
            EXTRACT(EPOCH FROM (q.cms_deadline - now())) / 3600.0 AS hours_until_deadline
        FROM pa_review_queue q
        LEFT JOIN pa_reviewers r ON q.assigned_reviewer_id = r.reviewer_id
        WHERE 1=1
    """
    params: dict = {}

    if status:
        query += " AND q.status = CAST(:status AS pa_review_status)"
        params["status"] = status
    if urgency:
        query += " AND q.urgency = CAST(:urgency AS pa_urgency)"
        params["urgency"] = urgency
    if service_type:
        query += " AND q.service_type = :svc_type"
        params["svc_type"] = service_type
    if reviewer_id:
        query += " AND q.assigned_reviewer_id = CAST(:rev_id AS uuid)"
        params["rev_id"] = reviewer_id

    query += """
        ORDER BY
            CASE q.urgency WHEN 'expedited' THEN 1 WHEN 'standard' THEN 2
                WHEN 'retrospective' THEN 3 END,
            q.cms_deadline ASC NULLS LAST,
            q.request_date ASC
        LIMIT 200
    """

    async with db.session() as session:
        result = await session.execute(text(query), params)
        rows = result.mappings().all()
        print(f"[Router] /requests returned {len(rows)} rows")
        return [PARequestListOut(**_coerce_row(r)) for r in rows]


@api.get("/requests/{req_id}", response_model=PARequestDetailOut, operation_id="getPARequest")
async def get_request(req_id: str):
    async with db.session() as session:
        result = await session.execute(
            text("""
                SELECT
                    q.auth_request_id,
                    q.member_id, q.member_name,
                    q.requesting_provider_npi, q.provider_name,
                    q.service_type, q.procedure_code, q.procedure_description,
                    q.diagnosis_codes, q.policy_id, q.policy_name,
                    q.line_of_business, q.clinical_summary,
                    q.urgency::text, q.estimated_cost,
                    q.status::text, q.determination_tier::text,
                    q.assigned_reviewer_id::text,
                    r.display_name AS reviewer_name,
                    r.role::text AS reviewer_role,
                    q.assigned_at,
                    q.ai_recommendation, q.ai_confidence,
                    q.tier1_auto_eligible, q.clinical_extraction,
                    q.determination_reason, q.denial_reason_code, q.reviewer_notes,
                    q.request_date, q.determination_date, q.turnaround_hours,
                    q.cms_compliant, q.cms_deadline,
                    q.appeal_filed, q.appeal_date, q.appeal_outcome,
                    q.created_at, q.updated_at,
                    EXTRACT(EPOCH FROM (q.cms_deadline - now())) / 3600.0 AS hours_until_deadline
                FROM pa_review_queue q
                LEFT JOIN pa_reviewers r ON q.assigned_reviewer_id = r.reviewer_id
                WHERE q.auth_request_id = :req_id
            """),
            {"req_id": req_id},
        )
        row = result.mappings().one_or_none()
        if not row:
            raise HTTPException(status_code=404, detail="PA request not found")

        # Action log
        audit_result = await session.execute(
            text("""
                SELECT a.action_id::text, a.auth_request_id,
                       r.display_name AS reviewer_name,
                       a.action_type, a.previous_status::text, a.new_status::text,
                       a.note, a.created_at
                FROM pa_review_actions a
                LEFT JOIN pa_reviewers r ON a.reviewer_id = r.reviewer_id
                WHERE a.auth_request_id = :req_id
                ORDER BY a.created_at DESC
            """),
            {"req_id": req_id},
        )
        audit_log = [ActionLogOut(**dict(r)) for r in audit_result.mappings().all()]

        req_data = dict(row)
        req_data["audit_log"] = audit_log
        return PARequestDetailOut(**req_data)


@api.post("/requests/{req_id}/assign", response_model=PARequestDetailOut, operation_id="assignReviewer")
async def assign_reviewer(req_id: str, assign_in: AssignReviewerIn):
    async with db.session() as session:
        check = await session.execute(
            text("SELECT status::text FROM pa_review_queue WHERE auth_request_id = :req_id"),
            {"req_id": req_id},
        )
        row = check.mappings().one_or_none()
        if not row:
            raise HTTPException(status_code=404, detail="PA request not found")

        old_status = row["status"]

        await session.execute(
            text("""
                UPDATE pa_review_queue
                SET assigned_reviewer_id = CAST(:rev_id AS uuid),
                    status = 'In Review'::pa_review_status
                WHERE auth_request_id = :req_id
            """),
            {"req_id": req_id, "rev_id": assign_in.reviewer_id},
        )

        await session.execute(
            text("""
                INSERT INTO pa_review_actions
                    (auth_request_id, reviewer_id, action_type, previous_status, new_status)
                VALUES (:req_id, CAST(:rev_id AS uuid), 'assignment',
                    CAST(:old AS pa_review_status), 'In Review'::pa_review_status)
            """),
            {"req_id": req_id, "rev_id": assign_in.reviewer_id, "old": old_status},
        )
        await session.commit()

    return await get_request(req_id)


@api.post("/requests/{req_id}/status", response_model=PARequestDetailOut, operation_id="updatePAStatus")
async def update_status(req_id: str, status_in: UpdateStatusIn):
    async with db.session() as session:
        check = await session.execute(
            text("SELECT status::text, assigned_reviewer_id::text FROM pa_review_queue WHERE auth_request_id = :req_id"),
            {"req_id": req_id},
        )
        row = check.mappings().one_or_none()
        if not row:
            raise HTTPException(status_code=404, detail="PA request not found")

        update_parts = ["status = CAST(:new_status AS pa_review_status)"]
        params: dict = {"req_id": req_id, "new_status": status_in.status.value}

        if status_in.determination_reason:
            update_parts.append("determination_reason = :det_reason")
            params["det_reason"] = status_in.determination_reason
        if status_in.denial_reason_code:
            update_parts.append("denial_reason_code = :denial_code")
            params["denial_code"] = status_in.denial_reason_code

        set_clause = ", ".join(update_parts)
        await session.execute(
            text(f"UPDATE pa_review_queue SET {set_clause} WHERE auth_request_id = :req_id"),
            params,
        )

        reviewer_id = row["assigned_reviewer_id"]
        await session.execute(
            text("""
                INSERT INTO pa_review_actions
                    (auth_request_id, reviewer_id, action_type, previous_status, new_status, note)
                VALUES (:req_id, CAST(:rev_id AS uuid), 'status_change',
                    CAST(:old AS pa_review_status), CAST(:new AS pa_review_status), :note)
            """),
            {
                "req_id": req_id, "rev_id": reviewer_id,
                "old": row["status"], "new": status_in.status.value,
                "note": status_in.note,
            },
        )
        await session.commit()

    return await get_request(req_id)


@api.post("/requests/{req_id}/notes", response_model=PARequestDetailOut, operation_id="addPANote")
async def add_note(req_id: str, note_in: AddNoteIn):
    async with db.session() as session:
        check = await session.execute(
            text("SELECT assigned_reviewer_id::text FROM pa_review_queue WHERE auth_request_id = :req_id"),
            {"req_id": req_id},
        )
        row = check.mappings().one_or_none()
        if not row:
            raise HTTPException(status_code=404, detail="PA request not found")

        await session.execute(
            text("""
                INSERT INTO pa_review_actions
                    (auth_request_id, reviewer_id, action_type, note)
                VALUES (:req_id, CAST(:rev_id AS uuid), 'note_added', :note)
            """),
            {"req_id": req_id, "rev_id": row["assigned_reviewer_id"], "note": note_in.note},
        )
        await session.commit()

    return await get_request(req_id)


# ===================================================================
# Reviewers
# ===================================================================

@api.get("/reviewers", response_model=list[ReviewerOut], operation_id="listReviewers")
async def list_reviewers():
    async with db.session() as session:
        result = await session.execute(text("""
            SELECT reviewer_id::text, email, display_name, role::text, department,
                   specialty, max_caseload, is_active
            FROM pa_reviewers WHERE is_active = TRUE ORDER BY display_name
        """))
        return [ReviewerOut(**dict(r)) for r in result.mappings().all()]


@api.get("/reviewers/caseload", response_model=list[ReviewerCaseload], operation_id="getReviewerCaseload")
async def get_reviewer_caseload():
    async with db.session() as session:
        result = await session.execute(text("""
            SELECT reviewer_id::text, display_name, role::text, specialty, max_caseload,
                   active_cases, expedited_cases, in_review, awaiting_info, available_capacity
            FROM v_reviewer_caseload ORDER BY active_cases DESC
        """))
        return [ReviewerCaseload(**dict(r)) for r in result.mappings().all()]


# ===================================================================
# Policy Library (from UC via Statement Execution)
# ===================================================================

@api.get("/policies", operation_id="listPolicies")
async def list_policies():
    policies = await asyncio.to_thread(
        lambda: _execute_sql_safe(
            "SELECT policy_id, policy_name, service_category, policy_summary "
            "FROM policy_summaries ORDER BY policy_name"
        )
    )
    return policies


@api.get("/policies/{policy_id}/rules", operation_id="getPolicyRules")
async def get_policy_rules_endpoint(policy_id: str):
    rules = await asyncio.to_thread(get_policy_rules, policy_id)
    return rules


@api.get("/requests/{req_id}/ml-prediction", operation_id="getMLPrediction")
async def get_ml_prediction_endpoint(req_id: str):
    prediction = await asyncio.to_thread(get_ml_prediction, req_id)
    if not prediction:
        return {"message": "No ML prediction available"}
    return prediction


# ===================================================================
# Agent
# ===================================================================

@api.post("/agent/query", response_model=AgentQueryOut, operation_id="queryPAAgent")
async def query_agent(query_in: AgentQueryIn):
    result = await asyncio.to_thread(
        query_pa_agent,
        query_in.auth_request_id or "",
        query_in.question,
    )
    return AgentQueryOut(**result)


# ===================================================================
# Helpers
# ===================================================================

def _coerce_row(row) -> dict:
    """Convert Decimal values to float for Pydantic compatibility."""
    return {k: float(v) if isinstance(v, Decimal) else v for k, v in dict(row).items()}


def _execute_sql_safe(sql: str) -> list[dict]:
    """Execute SQL with error handling, using agent module's SQL executor."""
    from .agent import _execute_sql, _CAT
    try:
        full_sql = sql
        # Prepend catalog if table names don't have it
        if "FROM " in sql and _CAT not in sql:
            full_sql = sql.replace("FROM ", f"FROM {_CAT}.prior_auth.")
        print(f"[Router] Executing SQL: {full_sql[:200]}")
        result = _execute_sql(full_sql)
        print(f"[Router] SQL returned {len(result)} rows")
        return result
    except Exception as e:
        print(f"[Router] SQL error: {e}")
        import traceback; traceback.print_exc()
        return []
