"""FastAPI routes for the PA Review Portal."""

import asyncio
import json
from decimal import Decimal
from typing import Optional

from databricks.sdk import WorkspaceClient
from fastapi import APIRouter, HTTPException, UploadFile, File
from fastapi.responses import StreamingResponse, Response
from sqlalchemy import text

from .database import db
from .agent import query_pa_agent, stream_pa_agent, get_pa_analytics, get_policy_rules, get_ml_prediction
from .agent import _execute_sql
from . import documents as docs
from . import correspondence as corr
from . import rules_engine as rules
from .sample_records import generate_sample_pdf, list_scenarios
from .env_config import (
    PA_AGENT_ENDPOINT, LLM_ENDPOINT, UC_CATALOG,
    UC_TRACE_SCHEMA, UC_TRACE_TABLE_PREFIX,
)

# Models this app invokes — used to scope the cost query.
OBSERVED_MODELS = [PA_AGENT_ENDPOINT, LLM_ENDPOINT]
from .models import (
    ActionLogOut,
    AddNoteIn,
    AgentQueryIn,
    AgentQueryOut,
    AppealDeterminationIn,
    AppealListOut,
    AssignAppealIn,
    AssignReviewerIn,
    BusinessRuleIn,
    BusinessRuleOut,
    ComplianceMetricsOut,
    CorrespondenceOut,
    DashboardStats,
    FileAppealIn,
    GenerateNoticeIn,
    OverdueRequestOut,
    RuleSimulationOut,
    PARequestDetailOut,
    PARequestListOut,
    PeerReviewDeterminationIn,
    PeerReviewOut,
    RequestPeerReviewIn,
    ReviewerCaseload,
    ReviewerOut,
    TurnaroundBucket,
    UpdateStatusIn,
    WeeklyTrend,
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
                    q.criteria_source, q.criteria_version,
                    q.criteria_effective_date::text AS criteria_effective_date,
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
# Compliance
# ===================================================================

@api.get("/compliance/metrics", response_model=ComplianceMetricsOut, operation_id="getComplianceMetrics")
async def get_compliance_metrics():
    async with db.session() as session:
        # Core compliance KPIs
        kpi_result = await session.execute(text("""
            SELECT
                ROUND(
                    SUM(CASE WHEN cms_compliant THEN 1 ELSE 0 END) * 100.0
                    / NULLIF(SUM(CASE WHEN status IN ('Approved', 'Denied', 'Partially Approved')
                        THEN 1 ELSE 0 END), 0), 2
                ) AS compliance_rate,
                ROUND(AVG(turnaround_hours) FILTER (WHERE urgency = 'standard' AND turnaround_hours IS NOT NULL), 1)
                    AS avg_turnaround_standard,
                ROUND(AVG(turnaround_hours) FILTER (WHERE urgency = 'expedited' AND turnaround_hours IS NOT NULL), 1)
                    AS avg_turnaround_expedited,
                COUNT(*) FILTER (WHERE status IN ('Pending Review', 'In Review', 'Additional Info Requested')
                    AND cms_deadline < now()) AS overdue_count,
                COUNT(*) FILTER (WHERE status IN ('Approved', 'Denied', 'Partially Approved'))
                    AS total_determined,
                COUNT(*) FILTER (WHERE determination_tier = 'tier_1_auto')
                    AS total_auto
            FROM pa_review_queue
        """))
        kpi = kpi_result.mappings().one()

        total_determined = kpi["total_determined"]
        total_auto = kpi["total_auto"]
        auto_rate = round(total_auto * 100.0 / total_determined, 2) if total_determined > 0 else None

        # Turnaround distribution buckets
        dist_result = await session.execute(text("""
            SELECT
                CASE
                    WHEN turnaround_hours < 24 THEN '0-24h'
                    WHEN turnaround_hours < 48 THEN '24-48h'
                    WHEN turnaround_hours < 72 THEN '48-72h'
                    WHEN turnaround_hours < 96 THEN '72-96h'
                    WHEN turnaround_hours < 120 THEN '96-120h'
                    ELSE '120h+'
                END AS bucket,
                COUNT(*) AS cnt,
                CASE WHEN turnaround_hours < 72 THEN TRUE ELSE FALSE END AS compliant
            FROM pa_review_queue
            WHERE turnaround_hours IS NOT NULL
            GROUP BY bucket, compliant
            ORDER BY MIN(turnaround_hours)
        """))
        distribution = [
            TurnaroundBucket(bucket=r["bucket"], count=r["cnt"], compliant=r["compliant"])
            for r in dist_result.mappings().all()
        ]

        # Weekly compliance trend
        trend_result = await session.execute(text("""
            SELECT
                to_char(date_trunc('week', determination_date), 'YYYY-MM-DD') AS week,
                ROUND(
                    SUM(CASE WHEN cms_compliant THEN 1 ELSE 0 END) * 100.0
                    / NULLIF(COUNT(*), 0), 2
                ) AS compliance_rate,
                COUNT(*) AS total
            FROM pa_review_queue
            WHERE determination_date IS NOT NULL
            GROUP BY date_trunc('week', determination_date)
            ORDER BY date_trunc('week', determination_date)
        """))
        weekly_trend = [
            WeeklyTrend(week=r["week"], compliance_rate=float(r["compliance_rate"] or 0), total=r["total"])
            for r in trend_result.mappings().all()
        ]

        return ComplianceMetricsOut(
            compliance_rate=float(kpi["compliance_rate"]) if kpi["compliance_rate"] else None,
            avg_turnaround_standard=float(kpi["avg_turnaround_standard"]) if kpi["avg_turnaround_standard"] else None,
            avg_turnaround_expedited=float(kpi["avg_turnaround_expedited"]) if kpi["avg_turnaround_expedited"] else None,
            overdue_count=kpi["overdue_count"],
            auto_adjudication_rate=auto_rate,
            total_determined=total_determined,
            total_auto=total_auto,
            turnaround_distribution=distribution,
            weekly_trend=weekly_trend,
        )


@api.get("/compliance/overdue", response_model=list[OverdueRequestOut], operation_id="getOverdueRequests")
async def get_overdue_requests():
    async with db.session() as session:
        result = await session.execute(text("""
            SELECT
                q.auth_request_id,
                q.member_name,
                q.service_type,
                q.procedure_code,
                q.urgency::text,
                r.display_name AS reviewer_name,
                q.cms_deadline,
                EXTRACT(EPOCH FROM (now() - q.cms_deadline)) / 3600.0 AS hours_overdue,
                q.request_date
            FROM pa_review_queue q
            LEFT JOIN pa_reviewers r ON q.assigned_reviewer_id = r.reviewer_id
            WHERE q.status IN ('Pending Review', 'In Review', 'Additional Info Requested')
              AND q.cms_deadline < now()
            ORDER BY q.cms_deadline ASC
        """))
        rows = result.mappings().all()
        return [OverdueRequestOut(**_coerce_row(r)) for r in rows]


# ===================================================================
# Appeals & Reconsiderations
# ===================================================================

@api.get("/appeals", response_model=list[AppealListOut], operation_id="listAppeals")
async def list_appeals(status: Optional[str] = None, urgency: Optional[str] = None):
    query = "SELECT * FROM v_appeal_queue WHERE 1=1"
    params: dict = {}
    if status:
        query += " AND status = :status"
        params["status"] = status
    if urgency:
        query += " AND urgency = :urgency"
        params["urgency"] = urgency
    async with db.session() as session:
        result = await session.execute(text(query), params)
        return [AppealListOut(**_coerce_row(r)) for r in result.mappings().all()]


@api.get("/appeals/{appeal_id}", response_model=AppealListOut, operation_id="getAppeal")
async def get_appeal(appeal_id: str):
    async with db.session() as session:
        result = await session.execute(
            text("SELECT * FROM v_appeal_queue WHERE appeal_id = CAST(:aid AS uuid)"),
            {"aid": appeal_id},
        )
        row = result.mappings().one_or_none()
        if not row:
            raise HTTPException(status_code=404, detail="Appeal not found")
        return AppealListOut(**_coerce_row(row))


@api.post("/appeals", response_model=AppealListOut, operation_id="fileAppeal")
async def file_appeal(appeal_in: FileAppealIn):
    """File an appeal against an existing determination.

    Captures the original determiner so routing can enforce a different
    appeals reviewer, and flips the source request into an 'Appealed' state.
    """
    async with db.session() as session:
        src = await session.execute(
            text("""
                SELECT status::text, assigned_reviewer_id::text
                FROM pa_review_queue WHERE auth_request_id = :req_id
            """),
            {"req_id": appeal_in.auth_request_id},
        )
        src_row = src.mappings().one_or_none()
        if not src_row:
            raise HTTPException(status_code=404, detail="Source PA request not found")
        if src_row["status"] not in ("Denied", "Partially Approved"):
            raise HTTPException(
                status_code=400,
                detail="Only denied or partially-approved determinations can be appealed",
            )

        result = await session.execute(
            text("""
                INSERT INTO pa_appeals
                    (auth_request_id, appeal_type, urgency, filed_by, filed_role,
                     filing_reason, original_reviewer_id, status)
                VALUES (:req_id, CAST(:atype AS appeal_type), CAST(:urg AS pa_urgency),
                        :filed_by, :filed_role, :reason,
                        CAST(:orig AS uuid), 'Received'::appeal_status)
                RETURNING appeal_id::text
            """),
            {
                "req_id": appeal_in.auth_request_id,
                "atype": appeal_in.appeal_type.value,
                "urg": appeal_in.urgency.value,
                "filed_by": appeal_in.filed_by,
                "filed_role": appeal_in.filed_role,
                "reason": appeal_in.filing_reason,
                "orig": src_row["assigned_reviewer_id"],
            },
        )
        appeal_id = result.mappings().one()["appeal_id"]

        await session.execute(
            text("""
                INSERT INTO pa_appeal_actions (appeal_id, action_type, new_status, note)
                VALUES (CAST(:aid AS uuid), 'filed', 'Received'::appeal_status, :note)
            """),
            {"aid": appeal_id, "note": appeal_in.filing_reason},
        )

        # Mark the originating request as appealed + record on its own audit log.
        await session.execute(
            text("""
                UPDATE pa_review_queue
                SET appeal_filed = TRUE, appeal_date = now(),
                    status = 'Appealed'::pa_review_status
                WHERE auth_request_id = :req_id
            """),
            {"req_id": appeal_in.auth_request_id},
        )
        await session.execute(
            text("""
                INSERT INTO pa_review_actions
                    (auth_request_id, action_type, note)
                VALUES (:req_id, 'appeal_filed', :note)
            """),
            {"req_id": appeal_in.auth_request_id, "note": "Appeal filed"},
        )
        await session.commit()

    return await get_appeal(appeal_id)


@api.post("/appeals/{appeal_id}/assign", response_model=AppealListOut, operation_id="assignAppeal")
async def assign_appeal(appeal_id: str, assign_in: AssignAppealIn):
    """Assign an appeals reviewer — rejects the original determiner (conflict of interest)."""
    async with db.session() as session:
        check = await session.execute(
            text("""
                SELECT status::text, original_reviewer_id::text
                FROM pa_appeals WHERE appeal_id = CAST(:aid AS uuid)
            """),
            {"aid": appeal_id},
        )
        row = check.mappings().one_or_none()
        if not row:
            raise HTTPException(status_code=404, detail="Appeal not found")
        if row["original_reviewer_id"] and row["original_reviewer_id"] == assign_in.reviewer_id:
            raise HTTPException(
                status_code=400,
                detail="Appeals cannot be assigned to the original determining reviewer",
            )

        old_status = row["status"]
        await session.execute(
            text("""
                UPDATE pa_appeals
                SET assigned_reviewer_id = CAST(:rev AS uuid),
                    status = 'In Review'::appeal_status
                WHERE appeal_id = CAST(:aid AS uuid)
            """),
            {"aid": appeal_id, "rev": assign_in.reviewer_id},
        )
        await session.execute(
            text("""
                INSERT INTO pa_appeal_actions
                    (appeal_id, reviewer_id, action_type, previous_status, new_status)
                VALUES (CAST(:aid AS uuid), CAST(:rev AS uuid), 'assignment',
                        CAST(:old AS appeal_status), 'In Review'::appeal_status)
            """),
            {"aid": appeal_id, "rev": assign_in.reviewer_id, "old": old_status},
        )
        await session.commit()

    return await get_appeal(appeal_id)


@api.post("/appeals/{appeal_id}/determination", response_model=AppealListOut, operation_id="decideAppeal")
async def decide_appeal(appeal_id: str, det_in: AppealDeterminationIn):
    """Record an appeal determination (Overturned / Partially Overturned / Upheld).

    Propagates the outcome back to the originating request so overturn metrics
    and the longitudinal case record stay consistent.
    """
    if det_in.status.value not in ("Overturned", "Partially Overturned", "Upheld"):
        raise HTTPException(
            status_code=400,
            detail="Appeal determination must be Overturned, Partially Overturned, or Upheld",
        )

    async with db.session() as session:
        check = await session.execute(
            text("""
                SELECT status::text, assigned_reviewer_id::text, auth_request_id
                FROM pa_appeals WHERE appeal_id = CAST(:aid AS uuid)
            """),
            {"aid": appeal_id},
        )
        row = check.mappings().one_or_none()
        if not row:
            raise HTTPException(status_code=404, detail="Appeal not found")

        await session.execute(
            text("""
                UPDATE pa_appeals
                SET status = CAST(:new AS appeal_status),
                    determination = :det,
                    determination_reason = :reason,
                    determination_reason_external = :reason_ext,
                    reviewer_notes_internal = :notes
                WHERE appeal_id = CAST(:aid AS uuid)
            """),
            {
                "aid": appeal_id,
                "new": det_in.status.value,
                "det": det_in.status.value,
                "reason": det_in.determination_reason,
                "reason_ext": det_in.determination_reason_external,
                "notes": det_in.reviewer_notes_internal,
            },
        )
        await session.execute(
            text("""
                INSERT INTO pa_appeal_actions
                    (appeal_id, reviewer_id, action_type, previous_status, new_status, note)
                VALUES (CAST(:aid AS uuid), CAST(:rev AS uuid), 'determination',
                        CAST(:old AS appeal_status), CAST(:new AS appeal_status), :note)
            """),
            {
                "aid": appeal_id, "rev": row["assigned_reviewer_id"],
                "old": row["status"], "new": det_in.status.value,
                "note": det_in.determination_reason,
            },
        )

        # Propagate outcome to the source determination.
        src_status = "Appeal Overturned" if det_in.status.value in (
            "Overturned", "Partially Overturned"
        ) else "Appeal Upheld"
        await session.execute(
            text("""
                UPDATE pa_review_queue
                SET status = CAST(:s AS pa_review_status),
                    appeal_outcome = :outcome
                WHERE auth_request_id = :req_id
            """),
            {"s": src_status, "outcome": det_in.status.value, "req_id": row["auth_request_id"]},
        )
        await session.commit()

    return await get_appeal(appeal_id)


# ===================================================================
# Peer / Physician Review (Clinical Reviews — escalation + P2P)
# ===================================================================

_PEER_COLS = """
    p.peer_review_id::text, p.auth_request_id,
    rb.display_name AS requested_by_name,
    pr.display_name AS peer_reviewer_name, pr.role::text AS peer_reviewer_role,
    p.requested_specialty, p.reason, p.status::text,
    p.p2p_requested, p.p2p_scheduled_at, p.p2p_completed_at, p.p2p_summary,
    p.determination, p.determination_notes, p.notified_at, p.created_at
"""


@api.get("/requests/{req_id}/peer-reviews", response_model=list[PeerReviewOut], operation_id="listPeerReviews")
async def list_peer_reviews(req_id: str):
    async with db.session() as session:
        result = await session.execute(
            text(f"""
                SELECT {_PEER_COLS}
                FROM pa_peer_reviews p
                LEFT JOIN pa_reviewers rb ON p.requested_by_id = rb.reviewer_id
                LEFT JOIN pa_reviewers pr ON p.peer_reviewer_id = pr.reviewer_id
                WHERE p.auth_request_id = :req_id
                ORDER BY p.created_at DESC
            """),
            {"req_id": req_id},
        )
        return [PeerReviewOut(**dict(r)) for r in result.mappings().all()]


@api.post("/requests/{req_id}/peer-reviews", response_model=PeerReviewOut, operation_id="requestPeerReview")
async def request_peer_review(req_id: str, pr_in: RequestPeerReviewIn):
    """Escalate a case to physician/peer review. If no reviewer is named, match by
    specialty against active Medical Directors / Peer Reviewers.
    """
    async with db.session() as session:
        check = await session.execute(
            text("SELECT assigned_reviewer_id::text FROM pa_review_queue WHERE auth_request_id = :req_id"),
            {"req_id": req_id},
        )
        row = check.mappings().one_or_none()
        if not row:
            raise HTTPException(status_code=404, detail="PA request not found")

        peer_id = pr_in.peer_reviewer_id
        # Specialty match when no reviewer explicitly chosen.
        if not peer_id:
            match = await session.execute(
                text("""
                    SELECT reviewer_id::text FROM pa_reviewers
                    WHERE is_active = TRUE
                      AND role IN ('Medical Director', 'Peer Reviewer')
                      AND (:spec IS NULL OR specialty ILIKE :spec_like)
                    ORDER BY (specialty ILIKE :spec_like) DESC, max_caseload DESC
                    LIMIT 1
                """),
                {"spec": pr_in.requested_specialty,
                 "spec_like": f"%{pr_in.requested_specialty}%" if pr_in.requested_specialty else "%"},
            )
            m = match.mappings().one_or_none()
            peer_id = m["reviewer_id"] if m else None

        result = await session.execute(
            text(f"""
                INSERT INTO pa_peer_reviews
                    (auth_request_id, requested_by_id, peer_reviewer_id,
                     requested_specialty, reason, status, p2p_requested,
                     p2p_scheduled_at)
                VALUES (:req_id, CAST(:rb AS uuid), CAST(:peer AS uuid),
                        :spec, :reason,
                        CASE WHEN :peer IS NULL THEN 'Requested' ELSE 'Scheduled' END::peer_review_status,
                        :p2p, CASE WHEN :p2p THEN now() + INTERVAL '1 day' ELSE NULL END)
                RETURNING peer_review_id::text
            """),
            {
                "req_id": req_id, "rb": row["assigned_reviewer_id"],
                "peer": peer_id, "spec": pr_in.requested_specialty,
                "reason": pr_in.reason, "p2p": pr_in.p2p_requested,
            },
        )
        peer_review_id = result.mappings().one()["peer_review_id"]

        # Flip the case into peer review + audit.
        await session.execute(
            text("""
                UPDATE pa_review_queue
                SET status = 'Peer Review Requested'::pa_review_status
                WHERE auth_request_id = :req_id
                  AND status IN ('Pending Review', 'In Review', 'Additional Info Requested')
            """),
            {"req_id": req_id},
        )
        await session.execute(
            text("""
                INSERT INTO pa_review_actions
                    (auth_request_id, reviewer_id, action_type, note)
                VALUES (:req_id, CAST(:rb AS uuid), 'peer_review_requested', :note)
            """),
            {"req_id": req_id, "rb": row["assigned_reviewer_id"],
             "note": pr_in.reason or "Escalated to physician/peer review."},
        )
        await session.commit()

    prs = await list_peer_reviews(req_id)
    return next((p for p in prs if p.peer_review_id == peer_review_id), prs[0])


@api.post("/peer-reviews/{peer_review_id}/determination", response_model=PeerReviewOut, operation_id="decidePeerReview")
async def decide_peer_review(peer_review_id: str, det_in: PeerReviewDeterminationIn):
    """Record the peer reviewer's recommendation + P2P outcome, and notify."""
    async with db.session() as session:
        check = await session.execute(
            text("SELECT auth_request_id FROM pa_peer_reviews WHERE peer_review_id = CAST(:pid AS uuid)"),
            {"pid": peer_review_id},
        )
        row = check.mappings().one_or_none()
        if not row:
            raise HTTPException(status_code=404, detail="Peer review not found")
        req_id = row["auth_request_id"]

        await session.execute(
            text("""
                UPDATE pa_peer_reviews
                SET status = 'Determination Made'::peer_review_status,
                    determination = :det,
                    determination_notes = :notes,
                    p2p_summary = COALESCE(:p2p_summary, p2p_summary),
                    p2p_completed_at = CASE WHEN p2p_requested THEN now() ELSE p2p_completed_at END,
                    notified_at = now()
                WHERE peer_review_id = CAST(:pid AS uuid)
            """),
            {"pid": peer_review_id, "det": det_in.determination,
             "notes": det_in.determination_notes, "p2p_summary": det_in.p2p_summary},
        )
        await session.execute(
            text("""
                INSERT INTO pa_review_actions
                    (auth_request_id, action_type, note)
                VALUES (:req_id, 'note_added', :note)
            """),
            {"req_id": req_id,
             "note": f"Peer review determination: {det_in.determination}. {det_in.determination_notes or ''}"},
        )
        await session.commit()

    prs = await list_peer_reviews(req_id)
    return next((p for p in prs if p.peer_review_id == peer_review_id), prs[0])


# ===================================================================
# Business Rules Engine (no-code adjudication/routing rules)
# ===================================================================

_RULE_COLS = """
    rule_id::text, name, description, category, line_of_business, service_type,
    conditions_json, action::text, action_detail, priority,
    effective_start_date::text, effective_end_date::text,
    version, status::text, created_by, approved_by, approved_at,
    created_at, updated_at
"""


async def _snapshot_rule(session, rule_id: str, change_type: str, changed_by: str | None,
                         change_reason: str | None) -> None:
    """Write an immutable version snapshot of a rule's current state."""
    await session.execute(
        text("""
            INSERT INTO pa_rule_versions
                (rule_id, version, change_type, snapshot_json, changed_by, change_reason)
            SELECT rule_id, version, :ctype, to_jsonb(r), :by, :reason
            FROM pa_business_rules r
            WHERE rule_id = CAST(:rid AS uuid)
        """),
        {"rid": rule_id, "ctype": change_type, "by": changed_by, "reason": change_reason},
    )


@api.get("/rules", response_model=list[BusinessRuleOut], operation_id="listRules")
async def list_rules(status: Optional[str] = None):
    query = f"SELECT {_RULE_COLS} FROM pa_business_rules"
    params: dict = {}
    if status:
        query += " WHERE status = CAST(:status AS rule_status)"
        params["status"] = status
    query += " ORDER BY priority ASC, name ASC"
    async with db.session() as session:
        result = await session.execute(text(query), params)
        return [BusinessRuleOut(**_coerce_row(r)) for r in result.mappings().all()]


@api.post("/rules", response_model=BusinessRuleOut, operation_id="createRule")
async def create_rule(rule_in: BusinessRuleIn):
    async with db.session() as session:
        result = await session.execute(
            text(f"""
                INSERT INTO pa_business_rules
                    (name, description, category, line_of_business, service_type,
                     conditions_json, action, action_detail, priority, status, created_by)
                VALUES (:name, :desc, :category, :lob, :svc,
                        CAST(:conditions AS jsonb), CAST(:action AS rule_action),
                        :action_detail, :priority, 'draft'::rule_status, 'business_admin')
                RETURNING {_RULE_COLS}
            """),
            {
                "name": rule_in.name, "desc": rule_in.description, "category": rule_in.category,
                "lob": rule_in.line_of_business, "svc": rule_in.service_type,
                "conditions": json.dumps(rule_in.conditions_json), "action": rule_in.action.value,
                "action_detail": rule_in.action_detail, "priority": rule_in.priority,
            },
        )
        created = result.mappings().one()
        await _snapshot_rule(session, created["rule_id"], "created", "business_admin", rule_in.change_reason)
        await session.commit()
        return BusinessRuleOut(**_coerce_row(created))


@api.put("/rules/{rule_id}", response_model=BusinessRuleOut, operation_id="updateRule")
async def update_rule(rule_id: str, rule_in: BusinessRuleIn):
    async with db.session() as session:
        exists = await session.execute(
            text("SELECT 1 FROM pa_business_rules WHERE rule_id = CAST(:rid AS uuid)"),
            {"rid": rule_id},
        )
        if not exists.first():
            raise HTTPException(status_code=404, detail="Rule not found")
        result = await session.execute(
            text(f"""
                UPDATE pa_business_rules
                SET name = :name, description = :desc, category = :category,
                    line_of_business = :lob, service_type = :svc,
                    conditions_json = CAST(:conditions AS jsonb),
                    action = CAST(:action AS rule_action), action_detail = :action_detail,
                    priority = :priority, version = version + 1
                WHERE rule_id = CAST(:rid AS uuid)
                RETURNING {_RULE_COLS}
            """),
            {
                "rid": rule_id, "name": rule_in.name, "desc": rule_in.description,
                "category": rule_in.category, "lob": rule_in.line_of_business,
                "svc": rule_in.service_type, "conditions": json.dumps(rule_in.conditions_json),
                "action": rule_in.action.value, "action_detail": rule_in.action_detail,
                "priority": rule_in.priority,
            },
        )
        updated = result.mappings().one()
        await _snapshot_rule(session, rule_id, "updated", "business_admin", rule_in.change_reason)
        await session.commit()
        return BusinessRuleOut(**_coerce_row(updated))


@api.post("/rules/{rule_id}/activate", response_model=BusinessRuleOut, operation_id="activateRule")
async def activate_rule(rule_id: str):
    """Approve + activate a rule for production (RFI: signoff workflow for deployment)."""
    async with db.session() as session:
        result = await session.execute(
            text(f"""
                UPDATE pa_business_rules
                SET status = 'active'::rule_status, approved_by = 'medical_director', approved_at = now()
                WHERE rule_id = CAST(:rid AS uuid)
                RETURNING {_RULE_COLS}
            """),
            {"rid": rule_id},
        )
        row = result.mappings().one_or_none()
        if not row:
            raise HTTPException(status_code=404, detail="Rule not found")
        await _snapshot_rule(session, rule_id, "activated", "medical_director", "Approved for production")
        await session.commit()
        return BusinessRuleOut(**_coerce_row(row))


@api.post("/rules/{rule_id}/retire", response_model=BusinessRuleOut, operation_id="retireRule")
async def retire_rule(rule_id: str):
    async with db.session() as session:
        result = await session.execute(
            text(f"""
                UPDATE pa_business_rules
                SET status = 'retired'::rule_status, effective_end_date = CURRENT_DATE
                WHERE rule_id = CAST(:rid AS uuid)
                RETURNING {_RULE_COLS}
            """),
            {"rid": rule_id},
        )
        row = result.mappings().one_or_none()
        if not row:
            raise HTTPException(status_code=404, detail="Rule not found")
        await _snapshot_rule(session, rule_id, "retired", "business_admin", "Retired")
        await session.commit()
        return BusinessRuleOut(**_coerce_row(row))


@api.get("/rules/conflicts", operation_id="getRuleConflicts")
async def get_rule_conflicts():
    """Detect active rules with overlapping scope but conflicting actions."""
    async with db.session() as session:
        result = await session.execute(
            text(f"SELECT {_RULE_COLS} FROM pa_business_rules WHERE status = 'active'")
        )
        rule_rows = [_coerce_row(r) for r in result.mappings().all()]
    return {"conflicts": rules.detect_conflicts(rule_rows)}


@api.post("/rules/{rule_id}/simulate", response_model=RuleSimulationOut, operation_id="simulateRule")
async def simulate_rule(rule_id: str):
    """Simulate a rule against historical gold_pa_requests (impact analysis)."""
    async with db.session() as session:
        result = await session.execute(
            text(f"SELECT {_RULE_COLS} FROM pa_business_rules WHERE rule_id = CAST(:rid AS uuid)"),
            {"rid": rule_id},
        )
        row = result.mappings().one_or_none()
        if not row:
            raise HTTPException(status_code=404, detail="Rule not found")
        rule = _coerce_row(row)

    # Historical requests from the UC gold table (via Statement Execution).
    historical = await asyncio.to_thread(
        _execute_sql_safe,
        "SELECT auth_request_id, line_of_business, service_type, procedure_code, "
        "diagnosis_codes, urgency, estimated_cost, determination "
        "FROM gold_pa_requests LIMIT 3000",
    )
    return RuleSimulationOut(**rules.simulate(rule, historical))


@api.get("/requests/{req_id}/rule-evaluation", operation_id="evaluateRequestRules")
async def evaluate_request_rules(req_id: str):
    """Evaluate the active no-code rules against a live request (parallel to Tier-1 SQL)."""
    async with db.session() as session:
        req = await session.execute(
            text("""
                SELECT auth_request_id, line_of_business, service_type, procedure_code,
                       diagnosis_codes, urgency, estimated_cost, tier1_auto_eligible
                FROM pa_review_queue WHERE auth_request_id = :req_id
            """),
            {"req_id": req_id},
        )
        req_row = req.mappings().one_or_none()
        if not req_row:
            raise HTTPException(status_code=404, detail="PA request not found")

        rule_result = await session.execute(
            text(f"SELECT {_RULE_COLS} FROM pa_business_rules WHERE status = 'active'")
        )
        rule_rows = [_coerce_row(r) for r in rule_result.mappings().all()]

    return rules.evaluate(rule_rows, _coerce_row(req_row))


# ===================================================================
# Correspondence — determination notices (Decision Processing)
# ===================================================================

_CORR_COLS = """
    notice_id::text, auth_request_id, notice_type::text, recipient, recipient_role,
    subject, body_markdown, body_redacted, redaction_notes, includes_appeal_rights,
    criteria_citation, template_version, pdf_path,
    delivery_channel::text, delivery_status::text,
    generated_by, generated_at, released_at
"""


@api.get("/requests/{req_id}/notices", response_model=list[CorrespondenceOut], operation_id="listNotices")
async def list_notices(req_id: str):
    async with db.session() as session:
        result = await session.execute(
            text(f"""
                SELECT {_CORR_COLS} FROM pa_correspondence
                WHERE auth_request_id = :req_id
                ORDER BY generated_at DESC
            """),
            {"req_id": req_id},
        )
        return [CorrespondenceOut(**dict(r)) for r in result.mappings().all()]


@api.post("/requests/{req_id}/notices", response_model=CorrespondenceOut, operation_id="generateNotice")
async def generate_notice(req_id: str, notice_in: GenerateNoticeIn):
    """Generate a determination notice: AI-draft rationale + regulatory scaffold,
    PHI-redaction gate, PDF render to the UC Volume, and a tracked correspondence row.
    """
    # 1. Gather case facts from the operational queue.
    async with db.session() as session:
        result = await session.execute(
            text("""
                SELECT auth_request_id, member_id, member_name,
                       requesting_provider_npi, provider_name,
                       procedure_code, procedure_description, line_of_business,
                       policy_id, policy_name, clinical_summary,
                       determination_reason, denial_reason_code
                FROM pa_review_queue WHERE auth_request_id = :req_id
            """),
            {"req_id": req_id},
        )
        row = result.mappings().one_or_none()
        if not row:
            raise HTTPException(status_code=404, detail="PA request not found")
        facts = dict(row)

    # 2. Build the notice (AI rationale + scaffold + redaction) and render a PDF.
    notice = await asyncio.to_thread(corr.build_notice, notice_in.notice_type.value, facts)
    try:
        pdf_path = await asyncio.to_thread(corr.render_notice_pdf, notice, facts)
    except Exception as e:
        print(f"[correspondence] PDF render failed: {e}")
        pdf_path = None

    # 3. Persist the correspondence row (draft state) + an audit action.
    async with db.session() as session:
        ins = await session.execute(
            text(f"""
                INSERT INTO pa_correspondence (
                    auth_request_id, notice_type, recipient, recipient_role,
                    subject, body_markdown, body_redacted, redaction_notes,
                    includes_appeal_rights, criteria_citation, template_version,
                    pdf_path, delivery_channel, delivery_status, generated_by
                ) VALUES (
                    :aid, CAST(:ntype AS notice_type), :recipient, :recipient_role,
                    :subject, :body, :redacted, :redaction_notes,
                    :appeal_rights, :citation, :tpl_version,
                    :pdf_path, CAST(:channel AS delivery_channel),
                    'draft'::delivery_status, 'ai_query'
                )
                RETURNING {_CORR_COLS}
            """),
            {
                "aid": req_id,
                "ntype": notice["notice_type"],
                "recipient": notice_in.recipient,
                "recipient_role": notice_in.recipient_role,
                "subject": notice["subject"],
                "body": notice["body_markdown"],
                "redacted": notice["body_redacted"],
                "redaction_notes": notice["redaction_notes"],
                "appeal_rights": notice["includes_appeal_rights"],
                "citation": notice["criteria_citation"],
                "tpl_version": notice["template_version"],
                "pdf_path": pdf_path,
                "channel": notice_in.delivery_channel,
            },
        )
        created = ins.mappings().one()
        await session.execute(
            text("""
                INSERT INTO pa_review_actions (auth_request_id, action_type, note, metadata_json)
                VALUES (:aid, 'auto_generated', :note, CAST(:meta AS jsonb))
            """),
            {
                "aid": req_id,
                "note": f"Generated {notice['notice_type']} notice ({notice['redaction_notes']}).",
                "meta": json.dumps({"notice_id": created["notice_id"], "pdf_path": pdf_path}),
            },
        )
        await session.commit()
        return CorrespondenceOut(**dict(created))


@api.post("/notices/{notice_id}/release", response_model=CorrespondenceOut, operation_id="releaseNotice")
async def release_notice(notice_id: str):
    """Release a drafted notice for delivery — only after the PHI gate has run."""
    async with db.session() as session:
        check = await session.execute(
            text("SELECT body_redacted FROM pa_correspondence WHERE notice_id = CAST(:nid AS uuid)"),
            {"nid": notice_id},
        )
        row = check.mappings().one_or_none()
        if not row:
            raise HTTPException(status_code=404, detail="Notice not found")
        if not row["body_redacted"]:
            raise HTTPException(status_code=400, detail="Notice has not passed the PHI-redaction gate")

        result = await session.execute(
            text(f"""
                UPDATE pa_correspondence
                SET delivery_status = 'released'::delivery_status, released_at = now()
                WHERE notice_id = CAST(:nid AS uuid)
                RETURNING {_CORR_COLS}
            """),
            {"nid": notice_id},
        )
        await session.commit()
        return CorrespondenceOut(**dict(result.mappings().one()))


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


@api.post("/requests/{req_id}/ai-decision", response_model=PARequestDetailOut, operation_id="recordAIDecision")
async def record_ai_decision(req_id: str, payload: dict):
    """Record a reviewer's acceptance or override of the AI recommendation.

    Governance requirement (RFI: AI & Advanced Intelligence — human oversight and
    override): every AI-assisted recommendation that a human accepts or overrides
    is logged to the immutable action trail with the reviewer's rationale.
    """
    action = (payload.get("action") or "").lower()  # 'accept' | 'override'
    if action not in ("accept", "override"):
        raise HTTPException(status_code=400, detail="action must be 'accept' or 'override'")
    reason = payload.get("reason") or ""

    async with db.session() as session:
        check = await session.execute(
            text("""SELECT assigned_reviewer_id::text, ai_recommendation, ai_confidence
                    FROM pa_review_queue WHERE auth_request_id = :req_id"""),
            {"req_id": req_id},
        )
        row = check.mappings().one_or_none()
        if not row:
            raise HTTPException(status_code=404, detail="PA request not found")

        verb = "accepted" if action == "accept" else "overrode"
        note = (
            f"Reviewer {verb} the AI recommendation "
            f"(confidence {row['ai_confidence']}). {reason}".strip()
        )
        await session.execute(
            text("""
                INSERT INTO pa_review_actions
                    (auth_request_id, reviewer_id, action_type, note, metadata_json)
                VALUES (:req_id, CAST(:rev AS uuid), 'ai_recommendation', :note, CAST(:meta AS jsonb))
            """),
            {
                "req_id": req_id, "rev": row["assigned_reviewer_id"], "note": note,
                "meta": json.dumps({
                    "human_action": action,
                    "ai_recommendation": row["ai_recommendation"],
                    "ai_confidence": float(row["ai_confidence"]) if row["ai_confidence"] is not None else None,
                    "reason": reason,
                }, default=str),
            },
        )
        await session.commit()

    return await get_request(req_id)


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


@api.post("/agent/query/stream", operation_id="queryPAAgentStream")
async def query_agent_stream(query_in: AgentQueryIn):
    """SSE variant of /agent/query — streams progress milestones then the review."""
    auth_request_id = query_in.auth_request_id or ""
    question = query_in.question

    async def event_source():
        loop = asyncio.get_running_loop()
        queue: asyncio.Queue = asyncio.Queue()
        _SENTINEL = object()

        def _produce():
            try:
                for event_type, payload in stream_pa_agent(auth_request_id, question):
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

@api.get("/observability/traces", operation_id="getObservabilityTraces")
async def get_observability_traces():
    """Recent agent + document traces from the UC OTel span tables."""
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
# Document Intake — upload, sample generation, auto-adjudication (SSE)
# ===================================================================

@api.get("/documents/scenarios", operation_id="listSampleScenarios")
async def list_sample_scenarios():
    """List the available sample-record scenarios for the generator."""
    return {"scenarios": list_scenarios()}


@api.get("/documents/sample", operation_id="downloadSampleRecord")
async def download_sample_record(scenario: str = "approvable"):
    """Generate a synthetic pre-populated medical-record PDF for download.

    Scenarios deliberately exercise the Auto-Approve / Needs-Review / Auto-Deny
    paths so a demo always has a document to upload.
    """
    pdf_bytes, filename = await asyncio.to_thread(generate_sample_pdf, scenario)
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@api.post("/documents/upload", operation_id="uploadDocument")
async def upload_document(file: UploadFile = File(...)):
    """Upload a medical record to the UC Volume; returns a document handle."""
    file_bytes = await file.read()
    if not file_bytes:
        raise HTTPException(status_code=400, detail="Empty file")
    handle = await asyncio.to_thread(docs.upload_document, file_bytes, file.filename or "upload.pdf")
    return handle


@api.post("/documents/adjudicate/stream", operation_id="adjudicateDocumentStream")
async def adjudicate_document_stream(payload: dict):
    """Stream the parse -> extract -> adjudicate -> write-back pipeline as SSE.

    Body: {document_id, filename, volume_path} (from /documents/upload).
    Emits milestone events so the UI shows each AI step executing in real time.
    """
    handle = {
        "document_id": payload.get("document_id", ""),
        "filename": payload.get("filename", "upload.pdf"),
        "volume_path": payload.get("volume_path", ""),
    }
    if not handle["volume_path"]:
        raise HTTPException(status_code=400, detail="volume_path is required")

    async def event_source():
        def sse(event_type: str, data: dict) -> str:
            return f"event: {event_type}\ndata: {json.dumps(data, default=str)}\n\n"

        try:
            yield sse("status", {"stage": "parsing",
                                 "message": "Parsing document with ai_parse_document…"})
            text_body = await asyncio.to_thread(docs.parse_document, handle["volume_path"])
            if not text_body:
                yield sse("error", {"message": "Document could not be parsed (no text extracted)."})
                return
            yield sse("parsed", {"text": text_body[:4000], "char_count": len(text_body)})

            yield sse("status", {"stage": "extracting",
                                 "message": "Extracting clinical facts with ai_extract…"})
            facts = await asyncio.to_thread(docs.extract_clinical_facts, text_body)
            yield sse("extracted", {"facts": facts})

            yield sse("status", {"stage": "adjudicating",
                                 "message": "Matching against medical policies (Tier-1 rules)…"})
            result = await asyncio.to_thread(docs.adjudicate, facts, text_body)
            yield sse("decision", result)

            # Write-back: create a real queue row + audit action.
            yield sse("status", {"stage": "persisting",
                                 "message": "Creating PA request in the review queue…"})
            try:
                async with db.session() as session:
                    auth_request_id = await docs.write_back_to_queue(session, facts, result, handle)
                yield sse("persisted", {"auth_request_id": auth_request_id})
            except Exception as e:
                yield sse("status", {"stage": "persist_error",
                                     "message": f"Queue write-back failed: {e}"})

            yield sse("done", {"decision": result["decision"]})
        except Exception as e:
            import traceback
            traceback.print_exc()
            yield sse("error", {"message": str(e)})

    return StreamingResponse(
        event_source(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


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
