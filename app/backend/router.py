"""FastAPI routes for the Population Health Command Center."""

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException
from sqlalchemy import text

import asyncio

from .database import db
from .genie import ask_genie
from .agent import search_members, get_member_360, get_case_notes, query_member_agent
from .models import (
    ActivityLogOut,
    AddNoteIn,
    AgentQueryIn,
    AgentQueryOut,
    AlertDetailOut,
    AlertListOut,
    AlertSource,
    AssignAlertIn,
    CareManagerCaseload,
    CareManagerOut,
    CareCycleStatus,
    CaseNoteOut,
    DashboardStats,
    GenieQuestionIn,
    GenieResponseOut,
    Member360Out,
    MemberListOut,
    RiskTier,
    UpdateStatusIn,
)

api = APIRouter(prefix="/api")


# ===================================================================
# Health check
# ===================================================================

@api.get("/health", operation_id="healthCheck")
async def health_check():
    """Health check with diagnostic info."""
    import os
    diag = {
        "status": "ok",
        "db_initialized": db._initialized,
        "lakebase_pg_url_set": bool(os.environ.get("LAKEBASE_PG_URL")),
        "lakebase_project": os.environ.get("LAKEBASE_PROJECT_ID", "not set"),
        "lakebase_database": os.environ.get("LAKEBASE_DATABASE_NAME", "not set"),
        "genie_space_id": os.environ.get("GENIE_SPACE_ID", "not set"),
        "sql_warehouse_id": os.environ.get("SQL_WAREHOUSE_ID", "not set"),
    }
    if db._initialized:
        try:
            async with db.session() as session:
                result = await session.execute(text("SELECT 1 AS ping"))
                diag["db_connected"] = True
        except Exception as e:
            diag["db_connected"] = False
            diag["db_error"] = str(e)
    return diag


# ===================================================================
# Dashboard
# ===================================================================

@api.get("/dashboard/stats", response_model=DashboardStats, operation_id="getDashboardStats")
async def get_dashboard_stats():
    """Get aggregate dashboard statistics."""
    async with db.session() as session:
        result = await session.execute(text("""
            SELECT
                COUNT(*) AS total_alerts,
                COUNT(*) FILTER (WHERE status = 'Unassigned') AS unassigned_count,
                COUNT(*) FILTER (WHERE risk_tier = 'Critical') AS critical_count,
                COUNT(*) FILTER (WHERE risk_tier = 'High') AS high_count,
                COUNT(*) FILTER (WHERE status = 'Resolved'
                    AND resolved_at >= date_trunc('month', now())) AS resolved_this_month,
                EXTRACT(EPOCH FROM AVG(assigned_at - created_at) FILTER
                    (WHERE assigned_at IS NOT NULL)) / 3600 AS avg_time_to_assign_hours
            FROM risk_stratification_alerts
        """))
        row = result.mappings().one()

        # Alerts by source
        source_result = await session.execute(text("""
            SELECT alert_source::text, COUNT(*) AS cnt
            FROM risk_stratification_alerts
            GROUP BY alert_source
        """))
        alerts_by_source = {r["alert_source"]: r["cnt"] for r in source_result.mappings()}

        # Alerts by status
        status_result = await session.execute(text("""
            SELECT status::text, COUNT(*) AS cnt
            FROM risk_stratification_alerts
            GROUP BY status
        """))
        alerts_by_status = {r["status"]: r["cnt"] for r in status_result.mappings()}

        return DashboardStats(
            total_alerts=row["total_alerts"],
            unassigned_count=row["unassigned_count"],
            critical_count=row["critical_count"],
            high_count=row["high_count"],
            resolved_this_month=row["resolved_this_month"],
            avg_time_to_assign_hours=round(row["avg_time_to_assign_hours"], 1)
            if row["avg_time_to_assign_hours"]
            else None,
            alerts_by_source=alerts_by_source,
            alerts_by_status=alerts_by_status,
        )


# ===================================================================
# Alerts
# ===================================================================

@api.get("/alerts", response_model=list[AlertListOut], operation_id="listAlerts")
async def list_alerts(
    status: Optional[str] = None,
    risk_tier: Optional[str] = None,
    alert_source: Optional[str] = None,
    care_manager_id: Optional[str] = None,
    unassigned_only: bool = False,
):
    """List alerts with optional filters."""
    query = """
        SELECT
            a.alert_id::text,
            a.patient_id,
            a.mrn,
            a.member_id,
            a.risk_tier::text,
            a.risk_score,
            a.primary_driver,
            a.alert_source::text,
            a.status::text,
            a.payer,
            cm.display_name AS care_manager_name,
            cm.role::text AS care_manager_role,
            CASE WHEN a.status = 'Unassigned'
                THEN to_char(now() - a.created_at, 'DD "d" HH24 "h"')
                ELSE NULL
            END AS time_unassigned,
            a.created_at,
            a.updated_at
        FROM risk_stratification_alerts a
        LEFT JOIN care_managers cm ON a.assigned_care_manager_id = cm.care_manager_id
        WHERE 1=1
    """
    params: dict = {}

    if unassigned_only:
        query += " AND a.status = 'Unassigned'"
    if status:
        query += " AND a.status = CAST(:status AS care_cycle_status)"
        params["status"] = status
    if risk_tier:
        query += " AND a.risk_tier = CAST(:risk_tier AS risk_tier)"
        params["risk_tier"] = risk_tier
    if alert_source:
        query += " AND a.alert_source = CAST(:alert_source AS alert_source)"
        params["alert_source"] = alert_source
    if care_manager_id:
        query += " AND a.assigned_care_manager_id = CAST(:cm_id AS uuid)"
        params["cm_id"] = care_manager_id

    query += """
        ORDER BY
            CASE a.risk_tier
                WHEN 'Critical' THEN 1 WHEN 'High' THEN 2 WHEN 'Elevated' THEN 3
                WHEN 'Moderate' THEN 4 WHEN 'Low' THEN 5
            END,
            a.created_at ASC
    """

    async with db.session() as session:
        result = await session.execute(text(query), params)
        rows = result.mappings().all()
        return [AlertListOut(**dict(r)) for r in rows]


@api.get("/alerts/{alert_id}", response_model=AlertDetailOut, operation_id="getAlert")
async def get_alert(alert_id: str):
    """Get full alert detail including activity log."""
    try:
        return await _get_alert_impl(alert_id)
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


async def _get_alert_impl(alert_id: str):
    async with db.session() as session:
        result = await session.execute(
            text("""
                SELECT
                    a.alert_id::text,
                    a.patient_id,
                    a.mrn,
                    a.member_id,
                    a.risk_tier::text,
                    a.risk_score,
                    a.primary_driver,
                    COALESCE(a.secondary_drivers, ARRAY[]::text[]) AS secondary_drivers,
                    a.alert_source::text,
                    a.assigned_care_manager_id::text,
                    cm.display_name AS care_manager_name,
                    cm.role::text AS care_manager_role,
                    a.assigned_at,
                    a.status::text,
                    a.status_changed_at,
                    a.max_hba1c,
                    a.max_blood_glucose,
                    a.peak_ed_visits_12mo,
                    a.last_encounter_date,
                    a.last_facility,
                    a.payer,
                    COALESCE(a.active_medications, ARRAY[]::text[]) AS active_medications,
                    a.notes,
                    a.created_at,
                    a.updated_at,
                    a.resolved_at
                FROM risk_stratification_alerts a
                LEFT JOIN care_managers cm
                    ON a.assigned_care_manager_id = cm.care_manager_id
                WHERE a.alert_id = CAST(:id AS uuid)
            """),
            {"id": alert_id},
        )
        row = result.mappings().one_or_none()
        if not row:
            raise HTTPException(status_code=404, detail="Alert not found")

        # Get activity log
        log_result = await session.execute(
            text("""
                SELECT
                    l.activity_id::text,
                    l.alert_id::text,
                    cm.display_name AS care_manager_name,
                    l.activity_type,
                    l.previous_status::text,
                    l.new_status::text,
                    l.note,
                    l.created_at
                FROM alert_activity_log l
                LEFT JOIN care_managers cm ON l.care_manager_id = cm.care_manager_id
                WHERE l.alert_id = CAST(:id AS uuid)
                ORDER BY l.created_at DESC
            """),
            {"id": alert_id},
        )
        activities = [ActivityLogOut(**dict(r)) for r in log_result.mappings().all()]

        alert_data = dict(row)
        alert_data["activity_log"] = activities
        return AlertDetailOut(**alert_data)


@api.post("/alerts/{alert_id}/assign", response_model=AlertDetailOut, operation_id="assignAlert")
async def assign_alert(alert_id: str, assign_in: AssignAlertIn):
    """Self-assign an alert to a care manager."""
    async with db.session() as session:
        # Verify alert exists and is unassigned or reassignable
        check = await session.execute(
            text("SELECT status::text FROM risk_stratification_alerts WHERE alert_id = CAST(:id AS uuid)"),
            {"id": alert_id},
        )
        row = check.mappings().one_or_none()
        if not row:
            raise HTTPException(status_code=404, detail="Alert not found")

        old_status = row["status"]

        # Update assignment
        await session.execute(
            text("""
                UPDATE risk_stratification_alerts
                SET assigned_care_manager_id = CAST(:cm_id AS uuid),
                    status = 'Assigned'::care_cycle_status
                WHERE alert_id = CAST(:id AS uuid)
            """),
            {"id": alert_id, "cm_id": assign_in.care_manager_id},
        )

        # Log the assignment
        await session.execute(
            text("""
                INSERT INTO alert_activity_log
                    (alert_id, care_manager_id, activity_type, previous_status, new_status)
                VALUES
                    (CAST(:alert_id AS uuid), CAST(:cm_id AS uuid), 'assignment',
                     CAST(:old_status AS care_cycle_status), 'Assigned'::care_cycle_status)
            """),
            {
                "alert_id": alert_id,
                "cm_id": assign_in.care_manager_id,
                "old_status": old_status,
            },
        )
        await session.commit()

    return await get_alert(alert_id)


@api.post("/alerts/{alert_id}/status", response_model=AlertDetailOut, operation_id="updateAlertStatus")
async def update_alert_status(alert_id: str, status_in: UpdateStatusIn):
    """Update the care cycle status of an alert."""
    async with db.session() as session:
        check = await session.execute(
            text("""
                SELECT status::text, assigned_care_manager_id::text
                FROM risk_stratification_alerts WHERE alert_id = CAST(:id AS uuid)
            """),
            {"id": alert_id},
        )
        row = check.mappings().one_or_none()
        if not row:
            raise HTTPException(status_code=404, detail="Alert not found")

        old_status = row["status"]
        cm_id = row["assigned_care_manager_id"]

        await session.execute(
            text("""
                UPDATE risk_stratification_alerts
                SET status = CAST(:new_status AS care_cycle_status)
                WHERE alert_id = CAST(:id AS uuid)
            """),
            {"id": alert_id, "new_status": status_in.status.value},
        )

        # Log status change
        await session.execute(
            text("""
                INSERT INTO alert_activity_log
                    (alert_id, care_manager_id, activity_type,
                     previous_status, new_status, note)
                VALUES
                    (CAST(:alert_id AS uuid), CAST(:cm_id AS uuid), 'status_change',
                     CAST(:old AS care_cycle_status), CAST(:new AS care_cycle_status), :note)
            """),
            {
                "alert_id": alert_id,
                "cm_id": cm_id,
                "old": old_status,
                "new": status_in.status.value,
                "note": status_in.note,
            },
        )
        await session.commit()

    return await get_alert(alert_id)


@api.post("/alerts/{alert_id}/notes", response_model=AlertDetailOut, operation_id="addAlertNote")
async def add_alert_note(alert_id: str, note_in: AddNoteIn):
    """Add a note to an alert."""
    async with db.session() as session:
        check = await session.execute(
            text("""
                SELECT assigned_care_manager_id::text
                FROM risk_stratification_alerts WHERE alert_id = CAST(:id AS uuid)
            """),
            {"id": alert_id},
        )
        row = check.mappings().one_or_none()
        if not row:
            raise HTTPException(status_code=404, detail="Alert not found")

        # Append note (keep latest in main column for quick reads)
        await session.execute(
            text("""
                UPDATE risk_stratification_alerts
                SET notes = :note
                WHERE alert_id = CAST(:id AS uuid)
            """),
            {"id": alert_id, "note": note_in.note},
        )

        # Log note
        await session.execute(
            text("""
                INSERT INTO alert_activity_log
                    (alert_id, care_manager_id, activity_type, note)
                VALUES
                    (CAST(:alert_id AS uuid), CAST(:cm_id AS uuid), 'note_added', :note)
            """),
            {
                "alert_id": alert_id,
                "cm_id": row["assigned_care_manager_id"],
                "note": note_in.note,
            },
        )
        await session.commit()

    return await get_alert(alert_id)


# ===================================================================
# Care Managers
# ===================================================================

@api.get(
    "/care-managers",
    response_model=list[CareManagerOut],
    operation_id="listCareManagers",
)
async def list_care_managers():
    """List all active care managers."""
    async with db.session() as session:
        result = await session.execute(text("""
            SELECT
                care_manager_id::text, email, display_name,
                role::text, department, max_caseload, is_active
            FROM care_managers
            WHERE is_active = TRUE
            ORDER BY display_name
        """))
        return [CareManagerOut(**dict(r)) for r in result.mappings().all()]


@api.get(
    "/care-managers/caseload",
    response_model=list[CareManagerCaseload],
    operation_id="getCaseloadDashboard",
)
async def get_caseload_dashboard():
    """Get caseload stats for all active care managers."""
    async with db.session() as session:
        result = await session.execute(text("""
            SELECT
                care_manager_id::text,
                display_name,
                role::text,
                max_caseload,
                active_cases,
                critical_cases,
                pending_outreach,
                pending_followup,
                available_capacity
            FROM v_care_manager_caseload
            ORDER BY active_cases DESC
        """))
        return [CareManagerCaseload(**dict(r)) for r in result.mappings().all()]


# ===================================================================
# Genie
# ===================================================================

@api.post("/genie/ask", response_model=GenieResponseOut, operation_id="askGenie")
async def ask_genie_endpoint(question_in: GenieQuestionIn):
    """Ask Genie a natural language question about patient data."""
    return ask_genie(question_in)


# ===================================================================
# Member 360
# ===================================================================

@api.get("/members", response_model=list[MemberListOut], operation_id="searchMembers")
async def search_members_endpoint(q: str = ""):
    """Search members by name or member ID."""
    if not q.strip():
        return []
    rows = await asyncio.to_thread(search_members, q)
    return [MemberListOut(**r) for r in rows]


@api.get("/members/{member_id}/360", response_model=Member360Out, operation_id="getMember360")
async def get_member_360_endpoint(member_id: str):
    """Get full Member 360 profile."""
    profile = await asyncio.to_thread(get_member_360, member_id)
    if not profile:
        raise HTTPException(status_code=404, detail=f"Member {member_id} not found")
    return Member360Out(**profile)


@api.get("/members/{member_id}/case-notes", response_model=list[CaseNoteOut], operation_id="getCaseNotes")
async def get_case_notes_endpoint(member_id: str):
    """Get recent case notes, call transcripts, and claims summaries for a member."""
    notes = await asyncio.to_thread(get_case_notes, member_id)
    return [CaseNoteOut(**n) for n in notes]


@api.post("/members/{member_id}/agent-query", response_model=AgentQueryOut, operation_id="queryMemberAgent")
async def query_agent_endpoint(member_id: str, query_in: AgentQueryIn):
    """Query the Member RAG Agent about a specific member."""
    result = await asyncio.to_thread(query_member_agent, member_id, query_in.question)
    return AgentQueryOut(**result)
