"""FastAPI routes for the Population Health Command Center."""

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse
from sqlalchemy import text

import asyncio
import json
import traceback

from .database import db
from .genie import ask_genie
from .agent import search_members, get_member_360, get_case_notes, query_member_agent, get_member_sdoh
from .agent_graph import query_supervisor_agent, stream_supervisor_agent
from .websocket import notifications
from .identity import UserIdentity, get_current_user
from .conversation_store import (
    get_or_create_conversation,
    save_message,
    load_history,
    list_conversations,
    save_feedback,
)
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
    ConversationListOut,
    ConversationMessageOut,
    DashboardStats,
    FeedbackIn,
    FeedbackOut,
    GenieQuestionIn,
    GenieResponseOut,
    Member360Out,
    MemberListOut,
    MemberSdohOut,
    NextBestActionOut,
    OutreachDraftIn,
    OutreachDraftOut,
    CohortSearchIn,
    CohortAnalyticsOut,
    CohortMemberOut,
    SaveCohortIn,
    SavedCohortOut,
    RiskTier,
    UpdateStatusIn,
)

api = APIRouter(prefix="/api")


# ===================================================================
# Embed config — workspace URL and Genie space for iframe embedding
# ===================================================================

@api.get("/embed-config", operation_id="getEmbedConfig")
async def get_embed_config():
    """Return workspace URL and Genie space ID for iframe embedding."""
    import os
    from .env_config import GENIE_SPACE_ID

    host = os.environ.get("DATABRICKS_HOST", "")
    if host and not host.startswith("http"):
        host = f"https://{host}"
    if not host:
        try:
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient()
            host = w.config.host or ""
        except Exception:
            pass

    return {
        "workspace_url": host,
        "genie_space_id": GENIE_SPACE_ID,
    }


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

    result = await get_alert(alert_id)
    await notifications.broadcast("alert_assigned", {
        "alert_id": alert_id,
        "member_id": result.member_id,
        "care_manager_name": result.care_manager_name,
        "status": "Assigned",
    })
    return result


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

    result = await get_alert(alert_id)
    await notifications.broadcast("alert_status_changed", {
        "alert_id": alert_id,
        "member_id": result.member_id,
        "old_status": old_status,
        "new_status": status_in.status.value,
        "risk_tier": result.risk_tier,
    })
    return result


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
async def query_agent_endpoint(
    member_id: str,
    query_in: AgentQueryIn,
    request: Request,
):
    """Query the Supervisor Agent about a specific member.
    Persists conversation history for multi-turn context."""
    user = get_current_user(request)

    # Get or create conversation thread
    conversation_id = await get_or_create_conversation(
        member_id, user.email, query_in.conversation_id
    )

    # Load prior messages for context
    history = await load_history(conversation_id)

    # Save the user's question
    await save_message(conversation_id, "human", query_in.question)

    # Run the agent with conversation history
    result = await asyncio.to_thread(
        query_supervisor_agent,
        member_id,
        query_in.question,
        conversation_id,
        history,
    )

    # Save the agent's response
    message_id = await save_message(
        conversation_id,
        "assistant",
        result["answer"],
        metadata={
            "category": result.get("category"),
            "specialists_consulted": result.get("specialists_consulted", []),
        },
    )

    result["conversation_id"] = conversation_id
    result["message_id"] = message_id
    return AgentQueryOut(**result)


@api.post("/members/{member_id}/agent-stream", operation_id="streamMemberAgent")
async def stream_agent_endpoint(
    member_id: str,
    query_in: AgentQueryIn,
    request: Request,
):
    """Stream the Supervisor Agent response via Server-Sent Events.
    Events: start, routing, tool_call, token, done, error."""
    user = get_current_user(request)

    conversation_id = await get_or_create_conversation(
        member_id, user.email, query_in.conversation_id
    )
    history = await load_history(conversation_id)
    await save_message(conversation_id, "human", query_in.question)

    async def event_generator():
        full_answer = ""
        async for event in stream_supervisor_agent(
            member_id, query_in.question, conversation_id, history
        ):
            if event["event"] == "token":
                token_data = json.loads(event["data"])
                full_answer = token_data.get("content", full_answer)
            yield f"event: {event['event']}\ndata: {event['data']}\n\n"

        # Save the agent response after streaming completes
        if full_answer:
            msg_id = await save_message(
                conversation_id, "assistant", full_answer
            )
            yield f"event: message_saved\ndata: {json.dumps({'message_id': msg_id, 'conversation_id': conversation_id})}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@api.post("/members/{member_id}/agent-query-legacy", response_model=AgentQueryOut, operation_id="queryMemberAgentLegacy")
async def query_agent_legacy_endpoint(member_id: str, query_in: AgentQueryIn):
    """Legacy single-shot RAG agent (kept for backward compatibility)."""
    result = await asyncio.to_thread(query_member_agent, member_id, query_in.question)
    return AgentQueryOut(**result)


# ===================================================================
# SDOH Profile
# ===================================================================

@api.get("/members/{member_id}/sdoh", response_model=MemberSdohOut, operation_id="getMemberSdoh")
async def get_member_sdoh_endpoint(member_id: str):
    """Get SDOH screening profile for a member."""
    sdoh = await asyncio.to_thread(get_member_sdoh, member_id)
    if not sdoh:
        return MemberSdohOut(member_id=member_id)
    # Convert string flags to booleans
    for flag in ["food_insecurity_flag", "housing_instability_flag",
                 "transportation_barrier_flag", "social_isolation_flag",
                 "financial_strain_flag"]:
        val = sdoh.get(flag)
        if isinstance(val, str):
            sdoh[flag] = val.lower() in ("true", "1", "t")
    if isinstance(sdoh.get("total_sdoh_flags"), str):
        sdoh["total_sdoh_flags"] = int(sdoh["total_sdoh_flags"])
    if isinstance(sdoh.get("composite_sdoh_risk_score"), str):
        sdoh["composite_sdoh_risk_score"] = float(sdoh["composite_sdoh_risk_score"])
    return MemberSdohOut(**sdoh)


# ===================================================================
# Next Best Action (AI-powered)
# ===================================================================

@api.get("/alerts/{alert_id}/next-actions", response_model=NextBestActionOut, operation_id="getNextBestActions")
async def get_next_best_actions(alert_id: str):
    """Generate AI-powered next best action recommendations for an alert."""
    alert_detail = await _get_alert_impl(alert_id)

    from .env_config import LLM_ENDPOINT
    from databricks.sdk import WorkspaceClient
    from openai import OpenAI

    def _generate():
        w = WorkspaceClient()
        token = w.config.authenticate().get("Authorization", "").replace("Bearer ", "")
        host = w.config.host.rstrip("/")
        client = OpenAI(api_key=token, base_url=f"{host}/serving-endpoints")

        prompt = f"""You are a care management clinical decision support system. Based on the following patient alert, generate 3-5 prioritized next best actions for the care manager.

Patient: {alert_detail.mrn or alert_detail.patient_id}
Risk Tier: {alert_detail.risk_tier}
Primary Driver: {alert_detail.primary_driver}
Secondary Drivers: {', '.join(alert_detail.secondary_drivers or [])}
Status: {alert_detail.status}
HbA1c: {alert_detail.max_hba1c or 'N/A'}
Blood Glucose: {alert_detail.max_blood_glucose or 'N/A'}
ED Visits (12mo): {alert_detail.peak_ed_visits_12mo or 'N/A'}
Active Medications: {', '.join(alert_detail.active_medications or [])}

Return a JSON array of actions. Each action should have:
- "action": short action title
- "priority": "High", "Medium", or "Low"
- "category": "Clinical", "Outreach", "Referral", "Administrative", or "Follow-Up"
- "detail": 1-2 sentence explanation

Return ONLY the JSON array, no markdown or other text."""

        resp = client.chat.completions.create(
            model=LLM_ENDPOINT,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=1024,
            temperature=0.3,
        )
        content = resp.choices[0].message.content.strip()
        # Parse JSON from response
        import re
        match = re.search(r'\[.*\]', content, re.DOTALL)
        if match:
            actions = json.loads(match.group())
        else:
            actions = []
        return actions

    try:
        actions = await asyncio.to_thread(_generate)
        return NextBestActionOut(
            actions=actions,
            rationale=f"Generated based on {alert_detail.risk_tier} risk alert with primary driver: {alert_detail.primary_driver}",
        )
    except Exception as e:
        print(f"[NBA] Error generating next best actions: {e}")
        traceback.print_exc()
        return NextBestActionOut(
            actions=[],
            rationale=f"Unable to generate recommendations: {str(e)}",
        )


# ===================================================================
# Care Plan Generation (AI-powered)
# ===================================================================

@api.post("/members/{member_id}/care-plan", operation_id="generateCarePlan")
async def generate_care_plan(member_id: str):
    """Generate an AI-powered care plan for a member."""
    from .env_config import LLM_ENDPOINT
    from databricks.sdk import WorkspaceClient
    from openai import OpenAI

    # Gather member context
    member = await asyncio.to_thread(get_member_360, member_id)
    sdoh = await asyncio.to_thread(get_member_sdoh, member_id)

    def _generate():
        w = WorkspaceClient()
        token = w.config.authenticate().get("Authorization", "").replace("Bearer ", "")
        host = w.config.host.rstrip("/")
        client = OpenAI(api_key=token, base_url=f"{host}/serving-endpoints")

        # Build context
        profile_summary = ""
        if member:
            profile_summary = f"""
Member: {member.get('first_name', '')} {member.get('last_name', '')} ({member_id})
Age: {member.get('age', 'Unknown')}, Gender: {member.get('gender', 'Unknown')}
LOB: {member.get('line_of_business', 'Unknown')}, Plan: {member.get('plan_type', 'Unknown')}
RAF Score: {member.get('raf_score', 'N/A')}, Risk Tier: {member.get('risk_tier', 'Unknown')}
HCC Codes: {member.get('hcc_codes', 'None')}
Top Diagnoses: {member.get('top_diagnoses', 'None')}
HEDIS Gaps: {member.get('hedis_gap_measures', 'None')} ({member.get('hedis_gap_count', 0)} open)
Medical Claims YTD: {member.get('medical_total_paid_ytd', 'N/A')}
Last Encounter: {member.get('last_encounter_date', 'Unknown')} ({member.get('last_encounter_type', 'Unknown')})"""

        sdoh_summary = ""
        if sdoh and int(sdoh.get("total_sdoh_flags", 0) or 0) > 0:
            flags = []
            for f in ["food_insecurity_flag", "housing_instability_flag", "transportation_barrier_flag", "social_isolation_flag", "financial_strain_flag"]:
                val = sdoh.get(f)
                if val and str(val).lower() in ("true", "1", "t"):
                    flags.append(f.replace("_flag", "").replace("_", " ").title())
            sdoh_summary = f"\nSDOH Flags: {', '.join(flags)}"

        prompt = f"""You are a clinical care plan generator for a healthcare payer. Based on the member profile below, generate a comprehensive care plan.

{profile_summary}{sdoh_summary}

Generate a structured care plan as valid JSON with this exact schema:
{{
  "summary": "2-3 sentence overview of the care plan rationale and goals",
  "goals": [
    {{
      "goal": "Specific, measurable clinical goal",
      "target_date": "Target date (e.g., '2026-08-01')",
      "status": "Not Started",
      "interventions": [
        {{
          "action": "Specific intervention or task",
          "responsible": "Role responsible (e.g., 'Care Manager', 'PCP', 'Specialist')",
          "frequency": "How often (e.g., 'Weekly', 'Monthly', 'One-time')",
          "status": "Not Started",
          "notes": "Brief note or rationale"
        }}
      ]
    }}
  ]
}}

Generate 3-5 goals with 2-4 interventions each. Goals should address:
- Chronic condition management (based on diagnoses and HCC codes)
- Care gap closure (based on HEDIS gaps)
- SDOH needs (if flags present)
- Preventive care and wellness
- Cost/utilization management (if high utilizer)

Return ONLY the JSON object, no markdown or explanation."""

        resp = client.chat.completions.create(
            model=LLM_ENDPOINT,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=3000,
            temperature=0.3,
        )

        text = resp.choices[0].message.content.strip()
        # Strip markdown code fences if present
        if text.startswith("```"):
            text = text.split("\n", 1)[1] if "\n" in text else text[3:]
            if text.endswith("```"):
                text = text[:-3].strip()

        plan = json.loads(text)
        plan["generated_at"] = datetime.utcnow().isoformat() + "Z"
        return plan

    try:
        plan = await asyncio.to_thread(_generate)
        return plan
    except Exception as e:
        print(f"[CarePlan] Error generating care plan: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Care plan generation failed: {str(e)}")


# ===================================================================
# Conversations & Feedback
# ===================================================================

@api.get(
    "/members/{member_id}/conversations",
    response_model=list[ConversationListOut],
    operation_id="listConversations",
)
async def list_conversations_endpoint(member_id: str, request: Request):
    """List conversation threads for the current user and member."""
    user = get_current_user(request)
    convos = await list_conversations(user.email, member_id)
    return [ConversationListOut(**c) for c in convos]


@api.get(
    "/conversations/{conversation_id}/messages",
    response_model=list[ConversationMessageOut],
    operation_id="getConversationMessages",
)
async def get_conversation_messages_endpoint(conversation_id: str):
    """Get message history for a conversation thread."""
    messages = await load_history(conversation_id, max_messages=50)
    return [ConversationMessageOut(**m) for m in messages]


@api.post("/feedback", response_model=FeedbackOut, operation_id="submitFeedback")
async def submit_feedback_endpoint(feedback_in: FeedbackIn, request: Request):
    """Submit thumbs-up/down feedback on an agent response."""
    user = get_current_user(request)
    feedback_id = await save_feedback(
        message_id=feedback_in.message_id,
        conversation_id=feedback_in.conversation_id,
        user_email=user.email,
        rating=feedback_in.rating,
        comment=feedback_in.comment,
    )
    return FeedbackOut(feedback_id=feedback_id)


@api.get("/me", operation_id="getCurrentUser")
async def get_current_user_endpoint(request: Request):
    """Return the current authenticated user's identity."""
    user = get_current_user(request)
    return {"email": user.email, "display_name": user.display_name}


# ===================================================================
# Outreach Drafting (AI-powered)
# ===================================================================

@api.post(
    "/members/{member_id}/outreach-draft",
    response_model=OutreachDraftOut,
    operation_id="generateOutreachDraft",
)
async def generate_outreach_draft(member_id: str, body: OutreachDraftIn):
    """Generate a personalized outreach script for a member."""
    from .env_config import LLM_ENDPOINT
    from databricks.sdk import WorkspaceClient
    from openai import OpenAI

    member = await asyncio.to_thread(get_member_360, member_id)
    sdoh = await asyncio.to_thread(get_member_sdoh, member_id)

    def _generate():
        w = WorkspaceClient()
        token = w.config.authenticate().get("Authorization", "").replace("Bearer ", "")
        host = w.config.host.rstrip("/")
        client = OpenAI(api_key=token, base_url=f"{host}/serving-endpoints")

        member_name = "the member"
        if member:
            first = member.get("first_name", "")
            last = member.get("last_name", "")
            member_name = f"{first} {last}".strip() or member.get("member_name", "the member")

        profile_lines = []
        if member:
            profile_lines.append(f"Name: {member_name}")
            profile_lines.append(f"Age: {member.get('age', 'Unknown')}, Gender: {member.get('gender', 'Unknown')}")
            profile_lines.append(f"Risk Tier: {member.get('risk_tier', 'Unknown')}, RAF Score: {member.get('raf_score', 'N/A')}")
            profile_lines.append(f"Top Diagnoses: {member.get('top_diagnoses', 'None')}")
            profile_lines.append(f"HEDIS Gaps: {member.get('hedis_gap_measures', 'None')}")
            profile_lines.append(f"Last Encounter: {member.get('last_encounter_date', 'Unknown')}")

        sdoh_lines = []
        if sdoh and int(sdoh.get("total_sdoh_flags", 0) or 0) > 0:
            for f in ["food_insecurity_flag", "housing_instability_flag", "transportation_barrier_flag", "social_isolation_flag", "financial_strain_flag"]:
                val = sdoh.get(f)
                if val and str(val).lower() in ("true", "1", "t"):
                    sdoh_lines.append(f.replace("_flag", "").replace("_", " ").title())

        channel = body.channel or "phone"
        channel_guidance = {
            "phone": "Write a warm, conversational phone script. Include an opening, key talking points, and a closing. Keep it under 400 words.",
            "email": "Write a professional email with a subject line. Be concise but empathetic. Include a clear call to action.",
            "sms": "Write a brief SMS message (under 160 characters). CRITICAL: SMS is an unsecured channel — do NOT include any Protected Health Information (PHI), diagnoses, conditions, medication names, or appointment details. Only include a friendly greeting by first name, a general wellness check-in, and a callback number (1-800-555-CARE). Be warm but vague.",
        }.get(channel, "Write an outreach script.")

        prompt = f"""You are a care management outreach specialist for Red Bricks Insurance. Generate a personalized {channel} outreach script.

Member Profile:
{chr(10).join(profile_lines) if profile_lines else "No profile data available."}

{"SDOH Concerns: " + ", ".join(sdoh_lines) if sdoh_lines else ""}
{"Additional Context: " + body.context if body.context else ""}

{channel_guidance}

Return valid JSON with this exact schema:
{{
  "subject": "Email subject line (null for phone/sms)",
  "script": "The full outreach script",
  "key_talking_points": ["Point 1", "Point 2", "Point 3"]
}}

Be empathetic, culturally sensitive, and avoid medical jargon. Never include real PHI placeholders.
Return ONLY the JSON object."""

        resp = client.chat.completions.create(
            model=LLM_ENDPOINT,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=2000,
            temperature=0.4,
        )

        text = resp.choices[0].message.content.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1] if "\n" in text else text[3:]
            if text.endswith("```"):
                text = text[:-3].strip()

        result = json.loads(text)
        result["channel"] = channel
        result["member_name"] = member_name
        result["generated_at"] = datetime.utcnow().isoformat() + "Z"
        return result

    try:
        draft = await asyncio.to_thread(_generate)
        return OutreachDraftOut(**draft)
    except Exception as e:
        print(f"[Outreach] Error generating draft: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Outreach draft generation failed: {str(e)}")


# ===================================================================
# Cohort Builder
# ===================================================================

@api.post(
    "/cohorts/search",
    response_model=CohortAnalyticsOut,
    operation_id="searchCohort",
)
async def search_cohort(body: CohortSearchIn):
    """Search for a cohort of members based on criteria and return analytics."""
    from .env_config import UC_CATALOG, SQL_WAREHOUSE_ID
    from databricks.sdk import WorkspaceClient

    def _search():
        w = WorkspaceClient()
        cat = f"`{UC_CATALOG}`"
        conditions = ["1=1"]

        if body.risk_tiers:
            tiers = ", ".join(f"'{t}'" for t in body.risk_tiers)
            conditions.append(f"risk_tier IN ({tiers})")
        if body.counties:
            counties = ", ".join(f"'{c}'" for c in body.counties)
            conditions.append(f"county IN ({counties})")
        if body.lines_of_business:
            lobs = ", ".join(f"'{l}'" for l in body.lines_of_business)
            conditions.append(f"line_of_business IN ({lobs})")
        if body.min_age is not None:
            conditions.append(f"CAST(age AS INT) >= {int(body.min_age)}")
        if body.max_age is not None:
            conditions.append(f"CAST(age AS INT) <= {int(body.max_age)}")
        if body.gender:
            conditions.append(f"gender = '{body.gender}'")
        if body.min_raf_score is not None:
            conditions.append(f"CAST(raf_score AS DOUBLE) >= {float(body.min_raf_score)}")
        if body.has_hedis_gaps:
            conditions.append("CAST(hedis_gap_count AS INT) > 0")
        if body.diagnoses_contain:
            conditions.append(f"LOWER(top_diagnoses) LIKE '%{body.diagnoses_contain.lower()}%'")

        where = " AND ".join(conditions)
        limit = min(body.limit or 100, 500)

        sql = f"""
        SELECT member_id, member_name, age, gender, county, risk_tier,
               raf_score, line_of_business, hedis_gap_count, top_diagnoses,
               total_paid_ytd
        FROM {cat}.analytics.gold_member_360
        WHERE {where}
        LIMIT {limit}
        """

        stmt = w.statement_execution.execute_statement(
            warehouse_id=SQL_WAREHOUSE_ID,
            statement=sql,
            wait_timeout="30s",
        )

        rows = []
        if stmt.result and stmt.result.data_array:
            cols = [c.name for c in stmt.manifest.schema.columns]
            for row in stmt.result.data_array:
                rows.append(dict(zip(cols, row)))

        # Compute analytics
        members = [CohortMemberOut(**r) for r in rows]
        risk_dist: dict[str, int] = {}
        gender_dist: dict[str, int] = {}
        lob_dist: dict[str, int] = {}
        county_counts: dict[str, int] = {}
        total_cost = 0.0
        total_raf = 0.0
        total_age = 0.0
        raf_count = 0
        age_count = 0
        total_gaps = 0

        for m in members:
            rt = m.risk_tier or "Unknown"
            risk_dist[rt] = risk_dist.get(rt, 0) + 1
            g = m.gender or "Unknown"
            gender_dist[g] = gender_dist.get(g, 0) + 1
            lob = m.line_of_business or "Unknown"
            lob_dist[lob] = lob_dist.get(lob, 0) + 1
            c = m.county or "Unknown"
            county_counts[c] = county_counts.get(c, 0) + 1
            try:
                total_cost += float(m.total_paid_ytd or 0)
            except (ValueError, TypeError):
                pass
            try:
                total_raf += float(m.raf_score or 0)
                raf_count += 1
            except (ValueError, TypeError):
                pass
            try:
                total_age += float(m.age or 0)
                age_count += 1
            except (ValueError, TypeError):
                pass
            try:
                total_gaps += int(m.hedis_gap_count or 0)
            except (ValueError, TypeError):
                pass

        # Top 10 counties
        top_counties = dict(sorted(county_counts.items(), key=lambda x: x[1], reverse=True)[:10])

        return CohortAnalyticsOut(
            total_members=len(members),
            risk_distribution=risk_dist,
            avg_raf_score=round(total_raf / raf_count, 2) if raf_count else None,
            avg_age=round(total_age / age_count, 1) if age_count else None,
            total_cost_ytd=round(total_cost, 2) if total_cost else None,
            avg_cost_per_member=round(total_cost / len(members), 2) if members and total_cost else None,
            total_hedis_gaps=total_gaps,
            gender_distribution=gender_dist,
            lob_distribution=lob_dist,
            top_counties=top_counties,
            members=members,
        )

    try:
        result = await asyncio.to_thread(_search)
        return result
    except Exception as e:
        print(f"[Cohort] Error searching cohort: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Cohort search failed: {str(e)}")


@api.get("/cohorts/filter-options", operation_id="getCohortFilterOptions")
async def get_cohort_filter_options():
    """Get available filter values for the cohort builder."""
    from .env_config import UC_CATALOG, SQL_WAREHOUSE_ID
    from databricks.sdk import WorkspaceClient

    def _fetch():
        w = WorkspaceClient()
        cat = f"`{UC_CATALOG}`"
        sql = f"""
        SELECT
            COLLECT_SET(risk_tier) AS risk_tiers,
            COLLECT_SET(county) AS counties,
            COLLECT_SET(line_of_business) AS lines_of_business,
            COLLECT_SET(gender) AS genders
        FROM {cat}.analytics.gold_member_360
        """
        stmt = w.statement_execution.execute_statement(
            warehouse_id=SQL_WAREHOUSE_ID,
            statement=sql,
            wait_timeout="30s",
        )
        if stmt.result and stmt.result.data_array:
            row = stmt.result.data_array[0]
            return {
                "risk_tiers": json.loads(row[0] or "[]"),
                "counties": sorted(json.loads(row[1] or "[]")),
                "lines_of_business": json.loads(row[2] or "[]"),
                "genders": json.loads(row[3] or "[]"),
            }
        return {"risk_tiers": [], "counties": [], "lines_of_business": [], "genders": []}

    try:
        return await asyncio.to_thread(_fetch)
    except Exception as e:
        print(f"[Cohort] Error fetching filter options: {e}")
        return {"risk_tiers": [], "counties": [], "lines_of_business": [], "genders": []}


# ===================================================================
# Saved Cohorts
# ===================================================================

@api.post(
    "/cohorts/save",
    response_model=SavedCohortOut,
    operation_id="saveCohort",
)
async def save_cohort(body: SaveCohortIn, request: Request):
    """Save a cohort definition for later reuse."""
    user = get_current_user(request)
    async with db.session() as session:
        result = await session.execute(
            text("""
                INSERT INTO saved_cohorts (cohort_name, description, criteria, member_count, created_by)
                VALUES (:name, :description, CAST(:criteria AS jsonb), :member_count, :created_by)
                RETURNING cohort_id::text, cohort_name, description,
                          criteria::text, member_count, created_by,
                          created_at::text
            """),
            {
                "name": body.cohort_name,
                "description": body.description,
                "criteria": json.dumps(body.criteria),
                "member_count": body.member_count,
                "created_by": user.email,
            },
        )
        await session.commit()
        row = result.mappings().one()
        return SavedCohortOut(
            cohort_id=row["cohort_id"],
            cohort_name=row["cohort_name"],
            description=row["description"],
            criteria=json.loads(row["criteria"]) if isinstance(row["criteria"], str) else row["criteria"],
            member_count=row["member_count"],
            created_by=row["created_by"],
            created_at=row["created_at"],
        )


@api.get(
    "/cohorts/saved",
    response_model=list[SavedCohortOut],
    operation_id="getSavedCohorts",
)
async def get_saved_cohorts():
    """List all saved cohorts ordered by most recent first."""
    async with db.session() as session:
        result = await session.execute(text("""
            SELECT cohort_id::text, cohort_name, description,
                   criteria::text, member_count, created_by,
                   created_at::text
            FROM saved_cohorts
            ORDER BY created_at DESC
            LIMIT 50
        """))
        rows = result.mappings().all()
        return [
            SavedCohortOut(
                cohort_id=r["cohort_id"],
                cohort_name=r["cohort_name"],
                description=r["description"],
                criteria=json.loads(r["criteria"]) if isinstance(r["criteria"], str) else r["criteria"],
                member_count=r["member_count"],
                created_by=r["created_by"],
                created_at=r["created_at"],
            )
            for r in rows
        ]


@api.delete(
    "/cohorts/saved/{cohort_id}",
    operation_id="deleteSavedCohort",
)
async def delete_saved_cohort(cohort_id: str):
    """Delete a saved cohort by ID."""
    async with db.session() as session:
        result = await session.execute(
            text("DELETE FROM saved_cohorts WHERE cohort_id = CAST(:id AS uuid) RETURNING cohort_id"),
            {"id": cohort_id},
        )
        if not result.fetchone():
            raise HTTPException(status_code=404, detail="Saved cohort not found")
        await session.commit()
    return {"status": "deleted", "cohort_id": cohort_id}
