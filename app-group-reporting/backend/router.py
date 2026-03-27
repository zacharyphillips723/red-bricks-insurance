"""FastAPI routes for the Group Reporting Portal."""

import asyncio

from fastapi import APIRouter, HTTPException

from .genie import ask_genie
from .agent import query_sales_coach
from .groups import (
    list_groups,
    get_report_card,
    get_experience,
    get_stop_loss,
    get_renewal,
    get_tcoc,
    report_high_cost_members,
    report_claims_trend,
    report_top_drugs,
    report_utilization_summary,
    report_risk_care_gaps,
)
from .models import (
    AgentChatIn,
    AgentChatOut,
    ClaimsTrendMonth,
    GenieQuestionIn,
    GenieResponseOut,
    GroupExperience,
    GroupListItem,
    GroupReportCard,
    GroupRenewal,
    GroupStopLoss,
    GroupTcocItem,
    HighCostMember,
    RiskCareGapsResponse,
    TopDrug,
    UtilizationRow,
)

api = APIRouter(prefix="/api")


# ===================================================================
# Health check
# ===================================================================

@api.get("/health", operation_id="healthCheck")
async def health_check():
    """Health check with diagnostic info."""
    import os
    return {
        "status": "ok",
        "app": "group-reporting-portal",
        "genie_space_id": os.environ.get("GENIE_SPACE_ID", "not set"),
        "sql_warehouse_id": os.environ.get("SQL_WAREHOUSE_ID", "not set"),
        "llm_endpoint": os.environ.get("LLM_ENDPOINT", "not set"),
        "enrichment": {
            "slack": bool(os.environ.get("SLACK_BOT_TOKEN", "").strip()),
            "glean": bool(os.environ.get("GLEAN_API_TOKEN", "").strip()),
            "salesforce": bool(os.environ.get("SF_INSTANCE_URL", "").strip()),
        },
    }


# ===================================================================
# Groups
# ===================================================================

@api.get("/groups", response_model=list[GroupListItem], operation_id="listGroups")
async def list_groups_endpoint(
    q: str = "",
    industry: str = "",
    funding_type: str = "",
    renewal_action: str = "",
):
    """List/filter employer groups."""
    rows = await asyncio.to_thread(list_groups, q, industry, funding_type, renewal_action)
    return [GroupListItem(**r) for r in rows]


@api.get("/groups/{group_id}/report-card", response_model=GroupReportCard, operation_id="getReportCard")
async def get_report_card_endpoint(group_id: str):
    """Get full report card for an employer group."""
    card = await asyncio.to_thread(get_report_card, group_id)
    if not card:
        raise HTTPException(status_code=404, detail=f"Group {group_id} not found")
    return GroupReportCard(**card)


@api.get("/groups/{group_id}/experience", response_model=GroupExperience, operation_id="getExperience")
async def get_experience_endpoint(group_id: str):
    """Get claims experience detail for a group."""
    data = await asyncio.to_thread(get_experience, group_id)
    if not data:
        raise HTTPException(status_code=404, detail=f"Group {group_id} not found")
    return GroupExperience(**data)


@api.get("/groups/{group_id}/stop-loss", response_model=GroupStopLoss, operation_id="getStopLoss")
async def get_stop_loss_endpoint(group_id: str):
    """Get stop-loss / reinsurance detail for a group."""
    data = await asyncio.to_thread(get_stop_loss, group_id)
    if not data:
        raise HTTPException(status_code=404, detail=f"Group {group_id} not found")
    return GroupStopLoss(**data)


@api.get("/groups/{group_id}/renewal", response_model=GroupRenewal, operation_id="getRenewal")
async def get_renewal_endpoint(group_id: str):
    """Get renewal pricing detail for a group."""
    data = await asyncio.to_thread(get_renewal, group_id)
    if not data:
        raise HTTPException(status_code=404, detail=f"Group {group_id} not found")
    return GroupRenewal(**data)


@api.get("/groups/{group_id}/tcoc", response_model=list[GroupTcocItem], operation_id="getTcoc")
async def get_tcoc_endpoint(group_id: str):
    """Get member cost tier distribution for a group."""
    rows = await asyncio.to_thread(get_tcoc, group_id)
    return [GroupTcocItem(**r) for r in rows]


# ===================================================================
# Standard Reports
# ===================================================================

@api.get("/groups/{group_id}/reports/high-cost-members", response_model=list[HighCostMember], operation_id="reportHighCostMembers")
async def report_high_cost_members_endpoint(group_id: str):
    """Top 10 costliest members in the group with clinical summary."""
    rows = await asyncio.to_thread(report_high_cost_members, group_id)
    return [HighCostMember(**r) for r in rows]


@api.get("/groups/{group_id}/reports/claims-trend", response_model=list[ClaimsTrendMonth], operation_id="reportClaimsTrend")
async def report_claims_trend_endpoint(group_id: str):
    """Monthly claims PMPM trend for the group."""
    rows = await asyncio.to_thread(report_claims_trend, group_id)
    return [ClaimsTrendMonth(**r) for r in rows]


@api.get("/groups/{group_id}/reports/top-drugs", response_model=list[TopDrug], operation_id="reportTopDrugs")
async def report_top_drugs_endpoint(group_id: str):
    """Top 10 drugs by plan paid for the group."""
    rows = await asyncio.to_thread(report_top_drugs, group_id)
    return [TopDrug(**r) for r in rows]


@api.get("/groups/{group_id}/reports/utilization", response_model=list[UtilizationRow], operation_id="reportUtilization")
async def report_utilization_endpoint(group_id: str):
    """Utilization breakdown by claim type."""
    rows = await asyncio.to_thread(report_utilization_summary, group_id)
    return [UtilizationRow(**r) for r in rows]


@api.get("/groups/{group_id}/reports/risk-care-gaps", response_model=RiskCareGapsResponse, operation_id="reportRiskCareGaps")
async def report_risk_care_gaps_endpoint(group_id: str):
    """Risk distribution and care gap summary."""
    data = await asyncio.to_thread(report_risk_care_gaps, group_id)
    return RiskCareGapsResponse(**data)


# ===================================================================
# Sales Coach Agent
# ===================================================================

@api.post("/agent/chat", response_model=AgentChatOut, operation_id="chatWithAgent")
async def chat_with_agent(chat_in: AgentChatIn):
    """Query the Sales Coach agent about a group."""
    result = await asyncio.to_thread(query_sales_coach, chat_in.group_id, chat_in.question)
    return AgentChatOut(**result)


# ===================================================================
# Genie
# ===================================================================

@api.post("/genie/ask", response_model=GenieResponseOut, operation_id="askGenie")
async def ask_genie_endpoint(question_in: GenieQuestionIn):
    """Ask Genie a natural language question about group data."""
    return await asyncio.to_thread(ask_genie, question_in)
