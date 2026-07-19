"""FastAPI routes for the Group Reporting Portal."""

import asyncio
import hashlib
import json
import math

from databricks.sdk import WorkspaceClient
from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse

from .genie import ask_genie
from .agent import query_sales_coach, stream_sales_coach
from .groups import _execute_sql
from .env_config import LLM_ENDPOINT, UC_CATALOG, UC_TRACE_SCHEMA, UC_TRACE_TABLE_PREFIX

OBSERVED_MODELS = [LLM_ENDPOINT]
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
    CompetitorBenchmark,
    CompetitiveBenchmarkResponse,
    GenieQuestionIn,
    GenieResponseOut,
    GroupExperience,
    GroupListItem,
    GroupReportCard,
    GroupRenewal,
    GroupStopLoss,
    GroupTcocItem,
    HighCostMember,
    RenewalScenarioIn,
    RenewalScenarioOut,
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
# PDF Report Card
# ===================================================================

def _build_report_card_html(card: dict) -> str:
    """Generate a branded HTML report card for print/PDF."""

    def _fmt(val, prefix="", suffix="", decimals=0):
        if not val:
            return "N/A"
        try:
            num = float(val)
            formatted = f"{num:,.{decimals}f}"
            return f"{prefix}{formatted}{suffix}"
        except (ValueError, TypeError):
            return str(val)

    def _pct(val):
        if not val:
            return "N/A"
        try:
            return f"{float(val) * 100:.1f}%"
        except (ValueError, TypeError):
            return str(val)

    def _score_color(score):
        try:
            s = int(score)
        except (ValueError, TypeError):
            return "#6b7280"
        if s >= 70:
            return "#16a34a"
        elif s >= 40:
            return "#ca8a04"
        return "#dc2626"

    health_score = card.get("group_health_score", "0")
    score_color = _score_color(health_score)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Report Card - {card.get('group_name', 'Unknown Group')}</title>
<style>
  @page {{ margin: 0.5in; }}
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; color: #1b1b1b; font-size: 11px; line-height: 1.4; }}
  .header {{ background: #FF3621; color: white; padding: 20px 30px; display: flex; justify-content: space-between; align-items: center; }}
  .header h1 {{ font-size: 20px; font-weight: 700; }}
  .header .subtitle {{ font-size: 11px; opacity: 0.9; margin-top: 2px; }}
  .header .logo {{ text-align: right; }}
  .header .logo .brand {{ font-size: 16px; font-weight: 700; }}
  .header .logo .tagline {{ font-size: 9px; opacity: 0.8; }}
  .meta-bar {{ background: #f8f8f8; padding: 10px 30px; display: flex; gap: 24px; font-size: 10px; color: #666; border-bottom: 1px solid #e5e5e5; }}
  .meta-bar span {{ display: flex; align-items: center; gap: 4px; }}
  .meta-bar .label {{ font-weight: 600; color: #333; }}
  .content {{ padding: 20px 30px; }}
  .score-badge {{ display: inline-flex; align-items: center; gap: 8px; padding: 4px 12px; border-radius: 20px; border: 2px solid {score_color}; }}
  .score-badge .number {{ font-size: 20px; font-weight: 700; color: {score_color}; }}
  .score-badge .text {{ font-size: 9px; color: #666; }}
  .section {{ margin-top: 16px; }}
  .section-title {{ font-size: 12px; font-weight: 700; color: #FF3621; border-bottom: 2px solid #FF3621; padding-bottom: 4px; margin-bottom: 10px; text-transform: uppercase; letter-spacing: 0.5px; }}
  .metrics-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; }}
  .metric {{ background: #fafafa; border: 1px solid #eee; border-radius: 6px; padding: 10px; }}
  .metric .label {{ font-size: 9px; color: #888; text-transform: uppercase; letter-spacing: 0.3px; }}
  .metric .value {{ font-size: 16px; font-weight: 700; color: #1b1b1b; margin-top: 2px; }}
  .metric .value.red {{ color: #dc2626; }}
  .metric .value.green {{ color: #16a34a; }}
  .metric .value.amber {{ color: #ca8a04; }}
  .two-col {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }}
  .gauge {{ margin-bottom: 8px; }}
  .gauge .gauge-label {{ display: flex; justify-content: space-between; font-size: 10px; margin-bottom: 3px; }}
  .gauge .gauge-bar {{ height: 6px; background: #e5e5e5; border-radius: 3px; overflow: hidden; }}
  .gauge .gauge-fill {{ height: 100%; border-radius: 3px; }}
  .footer {{ margin-top: 24px; padding-top: 12px; border-top: 1px solid #e5e5e5; display: flex; justify-content: space-between; font-size: 9px; color: #999; }}
  .renewal-badge {{ display: inline-block; padding: 3px 10px; border-radius: 12px; font-size: 10px; font-weight: 600; }}
  .badge-red {{ background: #fee2e2; color: #991b1b; }}
  .badge-amber {{ background: #fef3c7; color: #92400e; }}
  .badge-yellow {{ background: #fefce8; color: #854d0e; }}
  .badge-green {{ background: #dcfce7; color: #166534; }}
  @media print {{
    body {{ -webkit-print-color-adjust: exact; print-color-adjust: exact; }}
  }}
</style>
</head>
<body>
<div class="header">
  <div>
    <h1>{card.get('group_name', 'Unknown Group')}</h1>
    <div class="subtitle">Employer Group Report Card</div>
  </div>
  <div class="logo">
    <div class="brand">Red Bricks Insurance</div>
    <div class="tagline">Powered by Databricks</div>
  </div>
</div>

<div class="meta-bar">
  <span><span class="label">Group ID:</span> {card.get('group_id', 'N/A')}</span>
  <span><span class="label">Industry:</span> {card.get('industry', 'N/A')}</span>
  <span><span class="label">Size Tier:</span> {card.get('group_size_tier', 'N/A')}</span>
  <span><span class="label">Funding:</span> {card.get('funding_type', 'N/A')}</span>
  <span><span class="label">State:</span> {card.get('state', 'N/A')}</span>
  <span><span class="label">Renewal Date:</span> {card.get('renewal_date', 'N/A')}</span>
  <span>
    <span class="label">Health Score:</span>
    <span style="color: {score_color}; font-weight: 700;">{health_score}</span>
  </span>
</div>

<div class="content">
  <!-- Key Metrics -->
  <div class="section">
    <div class="section-title">Key Performance Metrics</div>
    <div class="metrics-grid">
      <div class="metric">
        <div class="label">Total Members</div>
        <div class="value">{_fmt(card.get('total_members'))}</div>
      </div>
      <div class="metric">
        <div class="label">Claims PMPM</div>
        <div class="value">{_fmt(card.get('claims_pmpm'), '$', '', 0)}</div>
      </div>
      <div class="metric">
        <div class="label">Loss Ratio</div>
        <div class="value {'red' if float(card.get('loss_ratio') or 0) > 1 else 'amber' if float(card.get('loss_ratio') or 0) > 0.85 else 'green'}">{_pct(card.get('loss_ratio'))}</div>
      </div>
      <div class="metric">
        <div class="label">Projected Renewal PMPM</div>
        <div class="value">{_fmt(card.get('projected_renewal_pmpm'), '$', '', 2)}</div>
      </div>
    </div>
  </div>

  <div class="two-col" style="margin-top: 16px;">
    <!-- Financial Overview -->
    <div class="section">
      <div class="section-title">Financial Overview</div>
      <div class="metrics-grid" style="grid-template-columns: repeat(2, 1fr);">
        <div class="metric">
          <div class="label">Premium Revenue</div>
          <div class="value">{_fmt(card.get('total_premium_revenue'), '$', '', 0)}</div>
        </div>
        <div class="metric">
          <div class="label">Total Claims Paid</div>
          <div class="value">{_fmt(card.get('total_claims_paid'), '$', '', 0)}</div>
        </div>
        <div class="metric">
          <div class="label">Medical PMPM</div>
          <div class="value">{_fmt(card.get('medical_pmpm'), '$', '', 0)}</div>
        </div>
        <div class="metric">
          <div class="label">Pharmacy PMPM</div>
          <div class="value">{_fmt(card.get('pharmacy_pmpm'), '$', '', 0)}</div>
        </div>
        <div class="metric">
          <div class="label">ER Visits/1000</div>
          <div class="value">{_fmt(card.get('er_visits_per_1000'), '', '', 1)}</div>
        </div>
        <div class="metric">
          <div class="label">IP Admits/1000</div>
          <div class="value">{_fmt(card.get('ip_admits_per_1000'), '', '', 1)}</div>
        </div>
      </div>
    </div>

    <!-- Renewal & Stop-Loss -->
    <div class="section">
      <div class="section-title">Renewal & Stop-Loss</div>
      <div class="metrics-grid" style="grid-template-columns: repeat(2, 1fr);">
        <div class="metric">
          <div class="label">Actual vs Expected</div>
          <div class="value {'red' if float(card.get('actual_to_expected') or 0) > 1 else 'green'}">{_fmt(card.get('actual_to_expected'), '', '', 4)}</div>
        </div>
        <div class="metric">
          <div class="label">Trend Factor</div>
          <div class="value">{_fmt(card.get('trend_factor'), '', '', 3)}</div>
        </div>
        <div class="metric">
          <div class="label">High-Cost Claimants</div>
          <div class="value">{_fmt(card.get('high_cost_claimants'))}</div>
        </div>
        <div class="metric">
          <div class="label">Specific SL Excess</div>
          <div class="value">{_fmt(card.get('specific_sl_excess'), '$', '', 0)}</div>
        </div>
        <div class="metric">
          <div class="label">Avg TCI</div>
          <div class="value">{_fmt(card.get('avg_tci'), '', '', 3)}</div>
        </div>
        <div class="metric">
          <div class="label">Pct High Cost</div>
          <div class="value">{_fmt(card.get('pct_high_cost'), '', '%', 1)}</div>
        </div>
      </div>
    </div>
  </div>

  <!-- Peer Benchmarks -->
  <div class="section">
    <div class="section-title">Peer Benchmarks (vs. {card.get('industry', 'N/A')} / {card.get('group_size_tier', 'N/A')})</div>
    <div class="metrics-grid" style="grid-template-columns: repeat(4, 1fr);">
      {"".join(f'''<div class="gauge">
        <div class="gauge-label">
          <span>{lbl}</span>
          <span style="font-weight:600; color: {'#16a34a' if (100 - round(float(card.get(key) or 0.5) * 100)) >= 70 else '#ca8a04' if (100 - round(float(card.get(key) or 0.5) * 100)) >= 40 else '#dc2626'};">{round(float(card.get(key) or 0.5) * 100)}th</span>
        </div>
        <div class="gauge-bar">
          <div class="gauge-fill" style="width:{round(float(card.get(key) or 0.5) * 100)}%; background: {'#16a34a' if (100 - round(float(card.get(key) or 0.5) * 100)) >= 70 else '#ca8a04' if (100 - round(float(card.get(key) or 0.5) * 100)) >= 40 else '#dc2626'};"></div>
        </div>
      </div>''' for lbl, key in [
        ("Claims PMPM", "claims_pmpm_pctl"),
        ("Loss Ratio", "loss_ratio_pctl"),
        ("ER Visits/1000", "er_visits_pctl"),
        ("Total Cost Index", "tci_pctl"),
      ])}
    </div>
    <div style="font-size:9px; color:#999; margin-top:4px;">Lower percentile = better performance relative to peers</div>
  </div>

  <div class="footer">
    <span>Generated by Red Bricks Insurance Group Reporting Portal</span>
    <span>Confidential - For Internal Use Only</span>
  </div>
</div>
</body>
</html>"""


@api.get("/groups/{group_id}/report-card-pdf", operation_id="getReportCardPdf")
async def get_report_card_pdf(group_id: str):
    """Generate a branded HTML report card for download/print-to-PDF."""
    card = await asyncio.to_thread(get_report_card, group_id)
    if not card:
        raise HTTPException(status_code=404, detail=f"Group {group_id} not found")

    html = _build_report_card_html(card)
    group_name = (card.get("group_name") or group_id).replace(" ", "_")
    return HTMLResponse(
        content=html,
        headers={
            "Content-Disposition": f'inline; filename="ReportCard_{group_name}.html"',
        },
    )


# ===================================================================
# Renewal Scenario Modeling
# ===================================================================

def _compute_renewal_scenario(card: dict, rate_change_pct: float) -> dict:
    """Compute projected metrics for a renewal rate change scenario."""
    current_pmpm = float(card.get("claims_pmpm") or 0)
    loss_ratio = float(card.get("loss_ratio") or 0.85)
    health_score = int(float(card.get("group_health_score") or 50))
    # Estimate tenure from group size tier (synthetic heuristic)
    size_tier = card.get("group_size_tier") or "Small"
    tenure_map = {"Small (1-50)": 3, "Mid-Market (51-250)": 5, "Medium (51-250)": 5, "Large (251-999)": 8, "Jumbo (1000+)": 12}
    tenure = tenure_map.get(size_tier, 5)

    # Project new PMPM: premium increases by rate_change_pct
    premium_multiplier = 1 + (rate_change_pct / 100.0)
    # Claims are projected using trend factor (unchanged by rate)
    trend_factor = float(card.get("trend_factor") or 1.06)
    projected_claims_pmpm = current_pmpm * trend_factor

    # Current premium can be inferred: premium = claims / loss_ratio
    current_premium_pmpm = current_pmpm / loss_ratio if loss_ratio > 0 else current_pmpm
    new_premium_pmpm = current_premium_pmpm * premium_multiplier
    projected_loss_ratio = projected_claims_pmpm / new_premium_pmpm if new_premium_pmpm > 0 else 1.0

    # Churn model: logistic function
    # Higher rate increases -> higher churn
    # Better health score -> lower churn (modifier ranges from -0.5 to +0.5)
    health_modifier = (50 - health_score) / 100.0  # positive when unhealthy
    # Longer tenure -> slightly lower churn
    tenure_modifier = -0.1 * min(tenure / 10.0, 1.0)
    logit = (rate_change_pct / 10.0) - 0.5 + health_modifier + tenure_modifier
    churn_prob = 1.0 / (1.0 + math.exp(-logit))

    return {
        "current_pmpm": round(current_pmpm, 2),
        "projected_pmpm": round(new_premium_pmpm, 2),
        "rate_change_pct": rate_change_pct,
        "current_loss_ratio": round(loss_ratio, 4),
        "projected_loss_ratio": round(projected_loss_ratio, 4),
        "churn_probability": round(churn_prob, 4),
        "health_score": health_score,
        "group_tenure_years": tenure,
    }


@api.post(
    "/groups/{group_id}/renewal-scenario",
    response_model=RenewalScenarioOut,
    operation_id="renewalScenario",
)
async def renewal_scenario_endpoint(group_id: str, scenario: RenewalScenarioIn):
    """Model a renewal rate change scenario."""
    card = await asyncio.to_thread(get_report_card, group_id)
    if not card:
        raise HTTPException(status_code=404, detail=f"Group {group_id} not found")
    result = _compute_renewal_scenario(card, scenario.rate_change_pct)
    return RenewalScenarioOut(**result)


# ===================================================================
# Competitive Benchmarking
# ===================================================================

_COMPETITOR_CARRIERS = [
    {
        "name": "Anthem BlueCross",
        "network": "Broad PPO",
        "wellness": ["Wellness Rewards", "Telehealth", "Mental Health EAP"],
        "satisfaction_base": 3.8,
    },
    {
        "name": "UnitedHealthcare",
        "network": "National PPO",
        "wellness": ["Rally Digital Health", "Real Appeal", "Fitness Discounts"],
        "satisfaction_base": 3.6,
    },
    {
        "name": "Aetna",
        "network": "Open Choice PPO",
        "wellness": ["Attain Wellness", "Mindfulness Programs", "Chronic Care"],
        "satisfaction_base": 3.7,
    },
    {
        "name": "Cigna",
        "network": "OAP Network",
        "wellness": ["Cigna Wellbeing", "Coaches", "Diabetes Prevention"],
        "satisfaction_base": 3.5,
    },
]

_NETWORK_SIZES = {
    "Small": {"Broad PPO": "12,000+", "National PPO": "18,000+", "Open Choice PPO": "14,000+", "OAP Network": "11,000+"},
    "Mid-Market": {"Broad PPO": "15,000+", "National PPO": "22,000+", "Open Choice PPO": "17,000+", "OAP Network": "14,000+"},
    "Large": {"Broad PPO": "20,000+", "National PPO": "30,000+", "Open Choice PPO": "22,000+", "OAP Network": "18,000+"},
    "Jumbo": {"Broad PPO": "25,000+", "National PPO": "35,000+", "Open Choice PPO": "28,000+", "OAP Network": "22,000+"},
}


def _generate_competitive_benchmark(card: dict) -> dict:
    """Generate deterministic synthetic competitor data for a group."""
    group_id = card.get("group_id", "")
    group_name = card.get("group_name", "Unknown")
    rb_pmpm = float(card.get("claims_pmpm") or 500)
    industry = card.get("industry") or "General"
    size_tier = card.get("group_size_tier") or "Mid-Market"

    competitors = []
    for carrier in _COMPETITOR_CARRIERS:
        # Deterministic seed from group_id + carrier name
        seed_str = f"{group_id}:{carrier['name']}"
        h = int(hashlib.sha256(seed_str.encode()).hexdigest(), 16)

        # PMPM varies +/- 5-15% from Red Bricks
        variation = ((h % 2100) - 1050) / 10000.0  # -0.105 to +0.105
        competitor_pmpm = round(rb_pmpm * (1 + variation), 2)

        # Network size based on size tier
        network = _NETWORK_SIZES.get(size_tier, _NETWORK_SIZES["Mid-Market"]).get(
            carrier["network"], "15,000+"
        )

        # Satisfaction: base +/- 0.3 deterministic
        sat_var = ((h >> 8) % 60 - 30) / 100.0
        satisfaction = round(max(2.5, min(5.0, carrier["satisfaction_base"] + sat_var)), 1)

        competitors.append(CompetitorBenchmark(
            carrier_name=carrier["name"],
            pmpm=competitor_pmpm,
            network_size=f"{network} providers",
            wellness_programs=carrier["wellness"],
            member_satisfaction=satisfaction,
        ))

    return {
        "group_id": group_id,
        "group_name": group_name,
        "red_bricks_pmpm": rb_pmpm,
        "sic_code": industry,
        "size_tier": size_tier,
        "competitors": competitors,
    }


@api.get(
    "/groups/{group_id}/competitive-benchmark",
    response_model=CompetitiveBenchmarkResponse,
    operation_id="getCompetitiveBenchmark",
)
async def get_competitive_benchmark_endpoint(group_id: str):
    """Get synthetic competitive benchmark data for a group."""
    card = await asyncio.to_thread(get_report_card, group_id)
    if not card:
        raise HTTPException(status_code=404, detail=f"Group {group_id} not found")
    result = _generate_competitive_benchmark(card)
    return CompetitiveBenchmarkResponse(**result)


# ===================================================================
# Sales Coach Agent
# ===================================================================

@api.post("/agent/chat", response_model=AgentChatOut, operation_id="chatWithAgent")
async def chat_with_agent(chat_in: AgentChatIn):
    """Query the Sales Coach agent about a group."""
    result = await asyncio.to_thread(query_sales_coach, chat_in.group_id, chat_in.question)
    return AgentChatOut(**result)


@api.post("/agent/chat/stream", operation_id="chatWithAgentStream")
async def chat_with_agent_stream(chat_in: AgentChatIn):
    """SSE variant of /agent/chat — streams progress milestones then the briefing."""
    group_id, question = chat_in.group_id, chat_in.question

    async def event_source():
        loop = asyncio.get_running_loop()
        queue: asyncio.Queue = asyncio.Queue()
        _SENTINEL = object()

        def _produce():
            try:
                for event_type, payload in stream_sales_coach(group_id, question):
                    loop.call_soon_threadsafe(queue.put_nowait, (event_type, payload))
            except Exception as e:  # pragma: no cover
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
async def observability_traces():
    spans_table = f"`{UC_CATALOG}`.`{UC_TRACE_SCHEMA}`.`{UC_TRACE_TABLE_PREFIX}_otel_spans`"
    sql = f"""
        SELECT trace_id, MIN(start_time_unix_nano) AS s, MAX(end_time_unix_nano) AS e,
               COUNT(*) AS span_count,
               CASE WHEN SUM(CASE WHEN status.code='STATUS_CODE_ERROR' THEN 1 ELSE 0 END)>0
                    THEN 'ERROR' ELSE 'OK' END AS trace_status
        FROM {spans_table} GROUP BY trace_id ORDER BY s DESC LIMIT 25
    """
    try:
        rows = await asyncio.to_thread(_execute_sql, sql)
        return {"traces": [{
            "request_id": d.get("trace_id", ""),
            "timestamp_ms": int(d.get("s") or 0) // 1_000_000,
            "execution_time_ms": (int(d.get("e") or 0) - int(d.get("s") or 0)) // 1_000_000,
            "status": d.get("trace_status", "UNKNOWN"),
            "span_count": int(d.get("span_count") or 0),
        } for d in rows]}
    except Exception as e:
        print(f"[observability] trace fetch error: {e}")
        return {"traces": [], "error": str(e)}


@api.get("/observability/costs", operation_id="getObservabilityCosts")
async def observability_costs():
    endpoints = ", ".join(f"'{m}'" for m in OBSERVED_MODELS)
    try:
        try:
            wid = WorkspaceClient().get_workspace_id()
            wf = f"AND eu.workspace_id = '{wid}'" if wid else ""
        except Exception:
            wf = ""
        rows = await asyncio.to_thread(_execute_sql, f"""
            SELECT se.endpoint_name AS endpoint, COUNT(*) AS request_count,
                   COALESCE(SUM(eu.input_token_count),0) AS total_input_tokens,
                   COALESCE(SUM(eu.output_token_count),0) AS total_output_tokens,
                   CASE se.endpoint_name
                     WHEN 'databricks-llama-4-maverick'
                       THEN ROUND(SUM(eu.input_token_count)*0.40/1000000 + SUM(eu.output_token_count)*1.60/1000000, 4)
                     ELSE 0 END AS estimated_cost_usd
            FROM system.serving.endpoint_usage eu
            JOIN system.serving.served_entities se ON eu.served_entity_id = se.served_entity_id
            WHERE se.endpoint_name IN ({endpoints})
              AND eu.request_time >= DATE_SUB(CURRENT_TIMESTAMP(), 30) {wf}
            GROUP BY se.endpoint_name ORDER BY request_count DESC
        """)
        return {"costs": rows}
    except Exception as e:
        print(f"[observability] cost query error: {e}")
        return {"costs": [], "error": str(e)}


# ===================================================================
# Genie
# ===================================================================

@api.post("/genie/ask", response_model=GenieResponseOut, operation_id="askGenie")
async def ask_genie_endpoint(question_in: GenieQuestionIn):
    """Ask Genie a natural language question about group data."""
    return await asyncio.to_thread(ask_genie, question_in)
