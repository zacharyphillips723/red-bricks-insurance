"""Pydantic models for the Group Reporting Portal API."""

from typing import Optional

from pydantic import BaseModel


# ---------------------------------------------------------------------------
# Group models
# ---------------------------------------------------------------------------

class GroupListItem(BaseModel):
    """Summary for group search/filter results."""
    group_id: str
    group_name: Optional[str] = None
    industry: Optional[str] = None
    group_size_tier: Optional[str] = None
    funding_type: Optional[str] = None
    state: Optional[str] = None
    total_members: Optional[str] = None
    active_members: Optional[str] = None
    claims_pmpm: Optional[str] = None
    loss_ratio: Optional[str] = None
    renewal_action: Optional[str] = None
    group_health_score: Optional[str] = None


class GroupReportCard(BaseModel):
    """Full group report card — all fields from gold_group_report_card."""
    group_id: str
    group_name: Optional[str] = None
    industry: Optional[str] = None
    group_size_tier: Optional[str] = None
    funding_type: Optional[str] = None
    state: Optional[str] = None
    total_members: Optional[str] = None
    active_members: Optional[str] = None
    total_member_months: Optional[str] = None
    total_premium_revenue: Optional[str] = None
    total_claims_paid: Optional[str] = None
    medical_claims_paid: Optional[str] = None
    pharmacy_claims_paid: Optional[str] = None
    claims_pmpm: Optional[str] = None
    medical_pmpm: Optional[str] = None
    pharmacy_pmpm: Optional[str] = None
    loss_ratio: Optional[str] = None
    ip_admits_per_1000: Optional[str] = None
    er_visits_per_1000: Optional[str] = None
    # Stop-loss
    high_cost_claimants: Optional[str] = None
    specific_sl_excess: Optional[str] = None
    aggregate_attachment_ratio: Optional[str] = None
    # Renewal
    actual_to_expected: Optional[str] = None
    credibility_factor: Optional[str] = None
    projected_renewal_pmpm: Optional[str] = None
    renewal_action: Optional[str] = None
    trend_factor: Optional[str] = None
    renewal_date: Optional[str] = None
    # TCOC
    avg_member_tcoc: Optional[str] = None
    avg_tci: Optional[str] = None
    pct_high_cost: Optional[str] = None
    high_cost_members: Optional[str] = None
    cost_tier_distribution: Optional[str] = None
    # Percentiles
    claims_pmpm_pctl: Optional[str] = None
    loss_ratio_pctl: Optional[str] = None
    er_visits_pctl: Optional[str] = None
    tci_pctl: Optional[str] = None
    group_health_score: Optional[str] = None


class GroupExperience(BaseModel):
    """Claims experience detail for a group."""
    group_id: str
    group_name: Optional[str] = None
    total_members: Optional[str] = None
    total_claims_paid: Optional[str] = None
    medical_claims_paid: Optional[str] = None
    pharmacy_claims_paid: Optional[str] = None
    specialty_rx_paid: Optional[str] = None
    medical_claim_count: Optional[str] = None
    pharmacy_claim_count: Optional[str] = None
    inpatient_claims: Optional[str] = None
    er_claims: Optional[str] = None
    claims_pmpm: Optional[str] = None
    medical_pmpm: Optional[str] = None
    pharmacy_pmpm: Optional[str] = None
    loss_ratio: Optional[str] = None
    ip_admits_per_1000: Optional[str] = None
    er_visits_per_1000: Optional[str] = None


class GroupStopLoss(BaseModel):
    """Stop-loss / reinsurance detail for a group."""
    group_id: str
    group_name: Optional[str] = None
    funding_type: Optional[str] = None
    claim_year: Optional[str] = None
    total_group_claims: Optional[str] = None
    high_cost_claimants: Optional[str] = None
    max_claimant_paid: Optional[str] = None
    specific_stop_loss_attachment: Optional[str] = None
    members_exceeding_specific_sl: Optional[str] = None
    specific_sl_excess_amount: Optional[str] = None
    aggregate_sl_threshold: Optional[str] = None
    aggregate_sl_excess_amount: Optional[str] = None
    aggregate_attachment_ratio: Optional[str] = None


class GroupRenewal(BaseModel):
    """Renewal pricing detail for a group."""
    group_id: str
    group_name: Optional[str] = None
    industry: Optional[str] = None
    group_size: Optional[str] = None
    enrolled_members: Optional[str] = None
    total_premium: Optional[str] = None
    total_claims_paid: Optional[str] = None
    loss_ratio: Optional[str] = None
    actual_claims_pmpm: Optional[str] = None
    expected_claims_pmpm: Optional[str] = None
    actual_to_expected_ratio: Optional[str] = None
    medical_trend_factor: Optional[str] = None
    credibility_factor: Optional[str] = None
    projected_renewal_pmpm: Optional[str] = None
    renewal_action: Optional[str] = None
    renewal_date: Optional[str] = None


class GroupTcocItem(BaseModel):
    """Member cost tier distribution for a group."""
    cost_tier: Optional[str] = None
    member_count: Optional[str] = None
    avg_tcoc: Optional[str] = None
    avg_tci: Optional[str] = None
    total_paid: Optional[str] = None


# ---------------------------------------------------------------------------
# Agent / Chat
# ---------------------------------------------------------------------------

class AgentChatIn(BaseModel):
    """Input for sales coach query."""
    group_id: str
    question: str


class AgentChatOut(BaseModel):
    """Response from the sales coach agent."""
    answer: str
    enrichment_sources: list[str] = []


# ---------------------------------------------------------------------------
# Genie
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Standard Reports
# ---------------------------------------------------------------------------

class HighCostMember(BaseModel):
    """High-cost member row."""
    member_id: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    age: Optional[str] = None
    gender: Optional[str] = None
    cost_tier: Optional[str] = None
    tci: Optional[str] = None
    raf_score: Optional[str] = None
    total_paid: Optional[str] = None
    medical_paid: Optional[str] = None
    pharmacy_paid: Optional[str] = None
    top_diagnoses: Optional[str] = None
    risk_tier: Optional[str] = None
    hedis_gap_count: Optional[str] = None
    hedis_gap_measures: Optional[str] = None
    hcc_count: Optional[str] = None
    member_months: Optional[str] = None


class ClaimsTrendMonth(BaseModel):
    """Monthly claims trend row."""
    month: Optional[str] = None
    medical_paid: Optional[str] = None
    pharmacy_paid: Optional[str] = None
    total_paid: Optional[str] = None
    medical_claims: Optional[str] = None
    pharmacy_claims: Optional[str] = None
    members: Optional[str] = None
    total_pmpm: Optional[str] = None
    medical_pmpm: Optional[str] = None
    pharmacy_pmpm: Optional[str] = None


class TopDrug(BaseModel):
    """Top drug row."""
    drug_name: Optional[str] = None
    therapeutic_class: Optional[str] = None
    is_specialty: Optional[str] = None
    fill_count: Optional[str] = None
    member_count: Optional[str] = None
    total_plan_paid: Optional[str] = None
    total_cost: Optional[str] = None
    total_member_copay: Optional[str] = None
    avg_cost_per_fill: Optional[str] = None


class UtilizationRow(BaseModel):
    """Utilization summary by claim type."""
    claim_type: Optional[str] = None
    claim_count: Optional[str] = None
    unique_members: Optional[str] = None
    total_paid: Optional[str] = None
    avg_paid_per_claim: Optional[str] = None
    total_billed: Optional[str] = None
    per_1000: Optional[str] = None
    top_diagnoses: Optional[str] = None


class RiskTierRow(BaseModel):
    """Risk tier distribution row."""
    risk_tier: Optional[str] = None
    member_count: Optional[str] = None
    avg_raf: Optional[str] = None


class CareGapSummary(BaseModel):
    """Care gap aggregate summary."""
    total_members: Optional[str] = None
    members_with_gaps: Optional[str] = None
    total_gaps: Optional[str] = None
    avg_raf_score: Optional[str] = None


class RisingRiskMember(BaseModel):
    """Rising risk member row."""
    member_id: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    age: Optional[str] = None
    tci: Optional[str] = None
    cost_tier: Optional[str] = None
    total_paid: Optional[str] = None
    top_diagnoses: Optional[str] = None
    hedis_gap_count: Optional[str] = None


class RiskCareGapsResponse(BaseModel):
    """Full risk & care gap report response."""
    cost_tiers: list[GroupTcocItem] = []
    risk_tiers: list[RiskTierRow] = []
    summary: CareGapSummary = CareGapSummary()
    rising_risk_members: list[RisingRiskMember] = []


# ---------------------------------------------------------------------------
# Genie
# ---------------------------------------------------------------------------

class GenieQuestionIn(BaseModel):
    """Input for asking Genie a question."""
    question: str
    conversation_id: Optional[str] = None


class GenieResponseOut(BaseModel):
    """Response from Genie."""
    conversation_id: str
    message_id: str
    sql_query: Optional[str] = None
    columns: list[str] = []
    rows: list[dict] = []
    row_count: int = 0
    description: Optional[str] = None
