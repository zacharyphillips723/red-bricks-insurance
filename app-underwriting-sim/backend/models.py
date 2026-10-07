"""Pydantic models for the Underwriting Simulation Portal."""

from datetime import datetime
from enum import Enum
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class SimulationType(str, Enum):
    PREMIUM_RATE = "premium_rate"
    BENEFIT_DESIGN = "benefit_design"
    GROUP_RENEWAL = "group_renewal"
    POPULATION_MIX = "population_mix"
    MEDICAL_TREND = "medical_trend"
    STOP_LOSS = "stop_loss"
    RISK_ADJUSTMENT = "risk_adjustment"
    UTILIZATION_CHANGE = "utilization_change"
    NEW_GROUP_QUOTE = "new_group_quote"
    IBNR_RESERVE = "ibnr_reserve"
    AGGREGATE_STOP_LOSS = "aggregate_stop_loss"


class SimulationStatus(str, Enum):
    DRAFT = "draft"
    COMPUTED = "computed"
    APPROVED = "approved"
    ARCHIVED = "archived"


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------

class SimulateIn(BaseModel):
    """Run a simulation."""
    simulation_type: SimulationType
    parameters: dict = Field(..., description="Scenario-specific input parameters")
    save: bool = Field(False, description="Save to Lakebase after computing")
    name: Optional[str] = Field(None, description="Simulation name (required if save=True)")


class ComparisonIn(BaseModel):
    """Create a comparison set."""
    comparison_name: str
    simulation_ids: list[UUID] = Field(..., min_length=2, max_length=4)
    notes: Optional[str] = None


class SimulationUpdateIn(BaseModel):
    """Update a simulation's status or notes."""
    status: Optional[SimulationStatus] = None
    notes: Optional[str] = None


class AgentChatIn(BaseModel):
    """Agent chat message."""
    message: str
    conversation_history: list[dict] = Field(default_factory=list)


class GenieQuestionIn(BaseModel):
    """Genie question."""
    question: str
    conversation_id: Optional[str] = None
    message_id: Optional[str] = None


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------

class SimulateOut(BaseModel):
    """Simulation results."""
    simulation_id: Optional[UUID] = None
    simulation_type: SimulationType
    baseline: dict[str, float]
    projected: dict[str, float]
    delta: dict[str, float]
    delta_pct: dict[str, float]
    narrative: str
    warnings: list[str] = Field(default_factory=list)


class SimulationListOut(BaseModel):
    """Simulation list item."""
    simulation_id: UUID
    simulation_name: str
    simulation_type: str
    status: str
    scope_lob: Optional[str] = None
    scope_group_id: Optional[str] = None
    narrative: Optional[str] = None
    created_by: str
    created_at: datetime
    updated_at: datetime


class SimulationDetailOut(BaseModel):
    """Full simulation detail."""
    simulation_id: UUID
    simulation_name: str
    simulation_type: str
    status: str
    parameters: dict
    results: Optional[dict] = None
    baseline_snapshot: Optional[dict] = None
    scope_lob: Optional[str] = None
    scope_group_id: Optional[str] = None
    notes: Optional[str] = None
    created_by: str
    created_at: datetime
    updated_at: datetime


class ComparisonOut(BaseModel):
    """Comparison set with simulation details."""
    comparison_id: UUID
    comparison_name: str
    simulation_ids: list[UUID]
    simulations: list[SimulationDetailOut] = Field(default_factory=list)
    notes: Optional[str] = None
    created_by: str
    created_at: datetime


class AuditLogEntry(BaseModel):
    """Audit trail entry."""
    audit_id: UUID
    simulation_id: UUID
    action: str
    actor: str
    details: Optional[dict] = None
    created_at: datetime


class BaselineSummaryOut(BaseModel):
    """Current book-level financials."""
    total_premium: float
    total_claims: float
    total_members: int
    total_member_months: int
    overall_mlr: float
    pmpm_by_lob: dict[str, float]
    mlr_by_lob: dict[str, float]
    member_count_by_lob: dict[str, int]


class AgentChatOut(BaseModel):
    """Agent response."""
    response: str
    simulation_results: Optional[list[SimulateOut]] = None


class GenieResponseOut(BaseModel):
    """Genie response."""
    conversation_id: Optional[str] = None
    message_id: Optional[str] = None
    sql_query: Optional[str] = None
    description: Optional[str] = None
    columns: list[str] = Field(default_factory=list)
    rows: list[list] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Actuarial Pricing — Rate Build-Up
# ---------------------------------------------------------------------------

class RateBuildupIn(BaseModel):
    """Input for community-rated actuarial pricing model."""
    group_id: Optional[str] = None
    avg_age_band: Optional[str] = Field(None, description="e.g. '26-35', '36-45'")
    county_type: Optional[str] = Field(None, description="urban, suburban, rural")
    sic_code: Optional[str] = Field(None, description="Industry SIC code or label")
    loss_ratio: Optional[float] = Field(None, description="Group's own loss ratio (0-2+)")
    credibility_factor: Optional[float] = Field(None, description="0.0-1.0 credibility weight")
    trend_pct: Optional[float] = Field(None, description="Annual medical trend %")
    lob: Optional[str] = Field("Commercial", description="Line of business")


class RateBuildupStep(BaseModel):
    """A single step in the rate build-up cascade."""
    step_name: str
    factor_label: str
    factor_value: float
    running_total: float
    description: str


class RateBuildupOut(BaseModel):
    """Full rate build-up result."""
    base_rate: float
    steps: list[RateBuildupStep]
    final_rate: float
    current_rate: Optional[float] = None
    rate_change: Optional[float] = None
    rate_change_pct: Optional[float] = None
    lob: str
    narrative: str


class FactorTable(BaseModel):
    """A reference factor table."""
    table_name: str
    description: str
    factors: list[dict]


class FactorTablesOut(BaseModel):
    """All actuarial factor tables."""
    age_factors: FactorTable
    area_factors: FactorTable
    industry_factors: FactorTable
    trend_factors: FactorTable
    experience_mod_ranges: FactorTable
    # 'uc_table' when factors were read from the governed UC table, else 'fallback'.
    source: Optional[str] = None


# ---------------------------------------------------------------------------
# Risk Pool Visualization
# ---------------------------------------------------------------------------

class DistributionBucket(BaseModel):
    """A histogram bucket for distribution comparisons."""
    label: str
    group_value: float
    book_value: float


class ConditionPrevalence(BaseModel):
    """Chronic condition prevalence comparison."""
    condition: str
    group_pct: float
    book_pct: float
    delta_pct: float


class CostDriver(BaseModel):
    """Top cost driver for a group."""
    category: str
    pmpm: float
    pct_of_total: float


class RiskPoolOut(BaseModel):
    """Risk pool analysis for a group vs book of business."""
    group_id: str
    group_member_count: int
    group_avg_raf: float
    book_avg_raf: float
    raf_distribution: list[DistributionBucket]
    age_distribution: list[DistributionBucket]
    condition_prevalence: list[ConditionPrevalence]
    top_cost_drivers: list[CostDriver]
    adverse_selection_flag: bool
    adverse_selection_severity: Optional[str] = None
    narrative: str


class BookOfBusinessSummaryOut(BaseModel):
    """Aggregate book-of-business risk summary."""
    total_members: int
    avg_raf: float
    avg_age: float
    raf_distribution: list[dict]
    age_distribution: list[dict]
    top_chronic_conditions: list[dict]


# ---------------------------------------------------------------------------
# Packaged Scenarios (Standard / Competitive / Retention / Custom)
# ---------------------------------------------------------------------------

class ScenarioPackageIn(BaseModel):
    """Input for generating packaged renewal pricing scenarios."""
    group_id: Optional[str] = Field(None, description="Group to renew; book averages used if no experience")
    lob: Optional[str] = Field(None, description="Line of business (book fallback)")
    target_mlr: float = Field(82.0, description="MLR target driving the Standard scenario")
    admin_load_pct: float = Field(12.0, description="Expense/admin load as % of premium")
    market_trend_pct: float = Field(7.0, description="Expected market renewal increase (retention curve center)")
    min_margin_pct: float = Field(3.0, description="Margin floor for the Retention scenario")
    competitor_pmpm: Optional[float] = Field(None, description="Competitor's quoted PMPM (drives Competitive)")
    competitor_within_pct: float = Field(2.0, description="Price within N% of the competitor")
    custom_rate_change_pct: Optional[float] = Field(None, description="Underwriter-defined rate change")


class ScenarioLeg(BaseModel):
    """One packaged pricing scenario."""
    name: str
    label: str
    rate_change_pct: float
    projected_premium: float
    projected_mlr: float
    margin_pct: float
    margin_dollars: float
    retention_probability: float
    expected_retained_margin: float
    rationale: str


class ScenarioPackageOut(BaseModel):
    """Packaged scenarios with an explainable recommendation."""
    group_id: Optional[str] = None
    lob: Optional[str] = None
    basis: str
    current_premium: float
    current_claims: float
    current_mlr: float
    member_count: int
    target_mlr: float
    admin_load_pct: float
    scenarios: list[ScenarioLeg]
    recommended_scenario: str
    recommendation_narrative: str


# ---------------------------------------------------------------------------
# Phase 2 — Funding arrangements
# ---------------------------------------------------------------------------

class FundingArrangementInfo(BaseModel):
    """Catalog entry describing a funding arrangement."""
    key: str
    label: str
    risk_bearer: str
    description: str


class FundingQuoteIn(BaseModel):
    """Price a quote under a specific funding arrangement."""
    arrangement: str = Field(..., description="One of the FUNDING_ARRANGEMENTS keys")
    group_name: Optional[str] = Field(None, description="Group/employer name for the saved quote")
    save: bool = Field(False, description="Persist the quote to app-state after pricing")
    parameters: dict = Field(
        default_factory=dict,
        description="Arrangement inputs (group_id, lob, member_count, and arrangement-specific knobs)",
    )


# ---------------------------------------------------------------------------
# Phase 2 — Approval routing
# ---------------------------------------------------------------------------

class AuthorityTier(BaseModel):
    """One tier of the approval authority matrix."""
    tier: int
    role: str
    max_dollar_impact: Optional[float] = None
    max_rate_change_pct: Optional[float] = None
    description: str


class ApprovalIn(BaseModel):
    """Create and route an approval request."""
    subject: str
    decision_type: str = Field("rate_action", description="e.g. rate_action, funding_quote")
    dollar_impact: float = 0.0
    rate_change_pct: float = 0.0
    group_id: Optional[str] = None
    lob: Optional[str] = None
    context: Optional[dict] = None


class ApprovalDecisionIn(BaseModel):
    """Record an approver's decision."""
    decision: str = Field(..., description="approved | rejected | needs_info")
    notes: Optional[str] = None


class ApprovalOut(BaseModel):
    """An approval request with its routing + decision state."""
    approval_id: str
    subject: str
    decision_type: str
    group_id: Optional[str] = None
    lob: Optional[str] = None
    dollar_impact: Optional[float] = None
    rate_change_pct: Optional[float] = None
    required_tier: Optional[int] = None
    required_role: Optional[str] = None
    status: str
    requested_by: str
    decided_by: Optional[str] = None
    decision_notes: Optional[str] = None
    context: Optional[dict] = None
    created_at: Optional[str] = None
    decided_at: Optional[str] = None


# ---------------------------------------------------------------------------
# Phase 2 — Factor governance
# ---------------------------------------------------------------------------

class FactorRow(BaseModel):
    """A single governed rating factor."""
    factor_type: str
    factor_key: str
    factor_value: float


class FactorVersionCreateIn(BaseModel):
    """Create a draft factor version (defaults to a snapshot of current factors)."""
    factors: Optional[list[FactorRow]] = None
    notes: Optional[str] = None
    source: str = "manual"


class FactorVersionOut(BaseModel):
    """A versioned factor set in the governance workflow."""
    version_id: str
    version: int
    status: str
    factors: list[FactorRow] = Field(default_factory=list)
    source: Optional[str] = None
    notes: Optional[str] = None
    created_by: str
    approved_by: Optional[str] = None
    published_by: Optional[str] = None
    created_at: Optional[str] = None
    approved_at: Optional[str] = None
    published_at: Optional[str] = None


# ---------------------------------------------------------------------------
# Phase 3 — intake, document intelligence, negotiation
# ---------------------------------------------------------------------------

class IntakeExtractIn(BaseModel):
    """Extract a structured submission from pasted document text."""
    text: str
    doc_type: str = Field("submission", description="e.g. census, sbc, competitor_quote, submission")


class IntakeParseIn(BaseModel):
    """Parse a free-text NL submission into a structured, completeness-gated memo."""
    text: str
    strategy_memo: bool = True


class RerateIn(BaseModel):
    """Re-rate a saved quote with a natural-language change."""
    instruction: str


# ---------------------------------------------------------------------------
# Phase 4 (optional) — Agentic memory / Underwriting Digital Twin
# ---------------------------------------------------------------------------

class PreflightIn(BaseModel):
    """Run the advisory pre-implementation check on a proposed policy/decision."""
    analyst_id: Optional[str] = None
    funding_arrangement: Optional[str] = None
    lob: Optional[str] = None
    group_id: Optional[str] = None
    group_size: Optional[int] = None
    industry: Optional[str] = None
    scenario_chosen: Optional[str] = None
    rationale: Optional[str] = None
    parameters: dict = Field(default_factory=dict)
    projected_margin: Optional[float] = None
    projected_mlr: Optional[float] = None
    dollar_impact: Optional[float] = None


class GuardrailIn(BaseModel):
    """Create a new actuary-owned guardrail version."""
    rule_type: str = Field(..., description="margin_floor | mlr_ceiling | authority_limit | prohibited")
    threshold: float
    severity: str = Field("warn", description="block | warn")
    scope: Optional[dict] = None


class OutcomeIn(BaseModel):
    """Record the observed outcome of a past decision (closes the loop)."""
    decision_id: str
    actual_mlr: Optional[float] = None
    actual_margin: Optional[float] = None
    retained: Optional[bool] = None
    note: Optional[str] = None
