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
