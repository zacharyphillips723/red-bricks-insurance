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
