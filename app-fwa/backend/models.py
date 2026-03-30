"""Pydantic models for the FWA Investigation Portal API."""

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class FraudSeverity(str, Enum):
    CRITICAL = "Critical"
    HIGH = "High"
    MEDIUM = "Medium"
    LOW = "Low"


class InvestigationStatus(str, Enum):
    OPEN = "Open"
    UNDER_REVIEW = "Under Review"
    EVIDENCE_GATHERING = "Evidence Gathering"
    REFERRED_TO_SIU = "Referred to SIU"
    RECOVERY_IN_PROGRESS = "Recovery In Progress"
    CLOSED_CONFIRMED = "Closed — Confirmed Fraud"
    CLOSED_NO_FRAUD = "Closed — No Fraud"
    CLOSED_INSUFFICIENT = "Closed — Insufficient Evidence"


class InvestigationType(str, Enum):
    PROVIDER = "Provider"
    MEMBER = "Member"
    NETWORK = "Network"


# ---------------------------------------------------------------------------
# Investigator models
# ---------------------------------------------------------------------------

class InvestigatorOut(BaseModel):
    investigator_id: str
    email: str
    display_name: str
    role: str
    department: Optional[str] = None
    max_caseload: int = 30
    is_active: bool = True


class InvestigatorCaseload(BaseModel):
    investigator_id: str
    display_name: str
    role: str
    max_caseload: int
    active_cases: int
    critical_cases: int
    evidence_gathering: int
    recovery_in_progress: int
    total_active_overpayment: float
    total_recovered: float
    available_capacity: int


# ---------------------------------------------------------------------------
# Investigation models
# ---------------------------------------------------------------------------

class InvestigationListOut(BaseModel):
    investigation_id: str
    investigation_type: Optional[str] = None
    target_type: Optional[str] = None
    target_id: Optional[str] = None
    target_name: Optional[str] = None
    fraud_types: Optional[list[str]] = []
    severity: Optional[str] = None
    status: Optional[str] = None
    source: Optional[str] = None
    estimated_overpayment: Optional[float] = None
    claims_involved_count: Optional[int] = None
    composite_risk_score: Optional[float] = None
    rules_risk_score: Optional[float] = None
    ml_risk_score: Optional[float] = None
    investigator_name: Optional[str] = None
    investigator_role: Optional[str] = None
    assigned_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    time_open: Optional[str] = None


class InvestigationDetailOut(BaseModel):
    investigation_id: str
    investigation_type: Optional[str] = None
    target_type: Optional[str] = None
    target_id: Optional[str] = None
    target_name: Optional[str] = None
    fraud_types: Optional[list[str]] = []
    severity: Optional[str] = None
    status: Optional[str] = None
    source: Optional[str] = None
    assigned_investigator_id: Optional[str] = None
    investigator_name: Optional[str] = None
    investigator_role: Optional[str] = None
    assigned_at: Optional[datetime] = None
    estimated_overpayment: Optional[float] = None
    confirmed_overpayment: Optional[float] = None
    recovered_amount: Optional[float] = None
    claims_involved_count: Optional[int] = None
    investigation_summary: Optional[str] = None
    evidence_summary: Optional[str] = None
    recommendation: Optional[str] = None
    rules_risk_score: Optional[float] = None
    ml_risk_score: Optional[float] = None
    composite_risk_score: Optional[float] = None
    audit_log: list["AuditLogOut"] = []
    evidence: list["EvidenceOut"] = []
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    closed_at: Optional[datetime] = None


class AssignInvestigatorIn(BaseModel):
    investigator_id: str


class UpdateStatusIn(BaseModel):
    status: InvestigationStatus
    note: Optional[str] = None


class AddNoteIn(BaseModel):
    note: str


class RecordRecoveryIn(BaseModel):
    recovered_amount: float
    note: Optional[str] = None


# ---------------------------------------------------------------------------
# Audit log
# ---------------------------------------------------------------------------

class AuditLogOut(BaseModel):
    audit_id: str
    investigation_id: str
    investigator_name: Optional[str] = None
    action_type: str
    previous_status: Optional[str] = None
    new_status: Optional[str] = None
    note: Optional[str] = None
    created_at: datetime


# ---------------------------------------------------------------------------
# Evidence
# ---------------------------------------------------------------------------

class EvidenceOut(BaseModel):
    evidence_id: str
    investigation_id: str
    evidence_type: str
    reference_id: Optional[str] = None
    description: str
    added_by_name: Optional[str] = None
    created_at: datetime


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------

class DashboardStats(BaseModel):
    total_investigations: int
    open_count: int
    critical_count: int
    high_count: int
    total_estimated_overpayment: float
    total_recovered: float
    recovery_rate: Optional[float] = None
    closed_this_month: int
    investigations_by_status: dict[str, int] = {}
    investigations_by_type: dict[str, int] = {}
    investigations_by_severity: dict[str, int] = {}


# ---------------------------------------------------------------------------
# Provider risk
# ---------------------------------------------------------------------------

class ProviderRiskOut(BaseModel):
    provider_npi: str
    provider_name: Optional[str] = None
    specialty: Optional[str] = None
    total_claims: Optional[str] = None
    total_billed: Optional[str] = None
    total_paid: Optional[str] = None
    billed_to_allowed_ratio: Optional[str] = None
    e5_visit_pct: Optional[str] = None
    denial_rate: Optional[str] = None
    fwa_signal_count: Optional[str] = None
    fwa_avg_score: Optional[str] = None
    fwa_estimated_overpayment: Optional[str] = None
    composite_risk_score: Optional[str] = None
    risk_tier: Optional[str] = None
    specialty_risk_rank: Optional[str] = None
    overall_risk_rank: Optional[str] = None


# ---------------------------------------------------------------------------
# Agent / Genie
# ---------------------------------------------------------------------------

class AgentQueryIn(BaseModel):
    question: str
    target_id: Optional[str] = None
    target_type: Optional[str] = None


class AgentQueryOut(BaseModel):
    answer: str
    sources: list[dict] = []


class GenieQuestionIn(BaseModel):
    question: str
    conversation_id: Optional[str] = None


class GenieResponseOut(BaseModel):
    conversation_id: str
    message_id: str
    sql_query: Optional[str] = None
    columns: list[str] = []
    rows: list[dict] = []
    row_count: int = 0
    description: Optional[str] = None
