"""Pydantic models for the PA Review Portal API."""

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class PAReviewStatus(str, Enum):
    PENDING = "Pending Review"
    IN_REVIEW = "In Review"
    ADDITIONAL_INFO = "Additional Info Requested"
    APPROVED = "Approved"
    DENIED = "Denied"
    PARTIALLY_APPROVED = "Partially Approved"
    PEER_REVIEW = "Peer Review Requested"
    APPEALED = "Appealed"
    APPEAL_OVERTURNED = "Appeal Overturned"
    APPEAL_UPHELD = "Appeal Upheld"


class PAUrgency(str, Enum):
    EXPEDITED = "expedited"
    STANDARD = "standard"
    RETROSPECTIVE = "retrospective"


# ---------------------------------------------------------------------------
# Reviewer models
# ---------------------------------------------------------------------------

class ReviewerOut(BaseModel):
    reviewer_id: str
    email: str
    display_name: str
    role: str
    department: Optional[str] = None
    specialty: Optional[str] = None
    max_caseload: int = 50
    is_active: bool = True


class ReviewerCaseload(BaseModel):
    reviewer_id: str
    display_name: str
    role: str
    specialty: Optional[str] = None
    max_caseload: int
    active_cases: int
    expedited_cases: int
    in_review: int
    awaiting_info: int
    available_capacity: int


# ---------------------------------------------------------------------------
# PA Request models
# ---------------------------------------------------------------------------

class PARequestListOut(BaseModel):
    auth_request_id: str
    member_id: str
    member_name: Optional[str] = None
    requesting_provider_npi: str
    provider_name: Optional[str] = None
    service_type: str
    procedure_code: str
    procedure_description: Optional[str] = None
    diagnosis_codes: Optional[str] = None
    policy_name: Optional[str] = None
    line_of_business: Optional[str] = None
    urgency: Optional[str] = None
    estimated_cost: Optional[float] = None
    status: Optional[str] = None
    determination_tier: Optional[str] = None
    ai_recommendation: Optional[str] = None
    ai_confidence: Optional[float] = None
    tier1_auto_eligible: Optional[bool] = None
    reviewer_name: Optional[str] = None
    reviewer_role: Optional[str] = None
    assigned_at: Optional[datetime] = None
    request_date: Optional[datetime] = None
    cms_deadline: Optional[datetime] = None
    cms_compliant: Optional[bool] = None
    time_open: Optional[str] = None
    hours_until_deadline: Optional[float] = None


class PARequestDetailOut(BaseModel):
    auth_request_id: str
    member_id: str
    member_name: Optional[str] = None
    requesting_provider_npi: str
    provider_name: Optional[str] = None
    service_type: str
    procedure_code: str
    procedure_description: Optional[str] = None
    diagnosis_codes: Optional[str] = None
    policy_id: Optional[str] = None
    policy_name: Optional[str] = None
    line_of_business: Optional[str] = None
    clinical_summary: Optional[str] = None
    urgency: Optional[str] = None
    estimated_cost: Optional[float] = None
    status: Optional[str] = None
    determination_tier: Optional[str] = None
    assigned_reviewer_id: Optional[str] = None
    reviewer_name: Optional[str] = None
    reviewer_role: Optional[str] = None
    assigned_at: Optional[datetime] = None
    ai_recommendation: Optional[str] = None
    ai_confidence: Optional[float] = None
    tier1_auto_eligible: Optional[bool] = None
    clinical_extraction: Optional[str] = None
    determination_reason: Optional[str] = None
    denial_reason_code: Optional[str] = None
    reviewer_notes: Optional[str] = None
    request_date: Optional[datetime] = None
    determination_date: Optional[datetime] = None
    turnaround_hours: Optional[float] = None
    cms_compliant: Optional[bool] = None
    cms_deadline: Optional[datetime] = None
    appeal_filed: Optional[bool] = None
    appeal_date: Optional[datetime] = None
    appeal_outcome: Optional[str] = None
    hours_until_deadline: Optional[float] = None
    audit_log: list["ActionLogOut"] = []
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class AssignReviewerIn(BaseModel):
    reviewer_id: str


class UpdateStatusIn(BaseModel):
    status: PAReviewStatus
    note: Optional[str] = None
    determination_reason: Optional[str] = None
    denial_reason_code: Optional[str] = None


class AddNoteIn(BaseModel):
    note: str


# ---------------------------------------------------------------------------
# Action log
# ---------------------------------------------------------------------------

class ActionLogOut(BaseModel):
    action_id: str
    auth_request_id: str
    reviewer_name: Optional[str] = None
    action_type: str
    previous_status: Optional[str] = None
    new_status: Optional[str] = None
    note: Optional[str] = None
    created_at: datetime


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------

class DashboardStats(BaseModel):
    total_requests: int
    pending_count: int
    in_review_count: int
    expedited_pending: int
    approved_count: int
    denied_count: int
    approval_rate: Optional[float] = None
    avg_turnaround_hours: Optional[float] = None
    cms_compliance_rate: Optional[float] = None
    overdue_count: int
    auto_adjudicated_count: int
    requests_by_status: dict[str, int] = {}
    requests_by_service_type: dict[str, int] = {}
    requests_by_urgency: dict[str, int] = {}


# ---------------------------------------------------------------------------
# Agent
# ---------------------------------------------------------------------------

class AgentQueryIn(BaseModel):
    question: str
    auth_request_id: Optional[str] = None


class AgentQueryOut(BaseModel):
    answer: str
    sources: list[dict] = []
