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
    criteria_source: Optional[str] = None
    criteria_version: Optional[str] = None
    criteria_effective_date: Optional[str] = None
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
# Compliance
# ---------------------------------------------------------------------------

class TurnaroundBucket(BaseModel):
    bucket: str
    count: int
    compliant: bool


class WeeklyTrend(BaseModel):
    week: str
    compliance_rate: float
    total: int


class ComplianceMetricsOut(BaseModel):
    compliance_rate: Optional[float] = None
    avg_turnaround_standard: Optional[float] = None
    avg_turnaround_expedited: Optional[float] = None
    overdue_count: int = 0
    auto_adjudication_rate: Optional[float] = None
    total_determined: int = 0
    total_auto: int = 0
    turnaround_distribution: list[TurnaroundBucket] = []
    weekly_trend: list[WeeklyTrend] = []


class OverdueRequestOut(BaseModel):
    auth_request_id: str
    member_name: Optional[str] = None
    service_type: str
    procedure_code: str
    urgency: Optional[str] = None
    reviewer_name: Optional[str] = None
    cms_deadline: Optional[datetime] = None
    hours_overdue: float
    request_date: Optional[datetime] = None


# ---------------------------------------------------------------------------
# Agent
# ---------------------------------------------------------------------------

class AgentQueryIn(BaseModel):
    question: str
    auth_request_id: Optional[str] = None


class AgentQueryOut(BaseModel):
    answer: str
    sources: list[dict] = []


# ---------------------------------------------------------------------------
# Appeals & Reconsiderations
# ---------------------------------------------------------------------------

class AppealType(str, Enum):
    STANDARD = "standard"
    EXPEDITED = "expedited"
    PROVIDER = "provider"
    MEMBER = "member"
    ADMINISTRATIVE = "administrative"
    CLINICAL = "clinical"


class AppealStatus(str, Enum):
    RECEIVED = "Received"
    IN_REVIEW = "In Review"
    ADDITIONAL_INFO = "Additional Info Requested"
    PEER_REVIEW = "Peer Review Requested"
    HEARING_SCHEDULED = "Hearing Scheduled"
    IRO_REFERRED = "IRO Referred"
    OVERTURNED = "Overturned"
    PARTIALLY_OVERTURNED = "Partially Overturned"
    UPHELD = "Upheld"


class FileAppealIn(BaseModel):
    auth_request_id: str
    appeal_type: AppealType = AppealType.STANDARD
    urgency: PAUrgency = PAUrgency.STANDARD
    filed_by: Optional[str] = None
    filed_role: Optional[str] = None
    filing_reason: Optional[str] = None


class AssignAppealIn(BaseModel):
    reviewer_id: str


class AppealDeterminationIn(BaseModel):
    status: AppealStatus  # Overturned / Partially Overturned / Upheld
    determination_reason: Optional[str] = None
    determination_reason_external: Optional[str] = None
    reviewer_notes_internal: Optional[str] = None


class AppealListOut(BaseModel):
    appeal_id: str
    auth_request_id: str
    member_name: Optional[str] = None
    service_type: Optional[str] = None
    procedure_code: Optional[str] = None
    procedure_description: Optional[str] = None
    line_of_business: Optional[str] = None
    original_denial_reason_code: Optional[str] = None
    original_determination_reason: Optional[str] = None
    original_status: Optional[str] = None
    appeal_type: Optional[str] = None
    urgency: Optional[str] = None
    filed_by: Optional[str] = None
    filed_date: Optional[datetime] = None
    status: Optional[str] = None
    determination: Optional[str] = None
    original_reviewer_name: Optional[str] = None
    appeal_reviewer_name: Optional[str] = None
    appeal_reviewer_role: Optional[str] = None
    assigned_at: Optional[datetime] = None
    cms_deadline: Optional[datetime] = None
    cms_compliant: Optional[bool] = None
    determination_date: Optional[datetime] = None
    turnaround_hours: Optional[float] = None
    hours_until_deadline: Optional[float] = None


# ---------------------------------------------------------------------------
# Correspondence / Decision Notices
# ---------------------------------------------------------------------------

class NoticeType(str, Enum):
    APPROVAL = "approval"
    DENIAL = "denial"
    PARTIAL_APPROVAL = "partial_approval"
    ADDITIONAL_INFO_REQUEST = "additional_info_request"
    APPEAL_ACKNOWLEDGEMENT = "appeal_acknowledgement"
    APPEAL_DETERMINATION = "appeal_determination"


class GenerateNoticeIn(BaseModel):
    notice_type: NoticeType
    recipient: Optional[str] = None
    recipient_role: Optional[str] = None
    delivery_channel: str = "portal"


class CorrespondenceOut(BaseModel):
    notice_id: str
    auth_request_id: Optional[str] = None
    notice_type: str
    recipient: Optional[str] = None
    recipient_role: Optional[str] = None
    subject: Optional[str] = None
    body_markdown: Optional[str] = None
    body_redacted: Optional[bool] = None
    redaction_notes: Optional[str] = None
    includes_appeal_rights: Optional[bool] = None
    criteria_citation: Optional[str] = None
    template_version: Optional[str] = None
    pdf_path: Optional[str] = None
    delivery_channel: Optional[str] = None
    delivery_status: Optional[str] = None
    generated_by: Optional[str] = None
    generated_at: Optional[datetime] = None
    released_at: Optional[datetime] = None


# ---------------------------------------------------------------------------
# Peer / Physician Review
# ---------------------------------------------------------------------------

class RequestPeerReviewIn(BaseModel):
    peer_reviewer_id: Optional[str] = None   # if omitted, matched by specialty
    requested_specialty: Optional[str] = None
    reason: Optional[str] = None
    p2p_requested: bool = False


class PeerReviewDeterminationIn(BaseModel):
    determination: str                        # uphold / overturn recommendation
    determination_notes: Optional[str] = None
    p2p_summary: Optional[str] = None


class PeerReviewOut(BaseModel):
    peer_review_id: str
    auth_request_id: str
    requested_by_name: Optional[str] = None
    peer_reviewer_name: Optional[str] = None
    peer_reviewer_role: Optional[str] = None
    requested_specialty: Optional[str] = None
    reason: Optional[str] = None
    status: Optional[str] = None
    p2p_requested: Optional[bool] = None
    p2p_scheduled_at: Optional[datetime] = None
    p2p_completed_at: Optional[datetime] = None
    p2p_summary: Optional[str] = None
    determination: Optional[str] = None
    determination_notes: Optional[str] = None
    notified_at: Optional[datetime] = None
    created_at: Optional[datetime] = None


# ---------------------------------------------------------------------------
# Business Rules Engine
# ---------------------------------------------------------------------------

class RuleAction(str, Enum):
    AUTO_APPROVE = "auto_approve"
    AUTO_DENY = "auto_deny"
    PEND = "pend"
    ROUTE = "route"


class BusinessRuleIn(BaseModel):
    name: str
    description: Optional[str] = None
    category: Optional[str] = None
    line_of_business: Optional[str] = None
    service_type: Optional[str] = None
    conditions_json: dict = {}
    action: RuleAction
    action_detail: Optional[str] = None
    priority: int = 100
    change_reason: Optional[str] = None


class BusinessRuleOut(BaseModel):
    rule_id: str
    name: str
    description: Optional[str] = None
    category: Optional[str] = None
    line_of_business: Optional[str] = None
    service_type: Optional[str] = None
    conditions_json: dict = {}
    action: str
    action_detail: Optional[str] = None
    priority: int = 100
    effective_start_date: Optional[str] = None
    effective_end_date: Optional[str] = None
    version: int = 1
    status: str = "draft"
    created_by: Optional[str] = None
    approved_by: Optional[str] = None
    approved_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class RuleSimulationOut(BaseModel):
    total_evaluated: int
    matched: int
    match_rate_pct: float
    action: Optional[str] = None
    would_agree: int = 0
    would_disagree: int = 0
    agreement_rate_pct: Optional[float] = None
    sample_matches: list[str] = []
