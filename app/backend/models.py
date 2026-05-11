"""Pydantic models for the Population Health Command Center API."""

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class RiskTier(str, Enum):
    CRITICAL = "Critical"
    HIGH = "High"
    ELEVATED = "Elevated"
    MODERATE = "Moderate"
    LOW = "Low"


class CareCycleStatus(str, Enum):
    UNASSIGNED = "Unassigned"
    ASSIGNED = "Assigned"
    OUTREACH_ATTEMPTED = "Outreach Attempted"
    OUTREACH_SUCCESSFUL = "Outreach Successful"
    ASSESSMENT_IN_PROGRESS = "Assessment In Progress"
    INTERVENTION_ACTIVE = "Intervention Active"
    FOLLOW_UP_SCHEDULED = "Follow-Up Scheduled"
    RESOLVED = "Resolved"
    ESCALATED = "Escalated"
    CLOSED_UNABLE_TO_REACH = "Closed — Unable to Reach"


class AlertSource(str, Enum):
    HIGH_GLUCOSE_NO_INSULIN = "High Glucose No Insulin"
    ED_HIGH_UTILIZER = "ED High Utilizer"
    SDOH_RISK = "SDOH Risk"
    READMISSION_RISK = "Readmission Risk"
    MANUAL = "Manual"


class CareManagerRole(str, Enum):
    RN = "RN"
    LPN = "LPN"
    NP = "NP"
    PA = "PA"
    SW = "SW"
    CHW = "CHW"
    PHARMACIST = "Pharmacist"
    MD = "MD"


# ---------------------------------------------------------------------------
# Care Manager models
# ---------------------------------------------------------------------------

class CareManagerOut(BaseModel):
    care_manager_id: str
    email: str
    display_name: str
    role: CareManagerRole
    department: Optional[str] = None
    max_caseload: int = 50
    is_active: bool = True


class CareManagerCaseload(BaseModel):
    care_manager_id: str
    display_name: str
    role: CareManagerRole
    max_caseload: int
    active_cases: int
    critical_cases: int
    pending_outreach: int
    pending_followup: int
    available_capacity: int


# ---------------------------------------------------------------------------
# Alert models
# ---------------------------------------------------------------------------

class AlertListOut(BaseModel):
    """Summary for list/table views."""
    alert_id: str
    patient_id: str
    mrn: Optional[str] = None
    member_id: Optional[str] = None
    risk_tier: RiskTier
    risk_score: Optional[float] = None
    primary_driver: str
    alert_source: AlertSource
    status: CareCycleStatus
    payer: Optional[str] = None
    care_manager_name: Optional[str] = None
    care_manager_role: Optional[str] = None
    time_unassigned: Optional[str] = None
    created_at: datetime
    updated_at: datetime


class AlertDetailOut(BaseModel):
    """Full detail for single alert view."""
    alert_id: str
    patient_id: str
    mrn: Optional[str] = None
    member_id: Optional[str] = None
    risk_tier: RiskTier
    risk_score: Optional[float] = None
    primary_driver: str
    secondary_drivers: Optional[list[str]] = []
    alert_source: AlertSource
    assigned_care_manager_id: Optional[str] = None
    care_manager_name: Optional[str] = None
    care_manager_role: Optional[str] = None
    assigned_at: Optional[datetime] = None
    status: CareCycleStatus
    status_changed_at: Optional[datetime] = None
    max_hba1c: Optional[float] = None
    max_blood_glucose: Optional[float] = None
    peak_ed_visits_12mo: Optional[int] = None
    last_encounter_date: Optional[datetime] = None
    last_facility: Optional[str] = None
    payer: Optional[str] = None
    active_medications: Optional[list[str]] = []
    notes: Optional[str] = None
    activity_log: list["ActivityLogOut"] = []
    created_at: datetime
    updated_at: datetime
    resolved_at: Optional[datetime] = None


class AssignAlertIn(BaseModel):
    """Input for self-assigning an alert."""
    care_manager_id: str


class UpdateStatusIn(BaseModel):
    """Input for updating alert status."""
    status: CareCycleStatus
    note: Optional[str] = None


class AddNoteIn(BaseModel):
    """Input for adding a note to an alert."""
    note: str


# ---------------------------------------------------------------------------
# Activity log
# ---------------------------------------------------------------------------

class ActivityLogOut(BaseModel):
    activity_id: str
    alert_id: str
    care_manager_name: Optional[str] = None
    activity_type: str
    previous_status: Optional[CareCycleStatus] = None
    new_status: Optional[CareCycleStatus] = None
    note: Optional[str] = None
    created_at: datetime


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


# ---------------------------------------------------------------------------
# Dashboard stats
# ---------------------------------------------------------------------------

class DashboardStats(BaseModel):
    total_alerts: int
    unassigned_count: int
    critical_count: int
    high_count: int
    resolved_this_month: int
    avg_time_to_assign_hours: Optional[float] = None
    alerts_by_source: dict[str, int] = {}
    alerts_by_status: dict[str, int] = {}


# ---------------------------------------------------------------------------
# Member 360
# ---------------------------------------------------------------------------

class MemberListOut(BaseModel):
    """Summary for member search results."""
    member_id: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    member_name: Optional[str] = None
    date_of_birth: Optional[str] = None
    age: Optional[str] = None
    gender: Optional[str] = None
    line_of_business: Optional[str] = None
    risk_tier: Optional[str] = None
    raf_score: Optional[str] = None
    county: Optional[str] = None


class LabResultOut(BaseModel):
    """Individual lab result."""
    lab_result_id: Optional[str] = None
    member_id: Optional[str] = None
    lab_name: Optional[str] = None
    value: Optional[str] = None
    unit: Optional[str] = None
    reference_range_low: Optional[str] = None
    reference_range_high: Optional[str] = None
    collection_date: Optional[str] = None
    is_abnormal: Optional[str] = None


class Member360Out(BaseModel):
    """Full member 360 profile — all fields from gold_member_360 plus recent labs."""
    member_id: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    member_name: Optional[str] = None
    date_of_birth: Optional[str] = None
    age: Optional[str] = None
    gender: Optional[str] = None
    address_line_1: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[str] = None
    county: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    line_of_business: Optional[str] = None
    plan_type: Optional[str] = None
    plan_id: Optional[str] = None
    group_name: Optional[str] = None
    eligibility_start_date: Optional[str] = None
    eligibility_end_date: Optional[str] = None
    monthly_premium: Optional[str] = None
    medical_claim_count: Optional[str] = None
    medical_total_paid_ytd: Optional[str] = None
    medical_total_billed_ytd: Optional[str] = None
    medical_member_responsibility_ytd: Optional[str] = None
    top_diagnoses: Optional[str] = None
    pharmacy_claim_count: Optional[str] = None
    pharmacy_spend_ytd: Optional[str] = None
    pharmacy_member_copay_ytd: Optional[str] = None
    total_paid_ytd: Optional[str] = None
    raf_score: Optional[str] = None
    hcc_codes: Optional[str] = None
    hcc_count: Optional[str] = None
    is_high_risk: Optional[str] = None
    risk_tier: Optional[str] = None
    hedis_gap_count: Optional[str] = None
    hedis_gap_measures: Optional[str] = None
    last_encounter_date: Optional[str] = None
    last_encounter_type: Optional[str] = None
    pcp_npi: Optional[str] = None
    recent_labs: list[LabResultOut] = []


class CaseNoteOut(BaseModel):
    """Case note / document metadata."""
    document_id: str
    member_id: str
    document_type: Optional[str] = None
    title: Optional[str] = None
    created_date: Optional[str] = None
    author: Optional[str] = None
    full_text: Optional[str] = None
    text_length: Optional[str] = None


class AgentQueryIn(BaseModel):
    """Input for querying the Member RAG Agent."""
    question: str
    conversation_id: Optional[str] = None


class AgentQueryOut(BaseModel):
    """Response from the Member RAG Agent."""
    answer: str
    sources: list[dict] = []
    conversation_id: Optional[str] = None
    specialists_consulted: list[str] = []
    message_id: Optional[str] = None
    category: Optional[str] = None


# ---------------------------------------------------------------------------
# Conversations & Feedback
# ---------------------------------------------------------------------------

class ConversationListOut(BaseModel):
    """Summary for conversation list."""
    conversation_id: str
    member_id: str
    title: Optional[str] = None
    message_count: int = 0
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class ConversationMessageOut(BaseModel):
    """Single message in a conversation."""
    role: str
    content: str
    metadata: dict = {}
    created_at: Optional[str] = None


class FeedbackIn(BaseModel):
    """Input for submitting agent feedback."""
    message_id: str
    conversation_id: str
    rating: str = Field(..., pattern="^(positive|negative)$")
    comment: Optional[str] = None


class FeedbackOut(BaseModel):
    """Response after submitting feedback."""
    feedback_id: str
    status: str = "saved"


class MemberSdohOut(BaseModel):
    """SDOH screening profile for a member."""
    member_id: str
    screening_date: Optional[str] = None
    county: Optional[str] = None
    food_insecurity_flag: bool = False
    housing_instability_flag: bool = False
    transportation_barrier_flag: bool = False
    social_isolation_flag: bool = False
    financial_strain_flag: bool = False
    total_sdoh_flags: int = 0
    composite_sdoh_risk_score: Optional[float] = None


class NextBestActionOut(BaseModel):
    """AI-generated next best action recommendation."""
    actions: list[dict] = Field(default_factory=list)
    rationale: str = ""


# ---------------------------------------------------------------------------
# Outreach Drafting
# ---------------------------------------------------------------------------

class OutreachDraftIn(BaseModel):
    """Request to generate an outreach draft."""
    channel: str = "phone"  # phone, email, sms
    context: Optional[str] = None  # optional additional context


class OutreachDraftOut(BaseModel):
    """AI-generated outreach script."""
    channel: str
    subject: Optional[str] = None  # for email
    script: str
    key_talking_points: list[str] = Field(default_factory=list)
    member_name: str = ""
    generated_at: str = ""


# ---------------------------------------------------------------------------
# Cohort Builder
# ---------------------------------------------------------------------------

class CohortSearchIn(BaseModel):
    """Cohort search criteria."""
    risk_tiers: list[str] = Field(default_factory=list)
    counties: list[str] = Field(default_factory=list)
    lines_of_business: list[str] = Field(default_factory=list)
    min_age: Optional[int] = None
    max_age: Optional[int] = None
    gender: Optional[str] = None
    min_raf_score: Optional[float] = None
    has_hedis_gaps: Optional[bool] = None
    diagnoses_contain: Optional[str] = None
    min_ed_visits: Optional[int] = None
    limit: int = 100


class CohortMemberOut(BaseModel):
    """Member in a cohort result."""
    member_id: str
    member_name: Optional[str] = None
    age: Optional[str] = None
    gender: Optional[str] = None
    county: Optional[str] = None
    risk_tier: Optional[str] = None
    raf_score: Optional[str] = None
    line_of_business: Optional[str] = None
    hedis_gap_count: Optional[str] = None
    top_diagnoses: Optional[str] = None
    total_paid_ytd: Optional[str] = None


class CohortAnalyticsOut(BaseModel):
    """Aggregate analytics for a cohort."""
    total_members: int = 0
    risk_distribution: dict = Field(default_factory=dict)
    avg_raf_score: Optional[float] = None
    avg_age: Optional[float] = None
    total_cost_ytd: Optional[float] = None
    avg_cost_per_member: Optional[float] = None
    total_hedis_gaps: int = 0
    gender_distribution: dict = Field(default_factory=dict)
    lob_distribution: dict = Field(default_factory=dict)
    top_counties: dict = Field(default_factory=dict)
    members: list[CohortMemberOut] = Field(default_factory=list)


class SaveCohortIn(BaseModel):
    """Input for saving a cohort."""
    cohort_name: str
    description: Optional[str] = None
    criteria: dict
    member_count: int


class SavedCohortOut(BaseModel):
    """Saved cohort record."""
    cohort_id: str
    cohort_name: str
    description: Optional[str] = None
    criteria: dict
    member_count: int
    created_by: str
    created_at: str
