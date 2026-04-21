"""Pydantic models for Network Adequacy Portal API."""

from typing import Optional

from pydantic import BaseModel


# --- Compliance ---

class ComplianceRow(BaseModel):
    county_fips: str
    county_name: str
    county_type: str
    cms_specialty_type: str
    max_distance_miles: int
    max_time_minutes: int
    total_members: int
    compliant_members: int
    pct_compliant: float
    is_compliant: bool
    gap_members: int
    avg_nearest_distance_mi: Optional[float] = None
    telehealth_available: Optional[bool] = None
    telehealth_credit_applied: Optional[bool] = None


class ComplianceSummary(BaseModel):
    overall_compliance_pct: float
    total_county_specialty_combos: int
    non_compliant_count: int
    total_gap_members: int


# --- Ghost Network ---

class GhostProviderRow(BaseModel):
    npi: str
    provider_name: str
    specialty: str
    cms_specialty_type: Optional[str] = None
    county: str
    county_fips: Optional[str] = None
    ghost_severity: str
    ghost_signal_count: int
    is_ghost_flagged: bool
    impact_members: int
    accepts_new_patients: Optional[bool] = None
    telehealth_capable: Optional[bool] = None
    panel_size: Optional[int] = None
    panel_capacity: Optional[int] = None
    appointment_wait_days: Optional[int] = None
    credentialing_status: Optional[str] = None
    last_claims_date: Optional[str] = None
    no_claims_12m: Optional[bool] = None
    no_claims_6m: Optional[bool] = None
    not_accepting: Optional[bool] = None
    extreme_wait: Optional[bool] = None
    credential_expired: Optional[bool] = None
    panel_full: Optional[bool] = None


class GhostSummary(BaseModel):
    total_flagged: int
    high_severity: int
    medium_severity: int
    low_severity: int
    total_impact_members: int


# --- Leakage ---

class LeakageBySpecialty(BaseModel):
    cms_specialty_type: str
    total_claims: int
    total_paid: float
    leakage_cost: float
    unique_members: int
    oon_providers: int


class LeakageByCounty(BaseModel):
    county_name: str
    county_type: str
    total_claims: int
    leakage_cost: float
    unique_members: int


class LeakageByReason(BaseModel):
    leakage_reason: str
    total_claims: int
    leakage_cost: float
    unique_members: int


class LeakageSummary(BaseModel):
    total_oon_claims: int
    total_leakage_cost: float
    total_oon_members: int
    by_specialty: list[LeakageBySpecialty]
    by_county: list[LeakageByCounty]
    by_reason: list[LeakageByReason]


# --- Recruitment ---

class RecruitmentTarget(BaseModel):
    rendering_provider_npi: str
    specialty: Optional[str] = None
    county_name: Optional[str] = None
    total_claims: int
    total_paid: float
    potential_savings: float
    members_served: int
    avg_member_distance_mi: Optional[float] = None
    recruitment_priority_score: float


# --- Network Gaps ---

class NetworkGap(BaseModel):
    county_name: str
    county_type: str
    cms_specialty_type: str
    pct_compliant: float
    gap_members: int
    gap_status: str
    priority_rank: int
    cms_threshold_miles: Optional[int] = None
    avg_nearest_distance_mi: Optional[float] = None
    telehealth_credit_applied: Optional[bool] = None


# --- Map / Geographic View ---

class CountyMapMetric(BaseModel):
    county_fips: str
    county_name: str
    county_type: str
    latitude: float = 0.0
    longitude: float = 0.0
    avg_compliance_pct: float = 0.0
    non_compliant_specialties: int = 0
    total_specialties: int = 0
    gap_members: int = 0
    ghost_flagged_count: int = 0
    ghost_high_count: int = 0
    ghost_impact_members: int = 0
    oon_claims: int = 0
    leakage_cost: float = 0.0
    oon_members: int = 0
    total_providers: int = 0
    inn_providers: int = 0
    oon_providers: int = 0


# --- Dashboard ---

class DashboardStats(BaseModel):
    compliance_summary: ComplianceSummary
    ghost_summary: GhostSummary
    total_leakage_cost: float
    total_oon_claims: int
    telehealth_credits_applied: int
    top_recruitment_targets: list[RecruitmentTarget]


# --- Genie ---

class GenieQuestionIn(BaseModel):
    question: str
    conversation_id: Optional[str] = None


class GenieResponseOut(BaseModel):
    conversation_id: str
    message_id: Optional[str] = None
    sql_query: Optional[str] = None
    columns: list[str] = []
    rows: list[dict] = []
    row_count: int = 0
    description: Optional[str] = None
