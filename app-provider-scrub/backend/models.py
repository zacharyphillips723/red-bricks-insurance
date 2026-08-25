"""Pydantic models for the Claim Scrubber / Denial Risk Predictor API."""

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class RequestType(str, Enum):
    CLAIM = "claim"              # post-service medical claim (837)
    PRIOR_AUTH = "prior_auth"    # pre-service authorization request


class ScrubDecision(str, Enum):
    CLEAN = "clean"                  # low denial risk — safe to submit
    AT_RISK = "at_risk"              # medium risk — review flagged issues
    LIKELY_DENIED = "likely_denied"  # high risk — fix before submitting


class ReasonLayer(str, Enum):
    RULE = "rule"    # deterministic pre-submission rule
    ML = "ml"        # denial-prediction / reason classifier
    RAG = "rag"      # medical-policy retrieval-augmented reasoning


# ---------------------------------------------------------------------------
# Draft claim / prior-auth input
# ---------------------------------------------------------------------------

class ClaimLine(BaseModel):
    cpt: str = Field(..., description="CPT/HCPCS procedure code")
    units: int = 1
    pos: Optional[str] = Field(default=None, description="Place-of-service code, e.g. '11' office")


class DraftClaimIn(BaseModel):
    member_id: str
    provider_npi: str
    date_of_service: str = Field(..., description="ISO date (YYYY-MM-DD)")
    request_type: RequestType = RequestType.CLAIM
    lines: list[ClaimLine] = []
    dx_codes: list[str] = []
    clinical_notes: Optional[str] = None
    billed_amount: Optional[float] = None
    line_of_business: Optional[str] = None
    # Resubmit affordances — the "fix it then re-scrub" loop.
    auth_reference: Optional[str] = Field(default=None, description="Approved auth number supplied on resubmit")
    resubmitted_from: Optional[str] = None


# ---------------------------------------------------------------------------
# Scrub output
# ---------------------------------------------------------------------------

class ReasonCard(BaseModel):
    carc_code: str
    reason_label: str
    reason_category: str
    likelihood: float = Field(..., description="0-1 likelihood this reason drives a denial")
    layer: ReasonLayer
    evidence: Optional[str] = None
    remediation: Optional[str] = None
    required_action: Optional[str] = None
    doc_needed: Optional[str] = None


class FeatureContribution(BaseModel):
    feature: str
    label: Optional[str] = None
    value: Optional[float] = None
    contribution: Optional[float] = None


class ScrubResultOut(BaseModel):
    session_id: str
    member_id: str
    member_name: Optional[str] = None
    provider_npi: str
    date_of_service: str
    request_type: RequestType
    risk_score: int = Field(..., description="0-100 composite denial-risk score")
    decision: ScrubDecision
    ml_denial_prob: Optional[float] = None
    ml_contributions: list[FeatureContribution] = []
    reason_cards: list[ReasonCard] = []
    resubmitted_from: Optional[str] = None
    trace_id: Optional[str] = None
    evaluated_at: Optional[datetime] = None


# ---------------------------------------------------------------------------
# User feedback (MLflow 3 assessments)
# ---------------------------------------------------------------------------

class FeedbackIn(BaseModel):
    trace_id: Optional[str] = None
    session_id: Optional[str] = None
    target: str = Field("overall", description="'overall' or a CARC code the feedback is about")
    value: bool = Field(..., description="True = thumbs up (useful/correct), False = thumbs down")
    rationale: Optional[str] = None


class FeedbackRow(BaseModel):
    session_id: Optional[str] = None
    trace_id: Optional[str] = None
    target: Optional[str] = None
    value: Optional[int] = None
    rationale: Optional[str] = None
    source_id: Optional[str] = None
    created_at: Optional[datetime] = None


# ---------------------------------------------------------------------------
# Members + reference
# ---------------------------------------------------------------------------

class MemberSearchOut(BaseModel):
    member_id: str
    member_name: Optional[str] = None
    line_of_business: Optional[str] = None
    is_active: Optional[bool] = None
    eligibility_start_date: Optional[str] = None
    eligibility_end_date: Optional[str] = None


class CarcReferenceOut(BaseModel):
    carc_code: str
    group_code: Optional[str] = None
    reason_category: Optional[str] = None
    description: Optional[str] = None
    patient_vs_payer: Optional[str] = None


# ---------------------------------------------------------------------------
# Scrub history (Lakebase)
# ---------------------------------------------------------------------------

class ScrubSessionSummary(BaseModel):
    session_id: str
    member_id: str
    member_name: Optional[str] = None
    provider_npi: Optional[str] = None
    date_of_service: Optional[str] = None
    request_type: Optional[str] = None
    risk_score: Optional[int] = None
    decision: Optional[str] = None
    finding_count: int = 0
    resubmitted_from: Optional[str] = None
    created_at: Optional[datetime] = None
