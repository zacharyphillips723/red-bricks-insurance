"""Front-of-funnel quote intake: document intelligence + NL submission parsing.

Phase 3 of the underwriting vision roadmap. Turns unstructured input — a pasted
census/SBC/competitor quote, or a free-text submission from an AE — into a
structured quote submission, then gates it on completeness ("pre-filled 78%;
missing: concessions, target BCR"). The AE *verifies* a pre-filled memo rather
than filling one from scratch.

Extraction uses the Foundation Model API (see llm.py). In production the same
structured rows can come from `ai_parse_document` / `ai_extract` over uploaded
files; here we accept pasted text so the demo needs no file-upload plumbing.
"""

from typing import Optional

from .llm import complete_json, complete_text

# Canonical submission schema: field -> human description used in the extract prompt.
SUBMISSION_SCHEMA: dict[str, str] = {
    "group_name": "employer / group name",
    "member_count": "number of covered members/employees (integer)",
    "lob": "line of business (Commercial, Medicare Advantage, Medicaid, Individual)",
    "industry": "industry or SIC sector",
    "avg_age_band": "average age band, one of 0-17,18-25,26-35,36-45,46-55,56-64,65+",
    "county_type": "geography: urban, suburban, or rural",
    "funding_arrangement": "funding type: fully_insured, aso, level_funded, specialty, stop_loss, disability",
    "current_premium": "current annual premium in dollars (number)",
    "current_claims": "current annual claims in dollars (number)",
    "effective_date": "requested effective date (YYYY-MM-DD if present)",
    "competitor_pmpm": "any competitor quoted PMPM in dollars (number)",
    "requested_rate_action": "requested or expected rate change as a percent (number)",
}

# Fields that must be present for the submission to be considered quote-ready.
REQUIRED_FIELDS = [
    "group_name", "member_count", "lob", "funding_arrangement",
    "current_premium", "current_claims", "effective_date",
]


def _extract_fields(text: str, context: str) -> dict:
    """LLM-extract the submission schema fields from free text."""
    field_lines = "\n".join(f"- {k}: {v}" for k, v in SUBMISSION_SCHEMA.items())
    system = (
        "You are an underwriting intake assistant. Extract a group health insurance quote "
        "submission from the provided text into a flat JSON object with these keys "
        f"(use null when a value is absent, do not guess):\n{field_lines}"
    )
    user = f"{context}\n\n---\n{text.strip()}"
    result = complete_json(system, user, max_tokens=1200)
    # Keep only known keys; drop LLM housekeeping keys.
    return {k: result.get(k) for k in SUBMISSION_SCHEMA if k in result}


def _completeness(fields: dict) -> dict:
    """Deterministic completeness gate over the required fields."""
    present = [
        f for f in REQUIRED_FIELDS
        if fields.get(f) not in (None, "", [])
    ]
    missing = [f for f in REQUIRED_FIELDS if f not in present]
    pct = round(len(present) / len(REQUIRED_FIELDS) * 100) if REQUIRED_FIELDS else 100
    return {
        "percent_complete": pct,
        "present_fields": present,
        "missing_fields": missing,
        "quote_ready": len(missing) == 0,
    }


def extract_submission(text: str, doc_type: str = "submission") -> dict:
    """Document-intelligence entrypoint: pasted census/SBC/competitor text → structured."""
    context = (
        f"The text below is a {doc_type} document. Extract the quote submission fields."
    )
    fields = _extract_fields(text, context)
    return {
        "doc_type": doc_type,
        "fields": fields,
        "completeness": _completeness(fields),
    }


def parse_intake(nl_text: str, strategy_memo: bool = True) -> dict:
    """NL quote intake: free-text submission → structured fields + completeness + memo."""
    fields = _extract_fields(
        nl_text,
        "The text below is a free-text new-business or renewal submission from an "
        "account executive. Extract the quote submission fields.",
    )
    completeness = _completeness(fields)

    memo: Optional[str] = None
    if strategy_memo:
        missing = ", ".join(completeness["missing_fields"]) or "none"
        memo = complete_text(
            "You are a senior underwriter. Write a concise 3-4 sentence underwriting strategy "
            "memo for this submission: note the risk posture, what to validate, and the single "
            "most important missing input to collect. Be specific and quantitative where possible. "
            "Do not fabricate numbers that are not provided.",
            f"Extracted submission: {fields}\nMissing required fields: {missing}",
            max_tokens=400,
        ).strip()

    return {
        "fields": fields,
        "completeness": completeness,
        "strategy_memo": memo,
    }
