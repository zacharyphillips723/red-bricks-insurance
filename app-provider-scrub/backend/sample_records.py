"""Sample draft claims/prior-auth requests for the Claim Scrubber demo.

Each scenario is engineered to trip a specific denial-risk layer so a demo
always has a compelling draft to scrub. Codes align with the real
silver_medical_policy_rules and the experimental CPTs used by the claims data
generator, so the downstream matching produces explainable results.

member_id is filled in at request time from a real generated member (see the
router's /scrub/samples endpoint) so the drafts are one-click runnable.
"""

# Valid 10-digit NPI reused across well-formed samples.
_GOOD_NPI = "1093817465"

SCENARIOS: dict[str, dict] = {
    "auth_missing": {
        "title": "Auth-managed procedure, no authorization on file",
        "expected": "CO-197 — prior authorization missing",
        "draft": {
            "request_type": "claim",
            "provider_npi": _GOOD_NPI,
            "date_of_service": "2026-06-15",
            "lines": [{"cpt": "27447", "units": 1, "pos": "22"}],
            "dx_codes": ["M17.11"],
            "billed_amount": 38500.0,
            "clinical_notes": (
                "62-year-old with end-stage right knee osteoarthritis. Failed 9 months of "
                "conservative therapy including NSAIDs, physical therapy, and corticosteroid "
                "injection. Imaging shows bone-on-bone joint-space loss. Total knee arthroplasty planned."
            ),
        },
    },
    "coding_mismatch": {
        "title": "Diagnosis does not support the procedure",
        "expected": "CO-11 — diagnosis inconsistent with procedure",
        "draft": {
            "request_type": "claim",
            "provider_npi": _GOOD_NPI,
            "date_of_service": "2026-06-15",
            "lines": [{"cpt": "27447", "units": 1, "pos": "22"}],
            "dx_codes": ["J45.909"],  # asthma — not a covered indication for TKA
            "billed_amount": 38500.0,
            "clinical_notes": "Patient scheduled for total knee arthroplasty.",
        },
    },
    "eligibility": {
        "title": "Service outside the member's coverage window",
        "expected": "CO-27 — coverage terminated / not eligible on DOS",
        "draft": {
            "request_type": "claim",
            "provider_npi": _GOOD_NPI,
            "date_of_service": "2019-01-10",  # well before any active coverage
            "lines": [{"cpt": "99213", "units": 1, "pos": "11"}],
            "dx_codes": ["E11.9"],
            "billed_amount": 210.0,
            "clinical_notes": "Routine follow-up visit.",
        },
    },
    "incomplete": {
        "title": "Draft missing required data",
        "expected": "CO-16 — claim lacks information / documentation",
        "draft": {
            "request_type": "claim",
            "provider_npi": "000",  # invalid NPI
            "date_of_service": "2026-06-15",
            "lines": [{"cpt": "27447", "units": 1, "pos": "22"}],
            "dx_codes": [],  # no diagnosis submitted
            "billed_amount": 38500.0,
            "clinical_notes": "",
        },
    },
    "experimental": {
        "title": "Experimental procedure / medical necessity unmet",
        "expected": "CO-55/CO-96 or CO-50 — via medical-policy RAG",
        "draft": {
            "request_type": "prior_auth",
            "provider_npi": _GOOD_NPI,
            "date_of_service": "2026-07-01",
            "lines": [{"cpt": "62323", "units": 1, "pos": "22"}],
            "dx_codes": ["M54.51"],
            "billed_amount": 4200.0,
            "clinical_notes": (
                "Chronic low back pain, 3 weeks duration. No red-flag symptoms. Requesting "
                "epidural steroid injection. No prior conservative therapy documented."
            ),
        },
    },
}


def list_sample_scenarios() -> list[dict]:
    return [
        {"scenario": key, "title": val["title"], "expected": val["expected"],
         "request_type": val["draft"]["request_type"]}
        for key, val in SCENARIOS.items()
    ]


def build_sample_draft(scenario: str, member_id: str, line_of_business: str | None = None) -> dict:
    """Return a full DraftClaimIn-shaped dict for the given scenario + member."""
    template = SCENARIOS.get(scenario, SCENARIOS["auth_missing"])["draft"]
    draft = {k: (v.copy() if isinstance(v, list) else v) for k, v in template.items()}
    draft["member_id"] = member_id
    if line_of_business:
        draft["line_of_business"] = line_of_business
    return draft
