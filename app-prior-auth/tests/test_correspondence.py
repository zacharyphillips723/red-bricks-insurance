"""Offline unit tests for determination-notice generation and the PHI gate.

Covers backend/correspondence.py without a workspace: the ai_query rationale
call is monkeypatched, so these assert the deterministic scaffolding and the
PHI-redaction behavior that gates release.
"""

import pytest

from backend import correspondence as corr


@pytest.fixture(autouse=True)
def _stub_llm(monkeypatch):
    """Make the ai_query rationale call return a canned body (no network)."""
    monkeypatch.setattr(
        corr, "_execute_sql", lambda sql: [{"body": "The requested service was reviewed against policy criteria."}]
    )


def _facts(**over):
    base = {
        "auth_request_id": "PA-0001",
        "member_id": "MBR123",
        "member_name": "Jane Doe",
        "requesting_provider_npi": "1234567890",
        "provider_name": "Dr. Smith",
        "procedure_code": "72148",
        "procedure_description": "MRI lumbar spine",
        "line_of_business": "Medicare Advantage",
        "policy_id": "MP-MRI-003",
        "policy_name": "Advanced Imaging — Lumbar MRI",
        "clinical_summary": "Chronic low back pain, failed conservative therapy.",
        "determination_reason": "Meets medical necessity criteria.",
        "denial_reason_code": None,
        "criteria_version": "InterQual 2026.1",
    }
    base.update(over)
    return base


# --------------------------------------------------------------------------
# PHI redaction gate
# --------------------------------------------------------------------------

@pytest.mark.parametrize("text,label", [
    ("SSN 123-45-6789 on file", "SSN"),
    ("member 123456789", "SSN"),
    ("call (701) 555-1234", "phone"),
    ("email jane@example.com", "email"),
    ("MRN: AB99312", "MRN"),
])
def test_redact_phi_masks_identifiers(text, label):
    clean, was_redacted, found = corr.redact_phi(text)
    assert was_redacted is True
    assert label in found
    assert "[REDACTED]" in clean


def test_redact_phi_leaves_clean_text():
    clean, was_redacted, found = corr.redact_phi("MRI lumbar spine approved.")
    assert was_redacted is False
    assert found == []
    assert clean == "MRI lumbar spine approved."


# --------------------------------------------------------------------------
# Notice assembly
# --------------------------------------------------------------------------

def test_denial_notice_includes_appeal_rights_and_citation():
    notice = corr.build_notice("denial", _facts(denial_reason_code="CO-197"))
    body = notice["body_markdown"]
    assert notice["includes_appeal_rights"] is True
    assert "Your Appeal Rights" in body
    assert "CO-197" in body
    assert "InterQual 2026.1" in notice["criteria_citation"]
    assert notice["body_redacted"] is True
    assert notice["template_version"]


def test_approval_notice_has_effective_dates_no_appeal_rights():
    notice = corr.build_notice("approval", _facts())
    body = notice["body_markdown"]
    assert notice["includes_appeal_rights"] is False
    assert "Your Appeal Rights" not in body
    assert "Authorization Effective Dates" in body


def test_notice_body_is_phi_safe():
    # A leaked SSN in the clinical summary must be masked in the rendered notice.
    notice = corr.build_notice("denial", _facts(clinical_summary="Patient SSN 123-45-6789."))
    assert "123-45-6789" not in notice["body_markdown"]


def test_unknown_notice_type_rejected():
    with pytest.raises(ValueError):
        corr.build_notice("not_a_type", _facts())


# --------------------------------------------------------------------------
# Multilingual + delivery validation + inbound classification
# --------------------------------------------------------------------------

def test_english_notice_not_translated():
    notice = corr.build_notice("approval", _facts(), language="en")
    assert notice["language"] == "en"
    # Canned _execute_sql body is only used for rationale, not a translation wrapper.
    assert "Translation to" not in notice["body_markdown"]


def test_spanish_notice_runs_translation_pass(monkeypatch):
    # Route rationale vs translation by prompt content.
    def fake_sql(sql):
        if "Translate the following" in sql:
            return [{"body": "AVISO (traducido)"}]
        return [{"body": "rationale"}]
    monkeypatch.setattr(corr, "_execute_sql", fake_sql)
    notice = corr.build_notice("denial", _facts(), language="es")
    assert notice["language"] == "es"
    assert "AVISO (traducido)" in notice["body_markdown"]


def test_validate_delivery_flags_missing_identity():
    status, notes = corr.validate_delivery({"member_id": None, "member_name": None})
    assert status == "warning"
    assert "beneficiary" in notes.lower()


def test_validate_delivery_passes_complete():
    status, _ = corr.validate_delivery(_facts())
    assert status == "passed"


def test_classify_inbound_keyword_fallback(monkeypatch):
    # ai_query unavailable -> keyword heuristic still classifies.
    def boom(sql):
        raise RuntimeError("no workspace")
    monkeypatch.setattr(corr, "_execute_sql", boom)
    r = corr.classify_inbound("Member letter to file an appeal of the denial.")
    assert r["classified_type"] == "appeal"
    assert r["classification_confidence"] == 0.5
