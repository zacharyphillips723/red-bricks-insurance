"""Unit tests for the Tier-1 deterministic adjudication engine.

Covers the pure logic in backend/documents.py that decides Auto-Approve /
Auto-Deny / Needs Clinical Review from extracted clinical facts:

  _split_codes      — code-field normalization (list / delimited / JSON-array)
  _has_value        — "field holds real content" predicate
  _match_policy     — exact procedure/diagnosis matching against policy rules
  adjudicate        — the tier-1 decision itself

These run fully offline: the one I/O seam (_execute_sql) is stubbed via the
`patch_sql` fixture. No workspace, no warehouse, no network.
"""

import pytest

from backend import documents
from backend.documents import _split_codes, _has_value, adjudicate


# ---------------------------------------------------------------------------
# _split_codes — the parsing that feeds every match decision
# ---------------------------------------------------------------------------

class TestSplitCodes:
    @pytest.mark.parametrize(
        "value, expected",
        [
            (None, []),
            ("", []),
            ("27447", ["27447"]),
            ("27447|27448", ["27447", "27448"]),          # pipe-delimited (UC storage form)
            ("27447, 27448", ["27447", "27448"]),          # comma + space
            ("27447 27448", ["27447", "27448"]),           # whitespace
            ('["95249","E0784"]', ["95249", "E0784"]),     # JSON-array string (ai_extract form)
            (["95249", "e0784"], ["95249", "E0784"]),      # python list, upcased
            (["27447|27448"], ["27447", "27448"]),         # list holding a delimited string
            ("e11.65", ["E11.65"]),                        # ICD-10 upcasing, dot preserved
        ],
    )
    def test_normalization(self, value, expected):
        assert _split_codes(value) == expected

    def test_malformed_json_falls_back_to_delimiter_split(self):
        # A bracketed-but-invalid JSON string must not raise. Because it does not
        # also end with ']', the JSON branch is skipped and it splits on
        # [|,;\s]+ — which does NOT include '[', so the stray bracket is kept.
        # Documents current behavior (not aspirational): the important guarantee
        # is "no exception", and this input shape never occurs from ai_extract.
        assert _split_codes("[27447") == ["[27447"]


# ---------------------------------------------------------------------------
# _has_value — documentation-sufficiency signal
# ---------------------------------------------------------------------------

class TestHasValue:
    @pytest.mark.parametrize(
        "value, expected",
        [
            (None, False),
            ("", False),
            ("   ", False),
            ("[]", False),
            ("{}", False),
            ('""', False),
            ("null", False),
            ("None", False),
            ([], False),
            ({}, False),
            ("HbA1c=8.2", True),
            (["metformin"], True),
            ({"k": "v"}, True),
            (0, True),   # a real scalar 0 is content, not emptiness
        ],
    )
    def test_has_value(self, value, expected):
        assert _has_value(value) is expected


# ---------------------------------------------------------------------------
# adjudicate — the Tier-1 decision engine
# ---------------------------------------------------------------------------

def _facts(**overrides):
    """A well-formed 'documented + matching' fact set; override per test."""
    base = {
        "procedure_codes": ["27447"],
        "diagnosis_codes": ["M17.11"],
        "clinical_summary": (
            "62yo with end-stage osteoarthritis of the right knee, failed 9 months "
            "of conservative therapy including NSAIDs and physical therapy."
        ),
    }
    base.update(overrides)
    return base


class TestAdjudicateDecisions:
    def test_auto_approve_on_full_match_and_documentation(self, patch_sql):
        result = adjudicate(_facts())
        assert result["decision"] == "Auto-Approve"
        assert result["confidence"] == pytest.approx(0.94)
        assert result["matched_policy"]["policy_id"] == "MP-KNEE-001"
        assert result["matched_policy"]["diagnosis_match"] is True
        assert result["has_documentation"] is True

    def test_auto_deny_when_no_policy_covers_procedure(self, patch_sql):
        # 99999 is not in any policy's covered set -> no policy match -> deny.
        result = adjudicate(_facts(procedure_codes=["99999"]))
        assert result["decision"] == "Auto-Deny"
        assert result["confidence"] == pytest.approx(0.80)
        assert result["matched_policy"] is None

    def test_needs_review_when_no_procedure_code_extracted(self, patch_sql):
        result = adjudicate(_facts(procedure_codes=[]))
        assert result["decision"] == "Needs Clinical Review"
        assert result["confidence"] == pytest.approx(0.30)
        assert "No procedure code" in " ".join(result["reasons"])

    def test_needs_review_when_diagnosis_does_not_align(self, patch_sql):
        # Procedure matches the knee policy, but the diagnosis is a diabetes code.
        result = adjudicate(_facts(diagnosis_codes=["E11.65"]))
        assert result["decision"] == "Needs Clinical Review"
        assert result["confidence"] == pytest.approx(0.55)
        assert result["matched_policy"]["procedure_match"] is True
        assert result["matched_policy"]["diagnosis_match"] is False

    def test_needs_review_when_documentation_insufficient(self, patch_sql):
        # Full code match but a too-short summary and no other clinical evidence.
        result = adjudicate(_facts(clinical_summary="Knee pain."))
        assert result["decision"] == "Needs Clinical Review"
        assert result["has_documentation"] is False

    def test_clinical_evidence_substitutes_for_short_summary(self, patch_sql):
        # Summary is short, but treatments_tried carries real evidence ->
        # documentation is considered sufficient and the case auto-approves.
        result = adjudicate(
            _facts(
                clinical_summary="Knee pain.",
                treatments_tried=["NSAIDs", "physical therapy", "corticosteroid injection"],
            )
        )
        assert result["has_documentation"] is True
        assert result["decision"] == "Auto-Approve"

    def test_policy_with_no_diagnosis_restriction_still_needs_review_without_dx_match(self, patch_sql):
        # MP-MRI-003 (72148) has diagnosis_codes = None, so nothing can match on
        # diagnosis -> even with docs, it is not a full match -> needs review.
        result = adjudicate(_facts(procedure_codes=["72148"], diagnosis_codes=["M54.5"]))
        assert result["matched_policy"]["policy_id"] == "MP-MRI-003"
        assert result["matched_policy"]["diagnosis_match"] is False
        assert result["decision"] == "Needs Clinical Review"


class TestAdjudicateReturnShape:
    """The write-back path (write_back_to_queue) depends on these keys existing."""

    REQUIRED_KEYS = {
        "decision", "confidence", "reasons", "matched_policy",
        "extracted_procedure_codes", "extracted_diagnosis_codes", "has_documentation",
    }

    def test_result_contains_all_contract_keys(self, patch_sql):
        result = adjudicate(_facts())
        assert self.REQUIRED_KEYS.issubset(result.keys())
        assert isinstance(result["reasons"], list) and result["reasons"]

    def test_decision_is_always_a_known_status(self, patch_sql):
        # Whatever the inputs, decision must map into _DECISION_TO_STATUS.
        for facts in (_facts(), _facts(procedure_codes=["99999"]), _facts(procedure_codes=[])):
            result = adjudicate(facts)
            assert result["decision"] in documents._DECISION_TO_STATUS
