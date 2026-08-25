"""Regression tests: invariants that must never silently break.

These lock down behaviors that a well-meaning refactor could quietly regress.
The headline one is EXACT code matching. documents.py's module docstring is
explicit that Tier-1 uses split-and-exact matching precisely to avoid the
substring false positives that a `LIKE '%code%'` approach produces (e.g. 27447
matching 274470, or 99213 matching 992130). If someone ever "simplifies" the
matcher back to substring logic, these tests fail loudly.
"""

import pytest

from backend.documents import adjudicate, _split_codes, _match_policy


# ---------------------------------------------------------------------------
# EXACT matching — the core anti-substring guarantee
# ---------------------------------------------------------------------------

class TestExactCodeMatching:
    def test_superstring_procedure_does_not_match_covered_code(self, patch_sql):
        # 274470 is a superstring of the covered 27447. Substring matching would
        # wrongly approve this; exact matching must NOT find a policy -> deny.
        result = adjudicate({
            "procedure_codes": ["274470"],
            "diagnosis_codes": ["M17.11"],
            "clinical_summary": "x" * 60,
        })
        assert result["matched_policy"] is None
        assert result["decision"] == "Auto-Deny"

    def test_substring_procedure_does_not_match_covered_code(self, patch_sql):
        # 2744 is a substring of 27447. Must not match.
        result = adjudicate({
            "procedure_codes": ["2744"],
            "diagnosis_codes": ["M17.11"],
            "clinical_summary": "x" * 60,
        })
        assert result["matched_policy"] is None
        assert result["decision"] == "Auto-Deny"

    def test_superstring_diagnosis_does_not_count_as_match(self, patch_sql):
        # Procedure matches the knee policy; diagnosis M17.110 is a superstring of
        # the covered M17.11 and must NOT be treated as a covered indication.
        result = adjudicate({
            "procedure_codes": ["27447"],
            "diagnosis_codes": ["M17.110"],
            "clinical_summary": "x" * 60,
        })
        assert result["matched_policy"]["procedure_match"] is True
        assert result["matched_policy"]["diagnosis_match"] is False
        assert result["decision"] == "Needs Clinical Review"

    def test_exact_procedure_and_diagnosis_still_approve(self, patch_sql):
        # The positive control for the three negatives above.
        result = adjudicate({
            "procedure_codes": ["27447"],
            "diagnosis_codes": ["M17.11"],
            "clinical_summary": "x" * 60,
        })
        assert result["decision"] == "Auto-Approve"


# ---------------------------------------------------------------------------
# Case- and format-insensitivity invariants
# ---------------------------------------------------------------------------

class TestCodeNormalizationInvariants:
    def test_hcpcs_letter_code_case_insensitive(self, patch_sql):
        # e0784 (lowercase) must match covered E0784.
        result = adjudicate({
            "procedure_codes": ["e0784"],
            "diagnosis_codes": ["e11.65"],
            "clinical_summary": "x" * 60,
        })
        assert result["decision"] == "Auto-Approve"
        assert result["matched_policy"]["policy_id"] == "MP-CGM-002"

    def test_json_array_and_pipe_forms_are_equivalent(self, patch_sql):
        json_form = adjudicate({
            "procedure_codes": '["95249"]',
            "diagnosis_codes": '["E11.65"]',
            "clinical_summary": "x" * 60,
        })
        pipe_form = adjudicate({
            "procedure_codes": "95249",
            "diagnosis_codes": "E11.65",
            "clinical_summary": "x" * 60,
        })
        assert json_form["decision"] == pipe_form["decision"] == "Auto-Approve"


# ---------------------------------------------------------------------------
# _match_policy preference: full (proc+dx) match wins over proc-only
# ---------------------------------------------------------------------------

class TestPolicyPreference:
    def test_prefers_policy_with_both_matches(self, monkeypatch):
        # Two policies cover procedure 11111; only the second also covers the
        # submitted diagnosis. _match_policy must return the fuller match.
        from backend import documents
        rows = [
            {"policy_id": "P-A", "policy_name": "A", "service_category": "s",
             "procedure_codes": "11111", "diagnosis_codes": "Z99.9"},
            {"policy_id": "P-B", "policy_name": "B", "service_category": "s",
             "procedure_codes": "11111", "diagnosis_codes": "E11.65"},
        ]
        monkeypatch.setattr(documents, "_execute_sql",
                            lambda *a, **k: [dict(r) for r in rows])
        best = _match_policy(["11111"], ["E11.65"])
        assert best["policy_id"] == "P-B"
        assert best["diagnosis_match"] is True


# ---------------------------------------------------------------------------
# Decision -> (queue status, tier) mapping contract
# ---------------------------------------------------------------------------

class TestDecisionStatusMapping:
    def test_auto_decisions_are_tier_1_auto(self):
        from backend.documents import _DECISION_TO_STATUS
        assert _DECISION_TO_STATUS["Auto-Approve"] == ("Approved", "tier_1_auto")
        assert _DECISION_TO_STATUS["Auto-Deny"] == ("Denied", "tier_1_auto")

    def test_needs_review_routes_to_manual(self):
        from backend.documents import _DECISION_TO_STATUS
        assert _DECISION_TO_STATUS["Needs Clinical Review"] == ("Pending Review", "manual")

    def test_cms_deadline_hours_match_urgency_slas(self):
        # Guards the CMS-0057-F turnaround SLAs the write-back path stamps.
        from backend.documents import _URGENCY_DEADLINE_HOURS
        assert _URGENCY_DEADLINE_HOURS == {
            "expedited": 72, "standard": 168, "retrospective": 336,
        }
