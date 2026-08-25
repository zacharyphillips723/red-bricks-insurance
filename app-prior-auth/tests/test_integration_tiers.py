"""Workspace-gated integration tests for all three tiers.

These require a live Databricks workspace: a running SQL warehouse, the
prior_auth gold tables, the registered XGBoost model, and the deployed PA agent
endpoint. They are SKIPPED by default so the unit suite stays hermetic and CI
stays fast/offline.

Enable with:
    RUN_DB_INTEGRATION=1 pytest tests/test_integration_tiers.py

and a valid Databricks auth context (DATABRICKS_HOST/TOKEN or a profile, plus
UC_CATALOG / SQL_WAREHOUSE_ID pointing at a deployed Red Bricks workspace).

What each tier's integration test asserts:
  Tier 1 (SQL) : gold_pa_tier1_evaluation returns the same exact-match verdict
                 as the Python engine for a controlled probe -> the two
                 implementations agree (the divergence risk flagged in
                 documents.py's docstring).
  Tier 2 (ML)  : the adjudication model scores into one of the known classes
                 with a valid confidence in [0, 1].
  Tier 3 (LLM) : the agent returns a briefing containing all five mandated
                 sections. Structure only — never assert exact LLM wording.
"""

import os

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_DB_INTEGRATION") != "1",
    reason="workspace integration tests; set RUN_DB_INTEGRATION=1 to run",
)


@pytest.fixture(scope="module")
def catalog():
    return os.environ["UC_CATALOG"]


# ---------------------------------------------------------------------------
# Tier 1 (SQL) — gold view must agree with the Python engine on exact matching
# ---------------------------------------------------------------------------

def test_gold_tier1_view_uses_exact_matching(catalog):
    """A procedure that is a *superstring* of a covered code must NOT be
    flagged procedure_code_match=true in gold_pa_tier1_evaluation.

    This is the SQL-side counterpart to the Python regression test and catches
    a silent reversion of the view to substring/LIKE matching.
    """
    from backend.agent import _execute_sql

    rows = _execute_sql(
        f"""
        SELECT COUNT(*) AS bad_matches
        FROM {catalog}.prior_auth.gold_pa_tier1_evaluation t
        JOIN {catalog}.prior_auth.silver_medical_policy_rules pr
          ON pr.policy_id = t.policy_id
         AND pr.rule_type = 'clinical_criteria'
        WHERE t.procedure_code_match = true
          AND NOT array_contains(split(pr.procedure_codes, '[|]'), t.procedure_code)
        """
    )
    assert int(rows[0]["bad_matches"]) == 0


def test_gold_tier1_eligible_implies_documentation(catalog):
    """tier1_auto_eligible must never be true without documentation (>50 chars)."""
    from backend.agent import _execute_sql

    rows = _execute_sql(
        f"""
        SELECT COUNT(*) AS violations
        FROM {catalog}.prior_auth.gold_pa_tier1_evaluation t
        JOIN {catalog}.prior_auth.silver_pa_requests r
          ON r.auth_request_id = t.auth_request_id
        WHERE t.tier1_auto_eligible = true
          AND length(coalesce(r.clinical_summary, '')) <= 50
        """
    )
    assert int(rows[0]["violations"]) == 0


# ---------------------------------------------------------------------------
# Tier 2 (ML) — scored predictions are well-formed
# ---------------------------------------------------------------------------

def test_ml_predictions_are_valid(catalog):
    from backend.agent import _execute_sql

    rows = _execute_sql(
        f"""
        SELECT predicted_determination, confidence
        FROM {catalog}.prior_auth.pa_ml_predictions
        LIMIT 200
        """
    )
    if not rows:
        pytest.skip("pa_ml_predictions is empty; run train_pa_model first")
    valid_classes = {"approved", "denied", "pended"}
    for r in rows:
        assert r["predicted_determination"] in valid_classes
        conf = float(r["confidence"])
        assert 0.0 <= conf <= 1.0


# ---------------------------------------------------------------------------
# Tier 3 (LLM) — agent returns a structurally complete briefing
# ---------------------------------------------------------------------------

REQUIRED_SECTIONS = [
    "## REQUEST SUMMARY",
    "## CLINICAL EVIDENCE",
    "## POLICY ANALYSIS",
    "## AI ASSESSMENT",
    "## RECOMMENDATION",
]


def test_agent_returns_all_required_sections(catalog):
    """Smoke + structure test. Picks a real request id and asks the agent for a
    briefing; asserts the five mandated sections are present. Never asserts on
    the model's specific wording — only that the contract structure holds.
    """
    from backend.agent import _execute_sql, query_pa_agent

    rows = _execute_sql(
        f"SELECT auth_request_id FROM {catalog}.prior_auth.gold_pa_requests LIMIT 1"
    )
    if not rows:
        pytest.skip("no PA requests available to brief on")
    req_id = rows[0]["auth_request_id"]

    result = query_pa_agent(req_id, "Give me a clinical review briefing for this request.")
    text = result.get("answer", "") if isinstance(result, dict) else str(result)
    missing = [s for s in REQUIRED_SECTIONS if s not in text]
    assert not missing, f"agent briefing missing sections: {missing}"
