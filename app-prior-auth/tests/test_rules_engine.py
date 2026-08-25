"""Offline unit tests for the no-code Business Rules Engine (backend/rules_engine.py).

Pure-Python module (no Databricks deps), so these run with zero workspace access.
"""

from backend import rules_engine as re_


def _rule(**over):
    base = {
        "rule_id": "r1", "name": "rule", "status": "active", "priority": 100,
        "line_of_business": None, "service_type": None,
        "action": "auto_approve", "conditions_json": {"all": []},
    }
    base.update(over)
    return base


# --------------------------------------------------------------------------
# Condition operators
# --------------------------------------------------------------------------

def test_op_eq_and_ne():
    r = _rule(conditions_json={"all": [{"field": "urgency", "op": "eq", "value": "standard"}]})
    assert re_.rule_matches(r, {"urgency": "standard"})
    assert not re_.rule_matches(r, {"urgency": "expedited"})


def test_op_in_and_contains_on_delimited_codes():
    r = _rule(conditions_json={"all": [{"field": "procedure_code", "op": "in", "value": ["27447", "27130"]}]})
    assert re_.rule_matches(r, {"procedure_code": "27447"})
    assert not re_.rule_matches(r, {"procedure_code": "99999"})
    # contains: diagnosis_codes stored pipe-delimited
    r2 = _rule(conditions_json={"all": [{"field": "diagnosis_codes", "op": "contains", "value": ["M17.11"]}]})
    assert re_.rule_matches(r2, {"diagnosis_codes": "M17.11|E11.65"})
    assert not re_.rule_matches(r2, {"diagnosis_codes": "E11.65"})


def test_numeric_ops():
    r = _rule(conditions_json={"all": [{"field": "estimated_cost", "op": "gt", "value": 25000}]})
    assert re_.rule_matches(r, {"estimated_cost": 30000})
    assert not re_.rule_matches(r, {"estimated_cost": 1000})


def test_scope_filters_gate_match():
    r = _rule(service_type="imaging",
              conditions_json={"all": [{"field": "urgency", "op": "eq", "value": "standard"}]})
    assert re_.rule_matches(r, {"service_type": "imaging", "urgency": "standard"})
    assert not re_.rule_matches(r, {"service_type": "surgery", "urgency": "standard"})


# --------------------------------------------------------------------------
# Evaluate — priority ordering
# --------------------------------------------------------------------------

def test_evaluate_returns_highest_priority_match():
    deny = _rule(rule_id="deny", name="deny", priority=10, action="auto_deny",
                 conditions_json={"all": [{"field": "procedure_code", "op": "in", "value": ["15780"]}]})
    approve = _rule(rule_id="appr", name="approve", priority=50, action="auto_approve",
                    service_type="imaging", conditions_json={"all": []})
    req = {"procedure_code": "15780", "service_type": "imaging"}
    result = re_.evaluate([approve, deny], req)
    assert result["action"] == "auto_deny"           # lower priority number wins
    assert result["fired_rule"]["name"] == "deny"
    assert len(result["matched_rules"]) == 2          # both matched, both reported


def test_evaluate_no_match():
    r = _rule(conditions_json={"all": [{"field": "urgency", "op": "eq", "value": "expedited"}]})
    result = re_.evaluate([r], {"urgency": "standard"})
    assert result["action"] is None
    assert result["matched_rules"] == []


def test_inactive_rules_ignored():
    r = _rule(status="draft", service_type="imaging", conditions_json={"all": []})
    assert re_.evaluate([r], {"service_type": "imaging"})["action"] is None


# --------------------------------------------------------------------------
# Simulate + conflicts
# --------------------------------------------------------------------------

def test_simulate_reports_match_and_agreement():
    r = _rule(action="auto_deny",
              conditions_json={"all": [{"field": "procedure_code", "op": "in", "value": ["15780"]}]})
    historical = [
        {"auth_request_id": "A1", "procedure_code": "15780", "determination": "denied"},
        {"auth_request_id": "A2", "procedure_code": "15780", "determination": "approved"},
        {"auth_request_id": "A3", "procedure_code": "27447", "determination": "approved"},
    ]
    sim = re_.simulate(r, historical)
    assert sim["total_evaluated"] == 3
    assert sim["matched"] == 2
    assert sim["would_agree"] == 1        # A1 denied matches auto_deny
    assert sim["would_disagree"] == 1     # A2 approved conflicts
    assert sim["agreement_rate_pct"] == 50.0


def test_detect_conflicts_finds_overlapping_actions():
    a = _rule(rule_id="a", name="A", service_type="imaging", action="auto_approve")
    b = _rule(rule_id="b", name="B", service_type="imaging", action="pend")
    c = _rule(rule_id="c", name="C", service_type="surgery", action="pend")
    conflicts = re_.detect_conflicts([a, b, c])
    assert len(conflicts) == 1
    ids = {conflicts[0]["rule_a"]["rule_id"], conflicts[0]["rule_b"]["rule_id"]}
    assert ids == {"a", "b"}


def test_no_conflict_when_actions_agree():
    a = _rule(rule_id="a", service_type="imaging", action="pend")
    b = _rule(rule_id="b", service_type="imaging", action="pend")
    assert re_.detect_conflicts([a, b]) == []
