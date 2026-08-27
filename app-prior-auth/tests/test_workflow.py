"""Offline unit tests for the Workflow Engine (backend/workflow.py)."""

from backend import workflow as wf


# --- route_case -----------------------------------------------------------

ROUTING_RULES = [
    {"routing_rule_id": "r-exp", "name": "Expedited", "priority": 10, "is_active": True,
     "conditions_json": {"all": [{"field": "urgency", "op": "eq", "value": "expedited"}]},
     "target_queue_id": "q-exp", "target_queue_name": "Expedited", "assignment_strategy": "specialty_match"},
    {"routing_rule_id": "r-bh", "name": "Behavioral health", "priority": 20, "is_active": True,
     "service_type": "behavioral_health", "conditions_json": {},
     "target_queue_id": "q-bh", "target_queue_name": "Behavioral Health", "assignment_strategy": "specialty_match"},
    {"routing_rule_id": "r-def", "name": "Default", "priority": 900, "is_active": True,
     "conditions_json": {}, "target_queue_id": "q-clin", "target_queue_name": "Clinical Review",
     "assignment_strategy": "least_loaded", "line_of_business": None, "service_type": None},
]


def test_route_expedited_wins_on_priority():
    # A default rule with empty conditions + no scope never matches (guarded),
    # so an expedited imaging case routes to the expedited queue.
    case = {"urgency": "expedited", "service_type": "imaging"}
    r = wf.route_case(ROUTING_RULES, case)
    assert r["routed"] is True
    assert r["target_queue_name"] == "Expedited"
    assert r["fired_rule"]["routing_rule_id"] == "r-exp"


def test_route_behavioral_health_by_scope():
    case = {"urgency": "standard", "service_type": "behavioral_health"}
    r = wf.route_case(ROUTING_RULES, case)
    assert r["target_queue_name"] == "Behavioral Health"


def test_route_no_match_returns_unrouted():
    # standard imaging: expedited rule fails, BH scope fails, default has no scope/conditions.
    case = {"urgency": "standard", "service_type": "imaging"}
    r = wf.route_case(ROUTING_RULES, case)
    assert r["routed"] is False
    assert r["target_queue_id"] is None


def test_inactive_rules_are_ignored():
    rules = [{**ROUTING_RULES[0], "is_active": False}]
    r = wf.route_case(rules, {"urgency": "expedited"})
    assert r["routed"] is False


# --- balance_recommendation ------------------------------------------------

def test_balance_moves_from_overloaded_to_capacity():
    workloads = [
        {"reviewer_id": "a", "display_name": "A", "active_cases": 60, "max_caseload": 50,
         "available_capacity": -10, "utilization_pct": 120.0},
        {"reviewer_id": "b", "display_name": "B", "active_cases": 20, "max_caseload": 50,
         "available_capacity": 30, "utilization_pct": 40.0},
    ]
    rec = wf.balance_recommendation(workloads, target_utilization=85.0)
    assert rec["overloaded_count"] == 1
    assert rec["underutilized_count"] == 1
    assert rec["rebalanced_cases"] >= 1
    assert rec["moves"][0]["from_name"] == "A"
    assert rec["moves"][0]["to_name"] == "B"


def test_balance_no_moves_when_all_balanced():
    workloads = [
        {"reviewer_id": "a", "display_name": "A", "active_cases": 40, "max_caseload": 50,
         "available_capacity": 10, "utilization_pct": 80.0},
    ]
    rec = wf.balance_recommendation(workloads)
    assert rec["moves"] == []
    assert rec["rebalanced_cases"] == 0


def test_balance_does_not_exceed_destination_capacity():
    workloads = [
        {"reviewer_id": "a", "display_name": "A", "active_cases": 100, "max_caseload": 50,
         "available_capacity": -50, "utilization_pct": 200.0},
        {"reviewer_id": "b", "display_name": "B", "active_cases": 48, "max_caseload": 50,
         "available_capacity": 2, "utilization_pct": 96.0},  # not under target -> no capacity
        {"reviewer_id": "c", "display_name": "C", "active_cases": 45, "max_caseload": 50,
         "available_capacity": 5, "utilization_pct": 90.0},  # also not under 85 -> excluded
    ]
    rec = wf.balance_recommendation(workloads, target_utilization=85.0)
    # No under-utilized reviewers below target with capacity -> no moves.
    assert rec["rebalanced_cases"] == 0


# --- detect_bottlenecks ----------------------------------------------------

def test_bottlenecks_rank_by_severity():
    queues = [
        {"queue_id": "q1", "name": "Q1", "open_cases": 10, "sla_breached": 5, "age_72h_plus": 2, "unassigned_cases": 1},
        {"queue_id": "q2", "name": "Q2", "open_cases": 8, "sla_breached": 0, "age_72h_plus": 1, "unassigned_cases": 0},
        {"queue_id": "q3", "name": "Q3", "open_cases": 0, "sla_breached": 0, "age_72h_plus": 0, "unassigned_cases": 0},
    ]
    ranked = wf.detect_bottlenecks(queues)
    assert ranked[0]["queue_id"] == "q1"           # most breaches -> top
    assert all(b["queue_id"] != "q3" for b in ranked)  # empty queue excluded


# --- triage_stalled --------------------------------------------------------

def test_triage_attaches_action_and_counts():
    cases = [
        {"auth_request_id": "A", "flag_reason": "sla_breached"},
        {"auth_request_id": "B", "flag_reason": "orphaned"},
        {"auth_request_id": "C", "flag_reason": "sla_breached"},
    ]
    out = wf.triage_stalled(cases)
    assert out["total"] == 3
    assert out["by_flag"]["sla_breached"] == 2
    assert out["cases"][0]["recommended_action"]  # non-empty action
