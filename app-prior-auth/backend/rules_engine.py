"""No-code Business Rules Engine (RFI: Business Rules Engine + Workflow Engine).

PARALLEL-FIRST: this evaluator runs alongside the existing Tier-1 SQL
(gold_pa_tier1_evaluation) — it does not replace it. Rules are data
(pa_business_rules.conditions_json), authored by business users in the Rules
Studio, so no code change is needed to add/modify adjudication or routing logic.

Pure Python with no Databricks imports so it is fully unit-testable offline.

Condition schema (conditions_json):
    {"all": [ {"field": "...", "op": "...", "value": ...}, ... ],
     "any": [ ... ]}
Both keys optional; "all" clauses must all pass, "any" clauses need one pass.
A request "field" is looked up on the request dict (case-insensitive keys).
Supported ops: eq, ne, in, not_in, gt, gte, lt, lte, contains, exists.
"""

from typing import Any, Iterable

_NUMERIC_OPS = {"gt", "gte", "lt", "lte"}


def _get_field(request: dict, field: str) -> Any:
    if field in request:
        return request[field]
    lowered = {str(k).lower(): v for k, v in request.items()}
    return lowered.get(str(field).lower())


def _as_list(value: Any) -> list:
    if isinstance(value, (list, tuple, set)):
        return list(value)
    if value is None:
        return []
    # pipe/comma-delimited string -> list (matches how codes are stored)
    return [v.strip() for v in str(value).replace("|", ",").split(",") if v.strip()]


def _eval_condition(cond: dict, request: dict) -> bool:
    field = cond.get("field")
    op = (cond.get("op") or "eq").lower()
    target = cond.get("value")
    actual = _get_field(request, field)

    if op == "exists":
        return actual is not None and str(actual) != ""
    if actual is None:
        return op in ("ne", "not_in")

    if op == "eq":
        return str(actual).lower() == str(target).lower()
    if op == "ne":
        return str(actual).lower() != str(target).lower()
    if op == "in":
        return str(actual).lower() in [str(t).lower() for t in _as_list(target)]
    if op == "not_in":
        return str(actual).lower() not in [str(t).lower() for t in _as_list(target)]
    if op == "contains":
        # actual (list or delimited string) contains ANY target value
        actual_items = [a.lower() for a in _as_list(actual)]
        return any(str(t).lower() in actual_items for t in _as_list(target))
    if op in _NUMERIC_OPS:
        try:
            a, t = float(actual), float(target)
        except (TypeError, ValueError):
            return False
        return {"gt": a > t, "gte": a >= t, "lt": a < t, "lte": a <= t}[op]
    return False


def rule_matches(rule: dict, request: dict) -> bool:
    """True if a request satisfies a rule's scope + conditions."""
    # Scope filters (NULL scope = applies to all)
    lob = rule.get("line_of_business")
    svc = rule.get("service_type")
    if lob and str(_get_field(request, "line_of_business") or "").lower() != str(lob).lower():
        return False
    if svc and str(_get_field(request, "service_type") or "").lower() != str(svc).lower():
        return False

    conditions = rule.get("conditions_json") or {}
    all_clauses = conditions.get("all") or []
    any_clauses = conditions.get("any") or []

    if all_clauses and not all(_eval_condition(c, request) for c in all_clauses):
        return False
    if any_clauses and not any(_eval_condition(c, request) for c in any_clauses):
        return False
    # A rule with neither clause set only matches on scope (guard against that).
    if not all_clauses and not any_clauses:
        return bool(lob or svc)
    return True


def evaluate(rules: Iterable[dict], request: dict) -> dict:
    """Evaluate active rules against a request in priority order.

    Returns the first firing rule's decision, plus every rule that matched (so
    the UI can show overlaps). Only status == 'active' rules are considered.
    """
    active = [r for r in rules if (r.get("status") or "active") == "active"]
    active.sort(key=lambda r: (r.get("priority", 100), str(r.get("name") or "")))

    fired = [r for r in active if rule_matches(r, request)]
    if not fired:
        return {"decision": None, "action": None, "matched_rules": [], "fired_rule": None}

    top = fired[0]
    return {
        "decision": top.get("action"),
        "action": top.get("action"),
        "action_detail": top.get("action_detail"),
        "fired_rule": {"rule_id": top.get("rule_id"), "name": top.get("name")},
        "matched_rules": [
            {"rule_id": r.get("rule_id"), "name": r.get("name"),
             "action": r.get("action"), "priority": r.get("priority")}
            for r in fired
        ],
    }


def simulate(rule: dict, historical: list[dict]) -> dict:
    """Simulate a single rule against historical requests (RFI: test/impact
    analysis before deployment). Returns match count + agreement with the
    recorded determination, so a reviewer can gauge a new rule's impact.
    """
    matched = [h for h in historical if rule_matches(rule, h)]
    action = rule.get("action")
    # Map rule action -> the determination it would have produced.
    action_to_determination = {
        "auto_approve": "approved",
        "auto_deny": "denied",
        "pend": "pended",
    }
    would_determine = action_to_determination.get(action)

    agree = disagree = 0
    if would_determine:
        for h in matched:
            actual = str(_get_field(h, "determination") or "").lower()
            if not actual:
                continue
            if actual == would_determine:
                agree += 1
            else:
                disagree += 1

    total = len(historical)
    return {
        "total_evaluated": total,
        "matched": len(matched),
        "match_rate_pct": round(len(matched) * 100.0 / total, 2) if total else 0.0,
        "action": action,
        "would_agree": agree,
        "would_disagree": disagree,
        "agreement_rate_pct": round(agree * 100.0 / (agree + disagree), 2) if (agree + disagree) else None,
        "sample_matches": [
            _get_field(h, "auth_request_id") for h in matched[:10]
            if _get_field(h, "auth_request_id")
        ],
    }


def detect_conflicts(rules: list[dict]) -> list[dict]:
    """Flag active rules with overlapping scope but DIFFERENT actions.

    Overlap heuristic: same (line_of_business, service_type) scope pair (treating
    NULL as a wildcard that overlaps anything) and different actions. Reports the
    higher-priority winner so the author can resolve or reorder.
    """
    active = [r for r in rules if (r.get("status") or "active") == "active"]

    def scope_overlaps(a: dict, b: dict) -> bool:
        for key in ("line_of_business", "service_type"):
            av, bv = a.get(key), b.get(key)
            if av and bv and str(av).lower() != str(bv).lower():
                return False
        return True

    conflicts = []
    for i in range(len(active)):
        for j in range(i + 1, len(active)):
            a, b = active[i], active[j]
            if a.get("action") != b.get("action") and scope_overlaps(a, b):
                winner, loser = sorted([a, b], key=lambda r: r.get("priority", 100))[:2]
                conflicts.append({
                    "rule_a": {"rule_id": a.get("rule_id"), "name": a.get("name"),
                               "action": a.get("action"), "priority": a.get("priority")},
                    "rule_b": {"rule_id": b.get("rule_id"), "name": b.get("name"),
                               "action": b.get("action"), "priority": b.get("priority")},
                    "winner_rule_id": winner.get("rule_id"),
                })
    return conflicts
