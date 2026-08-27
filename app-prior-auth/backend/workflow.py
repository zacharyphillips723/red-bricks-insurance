"""Workflow Engine & Management (RFI: Workflow Engine & Management tab).

Pure-Python work-management logic that runs over the operational queue:
  - route_case: pick a target queue/role for a case from prioritized, no-code
    routing rules (reuses the same conditions_json shape + evaluator as the
    Business Rules Engine, so business users author routing with no code).
  - balance_recommendation: given reviewer utilization, recommend moving work
    from overloaded reviewers to those with spare capacity (workload balancing).
  - detect_bottlenecks: rank work queues by backlog / aging / SLA breach so
    supervisors see where work is piling up (AI-assisted bottleneck detection).
  - triage_stalled: bucket stalled/orphaned/at-risk cases and suggest an action.

No Databricks imports — fully unit-testable offline. DB access stays in router.
"""

from typing import Any, Iterable

from .rules_engine import rule_matches


def route_case(routing_rules: Iterable[dict], case: dict) -> dict:
    """Evaluate prioritized routing rules against a case; return the first match.

    Each rule carries the pa_business_rules conditions_json shape plus a
    target_queue_id / target_role / assignment_strategy. Only active rules are
    considered, evaluated lowest-priority-number first.
    """
    active = [r for r in routing_rules if r.get("is_active", True)]
    active.sort(key=lambda r: (r.get("priority", 100), str(r.get("name") or "")))

    matched = [r for r in active if rule_matches(r, case)]
    if not matched:
        return {
            "routed": False,
            "target_queue_id": None,
            "target_queue_name": None,
            "target_role": None,
            "assignment_strategy": None,
            "fired_rule": None,
            "matched_rules": [],
        }
    top = matched[0]
    return {
        "routed": True,
        "target_queue_id": top.get("target_queue_id"),
        "target_queue_name": top.get("target_queue_name"),
        "target_role": top.get("target_role"),
        "assignment_strategy": top.get("assignment_strategy") or "least_loaded",
        "fired_rule": {"routing_rule_id": top.get("routing_rule_id"), "name": top.get("name")},
        "matched_rules": [
            {"routing_rule_id": r.get("routing_rule_id"), "name": r.get("name"),
             "target_queue_name": r.get("target_queue_name"), "priority": r.get("priority")}
            for r in matched
        ],
    }


def balance_recommendation(workloads: list[dict], target_utilization: float = 85.0) -> dict:
    """Recommend reallocation from overloaded reviewers to those with capacity.

    workloads: rows shaped like v_workload_balance (reviewer_id, display_name,
    active_cases, max_caseload, available_capacity, utilization_pct).
    Returns per-move suggestions plus a summary. Greedy: pull from the most
    over-utilized, push to the most under-utilized with spare capacity.
    """
    def util(w: dict) -> float:
        u = w.get("utilization_pct")
        if u is not None:
            return float(u)
        cap = w.get("max_caseload") or 0
        return float(w.get("active_cases", 0)) * 100.0 / cap if cap else 0.0

    over = sorted(
        [w for w in workloads if util(w) > target_utilization],
        key=util, reverse=True,
    )
    under = sorted(
        [w for w in workloads if util(w) < target_utilization and (w.get("available_capacity") or 0) > 0],
        key=lambda w: (w.get("available_capacity") or 0), reverse=True,
    )

    # Mutable capacity tracker for the under-utilized pool.
    spare = {w["reviewer_id"]: int(w.get("available_capacity") or 0) for w in under}
    moves: list[dict] = []

    for src in over:
        cap = src.get("max_caseload") or 0
        # Cases above the target utilization line are the movable surplus.
        surplus = int(round(src.get("active_cases", 0) - (target_utilization / 100.0) * cap))
        for dst in under:
            if surplus <= 0:
                break
            avail = spare.get(dst["reviewer_id"], 0)
            if avail <= 0:
                continue
            n = min(surplus, avail)
            if n <= 0:
                continue
            moves.append({
                "from_reviewer_id": src["reviewer_id"],
                "from_name": src.get("display_name"),
                "to_reviewer_id": dst["reviewer_id"],
                "to_name": dst.get("display_name"),
                "cases": n,
                "reason": f"{src.get('display_name')} at {util(src):.0f}% utilization; "
                          f"{dst.get('display_name')} has {avail} open slot(s).",
            })
            spare[dst["reviewer_id"]] -= n
            surplus -= n

    return {
        "overloaded_count": len(over),
        "underutilized_count": len(under),
        "target_utilization_pct": target_utilization,
        "moves": moves,
        "rebalanced_cases": sum(m["cases"] for m in moves),
    }


def detect_bottlenecks(queue_status: list[dict]) -> list[dict]:
    """Rank queues by a bottleneck score (SLA breach + backlog + aging).

    queue_status rows shaped like v_work_queue_status. Returns a ranked list with
    a human-readable reason so supervisors can act on the worst queues first.
    """
    ranked = []
    for q in queue_status:
        open_cases = int(q.get("open_cases") or 0)
        breached = int(q.get("sla_breached") or 0)
        aged = int(q.get("age_72h_plus") or 0)
        unassigned = int(q.get("unassigned_cases") or 0)
        if open_cases == 0:
            continue
        # Weighted score: SLA breaches hurt most, then long-aged, then unassigned.
        score = breached * 3 + aged * 2 + unassigned
        reasons = []
        if breached:
            reasons.append(f"{breached} past SLA")
        if aged:
            reasons.append(f"{aged} open >72h")
        if unassigned:
            reasons.append(f"{unassigned} unassigned")
        if not reasons:
            continue
        ranked.append({
            "queue_id": q.get("queue_id"),
            "name": q.get("name"),
            "open_cases": open_cases,
            "sla_breached": breached,
            "bottleneck_score": score,
            "reason": "; ".join(reasons),
        })
    ranked.sort(key=lambda r: r["bottleneck_score"], reverse=True)
    return ranked


# Recommended action per stalled-case flag (RFI: remediation for stalled/orphaned work).
_STALL_ACTION = {
    "sla_breached": "Escalate to supervisor immediately — regulatory deadline passed.",
    "orphaned": "Auto-assign to least-loaded eligible reviewer.",
    "stalled": "Prompt assigned reviewer; escalate if no action within 24h.",
    "at_risk": "Monitor — approaching SLA window.",
}


def triage_stalled(stalled_cases: list[dict]) -> dict:
    """Summarize stalled/orphaned work and attach a recommended remediation.

    Returns per-case rows enriched with a suggested action + counts by flag, so
    the UI can present an actionable 'stalled work' panel.
    """
    by_flag: dict[str, int] = {}
    enriched = []
    for c in stalled_cases:
        flag = c.get("flag_reason") or "at_risk"
        by_flag[flag] = by_flag.get(flag, 0) + 1
        enriched.append({**c, "recommended_action": _STALL_ACTION.get(flag, "Review manually.")})
    return {
        "total": len(stalled_cases),
        "by_flag": by_flag,
        "cases": enriched,
    }
