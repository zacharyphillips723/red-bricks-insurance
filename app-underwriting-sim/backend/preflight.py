"""Pre-implementation check — the Underwriting Digital Twin loop (Phase 4, optional).

Sits as an advisory gate between scenario selection and approve/implement. Before an
analyst commits a rating/underwriting policy it: recalls institutional precedent +
the analyst's own history, projects the dollar impact, evaluates deterministic
actuary-owned guardrails, asks the agent for a grounded critique that must cite the
specific precedent rows it reasons from, returns a GREEN/AMBER/RED verdict, and
writes the candidate decision back to memory so the twin tracks the team over time.

Human-in-the-loop by design — on million-dollar decisions it checks, it does not decide.
"""

from typing import Optional

from .approvals import evaluate_authority
from .data_loader import DataCache, _safe_float
from .llm import complete_json
from .simulation_engine import run_simulation
from .twin_memory import (
    build_context_text,
    record_decision,
    recall_semantic,
    recall_analyst,
    list_guardrails,
    get_profile,
    update_analyst_profile,
    size_band,
)


def _derive_impact(cache: DataCache, policy: dict) -> dict:
    """Ground the financial impact: run the sim engine when we have enough levers,
    else trust the projected numbers the caller supplied."""
    params = policy.get("parameters") or {}
    projected_margin = _safe_float(policy.get("projected_margin"), None) if policy.get("projected_margin") is not None else None
    projected_mlr = _safe_float(policy.get("projected_mlr"), None) if policy.get("projected_mlr") is not None else None
    dollar_impact = policy.get("dollar_impact")

    rate_change = _rate_change(policy)
    sim = None
    try:
        if policy.get("group_id") and rate_change:
            sim = run_simulation(cache, "group_renewal", {
                "group_id": policy["group_id"],
                "manual_rate_change_pct": rate_change,
            })
        elif rate_change:
            sim = run_simulation(cache, "premium_rate", {
                "rate_change_pct": rate_change,
                "lob": policy.get("lob"),
            })
    except Exception as e:  # pragma: no cover - defensive
        print(f"[preflight] impact simulation failed: {e}")

    if sim:
        proj = sim.get("projected", {})
        base = sim.get("baseline", {})
        if projected_mlr is None:
            projected_mlr = proj.get("mlr") or proj.get("group_mlr")
        # Dollar impact = premium delta vs baseline when available.
        prem_keys = ("total_premium", "group_premium")
        for k in prem_keys:
            if k in proj and k in base and dollar_impact is None:
                dollar_impact = proj[k] - base[k]
                break

    return {
        "projected_margin": projected_margin,
        "projected_mlr": projected_mlr,
        "dollar_impact": _safe_float(dollar_impact, 0.0),
        "rate_change_pct": rate_change,
        "simulation": sim,
    }


def _rate_change(policy: dict) -> float:
    params = policy.get("parameters") or {}
    for k in ("rate_change_pct", "manual_rate_change_pct", "requested_rate_action"):
        if params.get(k) is not None:
            return _safe_float(params.get(k), 0.0)
    if policy.get("rate_change_pct") is not None:
        return _safe_float(policy.get("rate_change_pct"), 0.0)
    return 0.0


def _eval_guardrails(guardrails: list[dict], impact: dict) -> list[dict]:
    """Deterministic guardrail evaluation (actuary-owned; the LLM never decides these)."""
    margin = impact.get("projected_margin")
    mlr = impact.get("projected_mlr")
    dollars = abs(_safe_float(impact.get("dollar_impact"), 0.0))
    results = []
    for g in guardrails:
        rule = g.get("rule_type")
        thr = _safe_float(g.get("threshold"), 0.0)
        tripped = False
        if rule == "margin_floor" and margin is not None:
            tripped = margin < thr
        elif rule == "mlr_ceiling" and mlr is not None:
            tripped = mlr > thr
        elif rule == "authority_limit":
            tripped = dollars > thr
        note = (g.get("scope") or {}).get("note") if isinstance(g.get("scope"), dict) else None
        results.append({
            "rule_type": rule,
            "threshold": thr,
            "severity": g.get("severity", "warn"),
            "tripped": tripped,
            "note": note,
        })
    return results


def _grounded_critique(policy: dict, precedent: list[dict], impact: dict) -> dict:
    """Ask the agent to critique the decision, grounded in cited precedent rows."""
    cited = []
    for p in precedent[:6]:
        oc = p.get("outcome") or {}
        cited.append({
            "decision_id": p.get("decision_id"),
            "arrangement": p.get("funding_arrangement"),
            "size_band": p.get("group_size_band"),
            "scenario": p.get("scenario_chosen"),
            "projected_margin": p.get("projected_margin"),
            "projected_mlr": p.get("projected_mlr"),
            "actual_mlr": oc.get("actual_mlr"),
            "retained": oc.get("retained"),
            "rationale": p.get("rationale"),
        })
    system = (
        "You are an underwriting digital twin providing an advisory pre-implementation check. "
        "Critique the proposed decision against the cited precedent and surface dissent or "
        "uncertainty — do not just agree. You MUST ground every claim in the provided precedent "
        "rows and reference their decision_id. If precedent is thin, say so. "
        'Respond as {"concern_level": "none|caution|high", "critique": "...", '
        '"cited_decision_ids": ["..."]}'
    )
    user = (
        f"Proposed decision: {build_context_text(policy)}\n"
        f"Projected margin: {impact.get('projected_margin')}%, MLR: {impact.get('projected_mlr')}%, "
        f"dollar impact: {impact.get('dollar_impact')}\n\n"
        f"Precedent rows: {cited}"
    )
    parsed = complete_json(system, user, max_tokens=700)
    if not isinstance(parsed, dict) or parsed.get("_error"):
        return {
            "concern_level": "none",
            "critique": "Grounded critique unavailable (model endpoint error); relying on "
                        "deterministic guardrails and recalled precedent only.",
            "cited_decision_ids": [c["decision_id"] for c in cited[:3]],
        }
    parsed.setdefault("concern_level", "none")
    parsed.setdefault("critique", "")
    parsed.setdefault("cited_decision_ids", [])
    return parsed


def _verdict(guardrail_results: list[dict], concern_level: str) -> str:
    if any(r["tripped"] and r["severity"] == "block" for r in guardrail_results):
        return "RED"
    if any(r["tripped"] and r["severity"] == "warn" for r in guardrail_results):
        return "AMBER"
    if concern_level in ("caution", "high"):
        return "AMBER"
    return "GREEN"


async def preflight_check(session, cache: DataCache, policy: dict) -> dict:
    """Run the full advisory pre-implementation check and persist the candidate decision."""
    analyst_id = policy.get("analyst_id") or "underwriter"
    band = policy.get("group_size_band") or size_band(policy.get("group_size"))
    policy = {**policy, "group_size_band": band}
    cohort = {
        "funding_arrangement": policy.get("funding_arrangement"),
        "lob": policy.get("lob"),
        "group_size_band": band,
    }

    # 1-2. Impact + context
    impact = _derive_impact(cache, policy)
    policy["projected_margin"] = impact["projected_margin"]
    policy["projected_mlr"] = impact["projected_mlr"]
    policy["dollar_impact"] = impact["dollar_impact"]
    context_text = build_context_text(policy)

    # 3. Recall (semantic cohort precedent + this analyst's history)
    precedent = await recall_semantic(session, context_text, cohort)
    analyst_history = await recall_analyst(session, analyst_id)
    profile = await get_profile(session, analyst_id)

    # 4. Guardrails (deterministic)
    guardrails = await list_guardrails(session, active_only=True)
    guardrail_results = _eval_guardrails(guardrails, impact)

    # 5. Grounded critique
    critique = _grounded_critique(policy, precedent, impact)

    # 6. Verdict
    verdict = _verdict(guardrail_results, critique.get("concern_level", "none"))
    required_tier = evaluate_authority(impact["dollar_impact"], impact["rate_change_pct"])

    # 7. Write-back candidate + update profile
    policy["twin_verdict"] = verdict
    decision_id = await record_decision(session, {**policy, "analyst_id": analyst_id, "status": "candidate"})
    await update_analyst_profile(session, analyst_id, policy)

    return {
        "decision_id": decision_id,
        "verdict": verdict,
        "dollar_impact": impact["dollar_impact"],
        "rate_change_pct": impact["rate_change_pct"],
        "projected_margin": impact["projected_margin"],
        "projected_mlr": impact["projected_mlr"],
        "guardrail_results": guardrail_results,
        "critique": critique.get("critique"),
        "concern_level": critique.get("concern_level"),
        "cited_decision_ids": critique.get("cited_decision_ids", []),
        "cited_precedents": precedent,
        "analyst_history_count": len(analyst_history),
        "analyst_profile": profile,
        "required_approval_tier": required_tier,
    }
