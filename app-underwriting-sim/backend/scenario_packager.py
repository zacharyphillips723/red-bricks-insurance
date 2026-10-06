"""Packaged quote scenarios — Standard / Competitive / Retention / Custom.

Phase 1 of the underwriting vision roadmap (see app-underwriting-sim/VISION_ROADMAP.md):
wraps the existing actuarial data layer to produce named renewal pricing scenarios,
each with projected premium, MLR, underwriting margin, and a modeled retention
probability, plus an explainable recommendation that trades margin against retention.

Pure-Python (reuses DataCache) — no warehouse or LLM calls, so it is fast and unit
testable. The retention response is a transparent, documented logistic curve rather
than a black box, to keep the recommendation explainable (NAIC AI alignment).
"""

import math

from .data_loader import DataCache, _safe_float, _safe_int

# Admin / expense load as a % of premium when the caller doesn't supply one.
_DEFAULT_ADMIN_LOAD_PCT = 12.0
# Expected market-wide renewal increase; the retention curve is centered on this.
_DEFAULT_MARKET_TREND_PCT = 7.0
# Minimum acceptable underwriting margin % — the floor for the Retention scenario.
_DEFAULT_MIN_MARGIN_PCT = 3.0


def retention_probability(rate_change_pct: float, market_trend_pct: float) -> float:
    """Transparent logistic retention curve as a function of the rate increase.

    Flat renewal → ~0.96 retention; at the market trend → ~0.87; it falls off as the
    increase outruns the market. Deterministic and documented by design so the
    recommendation stays explainable.
    """
    center = market_trend_pct + 4.0
    floor, ceil, sensitivity = 0.45, 0.97, 0.35
    return floor + (ceil - floor) / (1.0 + math.exp(sensitivity * (rate_change_pct - center)))


def _leg(
    name: str,
    label: str,
    rate_change_pct: float,
    grp_premium: float,
    grp_claims: float,
    admin_load_pct: float,
    market_trend_pct: float,
    rationale: str,
) -> dict:
    proj_premium = grp_premium * (1 + rate_change_pct / 100)
    proj_mlr = (grp_claims / proj_premium * 100) if proj_premium else 0.0
    margin_pct = 100.0 - proj_mlr - admin_load_pct
    margin_dollars = proj_premium * margin_pct / 100.0
    retention = retention_probability(rate_change_pct, market_trend_pct)
    return {
        "name": name,
        "label": label,
        "rate_change_pct": round(rate_change_pct, 2),
        "projected_premium": round(proj_premium, 2),
        "projected_mlr": round(proj_mlr, 2),
        "margin_pct": round(margin_pct, 2),
        "margin_dollars": round(margin_dollars, 2),
        "retention_probability": round(retention, 4),
        "expected_retained_margin": round(margin_dollars * retention, 2),
        "rationale": rationale,
    }


def package_scenarios(cache: DataCache, params: dict) -> dict:
    """Build the four named scenarios for a group renewal + a recommendation.

    Parameters (all optional unless noted):
        group_id (str): group to renew; falls back to book averages if no experience
        lob (str): line of business filter for the book fallback
        target_mlr (float): MLR target for the Standard scenario (default 82)
        admin_load_pct (float): expense load as % of premium (default 12)
        market_trend_pct (float): expected market renewal increase (default 7)
        min_margin_pct (float): margin floor for the Retention scenario (default 3)
        competitor_pmpm (float): competitor's quoted PMPM, drives Competitive
        competitor_within_pct (float): price within N% of competitor (default 2)
        custom_rate_change_pct (float): underwriter-defined rate change
    """
    group_id = params.get("group_id", "") or ""
    lob = params.get("lob")
    target_mlr = _safe_float(params.get("target_mlr"), 82.0)
    admin_load = _safe_float(params.get("admin_load_pct"), _DEFAULT_ADMIN_LOAD_PCT)
    market_trend = _safe_float(params.get("market_trend_pct"), _DEFAULT_MARKET_TREND_PCT)
    min_margin = _safe_float(params.get("min_margin_pct"), _DEFAULT_MIN_MARGIN_PCT)
    competitor_pmpm = params.get("competitor_pmpm")
    competitor_within_pct = _safe_float(params.get("competitor_within_pct"), 2.0)
    custom_rate_change_pct = params.get("custom_rate_change_pct")

    # Current position (renewal). Fall back to book-level if no group experience.
    experience = cache.get_group_experience(group_id) if group_id else []
    if experience:
        grp_premium = sum(_safe_float(r.get("total_premiums")) for r in experience)
        grp_claims = sum(_safe_float(r.get("total_claims_paid")) for r in experience)
        grp_members = sum(_safe_int(r.get("member_count")) for r in experience) or 1
        basis = f"group {group_id} experience"
    else:
        summary = cache.get_baseline_summary(lob=lob)
        grp_premium = summary["total_premium"]
        grp_claims = summary["total_claims"]
        grp_members = summary["total_members"] or 1
        basis = "book-of-business averages (no group experience found)"

    current_mlr = (grp_claims / grp_premium * 100) if grp_premium else 0.0

    # Standard — rate change required to hit the target MLR.
    required_premium = (grp_claims / (target_mlr / 100)) if target_mlr else grp_premium
    r_standard = ((required_premium / grp_premium) - 1) * 100 if grp_premium else 0.0

    # Competitive — match a competitor within N%, else undercut Standard by 3 points.
    if competitor_pmpm is not None:
        competitor_premium = _safe_float(competitor_pmpm) * grp_members * 12
        r_match = (((competitor_premium / grp_premium) - 1) * 100) if grp_premium else r_standard
        r_competitive = r_match + competitor_within_pct
        comp_rationale = f"Matches the competitor quote within {competitor_within_pct:.0f}%."
    else:
        r_competitive = r_standard - 3.0
        comp_rationale = "Undercuts the Standard rate by 3 points to stay competitive."

    # Retention — lowest increase that still clears the margin floor, capped at Standard.
    denom = 1 - (min_margin + admin_load) / 100
    premium_at_floor = (grp_claims / denom) if denom > 0 else required_premium
    r_floor = ((premium_at_floor / grp_premium) - 1) * 100 if grp_premium else 0.0
    r_retention = min(r_standard, max(r_floor, 0.0))

    # Custom — underwriter-defined, defaulting to Standard when unspecified.
    r_custom = (
        _safe_float(custom_rate_change_pct, r_standard)
        if custom_rate_change_pct is not None else r_standard
    )

    legs = [
        _leg("standard", "Standard — hit margin target", r_standard, grp_premium, grp_claims,
             admin_load, market_trend, f"Prices to the {target_mlr:.0f}% target MLR."),
        _leg("competitive", "Competitive — win the deal", r_competitive, grp_premium, grp_claims,
             admin_load, market_trend, comp_rationale),
        _leg("retention", "Retention — keep the group", r_retention, grp_premium, grp_claims,
             admin_load, market_trend,
             f"Lowest increase that still clears the {min_margin:.0f}% margin floor."),
        _leg("custom", "Custom — underwriter-defined", r_custom, grp_premium, grp_claims,
             admin_load, market_trend, "Underwriter-specified rate change."),
    ]

    # Recommendation: maximize expected retained margin (margin$ × retention probability).
    recommended = max(legs, key=lambda l: l["expected_retained_margin"])
    standard_leg = legs[0]
    rec = (
        f"Recommended: {recommended['label']} at {recommended['rate_change_pct']:+.1f}% "
        f"({recommended['projected_mlr']:.1f}% MLR, {recommended['margin_pct']:.1f}% margin, "
        f"{recommended['retention_probability'] * 100:.0f}% retention). "
    )
    if recommended["name"] != "standard":
        rec += (
            f"It beats Standard on expected retained margin "
            f"(${recommended['expected_retained_margin']:,.0f} vs "
            f"${standard_leg['expected_retained_margin']:,.0f}) once "
            f"{recommended['retention_probability'] * 100:.0f}% vs "
            f"{standard_leg['retention_probability'] * 100:.0f}% retention is priced in."
        )
    else:
        rec += "Standard pricing maximizes expected retained margin here."

    return {
        "group_id": group_id,
        "lob": lob,
        "basis": basis,
        "current_premium": round(grp_premium, 2),
        "current_claims": round(grp_claims, 2),
        "current_mlr": round(current_mlr, 2),
        "member_count": int(grp_members),
        "target_mlr": target_mlr,
        "admin_load_pct": admin_load,
        "scenarios": legs,
        "recommended_scenario": recommended["name"],
        "recommendation_narrative": rec,
    }
