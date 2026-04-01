"""Pure-Python simulation engine for underwriting what-if analysis.

Each simulation function takes the DataCache and a parameters dict, then returns
a standardised result dict with baseline, projected, delta, delta_pct, narrative,
and warnings.  All math is in-memory (<100 ms) — no Spark or warehouse queries
beyond the initial data_loader cache hits.
"""

from typing import Optional

from .data_loader import DataCache, _safe_float, _safe_int


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _lob(row: dict) -> str:
    """Extract LOB from a row, handling both column naming conventions."""
    return row.get("line_of_business") or row.get("lob") or "Unknown"


def _pct_change(old: float, new: float) -> float:
    """Return percentage change, safe for zero denominator."""
    return ((new - old) / old * 100) if old else 0.0


def _build_result(
    baseline: dict[str, float],
    projected: dict[str, float],
    narrative: str,
    warnings: list[str] | None = None,
) -> dict:
    delta = {k: projected.get(k, 0) - baseline.get(k, 0) for k in baseline}
    delta_pct = {k: _pct_change(baseline[k], projected.get(k, 0)) for k in baseline}
    return {
        "baseline": baseline,
        "projected": projected,
        "delta": delta,
        "delta_pct": delta_pct,
        "narrative": narrative,
        "warnings": warnings or [],
    }


# ---------------------------------------------------------------------------
# 1. Premium Rate Change
# ---------------------------------------------------------------------------

def simulate_premium_rate(cache: DataCache, params: dict) -> dict:
    """Adjust premium by a percentage and observe MLR impact.

    Parameters:
        rate_change_pct (float): e.g. 5.0 for a +5 % rate increase
        lob (str, optional): filter to a single line of business
    """
    rate_pct = _safe_float(params.get("rate_change_pct"), 0.0)
    lob = params.get("lob")
    summary = cache.get_baseline_summary(lob=lob)

    base_premium = summary["total_premium"]
    base_claims = summary["total_claims"]
    base_mlr = summary["overall_mlr"]

    proj_premium = base_premium * (1 + rate_pct / 100)
    proj_mlr = (base_claims / proj_premium * 100) if proj_premium else 0

    baseline = {"total_premium": base_premium, "total_claims": base_claims, "mlr": base_mlr}
    projected = {"total_premium": proj_premium, "total_claims": base_claims, "mlr": proj_mlr}

    direction = "increase" if rate_pct > 0 else "decrease"
    narrative = (
        f"A {abs(rate_pct):.1f}% premium rate {direction} moves total premium from "
        f"${base_premium:,.0f} to ${proj_premium:,.0f}. MLR shifts from "
        f"{base_mlr:.1f}% to {proj_mlr:.1f}%."
    )
    warnings = []
    if proj_mlr > 85:
        warnings.append("Projected MLR exceeds 85% — ACA rebate threshold risk.")
    if proj_mlr < 70:
        warnings.append("Projected MLR below 70% — may face regulatory scrutiny.")

    return _build_result(baseline, projected, narrative, warnings)


# ---------------------------------------------------------------------------
# 2. Benefit Design Change
# ---------------------------------------------------------------------------

def simulate_benefit_design(cache: DataCache, params: dict) -> dict:
    """Model impact of deductible / copay / coinsurance changes.

    Parameters:
        lob (str): line of business to pull benefit structure
        deductible_change_pct (float): % change to deductible
        copay_change_pct (float): % change to copay
        coinsurance_change_pct (float): % change to coinsurance
        elasticity_factor (float, optional): demand elasticity (default 0.15)
    """
    lob = params.get("lob", "Commercial")
    ded_pct = _safe_float(params.get("deductible_change_pct"), 0.0)
    copay_pct = _safe_float(params.get("copay_change_pct"), 0.0)
    coins_pct = _safe_float(params.get("coinsurance_change_pct"), 0.0)
    elasticity = _safe_float(params.get("elasticity_factor"), 0.15)

    benefits = cache.get_benefits_by_lob(lob)
    summary = cache.get_baseline_summary(lob=lob)

    # Use benefit-level elasticity if available
    if benefits:
        row = benefits[0]
        elasticity = _safe_float(row.get("elasticity_factor"), elasticity)

    base_claims = summary["total_claims"]
    base_premium = summary["total_premium"]
    base_mlr = summary["overall_mlr"]

    # Weighted cost-sharing shift → utilization response
    cost_sharing_shift = (ded_pct * 0.4 + copay_pct * 0.35 + coins_pct * 0.25) / 100
    utilization_impact = -cost_sharing_shift * elasticity
    proj_claims = base_claims * (1 + utilization_impact)
    proj_mlr = (proj_claims / base_premium * 100) if base_premium else 0

    baseline = {"total_claims": base_claims, "total_premium": base_premium, "mlr": base_mlr}
    projected = {"total_claims": proj_claims, "total_premium": base_premium, "mlr": proj_mlr}

    narrative = (
        f"Benefit design changes (deductible {ded_pct:+.1f}%, copay {copay_pct:+.1f}%, "
        f"coinsurance {coins_pct:+.1f}%) with elasticity {elasticity:.2f} shift claims "
        f"from ${base_claims:,.0f} to ${proj_claims:,.0f}. MLR moves from "
        f"{base_mlr:.1f}% to {proj_mlr:.1f}%."
    )
    warnings = []
    if abs(utilization_impact) > 0.10:
        warnings.append("Large utilization swing (>10%) — consider phased rollout.")

    return _build_result(baseline, projected, narrative, warnings)


# ---------------------------------------------------------------------------
# 3. Group Renewal Pricing
# ---------------------------------------------------------------------------

def simulate_group_renewal(cache: DataCache, params: dict) -> dict:
    """Credibility-weighted renewal pricing for a specific group.

    Parameters:
        group_id (str): group identifier
        manual_rate_change_pct (float): manual rate adjustment
        credibility_weight (float, optional): 0-1, default auto from data
    """
    group_id = params.get("group_id", "")
    manual_pct = _safe_float(params.get("manual_rate_change_pct"), 0.0)
    cred_override = params.get("credibility_weight")

    experience = cache.get_group_experience(group_id)
    renewal = cache.get_group_renewal(group_id)
    summary = cache.get_baseline_summary()

    # Group experience
    grp_premium = sum(_safe_float(r.get("total_premiums")) for r in experience)
    grp_claims = sum(_safe_float(r.get("total_claims_paid")) for r in experience)
    grp_mlr = (grp_claims / grp_premium * 100) if grp_premium else summary["overall_mlr"]

    # Credibility: use data if available, else default by member count
    grp_members = sum(_safe_int(r.get("member_count")) for r in experience) or 1
    if cred_override is not None:
        credibility = _safe_float(cred_override, 0.5)
    elif renewal:
        credibility = _safe_float(renewal[0].get("credibility_factor"), 0.5)
    else:
        credibility = min(grp_members / 1000, 1.0)

    book_mlr = summary["overall_mlr"]
    blended_mlr = credibility * grp_mlr + (1 - credibility) * book_mlr
    proj_premium = grp_premium * (1 + manual_pct / 100)
    proj_mlr = (grp_claims / proj_premium * 100) if proj_premium else 0

    baseline = {
        "group_premium": grp_premium,
        "group_claims": grp_claims,
        "group_mlr": grp_mlr,
        "credibility": credibility,
        "blended_mlr": blended_mlr,
    }
    projected = {
        "group_premium": proj_premium,
        "group_claims": grp_claims,
        "group_mlr": proj_mlr,
        "credibility": credibility,
        "blended_mlr": blended_mlr,
    }

    narrative = (
        f"Group {group_id}: credibility-weighted MLR is {blended_mlr:.1f}% "
        f"(credibility={credibility:.2f}). A {manual_pct:+.1f}% manual rate adjustment "
        f"moves premium from ${grp_premium:,.0f} to ${proj_premium:,.0f}, "
        f"shifting group MLR from {grp_mlr:.1f}% to {proj_mlr:.1f}%."
    )
    warnings = []
    if credibility < 0.3:
        warnings.append("Low credibility — group experience has limited predictive value.")
    if proj_mlr > 90:
        warnings.append("Projected group MLR exceeds 90% — potential loss.")

    return _build_result(baseline, projected, narrative, warnings)


# ---------------------------------------------------------------------------
# 4. Population Mix Shift
# ---------------------------------------------------------------------------

def simulate_population_mix(cache: DataCache, params: dict) -> dict:
    """Model impact of enrollment shifts across lines of business.

    Parameters:
        mix_changes (dict[str, float]): LOB → member count change
            e.g. {"Commercial": -500, "Medicare Advantage": 300}
    """
    mix_changes: dict = params.get("mix_changes", {})
    summary = cache.get_baseline_summary()
    pmpm_by_lob = summary["pmpm_by_lob"]
    members_by_lob = summary["member_count_by_lob"]

    base_members = summary["total_members"]
    base_premium = summary["total_premium"]
    base_claims = summary["total_claims"]

    # Apply member shifts
    proj_members_by_lob = {lob: count for lob, count in members_by_lob.items()}
    for lob, delta in mix_changes.items():
        proj_members_by_lob[lob] = proj_members_by_lob.get(lob, 0) + _safe_int(delta)

    proj_total_members = sum(proj_members_by_lob.values())

    # Weighted average PMPM
    base_weighted_pmpm = base_claims / (summary["total_member_months"] or 1)
    proj_weighted_claims = sum(
        proj_members_by_lob.get(lob, 0) * 12 * _safe_float(pmpm)
        for lob, pmpm in pmpm_by_lob.items()
    )
    proj_claims = proj_weighted_claims if proj_weighted_claims > 0 else base_claims

    # Scale premium proportionally to member change
    member_ratio = proj_total_members / base_members if base_members else 1
    proj_premium = base_premium * member_ratio
    proj_mlr = (proj_claims / proj_premium * 100) if proj_premium else 0

    baseline = {
        "total_members": float(base_members),
        "total_premium": base_premium,
        "total_claims": base_claims,
        "mlr": summary["overall_mlr"],
    }
    projected = {
        "total_members": float(proj_total_members),
        "total_premium": proj_premium,
        "total_claims": proj_claims,
        "mlr": proj_mlr,
    }

    changes_str = ", ".join(f"{lob}: {delta:+,}" for lob, delta in mix_changes.items())
    narrative = (
        f"Population mix shift ({changes_str}) moves total membership from "
        f"{base_members:,} to {proj_total_members:,}. Projected claims shift to "
        f"${proj_claims:,.0f} based on LOB-weighted PMPM."
    )

    return _build_result(baseline, projected, narrative)


# ---------------------------------------------------------------------------
# 5. Medical Trend Sensitivity
# ---------------------------------------------------------------------------

def simulate_medical_trend(cache: DataCache, params: dict) -> dict:
    """Project 12-month claims trajectory under a specified trend rate.

    Parameters:
        annual_trend_pct (float): annual medical trend, e.g. 7.0
        months (int, optional): projection horizon (default 12)
        lob (str, optional): filter to LOB
    """
    trend_pct = _safe_float(params.get("annual_trend_pct"), 7.0)
    months = _safe_int(params.get("months"), 12)
    lob = params.get("lob")
    summary = cache.get_baseline_summary(lob=lob)

    base_claims = summary["total_claims"]
    base_premium = summary["total_premium"]
    base_mlr = summary["overall_mlr"]

    monthly_trend = (1 + trend_pct / 100) ** (1 / 12) - 1
    proj_claims = base_claims * (1 + monthly_trend) ** months
    proj_mlr = (proj_claims / base_premium * 100) if base_premium else 0

    baseline = {"total_claims": base_claims, "total_premium": base_premium, "mlr": base_mlr}
    projected = {"total_claims": proj_claims, "total_premium": base_premium, "mlr": proj_mlr}

    narrative = (
        f"At {trend_pct:.1f}% annual medical trend over {months} months, claims grow "
        f"from ${base_claims:,.0f} to ${proj_claims:,.0f}. MLR rises from "
        f"{base_mlr:.1f}% to {proj_mlr:.1f}%."
    )
    warnings = []
    if proj_mlr > 85:
        warnings.append("Trend-adjusted MLR exceeds 85% rebate threshold.")
    if trend_pct > 10:
        warnings.append("Trend rate above 10% — verify with actuarial team.")

    return _build_result(baseline, projected, narrative, warnings)


# ---------------------------------------------------------------------------
# 6. Stop-Loss Threshold Change
# ---------------------------------------------------------------------------

def simulate_stop_loss(cache: DataCache, params: dict) -> dict:
    """Recalculate stop-loss exposure from member-level TCOC.

    Parameters:
        group_id (str): group to analyze
        current_threshold (float): current specific stop-loss attachment point
        new_threshold (float): proposed attachment point
    """
    group_id = params.get("group_id", "")
    current_thresh = _safe_float(params.get("current_threshold"), 250_000)
    new_thresh = _safe_float(params.get("new_threshold"), 300_000)

    member_tcoc = cache.get_member_tcoc_by_group(group_id)
    stop_loss_data = cache.get_group_stop_loss(group_id)

    # Calculate claims above threshold
    base_excess = sum(
        max(_safe_float(r.get("actual_cost")) - current_thresh, 0)
        for r in member_tcoc
    )
    proj_excess = sum(
        max(_safe_float(r.get("actual_cost")) - new_thresh, 0)
        for r in member_tcoc
    )
    base_claimants = sum(
        1 for r in member_tcoc if _safe_float(r.get("actual_cost")) > current_thresh
    )
    proj_claimants = sum(
        1 for r in member_tcoc if _safe_float(r.get("actual_cost")) > new_thresh
    )

    total_members = len(member_tcoc) or 1

    baseline = {
        "threshold": current_thresh,
        "excess_claims": base_excess,
        "claimants_above": float(base_claimants),
        "pct_above": base_claimants / total_members * 100,
    }
    projected = {
        "threshold": new_thresh,
        "excess_claims": proj_excess,
        "claimants_above": float(proj_claimants),
        "pct_above": proj_claimants / total_members * 100,
    }

    narrative = (
        f"Moving stop-loss threshold from ${current_thresh:,.0f} to ${new_thresh:,.0f} "
        f"for group {group_id}: excess claims shift from ${base_excess:,.0f} "
        f"({base_claimants} claimants) to ${proj_excess:,.0f} ({proj_claimants} claimants)."
    )
    warnings = []
    if proj_excess > base_excess * 1.5:
        warnings.append("Excess claims increase >50% — significant added risk.")
    if not member_tcoc:
        warnings.append("No member TCOC data found — results may be inaccurate.")

    return _build_result(baseline, projected, narrative, warnings)


# ---------------------------------------------------------------------------
# 7. Risk Adjustment / Coding Completeness
# ---------------------------------------------------------------------------

def simulate_risk_adjustment(cache: DataCache, params: dict) -> dict:
    """Model additional RAF capture and its revenue impact.

    Parameters:
        raf_improvement_pct (float): % improvement in RAF score capture
        lob (str, optional): filter (typically 'Medicare Advantage')
    """
    raf_pct = _safe_float(params.get("raf_improvement_pct"), 5.0)
    lob = params.get("lob", "Medicare Advantage")

    risk_data = cache.get_risk_adjustment()
    coding_data = cache.get_coding_completeness()
    summary = cache.get_baseline_summary(lob=lob)

    # Current RAF and revenue
    base_raf = 0.0
    base_revenue = 0.0
    if risk_data:
        for r in risk_data:
            if not lob or _lob(r) == lob:
                base_raf += _safe_float(r.get("avg_raf_score", r.get("raf_score")))
                base_revenue += _safe_float(r.get("risk_adjusted_revenue", r.get("ma_revenue")))
        if not base_raf:
            base_raf = 1.0
            base_revenue = summary["total_premium"]

    proj_raf = base_raf * (1 + raf_pct / 100)
    proj_revenue = base_revenue * (proj_raf / base_raf) if base_raf else base_revenue

    # Coding completeness context
    coding_rate = 0.0
    if coding_data:
        rates = [_safe_float(r.get("completeness_rate", r.get("coding_rate"))) for r in coding_data]
        coding_rate = sum(rates) / len(rates) if rates else 0

    baseline = {
        "raf_score": base_raf,
        "risk_adjusted_revenue": base_revenue,
        "coding_completeness": coding_rate,
    }
    projected = {
        "raf_score": proj_raf,
        "risk_adjusted_revenue": proj_revenue,
        "coding_completeness": min(coding_rate + raf_pct * 0.5, 100),
    }

    revenue_delta = proj_revenue - base_revenue
    narrative = (
        f"A {raf_pct:.1f}% RAF improvement moves the average score from "
        f"{base_raf:.3f} to {proj_raf:.3f}, generating an estimated "
        f"${revenue_delta:,.0f} in additional risk-adjusted revenue."
    )
    warnings = []
    if raf_pct > 15:
        warnings.append("RAF improvement >15% is aggressive — ensure clinical documentation supports it.")
    if coding_rate and coding_rate < 70:
        warnings.append(f"Current coding completeness is {coding_rate:.0f}% — significant upside opportunity.")

    return _build_result(baseline, projected, narrative, warnings)


# ---------------------------------------------------------------------------
# 8. Utilization Change by Category
# ---------------------------------------------------------------------------

def simulate_utilization_change(cache: DataCache, params: dict) -> dict:
    """Model PMPM impact from category-level utilization changes.

    Parameters:
        changes (dict[str, float]): category → % utilization change
            e.g. {"Inpatient": -5.0, "Outpatient": 3.0, "Pharmacy": 8.0}
        lob (str, optional): filter to LOB
    """
    changes: dict = params.get("changes", {})
    lob = params.get("lob")

    utilization = cache.get_utilization()
    summary = cache.get_baseline_summary(lob=lob)

    base_claims = summary["total_claims"]
    base_premium = summary["total_premium"]
    base_mlr = summary["overall_mlr"]
    member_months = summary["total_member_months"] or 1

    # Build per-category PMPM from utilization data
    category_pmpm: dict[str, float] = {}
    for r in utilization:
        if lob and _lob(r) != lob:
            continue
        cat = r.get("service_category", r.get("category", "Other"))
        pmpm = _safe_float(r.get("pmpm", r.get("cost_per_1000")))
        if pmpm and r.get("cost_per_1000"):
            pmpm = _safe_float(r.get("cost_per_1000")) / 12
        category_pmpm[cat] = category_pmpm.get(cat, 0) + pmpm

    # Apply changes
    total_impact = 0.0
    for cat, pct in changes.items():
        cat_pmpm = category_pmpm.get(cat, 0)
        if not cat_pmpm:
            # Estimate from total if category not found
            cat_pmpm = base_claims / member_months * 0.2  # assume ~20% per major category
        total_impact += cat_pmpm * (pct / 100) * member_months

    proj_claims = base_claims + total_impact
    proj_mlr = (proj_claims / base_premium * 100) if base_premium else 0

    baseline = {"total_claims": base_claims, "total_premium": base_premium, "mlr": base_mlr}
    projected = {"total_claims": proj_claims, "total_premium": base_premium, "mlr": proj_mlr}

    changes_str = ", ".join(f"{cat}: {pct:+.1f}%" for cat, pct in changes.items())
    narrative = (
        f"Utilization changes ({changes_str}) shift total claims from "
        f"${base_claims:,.0f} to ${proj_claims:,.0f}. MLR moves from "
        f"{base_mlr:.1f}% to {proj_mlr:.1f}%."
    )

    return _build_result(baseline, projected, narrative)


# ---------------------------------------------------------------------------
# 9. New Group Quote
# ---------------------------------------------------------------------------

def simulate_new_group_quote(cache: DataCache, params: dict) -> dict:
    """Price a new group based on peer-group experience.

    Parameters:
        proposed_members (int): expected member count
        lob (str): line of business
        age_band (str, optional): e.g. "35-44"
        industry (str, optional): SIC/industry code
        target_mlr (float, optional): desired MLR target (default 82)
    """
    proposed_members = _safe_int(params.get("proposed_members"), 100)
    lob = params.get("lob", "Commercial")
    target_mlr = _safe_float(params.get("target_mlr"), 82.0)

    summary = cache.get_baseline_summary(lob=lob)
    pmpm_data = cache.get_pmpm()

    # Find LOB PMPM
    lob_pmpm = 0.0
    for r in pmpm_data:
        if _lob(r) == lob:
            lob_pmpm = _safe_float(r.get("pmpm_paid"))
            break
    if not lob_pmpm:
        lob_pmpm = summary["total_claims"] / (summary["total_member_months"] or 1)

    projected_annual_claims = lob_pmpm * proposed_members * 12
    required_premium = (projected_annual_claims / target_mlr * 100) if target_mlr else 0
    required_pmpm = required_premium / (proposed_members * 12) if proposed_members else 0

    baseline = {
        "book_pmpm": lob_pmpm,
        "book_mlr": summary["overall_mlr"],
        "total_members": float(summary["total_members"]),
    }
    projected = {
        "quoted_pmpm": required_pmpm,
        "projected_annual_claims": projected_annual_claims,
        "required_annual_premium": required_premium,
        "target_mlr": target_mlr,
        "projected_members": float(proposed_members),
    }

    narrative = (
        f"New group quote for {proposed_members} members in {lob}: "
        f"at book PMPM of ${lob_pmpm:,.2f}, projected annual claims are "
        f"${projected_annual_claims:,.0f}. To hit {target_mlr:.0f}% target MLR, "
        f"required premium is ${required_premium:,.0f} (${required_pmpm:,.2f} PMPM)."
    )

    return _build_result(baseline, projected, narrative)


# ---------------------------------------------------------------------------
# 10. IBNR Reserve Adequacy
# ---------------------------------------------------------------------------

def simulate_ibnr_reserve(cache: DataCache, params: dict) -> dict:
    """Shift completion factors and observe IBNR reserve impact.

    Parameters:
        completion_factor_shift_pct (float): % shift to completion factors
            Positive = claims develop faster (lower IBNR), Negative = slower
        lob (str, optional): filter to LOB
    """
    cf_shift = _safe_float(params.get("completion_factor_shift_pct"), 0.0)
    lob = params.get("lob")

    cf_data = cache.get_ibnr_completion_factors()
    triangle = cache.get_ibnr_triangle()
    summary = cache.get_baseline_summary(lob=lob)

    base_claims = summary["total_claims"]

    # Compute base IBNR from completion factors
    base_ibnr = 0.0
    base_cf_avg = 0.0
    if cf_data:
        cfs = []
        for r in cf_data:
            if lob and _lob(r) != "Unknown" and _lob(r) != lob:
                continue
            cf = _safe_float(r.get("completion_factor", r.get("cf")))
            paid = _safe_float(r.get("paid_to_date", r.get("paid_claims")))
            if cf and cf < 1.0:
                ultimate = paid / cf if cf else paid
                base_ibnr += ultimate - paid
                cfs.append(cf)
        base_cf_avg = sum(cfs) / len(cfs) if cfs else 0.85

    if not base_ibnr:
        # Fallback: estimate IBNR as ~8% of claims
        base_ibnr = base_claims * 0.08
        base_cf_avg = 0.92

    # Shift completion factors
    proj_cf_avg = min(base_cf_avg * (1 + cf_shift / 100), 0.999)
    # Higher CF → lower IBNR (claims develop faster)
    if base_cf_avg > 0:
        proj_ibnr = base_ibnr * (1 - proj_cf_avg) / (1 - base_cf_avg) if (1 - base_cf_avg) else base_ibnr
    else:
        proj_ibnr = base_ibnr

    total_reserves = base_claims + base_ibnr
    proj_total_reserves = base_claims + proj_ibnr

    baseline = {
        "paid_claims": base_claims,
        "ibnr_reserve": base_ibnr,
        "total_reserves": total_reserves,
        "avg_completion_factor": base_cf_avg,
    }
    projected = {
        "paid_claims": base_claims,
        "ibnr_reserve": proj_ibnr,
        "total_reserves": proj_total_reserves,
        "avg_completion_factor": proj_cf_avg,
    }

    direction = "faster" if cf_shift > 0 else "slower"
    narrative = (
        f"Shifting completion factors {abs(cf_shift):.1f}% {direction} "
        f"(avg CF {base_cf_avg:.3f} → {proj_cf_avg:.3f}) moves IBNR reserves from "
        f"${base_ibnr:,.0f} to ${proj_ibnr:,.0f}. Total reserves shift from "
        f"${total_reserves:,.0f} to ${proj_total_reserves:,.0f}."
    )
    warnings = []
    reserve_change_pct = abs(_pct_change(base_ibnr, proj_ibnr))
    if reserve_change_pct > 20:
        warnings.append(f"IBNR change of {reserve_change_pct:.0f}% is material — actuarial review recommended.")
    if proj_ibnr < 0:
        warnings.append("Projected IBNR is negative — completion factors may be unrealistic.")

    return _build_result(baseline, projected, narrative, warnings)


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

SIMULATION_FUNCTIONS = {
    "premium_rate": simulate_premium_rate,
    "benefit_design": simulate_benefit_design,
    "group_renewal": simulate_group_renewal,
    "population_mix": simulate_population_mix,
    "medical_trend": simulate_medical_trend,
    "stop_loss": simulate_stop_loss,
    "risk_adjustment": simulate_risk_adjustment,
    "utilization_change": simulate_utilization_change,
    "new_group_quote": simulate_new_group_quote,
    "ibnr_reserve": simulate_ibnr_reserve,
}


def run_simulation(cache: DataCache, simulation_type: str, params: dict) -> dict:
    """Dispatch to the appropriate simulation function.

    Returns the standardised result dict (baseline, projected, delta, delta_pct,
    narrative, warnings).

    Raises:
        ValueError: if simulation_type is not recognized
    """
    fn = SIMULATION_FUNCTIONS.get(simulation_type)
    if not fn:
        raise ValueError(
            f"Unknown simulation type '{simulation_type}'. "
            f"Valid types: {', '.join(SIMULATION_FUNCTIONS.keys())}"
        )
    return fn(cache, params)
