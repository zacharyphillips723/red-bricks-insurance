"""Funding-arrangement routing and pricing.

Phase 2 of the underwriting vision roadmap (see app-underwriting-sim/VISION_ROADMAP.md):
a quote intake that branches by funding arrangement and dispatches to the right
calculation. The existing simulation engine already covers fully-insured renewal
math and stop-loss; this module adds the arrangement-specific build-ups
(ASO/self-funded, level-funded, specialty, specific stop-loss, disability) and a
single dispatcher so the funnel can route one input set to the correct calc.

Pure-Python (reuses DataCache) — fast and unit-testable, no warehouse/LLM calls
beyond the baseline cache hits.
"""

from typing import Callable

from .data_loader import DataCache, _safe_float, _safe_int


# ---------------------------------------------------------------------------
# Arrangement catalog (metadata surfaced to the UI)
# ---------------------------------------------------------------------------

FUNDING_ARRANGEMENTS: list[dict] = [
    {
        "key": "fully_insured",
        "label": "Fully Insured (FI)",
        "risk_bearer": "Carrier",
        "description": "Carrier bears 100% of claims risk; employer pays a fixed premium.",
    },
    {
        "key": "aso",
        "label": "ASO / Self-Funded",
        "risk_bearer": "Employer",
        "description": "Employer funds claims and pays an admin fee; stop-loss caps exposure.",
    },
    {
        "key": "level_funded",
        "label": "Level-Funded",
        "risk_bearer": "Shared",
        "description": "Fixed monthly funding (claims + admin + stop-loss) with a year-end surplus share.",
    },
    {
        "key": "specialty",
        "label": "Specialty (Dental / Vision)",
        "risk_bearer": "Carrier",
        "description": "Manual- or blended-rated ancillary coverage priced to a loss-ratio target.",
    },
    {
        "key": "stop_loss",
        "label": "Specific Stop-Loss",
        "risk_bearer": "Reinsurer",
        "description": "Reinsurance over a self-funded plan reimbursing claims above a per-member attachment.",
    },
    {
        "key": "disability",
        "label": "Disability (STD / LTD)",
        "risk_bearer": "Carrier",
        "description": "Payroll-rated income protection priced per $100 of covered monthly payroll.",
    },
]

_ARRANGEMENT_KEYS = {a["key"] for a in FUNDING_ARRANGEMENTS}

# Default admin / expense load as a % of premium (FI) when not supplied.
_DEFAULT_ADMIN_LOAD_PCT = 12.0
# Default carrier risk/profit margin target as a % of premium (FI).
_DEFAULT_MARGIN_PCT = 4.0


# ---------------------------------------------------------------------------
# Shared base economics
# ---------------------------------------------------------------------------

def _base_economics(cache: DataCache, params: dict) -> dict:
    """Resolve expected annual claims, member count, and claims PMPM.

    Prefers group experience; falls back to book-of-business averages for the LOB.
    """
    group_id = params.get("group_id") or ""
    lob = params.get("lob")
    member_override = params.get("member_count")

    experience = cache.get_group_experience(group_id) if group_id else []
    if experience:
        premium = sum(_safe_float(r.get("total_premiums")) for r in experience)
        claims = sum(_safe_float(r.get("total_claims_paid")) for r in experience)
        members = sum(_safe_int(r.get("member_count")) for r in experience) or 1
        basis = f"group {group_id} experience"
    else:
        summary = cache.get_baseline_summary(lob=lob)
        premium = summary["total_premium"]
        claims = summary["total_claims"]
        members = summary["total_members"] or 1
        basis = "book-of-business averages (no group experience found)"

    if member_override:
        members = _safe_int(member_override, members) or members

    member_months = members * 12
    claims_pmpm = (claims / member_months) if member_months else 0.0
    return {
        "basis": basis,
        "expected_claims": claims,
        "current_premium": premium,
        "member_count": members,
        "member_months": member_months,
        "claims_pmpm": claims_pmpm,
    }


def _line(label: str, pmpm: float, member_months: int, note: str = "") -> dict:
    return {
        "label": label,
        "pmpm": round(pmpm, 2),
        "annual": round(pmpm * member_months, 2),
        "note": note,
    }


def _result(
    arrangement: str,
    base: dict,
    line_items: list[dict],
    *,
    risk_bearer: str,
    employer_max_liability: float | None,
    narrative: str,
    warnings: list[str] | None = None,
    extra: dict | None = None,
) -> dict:
    total_annual = round(sum(li["annual"] for li in line_items), 2)
    total_pmpm = round(sum(li["pmpm"] for li in line_items), 2)
    # Dollar impact = modeled cost swing vs the fully-insured premium baseline.
    fi_baseline = base["current_premium"] or (base["expected_claims"] * 1.18)
    dollar_impact = round(total_annual - fi_baseline, 2)
    out = {
        "arrangement": arrangement,
        "basis": base["basis"],
        "member_count": base["member_count"],
        "expected_annual_claims": round(base["expected_claims"], 2),
        "risk_bearer": risk_bearer,
        "line_items": line_items,
        "total_annual_cost": total_annual,
        "total_pmpm": total_pmpm,
        "employer_max_liability": (
            round(employer_max_liability, 2) if employer_max_liability is not None else None
        ),
        "fi_premium_baseline": round(fi_baseline, 2),
        "dollar_impact_vs_fi": dollar_impact,
        "narrative": narrative,
        "warnings": warnings or [],
    }
    if extra:
        out.update(extra)
    return out


# ---------------------------------------------------------------------------
# Arrangement calculators
# ---------------------------------------------------------------------------

def _price_fully_insured(cache: DataCache, params: dict) -> dict:
    """Carrier bears risk: premium built up from expected claims + admin + margin."""
    base = _base_economics(cache, params)
    mm = base["member_months"]
    admin_pct = _safe_float(params.get("admin_load_pct"), _DEFAULT_ADMIN_LOAD_PCT)
    margin_pct = _safe_float(params.get("margin_pct"), _DEFAULT_MARGIN_PCT)
    trend_pct = _safe_float(params.get("trend_pct"), 7.0)

    projected_claims = base["expected_claims"] * (1 + trend_pct / 100)
    claims_pmpm = (projected_claims / mm) if mm else 0.0
    # Premium so that claims = (100 - admin - margin)% of premium.
    retention_pct = max(100.0 - admin_pct - margin_pct, 1.0)
    premium = projected_claims / (retention_pct / 100)
    premium_pmpm = (premium / mm) if mm else 0.0
    admin_pmpm = premium_pmpm * admin_pct / 100
    margin_pmpm = premium_pmpm * margin_pct / 100

    lines = [
        _line("Projected claims", claims_pmpm, mm, f"{trend_pct:.1f}% trend applied"),
        _line("Administrative load", admin_pmpm, mm, f"{admin_pct:.1f}% of premium"),
        _line("Risk / profit margin", margin_pmpm, mm, f"{margin_pct:.1f}% of premium"),
    ]
    narrative = (
        f"Fully-insured premium of ${premium:,.0f} (${premium_pmpm:,.2f} PMPM) built from "
        f"${projected_claims:,.0f} projected claims at a {retention_pct:.0f}% target loss ratio. "
        f"The carrier bears 100% of claims risk."
    )
    warnings = []
    if admin_pct + margin_pct > 25:
        warnings.append("Combined admin + margin load exceeds 25% — competitiveness risk.")
    return _result(
        "fully_insured", base, lines,
        risk_bearer="Carrier", employer_max_liability=round(premium, 2),
        narrative=narrative, warnings=warnings,
        extra={"annual_premium": round(premium, 2)},
    )


def _price_aso(cache: DataCache, params: dict) -> dict:
    """ASO / self-funded: employer funds claims + admin fee; stop-loss caps liability."""
    base = _base_economics(cache, params)
    mm = base["member_months"]
    admin_fee_pmpm = _safe_float(params.get("aso_admin_fee_pmpm"), 45.0)
    spec_attachment = _safe_float(params.get("specific_attachment"), 250_000)
    agg_corridor_pct = _safe_float(params.get("aggregate_corridor_pct"), 125.0)
    spec_premium_pmpm = _safe_float(params.get("specific_sl_premium_pmpm"), 0.0)
    agg_premium_pmpm = _safe_float(params.get("aggregate_sl_premium_pmpm"), 0.0)

    claims_pmpm = base["claims_pmpm"]
    # Default stop-loss premiums if the underwriter didn't supply them.
    if spec_premium_pmpm <= 0:
        spec_premium_pmpm = max(claims_pmpm * 0.08, 20.0)
    if agg_premium_pmpm <= 0:
        agg_premium_pmpm = max(claims_pmpm * 0.03, 6.0)

    lines = [
        _line("Expected claims (employer-funded)", claims_pmpm, mm, "Employer liability"),
        _line("ASO administrative fee", admin_fee_pmpm, mm, "Carrier revenue"),
        _line("Specific stop-loss premium", spec_premium_pmpm, mm,
              f"Attachment ${spec_attachment:,.0f}/member"),
        _line("Aggregate stop-loss premium", agg_premium_pmpm, mm,
              f"{agg_corridor_pct:.0f}% aggregate corridor"),
    ]
    agg_attachment = base["expected_claims"] * (agg_corridor_pct / 100)
    fixed_cost_pmpm = admin_fee_pmpm + spec_premium_pmpm + agg_premium_pmpm
    # Employer max liability ≈ aggregate attachment + fixed costs (claims capped by agg SL).
    employer_max = agg_attachment + fixed_cost_pmpm * mm
    narrative = (
        f"Self-funded (ASO) plan: the employer funds ${base['expected_claims']:,.0f} expected "
        f"claims and pays ${fixed_cost_pmpm:,.2f} PMPM in fixed costs (admin + stop-loss). "
        f"Specific stop-loss attaches at ${spec_attachment:,.0f}/member and the aggregate "
        f"corridor caps total claims at ${agg_attachment:,.0f} ({agg_corridor_pct:.0f}% of expected), "
        f"so modeled maximum employer liability is ${employer_max:,.0f}."
    )
    warnings = []
    if base["member_count"] < 100:
        warnings.append("Groups under 100 lives carry high claims volatility for self-funding.")
    return _result(
        "aso", base, lines,
        risk_bearer="Employer", employer_max_liability=employer_max,
        narrative=narrative, warnings=warnings,
        extra={
            "specific_attachment": round(spec_attachment, 2),
            "aggregate_attachment": round(agg_attachment, 2),
            "fixed_cost_pmpm": round(fixed_cost_pmpm, 2),
        },
    )


def _price_level_funded(cache: DataCache, params: dict) -> dict:
    """Level-funded: fixed monthly funding with a year-end surplus share."""
    base = _base_economics(cache, params)
    mm = base["member_months"]
    admin_fee_pmpm = _safe_float(params.get("admin_fee_pmpm"), 55.0)
    margin_pct = _safe_float(params.get("claims_margin_pct"), 15.0)
    surplus_share_pct = _safe_float(params.get("surplus_share_pct"), 50.0)

    claims_pmpm = base["claims_pmpm"]
    # Claims funding set above expected to buffer volatility.
    claims_funding_pmpm = claims_pmpm * (1 + margin_pct / 100)
    sl_premium_pmpm = max(claims_pmpm * 0.10, 25.0)

    lines = [
        _line("Claims funding", claims_funding_pmpm, mm, f"Expected + {margin_pct:.0f}% buffer"),
        _line("Administrative fee", admin_fee_pmpm, mm, "Carrier revenue"),
        _line("Stop-loss premium (bundled)", sl_premium_pmpm, mm, "Specific + aggregate"),
    ]
    annual_funding = (claims_funding_pmpm + admin_fee_pmpm + sl_premium_pmpm) * mm
    expected_surplus = max(claims_funding_pmpm - claims_pmpm, 0) * mm
    employer_surplus_refund = expected_surplus * (surplus_share_pct / 100)
    narrative = (
        f"Level-funded arrangement: fixed monthly funding of "
        f"${(claims_funding_pmpm + admin_fee_pmpm + sl_premium_pmpm):,.2f} PMPM "
        f"(${annual_funding:,.0f}/yr). Claims are funded {margin_pct:.0f}% above expected; if the "
        f"group runs favorably, the modeled year-end surplus of ${expected_surplus:,.0f} is shared "
        f"{surplus_share_pct:.0f}% to the employer (${employer_surplus_refund:,.0f} refund)."
    )
    warnings = []
    if margin_pct < 10:
        warnings.append("Claims buffer under 10% leaves little room for adverse experience.")
    return _result(
        "level_funded", base, lines,
        risk_bearer="Shared", employer_max_liability=round(annual_funding, 2),
        narrative=narrative,
        warnings=warnings,
        extra={
            "expected_surplus": round(expected_surplus, 2),
            "employer_surplus_refund": round(employer_surplus_refund, 2),
        },
    )


def _price_specialty(cache: DataCache, params: dict) -> dict:
    """Specialty dental/vision: manual/blended PMPM to a loss-ratio target."""
    base = _base_economics(cache, params)
    mm = base["member_months"]
    manual_pmpm = _safe_float(params.get("manual_pmpm"), 32.0)
    target_lr = _safe_float(params.get("target_loss_ratio"), 72.0)
    credibility = min(max(_safe_float(params.get("credibility"), 0.0), 0.0), 1.0)

    # Blend group's own experience PMPM with the manual rate by credibility.
    group_pmpm = base["claims_pmpm"] if base["claims_pmpm"] > 0 else manual_pmpm
    blended_claims_pmpm = credibility * group_pmpm + (1 - credibility) * manual_pmpm
    premium_pmpm = blended_claims_pmpm / (target_lr / 100) if target_lr else manual_pmpm
    retention_pmpm = premium_pmpm - blended_claims_pmpm

    lines = [
        _line("Blended claims cost", blended_claims_pmpm, mm,
              f"credibility {credibility:.2f} on group vs manual"),
        _line("Retention (admin + margin)", retention_pmpm, mm,
              f"to {target_lr:.0f}% target loss ratio"),
    ]
    narrative = (
        f"Specialty coverage priced at ${premium_pmpm:,.2f} PMPM — a credibility-blended "
        f"claims cost of ${blended_claims_pmpm:,.2f} loaded to a {target_lr:.0f}% target "
        f"loss ratio. The carrier bears the risk."
    )
    return _result(
        "specialty", base, lines,
        risk_bearer="Carrier", employer_max_liability=round(premium_pmpm * mm, 2),
        narrative=narrative,
        extra={"premium_pmpm": round(premium_pmpm, 2)},
    )


def _price_stop_loss(cache: DataCache, params: dict) -> dict:
    """Specific stop-loss reinsurance over a self-funded plan."""
    base = _base_economics(cache, params)
    mm = base["member_months"]
    attachment = _safe_float(params.get("specific_attachment"), 250_000)
    members = base["member_count"]

    member_tcoc = cache.get_member_tcoc_by_group(params.get("group_id") or "")
    if member_tcoc:
        expected_excess = sum(
            max(_safe_float(r.get("actual_cost")) - attachment, 0) for r in member_tcoc
        )
        claimants = sum(1 for r in member_tcoc if _safe_float(r.get("actual_cost")) > attachment)
        source = "member-level TCOC"
    else:
        # Actuarial estimate: ~1.5% of members pierce a $250K attachment, avg excess ~$120K.
        pierce_rate = max(0.015 * (250_000 / attachment), 0.002)
        claimants = round(members * pierce_rate)
        expected_excess = claimants * 120_000 * (250_000 / attachment)
        source = "actuarial large-claim estimate"

    # Reinsurer premium = expected excess loaded for risk + expense (loss ratio target ~70%).
    sl_loss_ratio = _safe_float(params.get("sl_target_loss_ratio"), 70.0)
    premium = expected_excess / (sl_loss_ratio / 100) if sl_loss_ratio else expected_excess
    premium_pmpm = (premium / mm) if mm else 0.0

    lines = [
        _line("Expected excess reimbursement", (expected_excess / mm) if mm else 0, mm, source),
        _line("Reinsurer load (expense + risk)", premium_pmpm - ((expected_excess / mm) if mm else 0), mm,
              f"to {sl_loss_ratio:.0f}% target loss ratio"),
    ]
    narrative = (
        f"Specific stop-loss at a ${attachment:,.0f} attachment: an estimated {claimants} "
        f"claimant(s) pierce the deductible for ${expected_excess:,.0f} of reinsured claims "
        f"({source}). Priced to a {sl_loss_ratio:.0f}% target loss ratio, the cover premium is "
        f"${premium:,.0f} (${premium_pmpm:,.2f} PMPM)."
    )
    warnings = []
    if attachment < 100_000:
        warnings.append("Attachment below $100K — frequent pierces, expect high premium and leveraged trend.")
    return _result(
        "stop_loss", base, lines,
        risk_bearer="Reinsurer", employer_max_liability=None,
        narrative=narrative, warnings=warnings,
        extra={
            "specific_attachment": round(attachment, 2),
            "estimated_claimants": int(claimants),
            "reinsured_claims": round(expected_excess, 2),
            "cover_premium": round(premium, 2),
        },
    )


def _price_disability(cache: DataCache, params: dict) -> dict:
    """Disability STD/LTD: rated per $100 of covered monthly payroll by occupation class."""
    base = _base_economics(cache, params)
    members = base["member_count"]
    avg_monthly_salary = _safe_float(params.get("avg_monthly_salary"), 5_500)
    product = (params.get("disability_product") or "LTD").upper()
    occ_class = _safe_int(params.get("occupation_class"), 2)

    # Base monthly rate per $100 of payroll; higher occupation class = higher risk.
    base_rate = 0.45 if product == "LTD" else 0.28  # per $100/mo
    occ_multiplier = {1: 0.85, 2: 1.0, 3: 1.25, 4: 1.6}.get(occ_class, 1.0)
    rate_per_100 = base_rate * occ_multiplier

    covered_payroll_monthly = members * avg_monthly_salary
    monthly_premium = covered_payroll_monthly / 100 * rate_per_100
    annual_premium = monthly_premium * 12
    premium_pmpm = (annual_premium / (members * 12)) if members else 0.0

    lines = [
        _line(f"{product} premium", premium_pmpm, members * 12,
              f"${rate_per_100:.3f} per $100 payroll, occ class {occ_class}"),
    ]
    narrative = (
        f"{product} income protection for {members} lives at an average "
        f"${avg_monthly_salary:,.0f} monthly salary: covered payroll of "
        f"${covered_payroll_monthly:,.0f}/mo at ${rate_per_100:.3f} per $100 (occupation class "
        f"{occ_class}) yields ${annual_premium:,.0f} annual premium (${premium_pmpm:,.2f} PMPM)."
    )
    warnings = []
    if occ_class >= 4:
        warnings.append("Occupation class 4 (heavy manual) — verify own-occupation definition and loads.")
    return _result(
        "disability", base, lines,
        risk_bearer="Carrier", employer_max_liability=round(annual_premium, 2),
        narrative=narrative, warnings=warnings,
        extra={
            "product": product,
            "rate_per_100_payroll": round(rate_per_100, 4),
            "annual_premium": round(annual_premium, 2),
        },
    )


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

_CALCULATORS: dict[str, Callable[[DataCache, dict], dict]] = {
    "fully_insured": _price_fully_insured,
    "aso": _price_aso,
    "level_funded": _price_level_funded,
    "specialty": _price_specialty,
    "stop_loss": _price_stop_loss,
    "disability": _price_disability,
}


def price_funding_arrangement(cache: DataCache, arrangement: str, params: dict) -> dict:
    """Route a quote to the correct funding-arrangement calculator.

    Raises:
        ValueError: if the arrangement key is unknown.
    """
    fn = _CALCULATORS.get(arrangement)
    if not fn:
        raise ValueError(
            f"Unknown funding arrangement '{arrangement}'. "
            f"Valid: {', '.join(_ARRANGEMENT_KEYS)}"
        )
    return fn(cache, params)
