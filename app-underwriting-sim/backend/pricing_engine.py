"""Community-rated actuarial pricing model and risk pool analysis.

Implements the rate build-up formula:
  Final Rate = Base Rate x Age Factor x Area Factor x Industry Factor x Experience Mod x Trend Factor

Risk pool analysis compares a group's risk profile against the book of business
using synthetic but realistic distributions.
"""

import time
from typing import Optional

from .data_loader import DataCache, _safe_float, _safe_int, _execute_sql, _CAT


# ---------------------------------------------------------------------------
# Actuarial Factor Tables (static reference data)
# ---------------------------------------------------------------------------

BASE_RATES: dict[str, float] = {
    "Commercial": 385.00,
    "Medicare Advantage": 925.00,
    "Medicaid": 310.00,
    "Individual": 420.00,
}

AGE_FACTORS: dict[str, float] = {
    "0-17": 0.72,
    "18-25": 0.85,
    "26-35": 0.92,
    "36-45": 1.00,
    "46-55": 1.15,
    "56-64": 1.25,
    "65+": 1.45,
}

AREA_FACTORS: dict[str, float] = {
    "urban": 0.95,
    "suburban": 1.00,
    "rural": 1.10,
}

INDUSTRY_FACTORS: dict[str, float] = {
    "healthcare": 1.15,
    "office": 0.90,
    "manufacturing": 1.05,
    "technology": 0.88,
    "retail": 0.98,
    "construction": 1.12,
    "education": 0.93,
    "finance": 0.91,
    "hospitality": 1.02,
    "transportation": 1.08,
    "government": 0.95,
    "agriculture": 1.06,
}

TREND_FACTORS: dict[str, float] = {
    "7%": 1.07,
    "8%": 1.08,
    "9%": 1.09,
    "10%": 1.10,
    "11%": 1.11,
    "12%": 1.12,
}

# Experience mod range: credibility-weighted blend bounds
EXPERIENCE_MOD_RANGE = {"min": 0.70, "max": 1.40, "neutral": 1.00}

# The dicts above are FALLBACK defaults. The source of truth is the governed UC
# table analytics.gold_pricing_factors; _load_governed_factors() overlays it so
# actuaries can update rate assumptions without a code deploy. If the table is
# absent/unreadable, the hardcoded values above are used.
_FACTORS_TABLE = f"{_CAT}.analytics.gold_pricing_factors"
_FACTOR_CACHE: dict = {"ts": 0.0, "data": None}
_FACTOR_TTL = 15 * 60


def _load_governed_factors() -> dict:
    """Load rate factors from the governed UC table, cached with a TTL.

    Returns a dict of the five factor maps merged over the hardcoded fallbacks:
    {base_rates, age_factors, area_factors, industry_factors, experience_mod, source}.
    'source' is 'uc_table' when any governed rows were read, else 'fallback'.
    """
    if _FACTOR_CACHE["data"] is not None and (time.time() - _FACTOR_CACHE["ts"] < _FACTOR_TTL):
        return _FACTOR_CACHE["data"]

    merged = {
        "base_rates": dict(BASE_RATES),
        "age_factors": dict(AGE_FACTORS),
        "area_factors": dict(AREA_FACTORS),
        "industry_factors": dict(INDUSTRY_FACTORS),
        "experience_mod": dict(EXPERIENCE_MOD_RANGE),
        "source": "fallback",
    }
    _bucket = {
        "base_rate": "base_rates",
        "age_factor": "age_factors",
        "area_factor": "area_factors",
        "industry_factor": "industry_factors",
        "experience_mod": "experience_mod",
    }
    try:
        rows = _execute_sql(
            f"SELECT factor_type, factor_key, factor_value FROM {_FACTORS_TABLE}"
        )
        if rows:
            for r in rows:
                key = _bucket.get(r.get("factor_type"))
                if key:
                    merged[key][r.get("factor_key")] = _safe_float(r.get("factor_value"))
            merged["source"] = "uc_table"
    except Exception as e:
        print(f"[pricing] governed factor load failed, using fallback: {e}")

    _FACTOR_CACHE.update(ts=time.time(), data=merged)
    return merged


def get_factor_tables() -> dict:
    """Return all factor tables as structured reference data (governed by UC)."""
    gf = _load_governed_factors()
    age = gf["age_factors"]
    area = gf["area_factors"]
    industry = gf["industry_factors"]
    emod = gf["experience_mod"]
    governed = gf["source"] == "uc_table"
    src_note = (
        " Sourced from the governed Unity Catalog table analytics.gold_pricing_factors."
        if governed else " Using built-in fallback values (governed UC table not available)."
    )
    return {
        "source": gf["source"],
        "age_factors": {
            "table_name": "Age Rating Factors",
            "description": "Community rating adjustment based on group average age band (ACA 3:1 ratio compliant)." + src_note,
            "factors": [{"age_band": k, "factor": v} for k, v in age.items()],
        },
        "area_factors": {
            "table_name": "Geographic Area Factors",
            "description": "Adjustment for county-level geographic cost variation." + src_note,
            "factors": [{"county_type": k, "factor": v} for k, v in area.items()],
        },
        "industry_factors": {
            "table_name": "Industry (SIC) Factors",
            "description": "Risk adjustment by Standard Industrial Classification code." + src_note,
            "factors": [{"industry": k, "factor": v} for k, v in industry.items()],
        },
        "trend_factors": {
            "table_name": "Medical Cost Trend Factors",
            "description": "Annual medical cost inflation projection factors.",
            "factors": [{"trend_rate": k, "factor": v} for k, v in TREND_FACTORS.items()],
        },
        "experience_mod_ranges": {
            "table_name": "Experience Modification Ranges",
            "description": "Credibility-weighted experience mod bounds (blend of group vs manual rate)." + src_note,
            "factors": [
                {"label": "Minimum (best experience)", "value": emod.get("min", 0.70)},
                {"label": "Neutral", "value": emod.get("neutral", 1.00)},
                {"label": "Maximum (worst experience)", "value": emod.get("max", 1.40)},
            ],
        },
    }


# ---------------------------------------------------------------------------
# Rate Build-Up Calculator
# ---------------------------------------------------------------------------

def _compute_experience_mod(
    loss_ratio: Optional[float],
    credibility: float,
    book_loss_ratio: float = 0.82,
) -> float:
    """Compute experience modification factor.

    Blends group's own loss ratio with manual (book) rate using credibility.
    Returns a factor centered at 1.0.
    """
    if loss_ratio is None:
        return 1.0

    # Group experience relativity = group LR / book LR
    group_relativity = loss_ratio / book_loss_ratio if book_loss_ratio else 1.0

    # Credibility-weighted blend: Z * group + (1 - Z) * manual
    blended = credibility * group_relativity + (1 - credibility) * 1.0

    # Clamp to allowed range
    return max(EXPERIENCE_MOD_RANGE["min"], min(EXPERIENCE_MOD_RANGE["max"], blended))


def compute_rate_buildup(
    cache: DataCache,
    avg_age_band: Optional[str] = None,
    county_type: Optional[str] = None,
    sic_code: Optional[str] = None,
    loss_ratio: Optional[float] = None,
    credibility_factor: Optional[float] = None,
    trend_pct: Optional[float] = None,
    lob: str = "Commercial",
    group_id: Optional[str] = None,
) -> dict:
    """Compute the full rate build-up with step-by-step factors.

    Returns a dict matching RateBuildupOut schema.
    """
    # ---- Governed factors (UC table, falling back to module defaults) ----
    gf = _load_governed_factors()
    base_rates = gf["base_rates"]
    age_factors = gf["age_factors"]
    area_factors = gf["area_factors"]
    industry_factors = gf["industry_factors"]

    # ---- Base Rate ----
    base_rate = base_rates.get(lob, 385.00)

    # ---- Resolve defaults from group data if group_id provided ----
    current_rate: Optional[float] = None
    if group_id:
        experience = cache.get_group_experience(group_id)
        if experience:
            row = experience[0]
            grp_premium = _safe_float(row.get("total_premiums"))
            grp_claims = _safe_float(row.get("total_claims_paid"))
            grp_members = _safe_int(row.get("member_count")) or 1
            grp_mm = grp_members * 12
            if grp_mm:
                current_rate = grp_premium / grp_mm
            if loss_ratio is None and grp_premium:
                loss_ratio = grp_claims / grp_premium
            if credibility_factor is None:
                credibility_factor = min(grp_members / 1000, 1.0)

    # ---- Apply defaults for missing params ----
    if avg_age_band is None:
        avg_age_band = "36-45"
    if county_type is None:
        county_type = "suburban"
    if sic_code is None:
        sic_code = "office"
    if credibility_factor is None:
        credibility_factor = 0.5
    if trend_pct is None:
        trend_pct = 8.0

    # ---- Look up factors ----
    age_factor = age_factors.get(avg_age_band, 1.0)
    area_factor = area_factors.get(county_type.lower(), 1.0)
    industry_factor = industry_factors.get(sic_code.lower(), 1.0)
    trend_factor = 1 + trend_pct / 100
    experience_mod = _compute_experience_mod(loss_ratio, credibility_factor)

    # ---- Build steps ----
    steps = []
    running = base_rate

    steps.append({
        "step_name": "base_rate",
        "factor_label": f"Base PMPM ({lob})",
        "factor_value": 1.0,
        "running_total": round(running, 2),
        "description": f"Starting base rate for {lob} book of business",
    })

    running *= age_factor
    steps.append({
        "step_name": "age_factor",
        "factor_label": f"Age Factor ({avg_age_band})",
        "factor_value": age_factor,
        "running_total": round(running, 2),
        "description": f"Age band {avg_age_band} adjustment",
    })

    running *= area_factor
    steps.append({
        "step_name": "area_factor",
        "factor_label": f"Area Factor ({county_type})",
        "factor_value": area_factor,
        "running_total": round(running, 2),
        "description": f"Geographic adjustment for {county_type} county",
    })

    running *= industry_factor
    steps.append({
        "step_name": "industry_factor",
        "factor_label": f"Industry Factor ({sic_code})",
        "factor_value": industry_factor,
        "running_total": round(running, 2),
        "description": f"Industry risk adjustment for {sic_code} sector",
    })

    running *= experience_mod
    steps.append({
        "step_name": "experience_mod",
        "factor_label": f"Experience Mod (Z={credibility_factor:.2f})",
        "factor_value": round(experience_mod, 4),
        "running_total": round(running, 2),
        "description": (
            f"Credibility-weighted experience modification "
            f"(credibility={credibility_factor:.2f}"
            + (f", group LR={loss_ratio:.2f}" if loss_ratio is not None else "")
            + ")"
        ),
    })

    running *= trend_factor
    steps.append({
        "step_name": "trend_factor",
        "factor_label": f"Trend Factor ({trend_pct:.0f}%)",
        "factor_value": round(trend_factor, 4),
        "running_total": round(running, 2),
        "description": f"Annual medical cost trend of {trend_pct:.1f}%",
    })

    final_rate = round(running, 2)

    # ---- Rate change vs current ----
    rate_change = None
    rate_change_pct = None
    if current_rate and current_rate > 0:
        rate_change = round(final_rate - current_rate, 2)
        rate_change_pct = round((final_rate - current_rate) / current_rate * 100, 2)

    # ---- Narrative ----
    parts = [f"Rate build-up for {lob}:"]
    parts.append(f"Base ${base_rate:.2f}")
    parts.append(f"x {age_factor:.2f} (age)")
    parts.append(f"x {area_factor:.2f} (area)")
    parts.append(f"x {industry_factor:.2f} (industry)")
    parts.append(f"x {experience_mod:.4f} (experience)")
    parts.append(f"x {trend_factor:.4f} (trend)")
    parts.append(f"= ${final_rate:.2f} PMPM.")
    if current_rate:
        direction = "increase" if final_rate > current_rate else "decrease"
        parts.append(
            f" Current rate: ${current_rate:.2f} PMPM. "
            f"Implied {direction} of ${abs(final_rate - current_rate):.2f} "
            f"({abs(rate_change_pct or 0):.1f}%)."
        )

    return {
        "base_rate": base_rate,
        "steps": steps,
        "final_rate": final_rate,
        "current_rate": current_rate,
        "rate_change": rate_change,
        "rate_change_pct": rate_change_pct,
        "lob": lob,
        "narrative": " ".join(parts),
    }


# ---------------------------------------------------------------------------
# Risk Pool Analysis
# ---------------------------------------------------------------------------

# Synthetic book-of-business distributions (realistic for a ~150K member plan)
BOOK_RAF_DISTRIBUTION = {
    "<0.5": 22.5,
    "0.5-1.0": 35.0,
    "1.0-1.5": 20.0,
    "1.5-2.0": 11.0,
    "2.0-3.0": 7.5,
    "3.0+": 4.0,
}

BOOK_AGE_DISTRIBUTION = {
    "0-17": 18.0,
    "18-25": 10.5,
    "26-35": 15.0,
    "36-45": 17.5,
    "46-55": 19.0,
    "56-64": 14.0,
    "65+": 6.0,
}

BOOK_CHRONIC_CONDITIONS = {
    "Hypertension": 28.5,
    "Diabetes (Type 2)": 14.2,
    "Hyperlipidemia": 22.1,
    "Obesity": 18.7,
    "Asthma/COPD": 11.3,
    "Depression/Anxiety": 16.4,
    "Chronic Kidney Disease": 4.8,
    "Heart Failure": 3.2,
    "Cancer (any)": 2.9,
    "Musculoskeletal": 19.5,
}

BOOK_AVG_RAF = 1.05
BOOK_AVG_AGE = 38.2


def _generate_group_profile(
    group_id: str, cache: DataCache
) -> dict:
    """Generate a deterministic-but-varied group risk profile from group_id hash."""
    import hashlib

    # Use group_id hash for deterministic variation
    h = int(hashlib.md5(group_id.encode()).hexdigest(), 16)

    # Try to get real group data first
    experience = cache.get_group_experience(group_id)
    member_count = 0
    if experience:
        member_count = sum(_safe_int(r.get("member_count")) for r in experience)

    if not member_count:
        member_count = 50 + (h % 950)  # 50-999 members

    # Generate RAF distribution with some skew based on group hash
    skew = ((h % 100) - 50) / 100  # -0.5 to +0.5
    group_raf_dist = {}
    for bucket, book_pct in BOOK_RAF_DISTRIBUTION.items():
        # Shift distribution based on skew
        if bucket in ("<0.5", "0.5-1.0"):
            adjustment = -skew * 8  # If positive skew, fewer healthy
        elif bucket in ("2.0-3.0", "3.0+"):
            adjustment = skew * 6  # If positive skew, more sick
        else:
            adjustment = skew * 2
        group_raf_dist[bucket] = max(1.0, book_pct + adjustment)

    # Normalize to 100
    total = sum(group_raf_dist.values())
    group_raf_dist = {k: round(v / total * 100, 1) for k, v in group_raf_dist.items()}

    # Group avg RAF
    raf_midpoints = {"<0.5": 0.3, "0.5-1.0": 0.75, "1.0-1.5": 1.25, "1.5-2.0": 1.75, "2.0-3.0": 2.5, "3.0+": 3.5}
    group_avg_raf = sum(
        group_raf_dist[b] / 100 * raf_midpoints[b] for b in group_raf_dist
    )
    group_avg_raf = round(group_avg_raf, 3)

    # Age distribution
    age_skew = ((h >> 8) % 100 - 50) / 100
    group_age_dist = {}
    for band, book_pct in BOOK_AGE_DISTRIBUTION.items():
        if band in ("0-17", "18-25"):
            adj = -age_skew * 5
        elif band in ("56-64", "65+"):
            adj = age_skew * 5
        else:
            adj = age_skew * 1
        group_age_dist[band] = max(1.0, book_pct + adj)
    total = sum(group_age_dist.values())
    group_age_dist = {k: round(v / total * 100, 1) for k, v in group_age_dist.items()}

    # Chronic conditions
    condition_skew = ((h >> 16) % 100 - 50) / 100
    group_conditions = {}
    for cond, book_pct in BOOK_CHRONIC_CONDITIONS.items():
        adj = condition_skew * book_pct * 0.3
        group_conditions[cond] = round(max(0.5, book_pct + adj), 1)

    # Cost drivers
    cost_categories = [
        ("Inpatient", 145.00),
        ("Outpatient/Ambulatory", 98.00),
        ("Pharmacy", 82.00),
        ("Professional Services", 65.00),
        ("Emergency", 28.00),
        ("Behavioral Health", 22.00),
        ("Lab/Diagnostics", 18.00),
    ]
    # Adjust by hash
    total_pmpm = sum(c[1] for c in cost_categories)
    top_drivers = []
    for i, (cat, pmpm) in enumerate(cost_categories[:5]):
        var = 1 + (((h >> (i * 4)) % 30) - 15) / 100
        adj_pmpm = round(pmpm * var, 2)
        top_drivers.append({
            "category": cat,
            "pmpm": adj_pmpm,
            "pct_of_total": round(adj_pmpm / total_pmpm * 100, 1),
        })

    return {
        "member_count": member_count,
        "avg_raf": group_avg_raf,
        "raf_distribution": group_raf_dist,
        "age_distribution": group_age_dist,
        "chronic_conditions": group_conditions,
        "top_cost_drivers": top_drivers,
    }


def compute_risk_pool(cache: DataCache, group_id: str) -> dict:
    """Compare group's risk profile against the book of business.

    Returns a dict matching RiskPoolOut schema.
    """
    profile = _generate_group_profile(group_id, cache)

    # Build RAF distribution comparison
    raf_dist = []
    for bucket in BOOK_RAF_DISTRIBUTION:
        raf_dist.append({
            "label": bucket,
            "group_value": profile["raf_distribution"].get(bucket, 0),
            "book_value": BOOK_RAF_DISTRIBUTION[bucket],
        })

    # Build age distribution comparison
    age_dist = []
    for band in BOOK_AGE_DISTRIBUTION:
        age_dist.append({
            "label": band,
            "group_value": profile["age_distribution"].get(band, 0),
            "book_value": BOOK_AGE_DISTRIBUTION[band],
        })

    # Build condition prevalence comparison
    conditions = []
    for cond in BOOK_CHRONIC_CONDITIONS:
        group_pct = profile["chronic_conditions"].get(cond, 0)
        book_pct = BOOK_CHRONIC_CONDITIONS[cond]
        conditions.append({
            "condition": cond,
            "group_pct": group_pct,
            "book_pct": book_pct,
            "delta_pct": round(group_pct - book_pct, 1),
        })
    # Sort by absolute delta descending
    conditions.sort(key=lambda x: abs(x["delta_pct"]), reverse=True)

    # Adverse selection detection
    raf_ratio = profile["avg_raf"] / BOOK_AVG_RAF if BOOK_AVG_RAF else 1.0
    adverse = raf_ratio > 1.10
    severity = None
    if raf_ratio > 1.30:
        severity = "high"
    elif raf_ratio > 1.20:
        severity = "moderate"
    elif raf_ratio > 1.10:
        severity = "low"

    # Narrative
    direction = "higher" if profile["avg_raf"] > BOOK_AVG_RAF else "lower"
    narrative = (
        f"Group {group_id} ({profile['member_count']} members) has an average RAF of "
        f"{profile['avg_raf']:.3f}, which is {direction} than the book average of "
        f"{BOOK_AVG_RAF:.3f} ({raf_ratio:.2f}x ratio)."
    )
    if adverse:
        narrative += (
            f" This group shows {severity}-severity adverse selection risk. "
            f"Consider experience-rating surcharge or enhanced underwriting review."
        )
    else:
        narrative += " This group's risk profile is within normal range of the book."

    return {
        "group_id": group_id,
        "group_member_count": profile["member_count"],
        "group_avg_raf": profile["avg_raf"],
        "book_avg_raf": BOOK_AVG_RAF,
        "raf_distribution": raf_dist,
        "age_distribution": age_dist,
        "condition_prevalence": conditions,
        "top_cost_drivers": profile["top_cost_drivers"],
        "adverse_selection_flag": adverse,
        "adverse_selection_severity": severity,
        "narrative": narrative,
    }


def get_book_of_business_summary() -> dict:
    """Return aggregate book-of-business risk summary.

    Returns a dict matching BookOfBusinessSummaryOut schema.
    """
    return {
        "total_members": 152340,
        "avg_raf": BOOK_AVG_RAF,
        "avg_age": BOOK_AVG_AGE,
        "raf_distribution": [
            {"bucket": k, "pct": v} for k, v in BOOK_RAF_DISTRIBUTION.items()
        ],
        "age_distribution": [
            {"band": k, "pct": v} for k, v in BOOK_AGE_DISTRIBUTION.items()
        ],
        "top_chronic_conditions": [
            {"condition": k, "prevalence_pct": v}
            for k, v in sorted(BOOK_CHRONIC_CONDITIONS.items(), key=lambda x: -x[1])
        ],
    }
