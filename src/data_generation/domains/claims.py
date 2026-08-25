# Red Bricks Insurance — claims domain (medical + pharmacy): allowed, charged, paid, codes, dates.

import random
from datetime import date, timedelta
from math import ceil
from typing import List, Dict, Any

from .. import reference_data
from ..dq import inject_dq_issue
from ..helpers import (
    generate_claim_id,
    generate_npi,
    random_date_between,
    weighted_choice,
    apply_payment_lag,
)

# Re-export reference data for claim-type logic
ICD10 = reference_data.ICD10_CODES
ICD10_W = reference_data.ICD10_WEIGHTS

# Claims window — must match the date range used for service/fill dates
_CLAIMS_START = date(2023, 1, 1)
_CLAIMS_END = date(2025, 12, 31)

# Procedure codes treated as experimental/investigational or high-scrutiny for
# medical necessity. These are real codes generated above; when a claim for one
# of them is denied it biases toward experimental / not-medically-necessary
# CARC codes so the denial-reason classifier has learnable signal.
_EXPERIMENTAL_CPT = {"62323", "29881", "43239"}

# "Generic"/non-specific primary diagnoses that, when paired with a costly
# procedure, look like a diagnosis↔procedure coding mismatch (CO-11).
_NONSPECIFIC_DX = {"R10.9", "M54.5", "Z23", "Z00.00"}

# Common, repeatable professional procedures that can trip frequency/visit
# limits (CO-151).
_HIGH_FREQUENCY_CPT = {"99213", "99214", "99215", "90834", "36415", "80053", "85025", "81001"}


def _pick_denial_reason(
    claim_type: str,
    procedure_code,
    primary_dx_code,
    billed: float,
    allowed: float,
    service_date: date,
    elig_end: date,
) -> str:
    """Choose a denial CARC code correlated with the claim's own features.

    Correlations (so an ML classifier can learn real signal rather than noise):
      - experimental procedures      -> CO-55 / CO-96 (mostly), some CO-50
      - high-cost / inpatient claims -> CO-197 (no auth) + CO-50 (not med nec)
      - non-specific dx + costly proc -> CO-11 (dx/procedure mismatch)
      - service near coverage end     -> CO-27 (coverage terminated)
      - repeatable professional codes -> CO-151 (frequency/limits)
      - otherwise                     -> broad fallback distribution
    """
    codes: list[str] = []
    weights: list[int] = []

    def add(code: str, w: int) -> None:
        codes.append(code)
        weights.append(w)

    proc = str(procedure_code) if procedure_code else ""
    dx = str(primary_dx_code) if primary_dx_code else ""

    # Feature-conditioned branches give the ML model real signal (and sensible
    # drivers), while every branch keeps CO-16 (missing/insufficient information)
    # prominent — mirroring real payer data where "additional information" is the
    # single largest denial category (~38-42% in CMS/state CARC reporting;
    # #1 root cause in the Experian 2024 State of Claims survey).
    if proc in _EXPERIMENTAL_CPT:
        add("CO-55", 40)
        add("CO-96", 28)
        add("CO-50", 22)
        add("CO-16", 10)
    elif billed >= 5000 or claim_type in ("Institutional_IP", "Institutional_OP"):
        add("CO-197", 30)   # high-cost services typically require prior auth
        add("CO-16", 28)    # ...but missing documentation is still the top driver
        add("CO-50", 22)    # medical-necessity scrutiny
        add("CO-11", 20)
    elif dx in _NONSPECIFIC_DX and (allowed or 0) >= 500:
        add("CO-11", 45)    # dx doesn't support the (costly) procedure
        add("CO-16", 35)
        add("CO-50", 20)
    elif service_date and elig_end and (elig_end - service_date).days <= 30:
        add("CO-27", 50)    # service at/after coverage termination
        add("CO-16", 30)
        add("CO-197", 20)
    elif proc in _HIGH_FREQUENCY_CPT:
        add("CO-151", 42)   # exceeded visit/frequency limits
        add("CO-16", 33)
        add("CO-18", 15)    # duplicate
        add("CO-97", 10)
    else:
        # Majority branch — "missing/insufficient information" (CARC 16) is the
        # dominant real-world denial reason, followed by eligibility, auth, and
        # coding, with a small tail of duplicate / timely-filing / bundled / other.
        add("CO-16", 46)    # missing/incomplete info — #1 real-world cause
        add("CO-27", 12)    # eligibility / coverage
        add("CO-197", 10)   # prior authorization absent
        add("CO-11", 8)     # dx/procedure coding mismatch
        add("CO-50", 6)     # medical necessity
        add("CO-18", 6)     # duplicate (→ other)
        add("CO-97", 5)     # bundled / inclusive (→ other)
        add("CO-29", 3)     # timely filing (→ other)
        add("PI-204", 2)    # non-covered per plan (→ other)
        add("CO-4", 2)      # modifier issue (→ other)

    return random.choices(codes, weights=weights, k=1)[0]


def _safe_date(val, fallback: date) -> date:
    """Parse a date value, returning fallback for DQ-injected or invalid values."""
    if val is None:
        return fallback
    if isinstance(val, date):
        return val
    try:
        d = date.fromisoformat(str(val))
        if d.year < 2020 or d.year > 2030:
            return fallback
        return d
    except (ValueError, TypeError):
        return fallback


def _coverage_months_in_window(enr: Dict[str, Any]) -> int:
    """Compute months of coverage within the claims window, matching gold SQL logic."""
    start = _safe_date(enr.get("eligibility_start_date"), _CLAIMS_START)
    end = _safe_date(enr.get("eligibility_end_date"), _CLAIMS_END)
    # Clip to claims window
    eff_start = max(start, _CLAIMS_START)
    eff_end = min(end, _CLAIMS_END)
    if eff_end <= eff_start:
        return 1
    months = (eff_end.year - eff_start.year) * 12 + (eff_end.month - eff_start.month)
    return max(1, ceil(months + (eff_end.day - eff_start.day) / 30.0))


def _safe_premium(enr: Dict[str, Any]) -> float:
    """Get premium, handling DQ-injected null/negative values."""
    val = enr.get("monthly_premium", 500)
    try:
        return max(0.0, float(val)) if val is not None else 500.0
    except (ValueError, TypeError):
        return 500.0


def generate_medical_claims(
    enrollment: List[Dict[str, Any]],
    providers: List[Dict[str, Any]],
    n_total: int = 50000,
) -> List[Dict[str, Any]]:
    """Generate medical claims: inpatient, outpatient, professional, ER. Billed >= allowed >= paid.
    enrollment: list of dicts with member_id and line_of_business (for utilization weighting).
    """
    claims = []
    # Target MLR by LOB — controls how much of premiums are consumed by claims
    target_mlr = {
        "Medicare Advantage": 0.88,
        "Medicaid": 0.87,
        "Commercial": 0.83,
        "ACA Marketplace": 0.82,
    }
    # Weight each member by their premium contribution * LOB target MLR
    # This ensures claims distribute proportionally to each LOB's premium pool
    member_weights = []
    for e in enrollment:
        premium = _safe_premium(e)
        coverage = _coverage_months_in_window(e)
        lob = e.get("line_of_business", "Commercial")
        mlr = target_mlr.get(lob, 0.85)
        member_weights.append(premium * coverage * mlr)
    provider_npis = [p["npi"] for p in providers if p.get("npi")]

    claim_types = ["Professional", "Institutional_OP", "ER", "Institutional_IP"]
    claim_weights = [60, 20, 10, 10]
    fallback_start = date(2023, 1, 1)
    fallback_end = date(2025, 12, 31)

    for i in range(n_total):
        claim_type = weighted_choice(claim_types, claim_weights)
        claim_id = generate_claim_id("MC")
        claim_line = random.randint(1, 3) if claim_type != "Professional" else 1
        enr = random.choices(enrollment, weights=member_weights, k=1)[0]
        member_id = enr["member_id"]
        # Constrain service date to member's eligibility window
        elig_start = enr.get("eligibility_start_date") or fallback_start
        elig_end = enr.get("eligibility_end_date") or fallback_end
        if isinstance(elig_start, str):
            elig_start = date.fromisoformat(elig_start)
        if isinstance(elig_end, str):
            elig_end = date.fromisoformat(elig_end)
        rendering_npi = random.choice(provider_npis) if provider_npis else generate_npi()
        billing_npi = random.choice(provider_npis) if provider_npis else generate_npi()
        service_date = random_date_between(elig_start, min(elig_end, fallback_end))
        paid_date = apply_payment_lag(service_date)

        dx_primary = weighted_choice(ICD10, ICD10_W)
        num_secondary = random.randint(0, 3)
        dx_secondary = random.choices(ICD10, weights=ICD10_W, k=num_secondary)

        claim_status = random.choices(
            ["Paid", "Denied", "Adjusted", "Pended"],
            weights=[76, 14, 6, 4], k=1
        )[0]
        # denial_reason is chosen below, once the procedure/dollars/dates are
        # known, so it can be correlated with the claim's features.
        denial_reason = None

        procedure_code = procedure_desc = revenue_code = revenue_desc = None
        drg_code = drg_desc = discharge_date = admission_type = discharge_status = None
        pos_code = pos_desc = bill_type = None
        cost_low = cost_high = 100.0

        if claim_type == "Professional":
            proc = random.choice(reference_data.CPT_PROFESSIONAL)
            procedure_code, procedure_desc, cost_low, cost_high = proc
            pos_code, pos_desc = random.choice(reference_data.POS_CODES["Professional"])
            billed = round(random.uniform(cost_low * 1.3, cost_high * 1.5), 2)
            allowed = round(random.uniform(cost_low, cost_high), 2)
        elif claim_type == "Institutional_IP":
            proc = random.choice(reference_data.CPT_INSTITUTIONAL_IP)
            procedure_code, procedure_desc, cost_low, cost_high = proc
            rev = random.choice(reference_data.REVENUE_CODES_IP)
            revenue_code, revenue_desc = rev
            drg = random.choice(reference_data.DRGS)
            drg_code, drg_desc, drg_low, drg_high = drg
            pos_code, pos_desc = reference_data.POS_CODES["Institutional_IP"][0]
            bill_type = random.choice(["111", "112", "113"])
            los = random.randint(1, 14)
            discharge_date = service_date + timedelta(days=los)
            admission_type = random.choice(["Elective", "Emergency", "Urgent"])
            discharge_status = random.choices(
                ["01-Home", "02-Short-term Hospital", "03-SNF", "20-Expired"],
                weights=[70, 5, 15, 10], k=1
            )[0]
            billed = round(random.uniform(cost_low * 1.5, cost_high * 2.0), 2)
            allowed = round(random.uniform(drg_low, drg_high), 2)
        elif claim_type == "Institutional_OP":
            proc = random.choice(reference_data.CPT_INSTITUTIONAL_OP)
            procedure_code, procedure_desc, cost_low, cost_high = proc
            rev = random.choice(reference_data.REVENUE_CODES_OP)
            revenue_code, revenue_desc = rev
            pos_code, pos_desc = random.choice(reference_data.POS_CODES["Institutional_OP"])
            bill_type = random.choice(["131", "132", "133"])
            billed = round(random.uniform(cost_low * 1.4, cost_high * 1.8), 2)
            allowed = round(random.uniform(cost_low, cost_high), 2)
        else:  # ER
            proc = random.choice(reference_data.CPT_ER)
            procedure_code, procedure_desc, cost_low, cost_high = proc
            revenue_code, revenue_desc = ("0450", "Emergency Room")
            pos_code, pos_desc = reference_data.POS_CODES["ER"][0]
            bill_type = random.choice(["131", "132"])
            billed = round(random.uniform(cost_low * 2.0, cost_high * 3.0), 2)
            allowed = round(random.uniform(cost_low * 1.2, cost_high * 1.5), 2)

        if claim_status == "Denied":
            denial_reason = _pick_denial_reason(
                claim_type, procedure_code, dx_primary[0],
                billed, allowed, service_date, elig_end,
            )
            paid_amount = 0.0
            copay = coinsurance = deductible = 0.0
            member_responsibility = billed
        else:
            copay = float(random.choice([0, 20, 25, 30, 50])) if claim_type == "Professional" else float(random.choice([0, 50, 100, 250]))
            deductible = round(random.uniform(0, min(allowed * 0.15, 500)), 2)
            coin_rate = random.choice([0.0, 0.1, 0.2, 0.3])
            coinsurance = round(max(0.0, (allowed - copay - deductible) * coin_rate), 2)
            member_responsibility = round(copay + coinsurance + deductible, 2)
            paid_amount = round(max(0.0, allowed - member_responsibility), 2)

        if allowed > billed:
            billed = round(allowed * 1.3, 2)

        claim = {
            "claim_id": claim_id,
            "claim_line_number": claim_line,
            "claim_type": claim_type.replace("_", " "),
            "member_id": member_id,
            "rendering_provider_npi": inject_dq_issue(rendering_npi, "code"),
            "billing_provider_npi": billing_npi,
            "service_from_date": inject_dq_issue(service_date.isoformat(), "date"),
            "service_to_date": (discharge_date or service_date).isoformat(),
            "paid_date": paid_date.isoformat(),
            "admission_date": service_date.isoformat() if claim_type == "Institutional_IP" else None,
            "discharge_date": discharge_date.isoformat() if discharge_date else None,
            "admission_type": admission_type,
            "discharge_status": discharge_status,
            "bill_type": bill_type,
            "place_of_service_code": pos_code,
            "place_of_service_desc": pos_desc,
            "procedure_code": inject_dq_issue(procedure_code, "code"),
            "procedure_desc": procedure_desc,
            "revenue_code": revenue_code,
            "revenue_desc": revenue_desc,
            "drg_code": drg_code,
            "drg_desc": drg_desc,
            "primary_diagnosis_code": inject_dq_issue(dx_primary[0], "code"),
            "primary_diagnosis_desc": dx_primary[1],
            "secondary_diagnosis_code_1": dx_secondary[0][0] if len(dx_secondary) > 0 else None,
            "secondary_diagnosis_code_2": dx_secondary[1][0] if len(dx_secondary) > 1 else None,
            "secondary_diagnosis_code_3": dx_secondary[2][0] if len(dx_secondary) > 2 else None,
            "billed_amount": billed,
            "allowed_amount": allowed,
            "paid_amount": paid_amount,
            "copay": copay,
            "coinsurance": coinsurance,
            "deductible": deductible,
            "member_responsibility": member_responsibility,
            "claim_status": claim_status,
            "denial_reason_code": denial_reason,
            "adjustment_reason": random.choice(reference_data.ADJUSTMENT_CODES) if claim_status == "Adjusted" else None,
            "source_system": random.choice(["FACETS", "QNXT", "HealthEdge"]),
            "_lob": enr.get("line_of_business", "Commercial"),
        }
        claims.append(claim)

    # --- Post-generation scaling: match dollar totals to target MLR per LOB ---
    # Medical's share of total claims spend (pharmacy takes the remainder)
    medical_share = {
        "Medicare Advantage": 0.82,
        "Medicaid": 0.88,
        "Commercial": 0.86,
        "ACA Marketplace": 0.87,
    }
    # Compute per-LOB premium pool from enrollment (using actual eligibility dates)
    lob_premium_pool: Dict[str, float] = {}
    for e in enrollment:
        lob = e.get("line_of_business", "Commercial")
        premium = _safe_premium(e)
        coverage = _coverage_months_in_window(e)
        lob_premium_pool[lob] = lob_premium_pool.get(lob, 0) + premium * coverage

    # Per-LOB medical claims budget = premiums × MLR × medical share
    lob_budget: Dict[str, float] = {}
    for lob, pool in lob_premium_pool.items():
        mlr = target_mlr.get(lob, 0.85)
        ms = medical_share.get(lob, 0.85)
        lob_budget[lob] = pool * mlr * ms

    # Actual per-LOB paid totals from generated claims
    lob_actual: Dict[str, float] = {}
    for c in claims:
        lob_actual[c["_lob"]] = lob_actual.get(c["_lob"], 0) + c["paid_amount"]

    # Compute scale factors
    lob_scale: Dict[str, float] = {}
    for lob in lob_actual:
        budget = lob_budget.get(lob, 0)
        actual = lob_actual[lob]
        lob_scale[lob] = budget / actual if actual > 0 else 1.0

    # Apply scaling, then DQ injection on dollar fields
    dollar_fields = ("billed_amount", "allowed_amount", "paid_amount",
                     "copay", "coinsurance", "deductible", "member_responsibility")
    for c in claims:
        sf = lob_scale.get(c.pop("_lob"), 1.0)
        for field in dollar_fields:
            c[field] = round(c[field] * sf, 2)
        c["billed_amount"] = inject_dq_issue(c["billed_amount"], "amount")
        c["allowed_amount"] = inject_dq_issue(c["allowed_amount"], "amount")

    return claims


def generate_pharmacy_claims(
    enrollment: List[Dict[str, Any]],
    providers: List[Dict[str, Any]],
    n: int = 20000,
) -> List[Dict[str, Any]]:
    """Generate pharmacy claims (NDC, fill date, cost, tier, status).
    enrollment: list of dicts with member_id and line_of_business.
    """
    claims = []
    # Weight pharmacy claims proportionally to premium pool (pharmacy ~15% of total claims)
    target_rx_share = {
        "Medicare Advantage": 0.18,
        "Medicaid": 0.12,
        "Commercial": 0.14,
        "ACA Marketplace": 0.13,
    }
    member_weights = []
    for e in enrollment:
        premium = _safe_premium(e)
        coverage = _coverage_months_in_window(e)
        lob = e.get("line_of_business", "Commercial")
        rx_share = target_rx_share.get(lob, 0.14)
        member_weights.append(premium * coverage * rx_share)
    prescriber_npis = [p["npi"] for p in providers if p.get("npi")]
    drug_weights = [2 if d[4] else 15 for d in reference_data.PHARMACY_DRUGS]
    pharmacy_names = ["CVS Pharmacy", "Walgreens", "Rite Aid", "Walmart Pharmacy", "Express Scripts Mail Order"]
    pharmacy_npis = [generate_npi() for _ in range(20)]
    fallback_start = date(2023, 1, 1)
    fallback_end = date(2025, 12, 31)

    for i in range(n):
        drug = weighted_choice(reference_data.PHARMACY_DRUGS, drug_weights)
        ndc, drug_name, therapeutic_class, avg_cost, is_specialty = drug
        enr = random.choices(enrollment, weights=member_weights, k=1)[0]
        member_id = enr["member_id"]
        # Constrain fill date to member's eligibility window
        elig_start = enr.get("eligibility_start_date") or fallback_start
        elig_end = enr.get("eligibility_end_date") or fallback_end
        if isinstance(elig_start, str):
            elig_start = date.fromisoformat(elig_start)
        if isinstance(elig_end, str):
            elig_end = date.fromisoformat(elig_end)
        prescriber_npi = random.choice(prescriber_npis) if prescriber_npis else generate_npi()
        pharmacy_npi = random.choice(pharmacy_npis)
        pharmacy_name = random.choice(pharmacy_names)
        fill_date = random_date_between(elig_start, min(elig_end, fallback_end))
        paid_date = apply_payment_lag(fill_date)
        days_supply = random.choice([30, 30, 60, 90])
        supply_factor = days_supply / 30.0
        ingredient_cost = round(avg_cost * supply_factor * random.uniform(0.85, 1.15), 2)
        dispensing_fee = round(random.uniform(1.50, 12.00), 2)
        total_cost = round(ingredient_cost + dispensing_fee, 2)
        quantity = random.choice([30, 60, 90]) if days_supply >= 30 else random.randint(1, 10)
        copay = round(random.uniform(0, 100), 2) if not is_specialty else round(random.uniform(50, 300), 2)
        plan_paid = round(max(0.0, total_cost - copay), 2)
        claim_status = random.choices(["Paid", "Reversed", "Rejected"], weights=[88, 7, 5], k=1)[0]
        if claim_status != "Paid":
            plan_paid = 0.0
        formulary_tier = "Tier 1 - Generic" if avg_cost < 20 else "Tier 2 - Preferred Brand" if avg_cost < 100 else "Tier 4 - Specialty"

        claim = {
            "claim_id": generate_claim_id("RX"),
            "member_id": member_id,
            "prescriber_npi": inject_dq_issue(prescriber_npi, "code"),
            "pharmacy_npi": pharmacy_npi,
            "pharmacy_name": pharmacy_name,
            "fill_date": inject_dq_issue(fill_date.isoformat(), "date"),
            "paid_date": paid_date.isoformat(),
            "ndc": ndc,
            "drug_name": drug_name,
            "therapeutic_class": therapeutic_class,
            "is_specialty": is_specialty,
            "days_supply": days_supply,
            "quantity": quantity,
            "ingredient_cost": ingredient_cost,
            "dispensing_fee": dispensing_fee,
            "total_cost": total_cost,
            "member_copay": copay,
            "plan_paid": plan_paid,
            "claim_status": claim_status,
            "formulary_tier": formulary_tier,
            "mail_order_flag": "Y" if "Mail Order" in pharmacy_name else "N",
            "_lob": enr.get("line_of_business", "Commercial"),
        }
        claims.append(claim)

    # --- Post-generation scaling: match pharmacy spend to target MLR × Rx share ---
    # Compute per-LOB premium pool (using actual eligibility dates)
    lob_premium_pool: Dict[str, float] = {}
    for e in enrollment:
        lob = e.get("line_of_business", "Commercial")
        premium = _safe_premium(e)
        coverage = _coverage_months_in_window(e)
        lob_premium_pool[lob] = lob_premium_pool.get(lob, 0) + premium * coverage

    # Target MLR for pharmacy
    target_mlr = {
        "Medicare Advantage": 0.88,
        "Medicaid": 0.87,
        "Commercial": 0.83,
        "ACA Marketplace": 0.82,
    }

    # Pharmacy budget = premiums × MLR × Rx share
    lob_budget: Dict[str, float] = {}
    for lob, pool in lob_premium_pool.items():
        mlr = target_mlr.get(lob, 0.85)
        rx = target_rx_share.get(lob, 0.14)
        lob_budget[lob] = pool * mlr * rx

    # Actual per-LOB plan_paid totals
    lob_actual: Dict[str, float] = {}
    for c in claims:
        lob_actual[c["_lob"]] = lob_actual.get(c["_lob"], 0) + c["plan_paid"]

    # Scale factors
    lob_scale: Dict[str, float] = {}
    for lob in lob_actual:
        budget = lob_budget.get(lob, 0)
        actual = lob_actual[lob]
        lob_scale[lob] = budget / actual if actual > 0 else 1.0

    # Apply scaling, then DQ injection
    rx_dollar_fields = ("ingredient_cost", "dispensing_fee", "total_cost", "member_copay", "plan_paid")
    for c in claims:
        sf = lob_scale.get(c.pop("_lob"), 1.0)
        for field in rx_dollar_fields:
            c[field] = round(c[field] * sf, 2)
        c["ingredient_cost"] = inject_dq_issue(c["ingredient_cost"], "amount")

    return claims
