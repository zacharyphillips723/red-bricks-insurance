# Red Bricks Insurance — claims domain (medical + pharmacy): allowed, charged, paid, codes, dates.

import random
from datetime import date, timedelta
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


def generate_medical_claims(
    enrollment: List[Dict[str, Any]],
    providers: List[Dict[str, Any]],
    n_total: int = 50000,
) -> List[Dict[str, Any]]:
    """Generate medical claims: inpatient, outpatient, professional, ER. Billed >= allowed >= paid.
    enrollment: list of dicts with member_id and line_of_business (for utilization weighting).
    """
    claims = []
    lob_weight = {
        "Medicare Advantage": 2.0,
        "Commercial": 1.0,
        "ACA Marketplace": 0.9,
        "Medicaid": 0.7,
    }
    member_ids = [e["member_id"] for e in enrollment]
    member_weights = [lob_weight.get(e.get("line_of_business", "Commercial"), 1.0) for e in enrollment]
    provider_npis = [p["npi"] for p in providers if p.get("npi")]

    claim_types = ["Professional", "Institutional_OP", "ER", "Institutional_IP"]
    claim_weights = [60, 20, 10, 10]
    service_start = date(2023, 1, 1)
    service_end = date(2025, 12, 31)

    for i in range(n_total):
        claim_type = weighted_choice(claim_types, claim_weights)
        claim_id = generate_claim_id("MC")
        claim_line = random.randint(1, 3) if claim_type != "Professional" else 1
        member_id = random.choices(member_ids, weights=member_weights, k=1)[0]
        rendering_npi = random.choice(provider_npis) if provider_npis else generate_npi()
        billing_npi = random.choice(provider_npis) if provider_npis else generate_npi()
        service_date = random_date_between(service_start, service_end)
        paid_date = apply_payment_lag(service_date)

        dx_primary = weighted_choice(ICD10, ICD10_W)
        num_secondary = random.randint(0, 3)
        dx_secondary = random.choices(ICD10, weights=ICD10_W, k=num_secondary)

        claim_status = random.choices(
            ["Paid", "Denied", "Adjusted", "Pended"],
            weights=[82, 8, 6, 4], k=1
        )[0]
        denial_reason = None
        if claim_status == "Denied":
            denial_reason = random.choice(reference_data.DENIAL_CODES)

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
            "billed_amount": inject_dq_issue(billed, "amount"),
            "allowed_amount": inject_dq_issue(allowed, "amount"),
            "paid_amount": paid_amount,
            "copay": copay,
            "coinsurance": coinsurance,
            "deductible": deductible,
            "member_responsibility": member_responsibility,
            "claim_status": claim_status,
            "denial_reason_code": denial_reason,
            "adjustment_reason": random.choice(reference_data.ADJUSTMENT_CODES) if claim_status == "Adjusted" else None,
            "source_system": random.choice(["FACETS", "QNXT", "HealthEdge"]),
        }
        claims.append(claim)
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
    lob_rx_weight = {"Medicare Advantage": 2.2, "Commercial": 1.0, "ACA Marketplace": 0.9, "Medicaid": 0.8}
    member_ids = [e["member_id"] for e in enrollment]
    member_weights = [lob_rx_weight.get(e.get("line_of_business", "Commercial"), 1.0) for e in enrollment]
    prescriber_npis = [p["npi"] for p in providers if p.get("npi")]
    drug_weights = [2 if d[4] else 15 for d in reference_data.PHARMACY_DRUGS]
    pharmacy_names = ["CVS Pharmacy", "Walgreens", "Rite Aid", "Walmart Pharmacy", "Express Scripts Mail Order"]
    pharmacy_npis = [generate_npi() for _ in range(20)]
    service_start = date(2023, 1, 1)
    service_end = date(2025, 12, 31)

    for i in range(n):
        drug = weighted_choice(reference_data.PHARMACY_DRUGS, drug_weights)
        ndc, drug_name, therapeutic_class, avg_cost, is_specialty = drug
        member_id = random.choices(member_ids, weights=member_weights, k=1)[0]
        prescriber_npi = random.choice(prescriber_npis) if prescriber_npis else generate_npi()
        pharmacy_npi = random.choice(pharmacy_npis)
        pharmacy_name = random.choice(pharmacy_names)
        fill_date = random_date_between(service_start, service_end)
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
            "ingredient_cost": inject_dq_issue(ingredient_cost, "amount"),
            "dispensing_fee": dispensing_fee,
            "total_cost": total_cost,
            "member_copay": copay,
            "plan_paid": plan_paid,
            "claim_status": claim_status,
            "formulary_tier": formulary_tier,
            "mail_order_flag": "Y" if "Mail Order" in pharmacy_name else "N",
        }
        claims.append(claim)
    return claims
