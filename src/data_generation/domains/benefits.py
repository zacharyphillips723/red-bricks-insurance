# Red Bricks Insurance — benefits domain (plan benefit schedules, cost-sharing,
# actuarial parameters, and utilization assumptions for digital twin modeling).

import random
from typing import Any, Dict, List

# Benefit categories and their typical sub-benefits per LOB
BENEFIT_SCHEDULE = {
    "Medical - Inpatient": [
        ("Inpatient Hospital Stay", "IP"),
        ("Skilled Nursing Facility", "SNF"),
        ("Inpatient Rehabilitation", "REHAB"),
    ],
    "Medical - Outpatient": [
        ("PCP Office Visit", "PCP"),
        ("Specialist Office Visit", "SPEC"),
        ("Urgent Care Visit", "UC"),
        ("Emergency Room Visit", "ER"),
        ("Outpatient Surgery", "OP_SURG"),
        ("Diagnostic Lab & X-Ray", "LAB"),
        ("Advanced Imaging (MRI/CT/PET)", "IMG"),
    ],
    "Medical - Preventive": [
        ("Annual Wellness Visit", "PREV"),
        ("Immunizations", "IMM"),
        ("Cancer Screenings", "SCREEN"),
    ],
    "Pharmacy": [
        ("Generic Drugs", "RX_GEN"),
        ("Preferred Brand Drugs", "RX_PREF"),
        ("Non-Preferred Brand Drugs", "RX_NPREF"),
        ("Specialty Drugs", "RX_SPEC"),
    ],
    "Behavioral Health": [
        ("Outpatient Mental Health Visit", "BH_OP"),
        ("Inpatient Mental Health", "BH_IP"),
        ("Substance Abuse Treatment", "BH_SA"),
        ("Telehealth Therapy", "BH_TELE"),
    ],
    "Ancillary": [
        ("Physical Therapy", "PT"),
        ("Occupational Therapy", "OT"),
        ("Speech Therapy", "ST"),
        ("Chiropractic Services", "CHIRO"),
        ("Durable Medical Equipment", "DME"),
        ("Home Health Services", "HH"),
    ],
    "Vision": [
        ("Annual Eye Exam", "VIS_EXAM"),
        ("Eyeglass Frames & Lenses", "VIS_GLASS"),
        ("Contact Lenses", "VIS_CONTACT"),
    ],
    "Dental": [
        ("Preventive Dental (cleaning)", "DENT_PREV"),
        ("Basic Dental (fillings)", "DENT_BASIC"),
        ("Major Dental (crowns/bridges)", "DENT_MAJOR"),
        ("Orthodontia", "DENT_ORTHO"),
    ],
}

# Expected utilization per 1000 members and average unit cost by benefit code
# (utilization_per_1000, avg_unit_cost, elasticity_factor)
# Elasticity: how sensitive utilization is to cost-sharing changes
# e.g., -0.2 means a 10% copay increase → 2% utilization drop
UTILIZATION_ASSUMPTIONS = {
    "IP":        (65, 18000, -0.05),
    "SNF":       (12, 8500, -0.08),
    "REHAB":     (8, 6000, -0.10),
    "PCP":       (3200, 175, -0.15),
    "SPEC":      (1800, 280, -0.20),
    "UC":        (450, 250, -0.25),
    "ER":        (380, 1800, -0.10),
    "OP_SURG":   (85, 5500, -0.12),
    "LAB":       (4500, 45, -0.05),
    "IMG":       (600, 950, -0.15),
    "PREV":      (800, 200, 0.0),     # Preventive: no elasticity (mandated)
    "IMM":       (600, 50, 0.0),
    "SCREEN":    (400, 350, 0.0),
    "RX_GEN":    (8500, 15, -0.08),
    "RX_PREF":   (3200, 120, -0.18),
    "RX_NPREF":  (800, 280, -0.25),
    "RX_SPEC":   (45, 5500, -0.10),
    "BH_OP":     (1200, 180, -0.20),
    "BH_IP":     (15, 12000, -0.08),
    "BH_SA":     (25, 8000, -0.10),
    "BH_TELE":   (900, 120, -0.12),
    "PT":        (600, 140, -0.18),
    "OT":        (200, 150, -0.18),
    "ST":        (120, 160, -0.18),
    "CHIRO":     (350, 75, -0.22),
    "DME":       (180, 450, -0.15),
    "HH":        (90, 250, -0.10),
    "VIS_EXAM":  (500, 120, -0.15),
    "VIS_GLASS": (350, 200, -0.20),
    "VIS_CONTACT": (200, 180, -0.20),
    "DENT_PREV": (1200, 150, -0.05),
    "DENT_BASIC": (600, 250, -0.18),
    "DENT_MAJOR": (120, 1200, -0.22),
    "DENT_ORTHO": (40, 5000, -0.15),
}

# Benefit descriptions for agent consumption
BENEFIT_DESCRIPTIONS = {
    "IP": "Covers inpatient hospital stays including room, board, nursing care, and ancillary services. Prior authorization required for elective admissions. Emergency admissions reviewed retrospectively.",
    "SNF": "Covers skilled nursing facility care following a qualifying hospital stay of 3+ days. Limited to medically necessary rehabilitation and skilled nursing services.",
    "REHAB": "Covers inpatient rehabilitation for conditions requiring intensive therapy (stroke recovery, joint replacement, traumatic injury). Prior authorization required.",
    "PCP": "Covers office visits with the member's designated primary care physician for evaluation, management, and routine care coordination.",
    "SPEC": "Covers office visits with specialist physicians. Referral from PCP may be required depending on plan type (HMO requires referral, PPO does not).",
    "UC": "Covers urgent care center visits for non-emergency conditions that require prompt attention outside of normal PCP office hours.",
    "ER": "Covers emergency room visits for conditions that a prudent layperson would consider an emergency. Non-emergency ER use may result in higher cost-sharing.",
    "OP_SURG": "Covers outpatient surgical procedures performed in ambulatory surgery centers or hospital outpatient departments. Prior authorization required for most procedures.",
    "LAB": "Covers diagnostic laboratory tests and basic radiology (X-ray) ordered by a treating physician. Preventive labs covered at $0 under ACA.",
    "IMG": "Covers advanced diagnostic imaging including MRI, CT scan, and PET scan. Prior authorization required to ensure medical necessity.",
    "PREV": "Covers annual wellness visits and preventive care services as defined by USPSTF A/B recommendations. No cost-sharing under ACA mandate.",
    "IMM": "Covers ACIP-recommended immunizations at no cost to the member under ACA preventive care mandate.",
    "SCREEN": "Covers recommended cancer screenings (mammography, colonoscopy, cervical, lung) per USPSTF guidelines at no cost-sharing.",
    "RX_GEN": "Covers FDA-approved generic medications. Lowest cost-sharing tier. 30-day retail or 90-day mail order supply.",
    "RX_PREF": "Covers preferred brand-name medications on the plan formulary. Moderate cost-sharing. Step therapy may apply.",
    "RX_NPREF": "Covers non-preferred brand medications. Higher cost-sharing applies. Prior authorization or step therapy may be required.",
    "RX_SPEC": "Covers specialty medications for complex conditions (biologics, oncology, autoimmune). Prior authorization and specialty pharmacy required. Often coinsurance-based.",
    "BH_OP": "Covers outpatient mental health visits including individual therapy, psychiatric evaluation, and medication management. Mental Health Parity Act applies.",
    "BH_IP": "Covers inpatient mental health treatment and crisis stabilization. Prior authorization required. Mental Health Parity Act ensures equivalent coverage to medical inpatient.",
    "BH_SA": "Covers substance use disorder treatment including detoxification, residential treatment, and intensive outpatient programs. Mental Health Parity Act applies.",
    "BH_TELE": "Covers telehealth-delivered therapy and psychiatric services. Same cost-sharing as in-person behavioral health visits.",
    "PT": "Covers physical therapy services for rehabilitation of musculoskeletal conditions, post-surgical recovery, and injury treatment. Visit limits may apply.",
    "OT": "Covers occupational therapy to help members regain ability to perform daily activities after illness, injury, or disability.",
    "ST": "Covers speech-language pathology services for communication disorders, swallowing difficulties, and cognitive-communication rehabilitation.",
    "CHIRO": "Covers chiropractic services for spinal manipulation and musculoskeletal treatment. Visit limits typically apply per plan year.",
    "DME": "Covers durable medical equipment (wheelchairs, CPAP, prosthetics, glucose monitors). Prior authorization required for items over $500.",
    "HH": "Covers skilled home health services including nursing, therapy, and aide services for homebound members. Prior authorization required.",
    "VIS_EXAM": "Covers one annual comprehensive eye exam including dilation and refraction for vision correction prescription.",
    "VIS_GLASS": "Covers eyeglass frames and lenses. Annual allowance applies. Member pays difference for frames exceeding allowance.",
    "VIS_CONTACT": "Covers contact lens fitting and annual supply. Allowance may substitute for eyeglass benefit. Elective contacts subject to plan limits.",
    "DENT_PREV": "Covers preventive dental services: cleanings (2/year), oral exams, bitewing X-rays, and fluoride treatment for children.",
    "DENT_BASIC": "Covers basic restorative dental: fillings, simple extractions, and periodontal maintenance. Subject to annual maximum.",
    "DENT_MAJOR": "Covers major dental procedures: crowns, bridges, dentures, root canals, and oral surgery. Subject to annual maximum and waiting period.",
    "DENT_ORTHO": "Covers orthodontic treatment (braces, aligners) for dependent children. Lifetime maximum applies. Adult orthodontia may not be covered.",
}

# Regulatory mandate flags by benefit code
REGULATORY_MANDATES = {
    "PREV": "ACA Section 2713 — Preventive services without cost-sharing",
    "IMM": "ACA Section 2713 — ACIP-recommended immunizations",
    "SCREEN": "ACA Section 2713 — USPSTF A/B recommended screenings",
    "BH_OP": "Mental Health Parity and Addiction Equity Act (MHPAEA)",
    "BH_IP": "Mental Health Parity and Addiction Equity Act (MHPAEA)",
    "BH_SA": "Mental Health Parity and Addiction Equity Act (MHPAEA)",
    "BH_TELE": "Mental Health Parity and Addiction Equity Act (MHPAEA)",
    "ER": "ACA — Emergency services must be covered without prior authorization",
}

# Clinical guideline references by benefit code
CLINICAL_GUIDELINES = {
    "PREV": "USPSTF A/B Recommendations; ACA Preventive Services List",
    "IMM": "CDC/ACIP Immunization Schedule",
    "SCREEN": "USPSTF Cancer Screening Guidelines (breast, colorectal, cervical, lung)",
    "RX_SPEC": "NCCN Clinical Practice Guidelines; FDA REMS Programs",
    "BH_OP": "APA Practice Guidelines for Mental Disorders",
    "BH_SA": "ASAM Criteria for Substance Use Disorder Treatment",
    "PT": "APTA Clinical Practice Guidelines",
    "DME": "CMS DME Coverage Criteria; LCD/NCD Policies",
    "IP": "InterQual/Milliman Clinical Guidelines for Inpatient Admission",
    "IMG": "ACR Appropriateness Criteria for Imaging",
}

# Allowed amount schedule options by LOB
ALLOWED_AMOUNT_SCHEDULES = {
    "Commercial":         ["130% Medicare", "140% Medicare", "Custom Negotiated", "UCR 80th Pctile"],
    "Medicare Advantage": ["100% Medicare", "105% Medicare"],
    "Medicaid":           ["100% Medicaid Fee Schedule", "110% Medicaid Fee Schedule"],
    "ACA Marketplace":    ["120% Medicare", "130% Medicare", "UCR 70th Pctile"],
}

# Network tier options
NETWORK_TIERS = {
    "HMO": "Narrow", "SNP": "Narrow", "Managed Care": "Narrow",
    "PPO": "Broad", "POS": "Moderate", "PFFS": "Broad", "HDHP": "Broad",
    "Platinum": "Broad", "Gold": "Broad", "Silver": "Moderate", "Bronze": "Moderate",
    "FFS": "Open Access",
}

# Actuarial value by plan type
ACTUARIAL_VALUES = {
    "Platinum": 90, "Gold": 80, "Silver": 70, "Bronze": 60,
    "HMO": 85, "PPO": 75, "POS": 78, "HDHP": 62, "SNP": 90,
    "PFFS": 72, "Managed Care": 88, "FFS": 70,
}

# Cost-sharing ranges by plan generosity tier
PLAN_GENEROSITY = {
    "rich":     {"copay": (0, 20),   "coins": (0, 10),  "oon_mult": 2.0, "oon_add": 20},
    "moderate": {"copay": (20, 50),  "coins": (10, 25), "oon_mult": 2.0, "oon_add": 20},
    "lean":     {"copay": (40, 75),  "coins": (20, 40), "oon_mult": 2.5, "oon_add": 25},
    "hdhp":     {"copay": (0, 0),    "coins": (10, 30), "oon_mult": 2.5, "oon_add": 30},
}

# Map plan types to generosity
PLAN_TYPE_GENEROSITY = {
    "PPO": "moderate", "HMO": "rich", "POS": "moderate", "HDHP": "hdhp",
    "PFFS": "moderate", "SNP": "rich",
    "Managed Care": "rich", "FFS": "moderate",
    "Platinum": "rich", "Gold": "moderate", "Silver": "lean", "Bronze": "lean",
}

# Deductible / OOP max ranges by LOB
ACCUM_RANGES = {
    "Commercial":         {"ind_ded": (500, 3000),  "fam_ded": (1000, 6000),  "ind_oop": (4000, 9100),   "fam_oop": (8000, 18200)},
    "Medicare Advantage": {"ind_ded": (0, 500),     "fam_ded": (0, 0),        "ind_oop": (3000, 7550),   "fam_oop": (0, 0)},
    "Medicaid":           {"ind_ded": (0, 100),     "fam_ded": (0, 200),      "ind_oop": (0, 1000),      "fam_oop": (0, 2000)},
    "ACA Marketplace":    {"ind_ded": (250, 7500),  "fam_ded": (500, 15000),  "ind_oop": (4000, 9100),   "fam_oop": (8000, 18200)},
}


def _pick_generosity(plan_type: str) -> dict:
    tier = PLAN_TYPE_GENEROSITY.get(plan_type, "moderate")
    return PLAN_GENEROSITY[tier]


def _cost_sharing(gen: dict, benefit_code: str) -> dict:
    """Generate cost-sharing for a benefit based on plan generosity."""
    if benefit_code in ("PREV", "IMM", "SCREEN"):
        return {
            "in_network_copay": 0.0,
            "in_network_coinsurance_pct": 0,
            "out_of_network_copay": 0.0,
            "out_of_network_coinsurance_pct": 0,
        }

    copay_lo, copay_hi = gen["copay"]
    coins_lo, coins_hi = gen["coins"]

    if benefit_code.startswith("RX_SPEC"):
        copay = 0.0
        coins = random.randint(25, 50)
    elif benefit_code.startswith("RX_"):
        copay = float(random.choice([5, 10, 15, 20, 30, 40, 50, 65]))
        coins = 0
    elif benefit_code in ("IP", "SNF", "REHAB", "BH_IP", "ER"):
        copay = float(random.choice([100, 150, 200, 250, 300, 350, 500]))
        coins = random.randint(coins_lo, coins_hi)
    else:
        copay = float(random.randint(copay_lo, copay_hi))
        coins = random.randint(coins_lo, coins_hi)

    oon_copay = round(copay * gen["oon_mult"], 2)
    oon_coins = min(coins + gen["oon_add"], 100)

    return {
        "in_network_copay": copay,
        "in_network_coinsurance_pct": coins,
        "out_of_network_copay": oon_copay,
        "out_of_network_coinsurance_pct": oon_coins,
    }


def generate_benefits(enrollment_data: List[Dict[str, Any]], seed: int = 42) -> List[Dict[str, Any]]:
    """Generate benefit schedule rows for each enrolled plan.

    Includes Tier 1 digital twin fields: actuarial parameters, utilization
    assumptions, elasticity factors, versioning, and agent-friendly metadata.

    Returns one row per (plan_id, benefit_name) combination — typically ~30 rows
    per plan. Total: ~150k rows for 5000 members.
    """
    random.seed(seed)
    benefits = []
    benefit_counter = 0

    # Cost trend assumptions for the plan year (vary slightly per LOB)
    medical_trend_by_lob = {
        "Commercial": round(random.uniform(1.06, 1.09), 3),
        "Medicare Advantage": round(random.uniform(1.04, 1.07), 3),
        "Medicaid": round(random.uniform(1.03, 1.06), 3),
        "ACA Marketplace": round(random.uniform(1.07, 1.10), 3),
    }
    pharmacy_trend_by_lob = {
        "Commercial": round(random.uniform(1.08, 1.12), 3),
        "Medicare Advantage": round(random.uniform(1.07, 1.11), 3),
        "Medicaid": round(random.uniform(1.05, 1.09), 3),
        "ACA Marketplace": round(random.uniform(1.09, 1.13), 3),
    }

    for enroll in enrollment_data:
        plan_id = enroll["plan_id"]
        lob = enroll["line_of_business"]
        plan_type = enroll["plan_type"]
        member_id = enroll["member_id"]

        gen = _pick_generosity(plan_type)
        accum = ACCUM_RANGES.get(lob, ACCUM_RANGES["Commercial"])

        # Plan-level accumulators
        ind_ded = round(random.uniform(*accum["ind_ded"]) / 50) * 50
        fam_ded = round(random.uniform(*accum["fam_ded"]) / 100) * 100 if accum["fam_ded"][1] > 0 else 0
        ind_oop = round(random.uniform(*accum["ind_oop"]) / 100) * 100
        fam_oop = round(random.uniform(*accum["fam_oop"]) / 100) * 100 if accum["fam_oop"][1] > 0 else 0

        # Plan-level Tier 1 fields
        actuarial_value = ACTUARIAL_VALUES.get(plan_type, 75)
        allowed_schedule = random.choice(ALLOWED_AMOUNT_SCHEDULES.get(lob, ["Custom Negotiated"]))
        network_tier = NETWORK_TIERS.get(plan_type, "Broad")
        medical_trend = medical_trend_by_lob.get(lob, 1.07)
        pharmacy_trend = pharmacy_trend_by_lob.get(lob, 1.10)
        # Age-sex factor: varies by member (simplified)
        age_sex_factor = round(random.uniform(0.5, 3.5), 3)

        # Benefit effective dates — aligned to plan year
        elig_start = enroll.get("eligibility_start_date") or "2024-01-01"
        benefit_effective = elig_start if isinstance(elig_start, str) else elig_start.isoformat()
        elig_end = enroll.get("eligibility_end_date") or "2026-12-31"
        benefit_termination = elig_end if isinstance(elig_end, str) else elig_end.isoformat()

        for category, sub_benefits in BENEFIT_SCHEDULE.items():
            if category == "Dental" and lob == "Medicare Advantage" and random.random() < 0.4:
                continue
            if category == "Vision" and random.random() < 0.2:
                continue

            for benefit_name, benefit_code in sub_benefits:
                benefit_counter += 1
                cost = _cost_sharing(gen, benefit_code)

                prior_auth = benefit_code in (
                    "IP", "SNF", "REHAB", "OP_SURG", "IMG",
                    "RX_SPEC", "BH_IP", "DME", "DENT_MAJOR", "DENT_ORTHO",
                )

                ded_applies = benefit_code not in ("PCP", "SPEC", "UC", "PREV", "IMM", "SCREEN")
                if plan_type == "HDHP":
                    ded_applies = True

                visit_limit = None
                annual_limit = None
                if benefit_code in ("PT", "OT", "ST"):
                    visit_limit = random.choice([20, 30, 40, 60])
                elif benefit_code == "CHIRO":
                    visit_limit = random.choice([12, 20, 26])
                elif benefit_code == "BH_OP":
                    visit_limit = random.choice([20, 30, 52, None])
                elif benefit_code == "DENT_PREV":
                    visit_limit = 2
                elif benefit_code == "VIS_GLASS":
                    annual_limit = random.choice([150, 200, 250])
                elif benefit_code == "DENT_MAJOR":
                    annual_limit = random.choice([1000, 1500, 2000])
                elif benefit_code == "DENT_ORTHO":
                    annual_limit = random.choice([1500, 2000, 2500])

                # Tier 1: utilization and actuarial fields
                util_base = UTILIZATION_ASSUMPTIONS.get(benefit_code, (500, 200, -0.15))
                util_per_1000 = util_base[0]
                unit_cost = util_base[1]
                elasticity = util_base[2]

                # Add some variance per plan (+/- 15%)
                expected_util = round(util_per_1000 * random.uniform(0.85, 1.15), 1)
                unit_cost_assumption = round(unit_cost * random.uniform(0.90, 1.10), 2)

                # Pick the right trend factor
                is_pharmacy = benefit_code.startswith("RX_")
                cost_trend = pharmacy_trend if is_pharmacy else medical_trend

                rec = {
                    "benefit_id": f"BEN{benefit_counter:07d}",
                    "plan_id": plan_id,
                    "member_id": member_id,
                    "line_of_business": lob,
                    "plan_type": plan_type,
                    "benefit_category": category,
                    "benefit_name": benefit_name,
                    "benefit_code": benefit_code,
                    # Cost-sharing
                    "in_network_copay": cost["in_network_copay"],
                    "in_network_coinsurance_pct": cost["in_network_coinsurance_pct"],
                    "out_of_network_copay": cost["out_of_network_copay"],
                    "out_of_network_coinsurance_pct": cost["out_of_network_coinsurance_pct"],
                    "deductible_applies": ded_applies,
                    "prior_auth_required": prior_auth,
                    "visit_limit": visit_limit,
                    "annual_limit": float(annual_limit) if annual_limit else None,
                    "coverage_level": random.choice(["Individual", "Family"]),
                    # Plan accumulators
                    "individual_deductible": float(ind_ded),
                    "family_deductible": float(fam_ded),
                    "individual_oop_max": float(ind_oop),
                    "family_oop_max": float(fam_oop),
                    # Tier 1: Actuarial / pricing levers
                    "actuarial_value_pct": actuarial_value,
                    "allowed_amount_schedule": allowed_schedule,
                    "network_tier": network_tier,
                    "cost_trend_factor": cost_trend,
                    "pharmacy_trend_factor": pharmacy_trend,
                    "age_sex_factor": age_sex_factor,
                    # Tier 1: Utilization modeling
                    "expected_utilization_per_1000": expected_util,
                    "unit_cost_assumption": unit_cost_assumption,
                    "elasticity_factor": elasticity,
                    # Tier 1: Benefit versioning
                    "benefit_effective_date": benefit_effective,
                    "benefit_termination_date": benefit_termination,
                    "benefit_version": 1,
                    "scenario_id": "baseline",
                    "is_baseline": True,
                    # Tier 1: Agent-friendly metadata
                    "benefit_description": BENEFIT_DESCRIPTIONS.get(benefit_code, ""),
                    "clinical_guideline_ref": CLINICAL_GUIDELINES.get(benefit_code),
                    "regulatory_mandate": REGULATORY_MANDATES.get(benefit_code),
                }
                benefits.append(rec)

    return benefits
