# Red Bricks Insurance — care management domain: disease mgmt, case mgmt, SDOH, TOC, care gaps.

import random
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

from ..dq import inject_dq_issue
from ..helpers import random_date_between, weighted_choice, COUNTIES

# ── Date window ──────────────────────────────────────────────────────────────
_CM_START = date(2023, 1, 1)
_CM_END = date(2025, 12, 31)

# ── Reference: Disease Management Programs ───────────────────────────────────
CARE_PROGRAMS = [
    {
        "program_id": "PGM001",
        "program_name": "Diabetes Management",
        "program_type": "Disease Management",
        "target_conditions": ["E11.9", "E11.65"],
        "milestones": [
            ("Initial Assessment", 7),
            ("Care Plan Created", 14),
            ("First Follow-Up", 30),
            ("HbA1c Recheck", 90),
            ("Annual Review", 365),
        ],
    },
    {
        "program_id": "PGM002",
        "program_name": "CHF Care",
        "program_type": "Disease Management",
        "target_conditions": ["I50.9"],
        "milestones": [
            ("Initial Assessment", 7),
            ("Weight Monitoring Setup", 14),
            ("Medication Optimization", 30),
            ("Cardiac Rehab Referral", 60),
            ("Quarterly Review", 90),
        ],
    },
    {
        "program_id": "PGM003",
        "program_name": "COPD Wellness",
        "program_type": "Disease Management",
        "target_conditions": ["J44.1"],
        "milestones": [
            ("Initial Assessment", 7),
            ("Pulmonary Rehab Referral", 21),
            ("Action Plan Created", 30),
            ("Smoking Cessation Check", 60),
            ("Quarterly Review", 90),
        ],
    },
    {
        "program_id": "PGM004",
        "program_name": "Behavioral Health Integration",
        "program_type": "Disease Management",
        "target_conditions": ["F32.9", "F41.1"],
        "milestones": [
            ("PHQ-9 Baseline", 7),
            ("Therapy Initiated", 14),
            ("Medication Review", 30),
            ("PHQ-9 Follow-Up", 60),
            ("Quarterly Review", 90),
        ],
    },
    {
        "program_id": "PGM005",
        "program_name": "Maternal Health",
        "program_type": "Disease Management",
        "target_conditions": ["O80"],
        "milestones": [
            ("Prenatal Risk Screen", 7),
            ("OB Visit Confirmed", 14),
            ("Trimester 2 Check", 90),
            ("Birth Plan Review", 180),
            ("Postpartum Follow-Up", 300),
        ],
    },
    {
        "program_id": "PGM006",
        "program_name": "Chronic Kidney Disease",
        "program_type": "Disease Management",
        "target_conditions": ["N18.3"],
        "milestones": [
            ("Initial Assessment", 7),
            ("Nephrology Referral", 21),
            ("Diet Counseling", 30),
            ("eGFR Recheck", 90),
            ("Annual Review", 365),
        ],
    },
]

# Condition code → program mapping for enrollment logic
_CONDITION_PROGRAM_MAP: Dict[str, str] = {}
for pgm in CARE_PROGRAMS:
    for code in pgm["target_conditions"]:
        _CONDITION_PROGRAM_MAP[code] = pgm["program_id"]

# ── HEDIS-style care gap measures ────────────────────────────────────────────
HEDIS_MEASURES = [
    ("HbA1c Screening", "E11.9", "Diabetes"),
    ("Breast Cancer Screening", None, "Preventive"),
    ("Colorectal Cancer Screening", None, "Preventive"),
    ("Blood Pressure Control", "I10", "Hypertension"),
    ("Statin Therapy", "E78.5", "Cardiovascular"),
    ("Depression Screening Follow-Up", "F32.9", "Behavioral Health"),
    ("Medication Reconciliation Post-Discharge", None, "Transitions"),
    ("Kidney Health Evaluation", "N18.3", "Renal"),
]

# ── Assessment types and valid ranges ────────────────────────────────────────
ASSESSMENT_TYPES = {
    "PHQ-9": {"min": 0, "max": 27, "risk_thresholds": [(5, "Low"), (10, "Moderate"), (15, "Moderately Severe"), (27, "Severe")]},
    "GAD-7": {"min": 0, "max": 21, "risk_thresholds": [(5, "Low"), (10, "Moderate"), (15, "Severe"), (21, "Severe")]},
    "PRAPARE": {"min": 0, "max": 20, "risk_thresholds": [(5, "Low"), (10, "Moderate"), (15, "High"), (20, "High")]},
    "Fall Risk": {"min": 0, "max": 10, "risk_thresholds": [(3, "Low"), (6, "Moderate"), (10, "High")]},
    "Functional Status": {"min": 0, "max": 100, "risk_thresholds": [(40, "Severe"), (60, "Moderate"), (80, "Mild"), (100, "Normal")]},
}

# ── SDOH risk factors ────────────────────────────────────────────────────────
SDOH_FACTORS = [
    "food_insecurity",
    "housing_instability",
    "transportation_barrier",
    "social_isolation",
    "financial_strain",
]

# Z-codes for SDOH (from claims)
SDOH_Z_CODES = [
    ("Z59.0", "Homelessness"),
    ("Z59.1", "Inadequate housing"),
    ("Z59.4", "Lack of adequate food"),
    ("Z56.0", "Unemployment"),
    ("Z56.9", "Problem related to employment"),
    ("Z63.0", "Problems in relationship with spouse or partner"),
    ("Z63.4", "Disappearance and death of family member"),
    ("Z65.9", "Problem related to unspecified psychosocial circumstance"),
    ("Z73.6", "Limitation of activities due to disability"),
]

# Community resources for SDOH referrals
COMMUNITY_RESOURCES = [
    ("Food Bank of Central NC", "food_insecurity"),
    ("Meals on Wheels", "food_insecurity"),
    ("SNAP Benefits Assistance", "food_insecurity"),
    ("Habitat for Humanity", "housing_instability"),
    ("Emergency Shelter Network", "housing_instability"),
    ("Section 8 Housing Assistance", "housing_instability"),
    ("Community Transit Program", "transportation_barrier"),
    ("Medical Transportation Service", "transportation_barrier"),
    ("Senior Center Activities", "social_isolation"),
    ("Peer Support Network", "social_isolation"),
    ("Financial Counseling Services", "financial_strain"),
    ("Prescription Assistance Program", "financial_strain"),
    ("United Way 211", "financial_strain"),
]

# ── Case management episode types and activity types ─────────────────────────
EPISODE_TYPES = ["Disease Management", "Utilization Management", "Transitions of Care", "Complex Care"]
EPISODE_WEIGHTS = [35, 25, 20, 20]

ACUITY_LEVELS = ["Low", "Moderate", "High", "Critical"]
ACUITY_WEIGHTS = [20, 40, 30, 10]

ACTIVITY_TYPES = [
    "Phone Call - Outbound",
    "Phone Call - Inbound",
    "Assessment Completed",
    "Care Plan Update",
    "Referral Made",
    "Barrier Resolution",
    "Medication Review",
    "Provider Coordination",
    "Education Session",
    "Home Visit",
]

CLOSE_REASONS = [
    "Goals Met",
    "Transferred to Another Program",
    "Member Declined Services",
    "Loss of Eligibility",
    "Deceased",
    "No Longer Meets Criteria",
]

# ── TOC barrier types ────────────────────────────────────────────────────────
TOC_BARRIERS = [
    "No transportation home",
    "No pharmacy access",
    "No PCP established",
    "Language barrier",
    "Caregiver unavailable",
    "Unable to reach member",
    "Medication not covered",
    "Skilled nursing bed unavailable",
    "DME not delivered",
]

# Discharge types
DISCHARGE_TYPES = ["Home", "SNF", "Home Health", "Rehab Facility", "AMA"]
DISCHARGE_WEIGHTS = [50, 20, 15, 10, 5]

# ── Gap intervention types ───────────────────────────────────────────────────
INTERVENTION_TYPES = ["Mailer", "Phone Call", "Text Message", "Appointment Scheduled", "Provider Outreach"]
INTERVENTION_WEIGHTS = [30, 25, 20, 15, 10]


# =============================================================================
# Generator: Static care program reference data
# =============================================================================
def generate_care_programs() -> List[Dict[str, Any]]:
    """Static reference table of disease management programs."""
    rows = []
    for pgm in CARE_PROGRAMS:
        rows.append({
            "program_id": pgm["program_id"],
            "program_name": pgm["program_name"],
            "program_type": pgm["program_type"],
            "target_conditions": ",".join(pgm["target_conditions"]),
            "milestones_json": str([{"name": m[0], "target_days": m[1]} for m in pgm["milestones"]]),
        })
    return rows


# =============================================================================
# Generator: Program enrollment
# =============================================================================
def generate_program_enrollment(
    members_data: List[Dict[str, Any]],
    enrollment_data: List[Dict[str, Any]],
    medical_claims: List[Dict[str, Any]],
    enrollment_rate: float = 0.30,
) -> List[Dict[str, Any]]:
    """Enroll members into disease management programs based on their diagnoses.

    Higher risk_score members are more likely to be enrolled.
    Condition codes from claims drive which program a member joins.
    """
    # Build member → diagnoses map from claims
    member_diagnoses: Dict[str, set] = {}
    for c in medical_claims:
        mid = c.get("member_id")
        dx = c.get("primary_diagnosis_code")
        if mid and dx:
            member_diagnoses.setdefault(mid, set()).add(dx)

    # Build member → risk_score map from enrollment
    member_risk: Dict[str, float] = {}
    member_lob: Dict[str, str] = {}
    for e in enrollment_data:
        mid = e.get("member_id")
        if mid:
            member_risk[mid] = e.get("risk_score", 1.0) or 1.0
            member_lob[mid] = e.get("line_of_business", "Commercial")

    referral_sources = ["PCP Referral", "Claims Analytics", "Health Risk Assessment", "Self-Referral", "ER Follow-Up"]
    enrollment_reasons = ["High Risk Score", "New Diagnosis", "Readmission Prevention", "Annual HRA Flag", "Quality Gap"]
    statuses = ["Active", "Completed", "Withdrawn", "On Hold"]
    status_weights = [50, 25, 15, 10]

    rows = []
    seq = 0
    for m in members_data:
        mid = m["member_id"]
        risk = member_risk.get(mid, 1.0)
        diagnoses = member_diagnoses.get(mid, set())

        # Higher risk → higher enrollment probability
        adjusted_rate = min(enrollment_rate * (1 + (risk - 1.0) * 0.5), 0.9)

        # Check each program for condition match
        for pgm in CARE_PROGRAMS:
            matching = diagnoses & set(pgm["target_conditions"])
            if matching and random.random() < adjusted_rate:
                seq += 1
                enroll_date = random_date_between(_CM_START, _CM_END - timedelta(days=90))
                status = weighted_choice(statuses, status_weights)
                disenroll_date = None
                if status in ("Completed", "Withdrawn"):
                    disenroll_date = (enroll_date + timedelta(days=random.randint(30, 365))).isoformat()

                rows.append({
                    "enrollment_id": inject_dq_issue(f"ENR{seq:07d}", "id"),
                    "member_id": inject_dq_issue(mid, "id"),
                    "program_id": pgm["program_id"],
                    "enrollment_date": inject_dq_issue(enroll_date.isoformat(), "date"),
                    "disenrollment_date": disenroll_date,
                    "status": status,
                    "referral_source": random.choice(referral_sources),
                    "enrollment_reason": random.choice(enrollment_reasons),
                    "line_of_business": member_lob.get(mid, "Commercial"),
                })

    return rows


# =============================================================================
# Generator: Case management episodes
# =============================================================================
def generate_case_episodes(
    members_data: List[Dict[str, Any]],
    enrollment_data: List[Dict[str, Any]],
    case_rate: float = 0.15,
) -> List[Dict[str, Any]]:
    """Generate case management episodes. Higher-risk members more likely to have cases."""
    member_risk = {e["member_id"]: e.get("risk_score", 1.0) or 1.0 for e in enrollment_data}

    # Generate case manager IDs
    case_manager_ids = [f"CM{100 + i}" for i in range(30)]

    rows = []
    seq = 0
    for m in members_data:
        mid = m["member_id"]
        risk = member_risk.get(mid, 1.0)
        adjusted_rate = min(case_rate * (1 + (risk - 1.0) * 0.4), 0.6)

        if random.random() < adjusted_rate:
            seq += 1
            open_date = random_date_between(_CM_START, _CM_END - timedelta(days=60))
            episode_type = weighted_choice(EPISODE_TYPES, EPISODE_WEIGHTS)
            acuity = weighted_choice(ACUITY_LEVELS, ACUITY_WEIGHTS)

            # Higher acuity → longer cases
            acuity_days = {"Low": (14, 60), "Moderate": (30, 120), "High": (60, 240), "Critical": (90, 365)}
            min_d, max_d = acuity_days[acuity]
            duration = random.randint(min_d, max_d)

            is_closed = random.random() < 0.6
            close_date = (open_date + timedelta(days=duration)).isoformat() if is_closed else None
            close_reason = random.choice(CLOSE_REASONS) if is_closed else None

            rows.append({
                "case_id": inject_dq_issue(f"CASE{seq:07d}", "id"),
                "member_id": inject_dq_issue(mid, "id"),
                "case_manager_id": random.choice(case_manager_ids),
                "episode_type": episode_type,
                "acuity": acuity,
                "open_date": inject_dq_issue(open_date.isoformat(), "date"),
                "close_date": close_date,
                "close_reason": close_reason,
            })

    return rows


# =============================================================================
# Generator: Case activities
# =============================================================================
def generate_case_activities(
    case_episodes: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Generate timestamped activities for each case episode."""
    rows = []
    seq = 0
    for case in case_episodes:
        case_id = case.get("case_id")
        if not case_id:
            continue

        open_str = case.get("open_date")
        close_str = case.get("close_date")
        try:
            open_dt = date.fromisoformat(str(open_str)) if open_str else _CM_START
        except (ValueError, TypeError):
            open_dt = _CM_START
        try:
            close_dt = date.fromisoformat(str(close_str)) if close_str else min(open_dt + timedelta(days=180), _CM_END)
        except (ValueError, TypeError):
            close_dt = min(open_dt + timedelta(days=180), _CM_END)

        # Generate 3-15 activities per case
        n_activities = random.randint(3, 15)
        for _ in range(n_activities):
            seq += 1
            rows.append({
                "activity_id": f"ACT{seq:08d}",
                "case_id": case_id,
                "activity_type": random.choice(ACTIVITY_TYPES),
                "activity_date": random_date_between(open_dt, close_dt).isoformat(),
                "duration_minutes": random.choice([5, 10, 15, 20, 30, 45, 60]),
                "notes": _generate_activity_note(),
            })

    return rows


def _generate_activity_note() -> str:
    """Short synthetic activity note."""
    notes = [
        "Discussed medication adherence. Member reports taking meds as prescribed.",
        "Reviewed care plan goals. Member making progress on dietary changes.",
        "Coordinated with PCP regarding lab results. Follow-up scheduled.",
        "Member missed scheduled appointment. Rescheduled for next week.",
        "Completed fall risk assessment. Low risk, no interventions needed.",
        "Referred to community health worker for transportation assistance.",
        "Conducted motivational interviewing. Member receptive to self-management.",
        "Reviewed discharge instructions with member. Medication list reconciled.",
        "Member reports increased anxiety. Referred to behavioral health.",
        "Confirmed PCP follow-up visit completed within 7 days of discharge.",
        "Assessed SDOH barriers. Food insecurity identified — referral placed.",
        "Home visit completed. Reviewed DME setup and medication storage.",
        "Caregiver education session — fall prevention and medication management.",
        "Member achieved HbA1c target. Transitioning to maintenance phase.",
        "Discussed smoking cessation resources. Member interested in quitline.",
    ]
    return random.choice(notes)


# =============================================================================
# Generator: Case assessments
# =============================================================================
def generate_case_assessments(
    case_episodes: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Generate structured assessments (PHQ-9, GAD-7, PRAPARE, etc.) for cases."""
    rows = []
    seq = 0
    for case in case_episodes:
        case_id = case.get("case_id")
        if not case_id:
            continue

        open_str = case.get("open_date")
        try:
            open_dt = date.fromisoformat(str(open_str)) if open_str else _CM_START
        except (ValueError, TypeError):
            open_dt = _CM_START

        # Each case gets 1-3 assessment types
        assessment_types = random.sample(list(ASSESSMENT_TYPES.keys()), k=random.randint(1, 3))
        for atype in assessment_types:
            info = ASSESSMENT_TYPES[atype]
            score = random.randint(info["min"], info["max"])

            # Determine risk level from thresholds
            risk_level = "Low"
            for threshold, level in info["risk_thresholds"]:
                if score <= threshold:
                    risk_level = level
                    break

            seq += 1
            rows.append({
                "assessment_id": inject_dq_issue(f"ASMT{seq:07d}", "id"),
                "case_id": case_id,
                "assessment_type": atype,
                "score": inject_dq_issue(score, "amount"),
                "risk_level": risk_level,
                "assessment_date": inject_dq_issue(
                    random_date_between(open_dt, min(open_dt + timedelta(days=30), _CM_END)).isoformat(),
                    "date",
                ),
            })

    return rows


# =============================================================================
# Generator: Member SDOH screenings
# =============================================================================
def generate_member_sdoh(
    members_data: List[Dict[str, Any]],
    enrollment_data: List[Dict[str, Any]],
    screening_rate: float = 0.40,
) -> List[Dict[str, Any]]:
    """Generate SDOH screening results. Rural members and lower-income LOBs have higher SDOH prevalence."""
    member_lob = {e["member_id"]: e.get("line_of_business", "Commercial") for e in enrollment_data}

    # LOB-based SDOH prevalence multiplier
    lob_sdoh_mult = {
        "Medicaid": 1.8,
        "Medicare Advantage": 1.3,
        "ACA Marketplace": 1.4,
        "Commercial": 0.6,
    }

    rows = []
    for m in members_data:
        if random.random() > screening_rate:
            continue

        mid = m["member_id"]
        county = m.get("county", random.choice(COUNTIES))
        lob = member_lob.get(mid, "Commercial")
        mult = lob_sdoh_mult.get(lob, 1.0)

        # Rural counties have higher SDOH rates
        rural_counties = {"Cumberland", "Buncombe", "Gaston"}
        if county in rural_counties:
            mult *= 1.3

        flags = {}
        for factor in SDOH_FACTORS:
            base_rate = {"food_insecurity": 0.15, "housing_instability": 0.10,
                         "transportation_barrier": 0.12, "social_isolation": 0.18,
                         "financial_strain": 0.20}[factor]
            flags[f"{factor}_flag"] = random.random() < (base_rate * mult)

        # Composite risk score (0-10)
        n_flags = sum(1 for v in flags.values() if v)
        composite = round(n_flags * 2.0 + random.uniform(0, 1), 1)
        composite = min(composite, 10.0)

        rows.append({
            "member_id": inject_dq_issue(mid, "id"),
            "screening_date": inject_dq_issue(
                random_date_between(_CM_START, _CM_END).isoformat(), "date"
            ),
            "county": county,
            **{k: v for k, v in flags.items()},
            "composite_sdoh_risk_score": inject_dq_issue(composite, "amount"),
        })

    return rows


# =============================================================================
# Generator: SDOH referrals
# =============================================================================
def generate_sdoh_referrals(
    member_sdoh: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Generate community resource referrals for members with SDOH flags."""
    rows = []
    seq = 0
    statuses = ["Completed", "Pending", "In Progress", "Declined", "Unable to Contact"]
    status_weights = [35, 20, 20, 15, 10]

    for sdoh in member_sdoh:
        mid = sdoh.get("member_id")
        if not mid:
            continue

        # For each active flag, maybe generate a referral
        for factor in SDOH_FACTORS:
            if sdoh.get(f"{factor}_flag") and random.random() < 0.6:
                # Pick a matching community resource
                matching = [r for r in COMMUNITY_RESOURCES if r[1] == factor]
                if not matching:
                    continue
                resource_name, _ = random.choice(matching)

                screening_str = sdoh.get("screening_date")
                try:
                    screen_dt = date.fromisoformat(str(screening_str))
                except (ValueError, TypeError):
                    screen_dt = _CM_START

                seq += 1
                status = weighted_choice(statuses, status_weights)
                ref_date = random_date_between(screen_dt, min(screen_dt + timedelta(days=30), _CM_END))

                rows.append({
                    "referral_id": f"REF{seq:07d}",
                    "member_id": mid,
                    "referral_type": factor,
                    "community_resource": resource_name,
                    "referral_date": ref_date.isoformat(),
                    "status": status,
                    "outcome": "Resolved" if status == "Completed" else ("Pending" if status in ("Pending", "In Progress") else "Unresolved"),
                })

    return rows


# =============================================================================
# Generator: SDOH Z-codes (extracted from claims)
# =============================================================================
def generate_sdoh_z_codes(
    member_sdoh: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Generate Z-code diagnosis records correlated with SDOH screening flags."""
    rows = []
    for sdoh in member_sdoh:
        mid = sdoh.get("member_id")
        if not mid:
            continue

        n_flags = sum(1 for f in SDOH_FACTORS if sdoh.get(f"{f}_flag"))
        if n_flags == 0:
            continue

        # Members with more SDOH flags get more Z-codes
        n_codes = random.randint(1, min(n_flags, 3))
        selected = random.sample(SDOH_Z_CODES, k=min(n_codes, len(SDOH_Z_CODES)))

        for code, desc in selected:
            rows.append({
                "member_id": mid,
                "z_code": code,
                "z_code_description": desc,
                "claim_date": random_date_between(_CM_START, _CM_END).isoformat(),
            })

    return rows


# =============================================================================
# Generator: Transitions of Care episodes
# =============================================================================
def generate_toc_episodes(
    members_data: List[Dict[str, Any]],
    medical_claims: List[Dict[str, Any]],
    toc_rate: float = 0.60,
) -> List[Dict[str, Any]]:
    """Generate TOC episodes from inpatient discharges."""
    # Find inpatient claims with discharge dates
    ip_discharges: Dict[str, List[Dict]] = {}
    for c in medical_claims:
        if c.get("claim_type") in ("Inpatient", "Institutional IP", "Institutional_IP") and c.get("discharge_date"):
            mid = c.get("member_id")
            if mid:
                ip_discharges.setdefault(mid, []).append(c)

    rows = []
    seq = 0
    for mid, claims in ip_discharges.items():
        for c in claims:
            if random.random() > toc_rate:
                continue
            seq += 1

            discharge_str = c.get("discharge_date")
            try:
                discharge_dt = date.fromisoformat(str(discharge_str))
            except (ValueError, TypeError):
                discharge_dt = random_date_between(_CM_START, _CM_END)

            readmission_risk = round(random.uniform(0.05, 0.65), 2)
            discharge_type = weighted_choice(DISCHARGE_TYPES, DISCHARGE_WEIGHTS)

            rows.append({
                "toc_id": inject_dq_issue(f"TOC{seq:07d}", "id"),
                "member_id": inject_dq_issue(mid, "id"),
                "discharge_date": inject_dq_issue(discharge_dt.isoformat(), "date"),
                "discharge_facility": c.get("billing_provider_npi", ""),
                "discharge_type": discharge_type,
                "readmission_risk_score": inject_dq_issue(readmission_risk, "amount"),
            })

    return rows


# =============================================================================
# Generator: TOC follow-up tracking
# =============================================================================
def generate_toc_followup(
    toc_episodes: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Generate follow-up tracking records for each TOC episode."""
    followup_types = [
        ("48hr_call", 2),
        ("7day_pcp", 7),
        ("med_reconciliation", 3),
    ]

    rows = []
    seq = 0
    for toc in toc_episodes:
        toc_id = toc.get("toc_id")
        if not toc_id:
            continue

        discharge_str = toc.get("discharge_date")
        try:
            discharge_dt = date.fromisoformat(str(discharge_str))
        except (ValueError, TypeError):
            discharge_dt = random_date_between(_CM_START, _CM_END)

        for ftype, target_days in followup_types:
            seq += 1
            due_date = discharge_dt + timedelta(days=target_days)

            # Completion rates vary by type
            completion_rates = {"48hr_call": 0.75, "7day_pcp": 0.60, "med_reconciliation": 0.70}
            completed = random.random() < completion_rates[ftype]

            if completed:
                # Completed within a reasonable window (may be late)
                actual_days = target_days + random.randint(-1, target_days)
                completed_date = (discharge_dt + timedelta(days=max(1, actual_days))).isoformat()
                status = "Completed"
            else:
                completed_date = None
                status = random.choice(["Overdue", "Unable to Reach", "Declined"])

            rows.append({
                "followup_id": f"FUP{seq:07d}",
                "toc_id": toc_id,
                "followup_type": ftype,
                "due_date": due_date.isoformat(),
                "completed_date": completed_date,
                "status": status,
            })

    return rows


# =============================================================================
# Generator: TOC barriers
# =============================================================================
def generate_toc_barriers(
    toc_episodes: List[Dict[str, Any]],
    barrier_rate: float = 0.35,
) -> List[Dict[str, Any]]:
    """Generate barriers to successful care transitions."""
    rows = []
    seq = 0
    for toc in toc_episodes:
        toc_id = toc.get("toc_id")
        if not toc_id or random.random() > barrier_rate:
            continue

        n_barriers = random.randint(1, 3)
        selected = random.sample(TOC_BARRIERS, k=min(n_barriers, len(TOC_BARRIERS)))
        for barrier in selected:
            seq += 1
            rows.append({
                "barrier_id": f"BAR{seq:07d}",
                "toc_id": toc_id,
                "barrier_type": barrier,
                "description": f"Barrier identified: {barrier}",
                "resolved_flag": random.random() < 0.65,
            })

    return rows


# =============================================================================
# Generator: Care gaps
# =============================================================================
def generate_care_gaps(
    members_data: List[Dict[str, Any]],
    medical_claims: List[Dict[str, Any]],
    gap_prevalence: float = 0.25,
) -> List[Dict[str, Any]]:
    """Generate open care gaps per member per HEDIS measure."""
    # Build member → diagnoses
    member_dx: Dict[str, set] = {}
    for c in medical_claims:
        mid = c.get("member_id")
        dx = c.get("primary_diagnosis_code")
        if mid and dx:
            member_dx.setdefault(mid, set()).add(dx)

    rows = []
    seq = 0
    priorities = ["High", "Medium", "Low"]
    priority_weights = [30, 50, 20]

    for m in members_data:
        mid = m["member_id"]
        diagnoses = member_dx.get(mid, set())

        for measure_name, condition_code, condition_category in HEDIS_MEASURES:
            # Condition-specific gaps only apply to members with that condition
            if condition_code and condition_code not in diagnoses:
                continue
            # Preventive gaps apply to everyone at the gap_prevalence rate
            if random.random() > gap_prevalence:
                continue

            seq += 1
            open_date = random_date_between(_CM_START, _CM_END - timedelta(days=30))

            rows.append({
                "gap_id": inject_dq_issue(f"GAP{seq:07d}", "id"),
                "member_id": inject_dq_issue(mid, "id"),
                "measure_name": measure_name,
                "condition": condition_category,
                "gap_open_date": inject_dq_issue(open_date.isoformat(), "date"),
                "priority": weighted_choice(priorities, priority_weights),
            })

    return rows


# =============================================================================
# Generator: Gap interventions
# =============================================================================
def generate_gap_interventions(
    care_gaps: List[Dict[str, Any]],
    intervention_rate: float = 0.70,
) -> List[Dict[str, Any]]:
    """Generate outreach attempts to close care gaps."""
    rows = []
    seq = 0
    outcomes = ["Appointment Scheduled", "No Response", "Declined", "Already Completed", "Left Message"]
    outcome_weights = [30, 25, 10, 15, 20]

    for gap in care_gaps:
        gap_id = gap.get("gap_id")
        if not gap_id or random.random() > intervention_rate:
            continue

        open_str = gap.get("gap_open_date")
        try:
            open_dt = date.fromisoformat(str(open_str))
        except (ValueError, TypeError):
            open_dt = _CM_START

        # 1-4 intervention attempts per gap
        n_attempts = random.randint(1, 4)
        for attempt in range(n_attempts):
            seq += 1
            int_date = open_dt + timedelta(days=random.randint(7 * attempt, 30 * (attempt + 1)))
            if int_date > _CM_END:
                break

            rows.append({
                "intervention_id": f"INT{seq:08d}",
                "gap_id": gap_id,
                "intervention_type": weighted_choice(INTERVENTION_TYPES, INTERVENTION_WEIGHTS),
                "intervention_date": int_date.isoformat(),
                "outcome": weighted_choice(outcomes, outcome_weights),
            })

    return rows


# =============================================================================
# Generator: Gap closure events
# =============================================================================
def generate_gap_closure_events(
    care_gaps: List[Dict[str, Any]],
    gap_interventions: List[Dict[str, Any]],
    closure_rate: float = 0.45,
) -> List[Dict[str, Any]]:
    """Generate closure events for care gaps that were successfully closed."""
    # Build gap_id → latest intervention date
    gap_last_intervention: Dict[str, str] = {}
    for i in gap_interventions:
        gid = i.get("gap_id")
        idate = i.get("intervention_date")
        if gid and idate:
            if gid not in gap_last_intervention or idate > gap_last_intervention[gid]:
                gap_last_intervention[gid] = idate

    closure_methods = ["Lab Completed", "Visit Completed", "Self-Reported", "Claims-Based Closure", "Chart Review"]

    rows = []
    seq = 0
    for gap in care_gaps:
        gap_id = gap.get("gap_id")
        if not gap_id or random.random() > closure_rate:
            continue

        open_str = gap.get("gap_open_date")
        try:
            open_dt = date.fromisoformat(str(open_str))
        except (ValueError, TypeError):
            open_dt = _CM_START

        # Closure happens after last intervention or 30-90 days after open
        last_int_str = gap_last_intervention.get(gap_id)
        try:
            base_dt = date.fromisoformat(str(last_int_str)) if last_int_str else open_dt
        except (ValueError, TypeError):
            base_dt = open_dt

        days_to_close = random.randint(1, 60)
        closure_dt = base_dt + timedelta(days=days_to_close)
        if closure_dt > _CM_END:
            closure_dt = _CM_END

        seq += 1
        rows.append({
            "closure_id": f"CLO{seq:07d}",
            "gap_id": gap_id,
            "closure_date": closure_dt.isoformat(),
            "closure_method": random.choice(closure_methods),
            "days_to_close": (closure_dt - open_dt).days,
        })

    return rows
