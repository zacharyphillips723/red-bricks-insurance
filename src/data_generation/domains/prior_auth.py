# Red Bricks Insurance — prior authorization domain: generates synthetic PA requests with
# realistic determination distributions, turnaround times, and clinical summaries.

import random
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

from faker import Faker

from ..helpers import random_date_between, weighted_choice, generate_claim_id
from .medical_policies import POLICIES

fake = Faker()
Faker.seed(42)

# ---------------------------------------------------------------------------
# PA request window — CY2025 to align with CMS-0057-F reporting
# ---------------------------------------------------------------------------
_PA_START = date(2025, 1, 1)
_PA_END = date(2025, 12, 31)

# ---------------------------------------------------------------------------
# Service type mappings — each links to a medical policy and its codes
# ---------------------------------------------------------------------------

SERVICE_TYPES = []
for pol in POLICIES:
    for svc in pol["covered_services"]:
        SERVICE_TYPES.append({
            "service_type": pol["service_category"],
            "policy_id": pol["policy_id"],
            "policy_name": pol["policy_name"],
            "procedure_code": svc["code"],
            "procedure_desc": svc["description"],
            "code_system": svc["system"],
            "cost_range": svc["cost_range"],
            "diagnosis_codes": [dx[0] for dx in pol["diagnosis_codes"]],
        })

# Weights by service category (relative frequency of PA requests)
_CATEGORY_WEIGHTS = {
    "diabetes_management": 20,
    "cardiovascular": 18,
    "orthopedic": 15,
    "behavioral_health": 15,
    "specialty_pharmacy": 18,
    "diagnostic_imaging": 14,
}

# ---------------------------------------------------------------------------
# Determination logic — controls the realistic distribution of outcomes
# ---------------------------------------------------------------------------

# Overall target: ~65% auto-approved, ~20% clinical review approved, ~10% denied, ~5% pended
DETERMINATION_WEIGHTS = {
    "auto_approved": 65,
    "clinical_review_approved": 20,
    "denied": 10,
    "pended": 5,
}

DENIAL_REASONS = [
    ("MEDICAL_NECESSITY", "Does not meet medical necessity criteria per policy"),
    ("STEP_THERAPY", "Step therapy requirements not completed"),
    ("MISSING_DOCUMENTATION", "Required clinical documentation not submitted"),
    ("OUT_OF_NETWORK", "Requesting provider is out-of-network; in-network alternative available"),
    ("EXPERIMENTAL", "Service is investigational or experimental"),
    ("BENEFIT_EXCLUSION", "Service is excluded under the member's benefit plan"),
    ("FREQUENCY_LIMIT", "Service exceeds frequency or visit limits"),
    ("DUPLICATE_REQUEST", "Duplicate authorization request for same service"),
]

DENIAL_REASON_WEIGHTS = [25, 20, 18, 10, 5, 10, 8, 4]

REVIEWER_TYPES = {
    "auto_approved": "system_auto",
    "clinical_review_approved": "clinical_reviewer",
    "denied": "medical_director",
    "pended": "clinical_reviewer",
}

# ---------------------------------------------------------------------------
# Clinical summary templates
# ---------------------------------------------------------------------------

_CLINICAL_SUMMARIES = {
    "diabetes_management": [
        "Patient with Type 2 DM, current HbA1c {hba1c}%. On metformin {met_dose}mg daily x {months}mo. "
        "Requesting {procedure} for improved glycemic control. {extra}",
        "T2DM patient with {complication}. Failed dual oral therapy (metformin + {second_agent}). "
        "HbA1c trending from {prev_hba1c}% to {hba1c}% over past 6 months. {extra}",
    ],
    "cardiovascular": [
        "Patient with {dx_desc}. Recent stress test: {stress_result}. EF {ef}% on echo. "
        "Currently on {meds}. Requesting {procedure}. {extra}",
        "{age}yo {gender} presenting with {symptom}. Troponin {troponin}. EKG shows {ekg_finding}. "
        "Requesting urgent {procedure}. {extra}",
    ],
    "orthopedic": [
        "Patient with {dx_desc}. MRI shows {mri_finding}. Completed {pt_sessions} PT sessions over "
        "{pt_weeks} weeks with {pt_response} response. WOMAC score: {womac}. BMI: {bmi}. "
        "Requesting {procedure}. {extra}",
        "History of {dx_desc} x {duration} months. Failed conservative management including "
        "{conservative_tx}. Functional limitations: {limitations}. {extra}",
    ],
    "behavioral_health": [
        "Patient with {dx_desc}. PHQ-9 score: {phq9}. GAD-7 score: {gad7}. "
        "Current medications: {meds}. Failed {num_trials} medication trial(s). "
        "Requesting {procedure}. {extra}",
        "{age}yo with treatment-resistant {dx_desc}. On {med1} x {med1_weeks}wk and {med2} x "
        "{med2_weeks}wk with inadequate response. C-SSRS: {cssrs}. {extra}",
    ],
    "specialty_pharmacy": [
        "Patient with {dx_desc}. Disease activity: {activity}. Failed {failed_tx} x {duration}mo. "
        "TB screen: {tb_screen}. Requesting {procedure}. {extra}",
        "{dx_desc} with inadequate response to {failed_tx}. DAS28: {das28}. "
        "BSA: {bsa}%. Requesting step-up to {procedure}. {extra}",
    ],
    "diagnostic_imaging": [
        "Patient presenting with {symptom} x {duration} weeks. Initial workup: {initial_workup}. "
        "Failed {weeks_conservative}wk conservative management. Requesting {procedure}. {extra}",
        "{age}yo with {dx_desc}. {clinical_concern}. Prior imaging: {prior_imaging}. "
        "Requesting {procedure} for further evaluation. {extra}",
    ],
}


def _generate_clinical_summary(service_type: str, procedure_desc: str) -> str:
    """Generate a realistic clinical summary for a PA request."""
    templates = _CLINICAL_SUMMARIES.get(service_type, _CLINICAL_SUMMARIES["diagnostic_imaging"])
    template = random.choice(templates)

    # Build a context dict with plausible clinical values
    ctx = {
        "procedure": procedure_desc,
        "age": random.randint(30, 85),
        "gender": random.choice(["male", "female"]),
        "hba1c": round(random.uniform(6.5, 12.0), 1),
        "prev_hba1c": round(random.uniform(7.0, 10.0), 1),
        "met_dose": random.choice([1000, 1500, 2000]),
        "months": random.randint(3, 24),
        "second_agent": random.choice(["glipizide", "empagliflozin", "sitagliptin"]),
        "complication": random.choice(["peripheral neuropathy", "nephropathy", "retinopathy"]),
        "dx_desc": random.choice(["chronic knee pain", "lumbar radiculopathy", "major depression",
                                   "rheumatoid arthritis", "coronary artery disease", "persistent headache"]),
        "stress_result": random.choice(["positive for ischemia", "indeterminate", "mildly positive"]),
        "ef": random.randint(20, 60),
        "meds": random.choice(["metoprolol + lisinopril + atorvastatin", "carvedilol + losartan",
                                "amlodipine + aspirin + rosuvastatin"]),
        "symptom": random.choice(["exertional chest pain", "dyspnea on exertion", "palpitations",
                                   "lower extremity edema", "persistent low back pain"]),
        "troponin": random.choice(["negative", "0.04 (borderline)", "0.12 (elevated)"]),
        "ekg_finding": random.choice(["NSR", "ST depression in V4-V6", "new LBBB", "sinus bradycardia"]),
        "mri_finding": random.choice(["moderate meniscal tear", "disc herniation at L4-L5",
                                       "grade 3 chondromalacia", "rotator cuff partial tear"]),
        "pt_sessions": random.randint(4, 12),
        "pt_weeks": random.randint(4, 12),
        "pt_response": random.choice(["minimal", "partial", "poor", "no significant"]),
        "womac": random.randint(40, 90),
        "bmi": round(random.uniform(22, 42), 1),
        "duration": random.randint(3, 36),
        "conservative_tx": random.choice(["PT, NSAIDs, steroid injection", "PT, bracing, activity modification",
                                           "PT, oral steroids, TENS"]),
        "limitations": random.choice(["unable to climb stairs", "cannot walk > 1 block",
                                       "difficulty with ADLs", "unable to work"]),
        "phq9": random.randint(5, 27),
        "gad7": random.randint(5, 21),
        "num_trials": random.randint(1, 4),
        "med1": random.choice(["sertraline 200mg", "fluoxetine 40mg", "venlafaxine 225mg"]),
        "med1_weeks": random.randint(8, 24),
        "med2": random.choice(["bupropion 300mg", "duloxetine 60mg", "mirtazapine 30mg"]),
        "med2_weeks": random.randint(6, 16),
        "cssrs": random.choice(["1 (wish to be dead)", "2 (nonspecific active thoughts)",
                                  "3 (active ideation without plan)", "0 (no ideation)"]),
        "activity": random.choice(["moderate", "severe", "high disease activity"]),
        "failed_tx": random.choice(["methotrexate 20mg/wk", "adalimumab biosimilar", "sulfasalazine + HCQ"]),
        "tb_screen": random.choice(["negative", "negative (QuantiFERON)"]),
        "das28": round(random.uniform(2.6, 6.5), 1),
        "bsa": random.randint(5, 40),
        "initial_workup": random.choice(["X-ray unremarkable", "labs normal", "ultrasound inconclusive"]),
        "weeks_conservative": random.randint(4, 8),
        "clinical_concern": random.choice(["concerning for malignancy", "r/o PE", "eval for cord compression"]),
        "prior_imaging": random.choice(["X-ray 3mo ago showed no acute findings",
                                          "CT without contrast was inconclusive", "none"]),
        "extra": random.choice([
            "No known drug allergies.",
            "Patient is compliant with all appointments.",
            "Provider recommends proceeding urgently.",
            "Member has exhausted conservative options.",
            "Insurance verification confirmed active coverage.",
            "",
        ]),
    }

    # Use safe formatting — skip keys not in template
    try:
        return template.format(**ctx)
    except KeyError:
        return f"PA request for {procedure_desc}. Clinical details on file with requesting provider."


def generate_prior_auth_requests(
    member_ids: List[str],
    enrollment_data: List[Dict[str, Any]],
    providers_data: List[Dict[str, Any]],
    n_requests: int = 10000,
) -> List[Dict[str, Any]]:
    """
    Generate synthetic prior authorization requests.

    Distribution:
    - ~65% auto-approved (Tier 1 deterministic rules pass)
    - ~20% approved after clinical review (Tier 2/3)
    - ~10% denied
    - ~5% pended (awaiting additional information)
    Of denials: ~40% have appeal_filed=True, ~80% of those are overturned.
    """
    requests = []
    provider_npis = [p["npi"] for p in providers_data if p.get("npi")]

    # Build member -> enrollment lookup for LOB context
    member_enrollment = {}
    for e in enrollment_data:
        mid = e.get("member_id")
        if mid:
            member_enrollment[mid] = e

    # Weight service types by category
    svc_weights = [_CATEGORY_WEIGHTS.get(s["service_type"], 10) for s in SERVICE_TYPES]

    determination_options = list(DETERMINATION_WEIGHTS.keys())
    determination_wts = list(DETERMINATION_WEIGHTS.values())

    for i in range(n_requests):
        auth_id = f"PA-2025-{i+1:06d}"
        member_id = random.choice(member_ids)
        requesting_npi = random.choice(provider_npis)

        # Pick a service
        svc = weighted_choice(SERVICE_TYPES, svc_weights)
        service_type = svc["service_type"]
        procedure_code = svc["procedure_code"]
        procedure_desc = svc["procedure_desc"]
        policy_id = svc["policy_id"]
        cost_range = svc["cost_range"]

        # Pick 1-3 diagnosis codes from the policy's applicable codes
        n_dx = random.randint(1, min(3, len(svc["diagnosis_codes"])))
        diagnosis_codes = random.sample(svc["diagnosis_codes"], n_dx)

        # Urgency: 80% standard, 20% expedited
        urgency = weighted_choice(["standard", "expedited"], [80, 20])

        # Request date within CY2025
        request_date = random_date_between(_PA_START, _PA_END)

        # Determination
        determination = weighted_choice(determination_options, determination_wts)
        reviewer_type = REVIEWER_TYPES[determination]

        # Turnaround time based on determination type and urgency
        if determination == "auto_approved":
            turnaround_hours = random.randint(0, 4)  # near-instant
        elif determination == "pended":
            turnaround_hours = random.randint(48, 168 * 2)  # pended can be long
        elif urgency == "expedited":
            turnaround_hours = random.randint(4, 72)  # CMS: 72hr max for expedited
        else:
            turnaround_hours = random.randint(24, 168)  # CMS: 7-day max for standard

        determination_date = request_date + timedelta(hours=turnaround_hours)

        # Determination reason
        if determination == "auto_approved":
            determination_reason = "Meets all clinical criteria per automated rules engine"
            denial_reason_code = None
        elif determination == "clinical_review_approved":
            determination_reason = random.choice([
                "Approved after clinical reviewer confirmed medical necessity",
                "Additional documentation reviewed; criteria met per policy guidelines",
                "Peer-to-peer review with requesting provider confirmed appropriateness",
                "Clinical reviewer approved based on submitted clinical summary and labs",
            ])
            denial_reason_code = None
        elif determination == "denied":
            denial = weighted_choice(DENIAL_REASONS, DENIAL_REASON_WEIGHTS)
            denial_reason_code = denial[0]
            determination_reason = denial[1]
        else:  # pended
            determination_reason = random.choice([
                "Awaiting additional clinical documentation from requesting provider",
                "Peer-to-peer review requested; awaiting scheduling",
                "Incomplete submission — missing required lab results",
                "Step therapy documentation insufficient; provider contacted for records",
            ])
            denial_reason_code = None

        # Clinical summary
        clinical_summary = _generate_clinical_summary(service_type, procedure_desc)

        # Estimated cost
        estimated_cost = round(random.uniform(cost_range[0], cost_range[1]), 2)

        # Appeal logic for denials
        appeal_filed = False
        appeal_outcome = None
        if determination == "denied":
            appeal_filed = random.random() < 0.40  # 40% appeal rate
            if appeal_filed:
                appeal_outcome = weighted_choice(
                    ["overturned", "upheld", "partially_overturned"],
                    [65, 25, 10],  # ~80% fully or partially overturned
                )

        # Map determination to a clean status
        if determination == "auto_approved":
            status = "approved"
            determination_tier = "tier_1_auto"
        elif determination == "clinical_review_approved":
            status = "approved"
            determination_tier = random.choice(["tier_2_ml", "tier_3_clinical"])
        elif determination == "denied":
            status = "denied"
            determination_tier = "tier_3_clinical"
        else:
            status = "pended"
            determination_tier = "tier_2_ml"

        # Get LOB from enrollment
        enr = member_enrollment.get(member_id, {})
        line_of_business = enr.get("line_of_business", "Commercial")

        requests.append({
            "auth_request_id": auth_id,
            "member_id": member_id,
            "requesting_provider_npi": requesting_npi,
            "service_type": service_type,
            "procedure_code": procedure_code,
            "procedure_description": procedure_desc,
            "policy_id": policy_id,
            "diagnosis_codes": "|".join(diagnosis_codes),
            "urgency": urgency,
            "line_of_business": line_of_business,
            "request_date": request_date.isoformat(),
            "determination": status,
            "determination_tier": determination_tier,
            "determination_date": determination_date.isoformat(),
            "determination_reason": determination_reason,
            "denial_reason_code": denial_reason_code,
            "reviewer_type": reviewer_type,
            "clinical_summary": clinical_summary,
            "estimated_cost": estimated_cost,
            "turnaround_hours": turnaround_hours,
            "appeal_filed": appeal_filed,
            "appeal_outcome": appeal_outcome,
        })

    return requests
