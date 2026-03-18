# Red Bricks Insurance — clinical domain: labs, vitals, encounters (JSON for dbignite).

import random
from datetime import date
from typing import List, Dict, Any

from ..dq import inject_dq_issue
from ..helpers import random_date_between

# Correlate with diagnosis: e.g. E11.9 (diabetes) -> higher glucose, HbA1c
DIAGNOSIS_LAB_MAP = {
    "E11.9": {"labs": ["glucose", "HbA1c"], "glucose_range": (100, 180), "HbA1c_range": (6.5, 10.0)},
    "E11.65": {"labs": ["glucose", "HbA1c", "creatinine"], "glucose_range": (120, 250), "HbA1c_range": (7.0, 12.0)},
    "I10": {"labs": ["systolic_bp", "diastolic_bp", "creatinine"], "systolic_range": (140, 180), "diastolic_range": (90, 110)},
    "I50.9": {"labs": ["BNP", "creatinine", "potassium"], "BNP_range": (400, 2000)},
    "N18.3": {"labs": ["creatinine", "eGFR", "potassium"], "creatinine_range": (1.5, 4.0), "eGFR_range": (30, 59)},
    "J44.1": {"labs": ["oxygen_sat", "WBC"], "oxygen_sat_range": (88, 94)},
}

COMMON_LABS = [
    ("glucose", "mg/dL", 70, 99),
    ("HbA1c", "%", 4.0, 5.6),
    ("creatinine", "mg/dL", 0.7, 1.2),
    ("eGFR", "mL/min/1.73m2", 90, 120),
    ("total_cholesterol", "mg/dL", 125, 200),
    ("LDL", "mg/dL", 70, 130),
    ("HDL", "mg/dL", 40, 60),
    ("WBC", "K/uL", 4.5, 11.0),
    ("hemoglobin", "g/dL", 12.0, 17.0),
    ("BNP", "pg/mL", 0, 100),
]

VITAL_NAMES = ["systolic_bp", "diastolic_bp", "heart_rate", "temperature", "weight_kg", "height_cm", "BMI"]


def _lab_value_for_dx(lab_name: str, primary_dx: str) -> float:
    if primary_dx in DIAGNOSIS_LAB_MAP:
        info = DIAGNOSIS_LAB_MAP[primary_dx]
        if lab_name in info:
            key = f"{lab_name}_range"
            if key in info:
                low, high = info[key]
                return round(random.uniform(low, high), 2)
    # Default from COMMON_LABS
    for name, unit, lo, hi in COMMON_LABS:
        if name == lab_name:
            return round(random.uniform(lo, hi), 2)
    return round(random.uniform(0, 100), 2)


def generate_clinical_events(
    member_ids: List[str],
    primary_dx_by_member: Dict[str, str],
    provider_npis: List[str],
    n_encounters: int = 8000,
    n_lab_results: int = 15000,
    n_vitals: int = 12000,
) -> tuple[List[Dict], List[Dict], List[Dict]]:
    """
    Generate encounters, lab results, and vitals. Returns (encounters, labs, vitals).
    primary_dx_by_member: optional member_id -> ICD10 primary diagnosis for correlation.
    Output is suitable for JSON serialization (dbignite parsing later).
    """
    encounters = []
    labs = []
    vitals = []
    service_start = date(2023, 1, 1)
    service_end = date(2025, 12, 31)

    encounter_ids = [f"ENC{1000000 + i}" for i in range(n_encounters)]
    for i, enc_id in enumerate(encounter_ids):
        member_id = random.choice(member_ids)
        provider_npi = random.choice(provider_npis) if provider_npis else "0"
        dos = random_date_between(service_start, service_end)
        encounter_type = random.choices(
            ["office", "outpatient", "inpatient", "emergency", "telehealth"],
            weights=[50, 25, 5, 10, 10], k=1
        )[0]
        enc = {
            "encounter_id": enc_id,
            "member_id": member_id,
            "provider_npi": inject_dq_issue(provider_npi, "code"),
            "date_of_service": inject_dq_issue(dos.isoformat(), "date"),
            "encounter_type": encounter_type,
            "visit_type": random.choice(["routine", "follow_up", "acute", "preventive"]),
        }
        encounters.append(enc)

    for i in range(n_lab_results):
        member_id = random.choice(member_ids)
        primary_dx = primary_dx_by_member.get(member_id)
        lab_name, unit, lo, hi = random.choice(COMMON_LABS)
        value = _lab_value_for_dx(lab_name, primary_dx) if primary_dx else round(random.uniform(lo, hi), 2)
        draw_date = random_date_between(service_start, service_end)
        labs.append({
            "lab_result_id": f"LAB{i}",
            "member_id": member_id,
            "lab_name": lab_name,
            "value": inject_dq_issue(value, "amount"),
            "unit": unit,
            "reference_range_low": float(lo),
            "reference_range_high": float(hi),
            "collection_date": inject_dq_issue(draw_date.isoformat(), "date"),
        })

    for i in range(n_vitals):
        member_id = random.choice(member_ids)
        vital_name = random.choice(VITAL_NAMES)
        if vital_name == "systolic_bp":
            value = float(random.randint(100, 160))
        elif vital_name == "diastolic_bp":
            value = float(random.randint(60, 100))
        elif vital_name == "heart_rate":
            value = float(random.randint(55, 100))
        elif vital_name == "temperature":
            value = round(random.uniform(36.1, 37.5), 1)
        elif vital_name == "weight_kg":
            value = round(random.uniform(50, 120), 1)
        elif vital_name == "height_cm":
            value = float(random.randint(150, 195))
        elif vital_name == "BMI":
            value = round(random.uniform(18, 40), 1)
        else:
            value = round(random.uniform(0, 100), 1)
        measure_date = random_date_between(service_start, service_end)
        vitals.append({
            "vital_id": f"VIT{i}",
            "member_id": member_id,
            "vital_name": vital_name,
            "value": inject_dq_issue(value, "amount"),
            "measurement_date": inject_dq_issue(measure_date.isoformat(), "date"),
        })

    return encounters, labs, vitals
