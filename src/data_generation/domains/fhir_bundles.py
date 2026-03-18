# Red Bricks Insurance — FHIR R4 Bundle generator.
#
# Produces one FHIR Bundle (JSON) per member containing:
#   Patient, Encounter, Condition, Observation (labs + vitals)
#
# These bundles are written to a UC Volume and parsed by dbignite
# into clinical domain tables in Unity Catalog.

import json
import random
import uuid
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from ..dq import inject_dq_issue
from ..helpers import random_date_between

# ---------------------------------------------------------------------------
# FHIR coding systems
# ---------------------------------------------------------------------------
ENCOUNTER_CLASS_MAP = {
    "office": {"code": "AMB", "display": "ambulatory"},
    "outpatient": {"code": "AMB", "display": "ambulatory"},
    "inpatient": {"code": "IMP", "display": "inpatient encounter"},
    "emergency": {"code": "EMER", "display": "emergency"},
    "telehealth": {"code": "VR", "display": "virtual"},
}

ENCOUNTER_TYPE_MAP = {
    "routine": {"code": "185349003", "display": "Encounter for check up"},
    "follow_up": {"code": "390906007", "display": "Follow-up encounter"},
    "acute": {"code": "702927004", "display": "Acute care encounter"},
    "preventive": {"code": "410620009", "display": "Well child visit"},
}

LOINC_MAP = {
    "glucose": {"code": "2345-7", "display": "Glucose [Mass/volume] in Serum or Plasma"},
    "HbA1c": {"code": "4548-4", "display": "Hemoglobin A1c/Hemoglobin.total in Blood"},
    "creatinine": {"code": "2160-0", "display": "Creatinine [Mass/volume] in Serum or Plasma"},
    "eGFR": {"code": "48642-3", "display": "eGFR by CKD-EPI"},
    "total_cholesterol": {"code": "2093-3", "display": "Cholesterol [Mass/volume] in Serum or Plasma"},
    "LDL": {"code": "2089-1", "display": "LDL Cholesterol"},
    "HDL": {"code": "2085-9", "display": "HDL Cholesterol"},
    "WBC": {"code": "6690-2", "display": "Leukocytes [#/volume] in Blood"},
    "hemoglobin": {"code": "718-7", "display": "Hemoglobin [Mass/volume] in Blood"},
    "BNP": {"code": "30934-4", "display": "BNP [Mass/volume] in Serum or Plasma"},
    "systolic_bp": {"code": "8480-6", "display": "Systolic blood pressure"},
    "diastolic_bp": {"code": "8462-4", "display": "Diastolic blood pressure"},
    "heart_rate": {"code": "8867-4", "display": "Heart rate"},
    "temperature": {"code": "8310-5", "display": "Body temperature"},
    "weight_kg": {"code": "29463-7", "display": "Body weight"},
    "height_cm": {"code": "8302-2", "display": "Body height"},
    "BMI": {"code": "39156-5", "display": "Body mass index"},
    "oxygen_sat": {"code": "2708-6", "display": "Oxygen saturation in Arterial blood"},
    "potassium": {"code": "2823-3", "display": "Potassium [Moles/volume] in Serum or Plasma"},
}

UCUM_MAP = {
    "mg/dL": "mg/dL",
    "%": "%",
    "mL/min/1.73m2": "mL/min/{1.73_m2}",
    "K/uL": "10*3/uL",
    "g/dL": "g/dL",
    "pg/mL": "pg/mL",
    "mmHg": "mm[Hg]",
    "bpm": "/min",
    "Cel": "Cel",
    "kg": "kg",
    "cm": "cm",
    "kg/m2": "kg/m2",
}

UNIT_UCUM = {
    "glucose": ("mg/dL", "mg/dL"),
    "HbA1c": ("%", "%"),
    "creatinine": ("mg/dL", "mg/dL"),
    "eGFR": ("mL/min/1.73m2", "mL/min/{1.73_m2}"),
    "total_cholesterol": ("mg/dL", "mg/dL"),
    "LDL": ("mg/dL", "mg/dL"),
    "HDL": ("mg/dL", "mg/dL"),
    "WBC": ("K/uL", "10*3/uL"),
    "hemoglobin": ("g/dL", "g/dL"),
    "BNP": ("pg/mL", "pg/mL"),
    "systolic_bp": ("mmHg", "mm[Hg]"),
    "diastolic_bp": ("mmHg", "mm[Hg]"),
    "heart_rate": ("bpm", "/min"),
    "temperature": ("Cel", "Cel"),
    "weight_kg": ("kg", "kg"),
    "height_cm": ("cm", "cm"),
    "BMI": ("kg/m2", "kg/m2"),
    "oxygen_sat": ("%", "%"),
    "potassium": ("mEq/L", "meq/L"),
}

# ICD-10 diagnosis info for Condition resources
ICD10_DISPLAY = {
    "E11.9": "Type 2 diabetes mellitus without complications",
    "E11.65": "Type 2 diabetes mellitus with hyperglycemia",
    "I10": "Essential (primary) hypertension",
    "I50.9": "Heart failure, unspecified",
    "N18.3": "Chronic kidney disease, stage 3 (moderate)",
    "J44.1": "Chronic obstructive pulmonary disease with acute exacerbation",
    "M54.5": "Low back pain",
    "F32.9": "Major depressive disorder, single episode, unspecified",
    "J06.9": "Acute upper respiratory infection, unspecified",
    "K21.0": "Gastro-esophageal reflux disease with esophagitis",
    "M17.11": "Primary osteoarthritis, right knee",
    "G47.33": "Obstructive sleep apnea",
    "E78.5": "Dyslipidemia, unspecified",
    "R10.9": "Unspecified abdominal pain",
    "Z00.00": "Encounter for general adult medical examination",
}

# Observation category
VITAL_SIGNS_CATEGORY = {
    "coding": [{"system": "http://terminology.hl7.org/CodeSystem/observation-category",
                "code": "vital-signs", "display": "Vital Signs"}]
}
LAB_CATEGORY = {
    "coding": [{"system": "http://terminology.hl7.org/CodeSystem/observation-category",
                "code": "laboratory", "display": "Laboratory"}]
}

LAB_NAMES = {"glucose", "HbA1c", "creatinine", "eGFR", "total_cholesterol",
             "LDL", "HDL", "WBC", "hemoglobin", "BNP", "oxygen_sat", "potassium"}


# ---------------------------------------------------------------------------
# Helper: build FHIR resources
# ---------------------------------------------------------------------------

def _uid() -> str:
    return str(uuid.uuid4())


def _make_patient(member: Dict[str, Any]) -> Dict[str, Any]:
    """Build a FHIR Patient resource from a member record."""
    gender_map = {"M": "male", "F": "female"}
    return {
        "resourceType": "Patient",
        "id": member["member_id"],
        "identifier": [
            {
                "type": {"coding": [{"system": "http://terminology.hl7.org/CodeSystem/v2-0203",
                                     "code": "MB", "display": "Member Number"}]},
                "system": "urn:oid:2.16.840.1.113883.3.redbricks",
                "value": member["member_id"],
            }
        ],
        "name": [{"family": member.get("last_name", "Unknown"),
                  "given": [member.get("first_name", "Unknown")]}],
        "gender": gender_map.get(member.get("gender", ""), "unknown"),
        "birthDate": member.get("date_of_birth", "1970-01-01"),
        "address": [{
            "line": [member.get("address_line_1", "")],
            "city": member.get("city", ""),
            "state": member.get("state", "NC"),
            "postalCode": member.get("zip_code", ""),
            "district": member.get("county", ""),
        }],
        "telecom": [
            {"system": "phone", "value": member.get("phone", "")},
            {"system": "email", "value": member.get("email", "")},
        ],
    }


def _make_encounter(enc: Dict[str, Any], member_id: str) -> Dict[str, Any]:
    """Build a FHIR Encounter resource."""
    enc_class = ENCOUNTER_CLASS_MAP.get(enc["encounter_type"], {"code": "AMB", "display": "ambulatory"})
    enc_type = ENCOUNTER_TYPE_MAP.get(enc["visit_type"], {"code": "185349003", "display": "Encounter for check up"})
    dos = enc.get("date_of_service", "2024-01-01")

    return {
        "resourceType": "Encounter",
        "id": enc["encounter_id"],
        "status": "finished",
        "class": {
            "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
            "code": enc_class["code"],
            "display": enc_class["display"],
        },
        "type": [{
            "coding": [{
                "system": "http://snomed.info/sct",
                "code": enc_type["code"],
                "display": enc_type["display"],
            }]
        }],
        "subject": {"reference": f"Patient/{member_id}"},
        "participant": [{
            "individual": {"reference": f"Practitioner/{enc.get('provider_npi', 'unknown')}",
                           "display": f"NPI: {enc.get('provider_npi', '')}"}
        }] if enc.get("provider_npi") else [],
        "period": {"start": dos, "end": dos},
    }


def _make_condition(member_id: str, icd10_code: str, encounter_id: Optional[str] = None) -> Dict[str, Any]:
    """Build a FHIR Condition resource from an ICD-10 code."""
    display = ICD10_DISPLAY.get(icd10_code, icd10_code)
    resource = {
        "resourceType": "Condition",
        "id": _uid(),
        "clinicalStatus": {
            "coding": [{"system": "http://terminology.hl7.org/CodeSystem/condition-clinical",
                        "code": "active"}]
        },
        "verificationStatus": {
            "coding": [{"system": "http://terminology.hl7.org/CodeSystem/condition-ver-status",
                        "code": "confirmed"}]
        },
        "category": [{
            "coding": [{"system": "http://terminology.hl7.org/CodeSystem/condition-category",
                        "code": "encounter-diagnosis", "display": "Encounter Diagnosis"}]
        }],
        "code": {
            "coding": [{
                "system": "http://hl7.org/fhir/sid/icd-10-cm",
                "code": icd10_code,
                "display": display,
            }],
            "text": display,
        },
        "subject": {"reference": f"Patient/{member_id}"},
    }
    if encounter_id:
        resource["encounter"] = {"reference": f"Encounter/{encounter_id}"}
    return resource


def _make_observation(
    member_id: str,
    obs_name: str,
    value: float,
    obs_date: str,
    ref_low: Optional[float] = None,
    ref_high: Optional[float] = None,
    encounter_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Build a FHIR Observation resource for a lab result or vital sign."""
    loinc = LOINC_MAP.get(obs_name, {"code": "unknown", "display": obs_name})
    unit_display, unit_ucum = UNIT_UCUM.get(obs_name, ("", ""))
    is_vital = obs_name not in LAB_NAMES
    category = VITAL_SIGNS_CATEGORY if is_vital else LAB_CATEGORY

    resource: Dict[str, Any] = {
        "resourceType": "Observation",
        "id": _uid(),
        "status": "final",
        "category": [category],
        "code": {
            "coding": [{
                "system": "http://loinc.org",
                "code": loinc["code"],
                "display": loinc["display"],
            }],
            "text": loinc["display"],
        },
        "subject": {"reference": f"Patient/{member_id}"},
        "effectiveDateTime": obs_date,
        "valueQuantity": {
            "value": value,
            "unit": unit_display,
            "system": "http://unitsofmeasure.org",
            "code": unit_ucum,
        },
    }
    if encounter_id:
        resource["encounter"] = {"reference": f"Encounter/{encounter_id}"}
    if ref_low is not None and ref_high is not None:
        resource["referenceRange"] = [{
            "low": {"value": ref_low, "unit": unit_display, "system": "http://unitsofmeasure.org", "code": unit_ucum},
            "high": {"value": ref_high, "unit": unit_display, "system": "http://unitsofmeasure.org", "code": unit_ucum},
        }]
    return resource


# ---------------------------------------------------------------------------
# Main generator: produces FHIR R4 Bundles
# ---------------------------------------------------------------------------

def generate_fhir_bundles(
    members_data: List[Dict[str, Any]],
    encounters: List[Dict[str, Any]],
    labs: List[Dict[str, Any]],
    vitals: List[Dict[str, Any]],
    primary_dx_by_member: Dict[str, str],
    secondary_dx_by_member: Optional[Dict[str, List[str]]] = None,
) -> List[str]:
    """
    Generate one FHIR R4 Bundle (JSON string) per member.

    Each bundle is a ``transaction`` bundle containing:
      - 1 Patient resource
      - N Encounter resources
      - N Condition resources (from diagnosis codes on claims)
      - N Observation resources (labs + vitals with LOINC codes)

    Returns a list of JSON strings, each being a complete FHIR Bundle.
    """
    # Index encounters, labs, vitals by member_id
    enc_by_member: Dict[str, List[Dict]] = {}
    for e in encounters:
        mid = e.get("member_id")
        if mid:
            enc_by_member.setdefault(mid, []).append(e)

    lab_by_member: Dict[str, List[Dict]] = {}
    for l in labs:
        mid = l.get("member_id")
        if mid:
            lab_by_member.setdefault(mid, []).append(l)

    vit_by_member: Dict[str, List[Dict]] = {}
    for v in vitals:
        mid = v.get("member_id")
        if mid:
            vit_by_member.setdefault(mid, []).append(v)

    if secondary_dx_by_member is None:
        secondary_dx_by_member = {}

    bundles: List[str] = []

    for member in members_data:
        mid = member["member_id"]
        entries: List[Dict[str, Any]] = []

        # Patient
        patient = _make_patient(member)
        entries.append({
            "fullUrl": f"urn:uuid:{mid}",
            "resource": patient,
            "request": {"method": "PUT", "url": f"Patient/{mid}"},
        })

        # Encounters
        member_encounters = enc_by_member.get(mid, [])
        first_enc_id = member_encounters[0]["encounter_id"] if member_encounters else None
        for enc in member_encounters:
            encounter = _make_encounter(enc, mid)
            entries.append({
                "fullUrl": f"urn:uuid:{enc['encounter_id']}",
                "resource": encounter,
                "request": {"method": "PUT", "url": f"Encounter/{enc['encounter_id']}"},
            })

        # Conditions (from claims diagnosis codes)
        dx_codes = set()
        primary = primary_dx_by_member.get(mid)
        if primary and primary != "INVALID":
            dx_codes.add(primary)
        for dx in secondary_dx_by_member.get(mid, []):
            if dx and dx != "INVALID":
                dx_codes.add(dx)

        for dx_code in dx_codes:
            condition = _make_condition(mid, dx_code, first_enc_id)
            entries.append({
                "fullUrl": f"urn:uuid:{condition['id']}",
                "resource": condition,
                "request": {"method": "PUT", "url": f"Condition/{condition['id']}"},
            })

        # Observations — labs
        for lab in lab_by_member.get(mid, []):
            if lab.get("value") is None or not isinstance(lab["value"], (int, float)):
                continue
            obs = _make_observation(
                member_id=mid,
                obs_name=lab["lab_name"],
                value=lab["value"],
                obs_date=lab.get("collection_date", "2024-01-01"),
                ref_low=lab.get("reference_range_low"),
                ref_high=lab.get("reference_range_high"),
            )
            entries.append({
                "fullUrl": f"urn:uuid:{obs['id']}",
                "resource": obs,
                "request": {"method": "PUT", "url": f"Observation/{obs['id']}"},
            })

        # Observations — vitals
        for vit in vit_by_member.get(mid, []):
            if vit.get("value") is None or not isinstance(vit["value"], (int, float)):
                continue
            obs = _make_observation(
                member_id=mid,
                obs_name=vit["vital_name"],
                value=vit["value"],
                obs_date=vit.get("measurement_date", "2024-01-01"),
            )
            entries.append({
                "fullUrl": f"urn:uuid:{obs['id']}",
                "resource": obs,
                "request": {"method": "PUT", "url": f"Observation/{obs['id']}"},
            })

        bundle = {
            "resourceType": "Bundle",
            "id": _uid(),
            "type": "transaction",
            "entry": entries,
        }
        bundles.append(json.dumps(bundle))

    return bundles
