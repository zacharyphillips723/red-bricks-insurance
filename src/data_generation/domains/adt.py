# Red Bricks Insurance — ADT (Admit, Discharge, Transfer) feed generator.
#
# Generates realistic HL7-style ADT events for existing members to simulate
# real-time hospital notifications. Payers receive these from partner hospitals
# to trigger care management alerts (readmission risk, TOC follow-up, ED utilization).
#
# Event types:
#   A01 - Admit        → Triggers readmission risk check, care manager notification
#   A02 - Transfer     → Updates care location, may trigger escalation
#   A03 - Discharge    → Triggers TOC protocol (48hr call, 7-day PCP visit)
#   A04 - Registration → ED visit tracking, high-utilizer flagging

import random
import uuid
from datetime import date, datetime, timedelta
from typing import Any, Dict, List

from ..helpers import random_date_between, weighted_choice, COUNTIES

# ── ADT event types and weights ──────────────────────────────────────────────
ADT_EVENT_TYPES = ["A01", "A02", "A03", "A04"]
ADT_EVENT_WEIGHTS = [0.25, 0.10, 0.30, 0.35]  # Discharges + registrations most common

ADT_EVENT_DESCRIPTIONS = {
    "A01": "Admit",
    "A02": "Transfer",
    "A03": "Discharge",
    "A04": "Registration",
}

# ── Facilities (NC hospital network) ────────────────────────────────────────
FACILITIES = [
    {"facility_id": "FAC001", "facility_name": "WakeMed Raleigh Campus", "facility_type": "Hospital", "county": "Wake"},
    {"facility_id": "FAC002", "facility_name": "Duke University Hospital", "facility_type": "Hospital", "county": "Durham"},
    {"facility_id": "FAC003", "facility_name": "Atrium Health Carolinas Medical Center", "facility_type": "Hospital", "county": "Mecklenburg"},
    {"facility_id": "FAC004", "facility_name": "Cone Health Moses Cone Hospital", "facility_type": "Hospital", "county": "Guilford"},
    {"facility_id": "FAC005", "facility_name": "Novant Health Forsyth Medical Center", "facility_type": "Hospital", "county": "Forsyth"},
    {"facility_id": "FAC006", "facility_name": "Cape Fear Valley Medical Center", "facility_type": "Hospital", "county": "Cumberland"},
    {"facility_id": "FAC007", "facility_name": "Mission Hospital", "facility_type": "Hospital", "county": "Buncombe"},
    {"facility_id": "FAC008", "facility_name": "New Hanover Regional Medical Center", "facility_type": "Hospital", "county": "New Hanover"},
    {"facility_id": "FAC009", "facility_name": "CaroMont Regional Medical Center", "facility_type": "Hospital", "county": "Gaston"},
    {"facility_id": "FAC010", "facility_name": "Atrium Health Cabarrus", "facility_type": "Hospital", "county": "Cabarrus"},
]

# ── Admit reasons / chief complaints ────────────────────────────────────────
ADMIT_REASONS = [
    ("Chest Pain", "I20.9", "Cardiology"),
    ("Shortness of Breath", "R06.0", "Pulmonology"),
    ("Diabetic Ketoacidosis", "E11.10", "Endocrinology"),
    ("CHF Exacerbation", "I50.9", "Cardiology"),
    ("COPD Exacerbation", "J44.1", "Pulmonology"),
    ("Pneumonia", "J18.9", "Pulmonology"),
    ("Sepsis", "A41.9", "Infectious Disease"),
    ("Fall / Fracture", "W19", "Orthopedics"),
    ("Acute Kidney Injury", "N17.9", "Nephrology"),
    ("Stroke / TIA", "I63.9", "Neurology"),
    ("Abdominal Pain", "R10.9", "Gastroenterology"),
    ("Mental Health Crisis", "F32.9", "Behavioral Health"),
    ("Substance Use / Overdose", "F19.20", "Behavioral Health"),
    ("Cellulitis / Skin Infection", "L03.90", "General Medicine"),
    ("Urinary Tract Infection", "N39.0", "General Medicine"),
]

# ── Discharge dispositions ──────────────────────────────────────────────────
DISCHARGE_DISPOSITIONS = [
    ("Home", 0.55),
    ("Home with Home Health", 0.15),
    ("Skilled Nursing Facility", 0.10),
    ("Rehabilitation Facility", 0.05),
    ("Against Medical Advice", 0.03),
    ("Transferred to Another Facility", 0.05),
    ("Expired", 0.02),
    ("Hospice", 0.03),
    ("Left Without Being Seen", 0.02),
]

# ── Patient classes ─────────────────────────────────────────────────────────
PATIENT_CLASSES = {
    "A01": [("Inpatient", 0.70), ("Observation", 0.20), ("Emergency", 0.10)],
    "A02": [("Inpatient", 0.85), ("Observation", 0.15)],
    "A03": [("Inpatient", 0.60), ("Observation", 0.15), ("Emergency", 0.25)],
    "A04": [("Emergency", 0.70), ("Outpatient", 0.20), ("Urgent Care", 0.10)],
}

# ── Attending physicians (synthetic) ────────────────────────────────────────
ATTENDING_PHYSICIANS = [
    ("Dr. Sarah Chen", "1234567890"),
    ("Dr. Michael Rivera", "2345678901"),
    ("Dr. Angela Thompson", "3456789012"),
    ("Dr. Robert Kim", "4567890123"),
    ("Dr. Patricia Williams", "5678901234"),
    ("Dr. James Patterson", "6789012345"),
    ("Dr. Linda Nguyen", "7890123456"),
    ("Dr. David Okafor", "8901234567"),
]


def generate_adt_events(
    member_ids: List[str],
    start_date: date | None = None,
    end_date: date | None = None,
    events_per_batch: int = 50,
) -> List[Dict[str, Any]]:
    """Generate a batch of ADT events for a subset of members.

    Args:
        member_ids: Pool of member IDs to generate events for.
        start_date: Earliest event date. Defaults to 7 days ago.
        end_date: Latest event date. Defaults to today.
        events_per_batch: Number of events to generate per batch.

    Returns:
        List of ADT event dicts ready for Spark DataFrame creation.
    """
    if start_date is None:
        start_date = date.today() - timedelta(days=7)
    if end_date is None:
        end_date = date.today()

    events = []
    # Pick a subset of members who have encounters this batch
    selected_members = random.sample(member_ids, min(events_per_batch, len(member_ids)))

    for member_id in selected_members:
        event_type = weighted_choice(ADT_EVENT_TYPES, ADT_EVENT_WEIGHTS)
        facility = random.choice(FACILITIES)
        reason, dx_code, service_line = random.choice(ADMIT_REASONS)
        physician_name, physician_npi = random.choice(ATTENDING_PHYSICIANS)
        event_dt = random_date_between(start_date, end_date)

        # Generate realistic timestamps
        hour = random.choices(
            range(24),
            weights=[2, 1, 1, 1, 1, 2, 3, 5, 7, 8, 9, 8, 7, 7, 8, 8, 7, 6, 5, 4, 3, 3, 3, 2],
            k=1,
        )[0]
        minute = random.randint(0, 59)
        event_timestamp = datetime(event_dt.year, event_dt.month, event_dt.day, hour, minute, 0)

        # Patient class depends on event type
        patient_class = weighted_choice(
            [pc[0] for pc in PATIENT_CLASSES[event_type]],
            [pc[1] for pc in PATIENT_CLASSES[event_type]],
        )

        # Admit events get an expected LOS
        expected_los_days = None
        if event_type == "A01" and patient_class == "Inpatient":
            expected_los_days = random.choices([1, 2, 3, 4, 5, 7, 10, 14], weights=[5, 15, 20, 20, 15, 10, 10, 5], k=1)[0]

        # Discharge events get a disposition
        discharge_disposition = None
        if event_type == "A03":
            discharge_disposition = weighted_choice(
                [d[0] for d in DISCHARGE_DISPOSITIONS],
                [d[1] for d in DISCHARGE_DISPOSITIONS],
            )

        # Readmission flag — was this member admitted within 30 days of a prior discharge?
        is_readmission = random.random() < 0.12 if event_type == "A01" else False

        event = {
            "adt_event_id": str(uuid.uuid4()),
            "message_control_id": f"MSG{random.randint(100000000, 999999999)}",
            "event_type": event_type,
            "event_description": ADT_EVENT_DESCRIPTIONS[event_type],
            "event_timestamp": event_timestamp.isoformat(),
            "member_id": member_id,
            "patient_class": patient_class,
            "facility_id": facility["facility_id"],
            "facility_name": facility["facility_name"],
            "facility_type": facility["facility_type"],
            "facility_county": facility["county"],
            "attending_physician_name": physician_name,
            "attending_physician_npi": physician_npi,
            "admit_reason": reason,
            "primary_diagnosis_code": dx_code,
            "service_line": service_line,
            "expected_los_days": expected_los_days,
            "discharge_disposition": discharge_disposition,
            "is_readmission": is_readmission,
            "acuity_level": random.choice(["1-Resuscitation", "2-Emergent", "3-Urgent", "4-Less Urgent", "5-Non-Urgent"]) if event_type == "A04" else None,
            "source_system": weighted_choice(["Epic", "Cerner", "MEDITECH", "Allscripts"], [0.45, 0.30, 0.15, 0.10]),
            "sending_facility": facility["facility_name"],
            "receiving_facility": "Red Bricks Insurance",
            "processed_at": None,  # Set by the pipeline when processed
        }
        events.append(event)

    return events


def generate_adt_feed(
    member_ids: List[str],
    num_batches: int = 10,
    batch_interval_hours: int = 3,
    events_per_batch: int = 15,
    start_date: date | None = None,
) -> List[Dict[str, Any]]:
    """Generate multiple batches of ADT events spread over time.

    Simulates a continuous ADT feed from partner hospitals over several days.

    Args:
        member_ids: Pool of member IDs.
        num_batches: Number of feed batches to generate.
        batch_interval_hours: Hours between batches.
        events_per_batch: Events per batch.
        start_date: When the feed starts. Defaults to 30 days ago.

    Returns:
        All events across all batches, sorted by timestamp.
    """
    if start_date is None:
        start_date = date.today() - timedelta(days=30)

    all_events = []
    for batch_idx in range(num_batches):
        batch_start = start_date + timedelta(hours=batch_idx * batch_interval_hours)
        batch_end = batch_start + timedelta(hours=batch_interval_hours)

        batch = generate_adt_events(
            member_ids,
            start_date=batch_start,
            end_date=min(batch_end, date.today()),
            events_per_batch=events_per_batch,
        )

        # Tag batch metadata
        for evt in batch:
            evt["batch_id"] = f"BATCH{batch_idx + 1:04d}"
            evt["batch_timestamp"] = (
                datetime(batch_start.year, batch_start.month, batch_start.day)
                + timedelta(hours=batch_idx * batch_interval_hours)
            ).isoformat()

        all_events.extend(batch)

    # Sort by event timestamp
    all_events.sort(key=lambda e: e["event_timestamp"])
    return all_events
