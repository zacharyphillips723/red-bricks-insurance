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

import math
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
# Each entry: (reason, dx_code, service_line, dx_readmit_weight).
# dx_readmit_weight is a clinically informed log-odds bump applied to the 30-day
# readmission model in generate_readmission_episodes(). Chronic exacerbations
# (CHF, COPD, DKA, sepsis, AKI) are the classic high-readmit conditions; acute
# self-limited events (fracture, UTI, cellulitis) readmit far less often.
ADMIT_REASONS = [
    ("Chest Pain", "I20.9", "Cardiology", 0.3),
    ("Shortness of Breath", "R06.0", "Pulmonology", 0.4),
    ("Diabetic Ketoacidosis", "E11.10", "Endocrinology", 0.9),
    ("CHF Exacerbation", "I50.9", "Cardiology", 1.2),
    ("COPD Exacerbation", "J44.1", "Pulmonology", 1.0),
    ("Pneumonia", "J18.9", "Pulmonology", 0.5),
    ("Sepsis", "A41.9", "Infectious Disease", 0.9),
    ("Fall / Fracture", "W19", "Orthopedics", -0.5),
    ("Acute Kidney Injury", "N17.9", "Nephrology", 0.8),
    ("Stroke / TIA", "I63.9", "Neurology", 0.6),
    ("Abdominal Pain", "R10.9", "Gastroenterology", 0.0),
    ("Mental Health Crisis", "F32.9", "Behavioral Health", 0.7),
    ("Substance Use / Overdose", "F19.20", "Behavioral Health", 1.0),
    ("Cellulitis / Skin Infection", "L03.90", "General Medicine", -0.3),
    ("Urinary Tract Infection", "N39.0", "General Medicine", -0.4),
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

# Discharge-disposition log-odds bump for the readmission model. Discharge to a
# post-acute facility (SNF/rehab) or against medical advice signals an unstable
# transition of care and drives 30-day readmission; hospice/expired terminal
# dispositions are excluded from index-stay generation entirely.
DISPOSITION_READMIT_WEIGHT = {
    "Home": -0.2,
    "Home with Home Health": 0.3,
    "Skilled Nursing Facility": 0.9,
    "Rehabilitation Facility": 0.5,
    "Against Medical Advice": 1.3,
    "Transferred to Another Facility": 0.4,
}

# Dispositions eligible to serve as an index-stay discharge (i.e. the member is
# alive and back in the community, so a readmission is possible).
INDEX_DISCHARGE_DISPOSITIONS = [
    ("Home", 0.52),
    ("Home with Home Health", 0.18),
    ("Skilled Nursing Facility", 0.12),
    ("Rehabilitation Facility", 0.06),
    ("Against Medical Advice", 0.04),
    ("Transferred to Another Facility", 0.08),
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
        reason, dx_code, service_line, _dx_readmit_w = random.choice(ADMIT_REASONS)
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


# ─────────────────────────────────────────────────────────────────────────────
# Readmission episodes — member-linked A01→A03→(readmit A01) sequences with a
# clinically grounded, feature-correlated 30-day readmission label.
#
# The flat generators above (generate_adt_events / generate_adt_feed) emit
# independent random events that drive the live ADT dashboard and alert feed.
# They are NOT suitable for training a readmission model: is_readmission is a
# flat coin flip and admissions/discharges are never linked into episodes, so a
# "readmitted within 30 days of discharge" label cannot be derived.
#
# generate_readmission_episodes() instead emits complete inpatient episodes for
# a member (index admit → index discharge → optional readmit admit + discharge)
# where the readmission outcome is a logistic function of the features a payer
# actually sees at discharge: index length of stay, discharge disposition,
# primary diagnosis, prior utilization, and member risk (RAF / HCC / age).
# Index discharges are placed in a fully-observed historical window so every
# training row has a complete 30-day look-forward.
# ─────────────────────────────────────────────────────────────────────────────

# Logistic intercept tuned so the marginal 30-day readmission rate lands in the
# realistic payer range (~14–18%) given the feature distribution below.
_READMIT_INTERCEPT = -3.05


def _readmit_logit(
    *,
    dx_readmit_weight: float,
    disposition: str,
    los_days: int,
    patient_class: str,
    prior_admits_6mo: int,
    raf_score: float,
    hcc_count: int,
    age: int,
) -> float:
    """Log-odds of a 30-day unplanned readmission for one index stay."""
    z = _READMIT_INTERCEPT
    z += dx_readmit_weight
    z += DISPOSITION_READMIT_WEIGHT.get(disposition, 0.0)
    # Length of stay: each inpatient day adds risk, saturating around 10 days.
    z += 0.11 * min(los_days, 10)
    # Observation stays readmit less than true inpatient admissions.
    if patient_class != "Inpatient":
        z -= 0.4
    # Prior utilization is the single strongest real-world predictor.
    z += 0.45 * min(prior_admits_6mo, 4)
    # Member clinical risk.
    z += 0.35 * max(raf_score - 1.0, 0.0)
    z += 0.20 * min(hcc_count, 5)
    # Age: older members readmit more; centered at 65.
    z += 0.012 * max(age - 65, 0)
    return z


def _sigmoid(z: float) -> float:
    return 1.0 / (1.0 + math.exp(-z))


def _timestamp_on(day: date) -> str:
    """Realistic intra-day timestamp for an ADT event."""
    hour = random.choices(
        range(24),
        weights=[2, 1, 1, 1, 1, 2, 3, 5, 7, 8, 9, 8, 7, 7, 8, 8, 7, 6, 5, 4, 3, 3, 3, 2],
        k=1,
    )[0]
    return datetime(day.year, day.month, day.day, hour, random.randint(0, 59), 0).isoformat()


def _build_episode_event(
    *,
    member_id: str,
    event_type: str,
    event_day: date,
    facility: Dict[str, str],
    reason: str,
    dx_code: str,
    service_line: str,
    physician_name: str,
    physician_npi: str,
    patient_class: str,
    expected_los_days: int | None = None,
    discharge_disposition: str | None = None,
    is_readmission: bool = False,
    batch_id: str = "EPISODE_HIST",
) -> Dict[str, Any]:
    """Build a flat ADT event dict matching the shape emitted by generate_adt_events.

    Keeping the schema identical means silver/gold pipelines and the Lakebase
    alert-seeding logic consume episode events with no downstream changes.
    """
    return {
        "adt_event_id": str(uuid.uuid4()),
        "message_control_id": f"MSG{random.randint(100000000, 999999999)}",
        "event_type": event_type,
        "event_description": ADT_EVENT_DESCRIPTIONS[event_type],
        "event_timestamp": _timestamp_on(event_day),
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
        "acuity_level": None,
        "source_system": weighted_choice(["Epic", "Cerner", "MEDITECH", "Allscripts"], [0.45, 0.30, 0.15, 0.10]),
        "sending_facility": facility["facility_name"],
        "receiving_facility": "Red Bricks Insurance",
        "processed_at": None,
        "batch_id": batch_id,
        "batch_timestamp": datetime.now().isoformat(),
    }


def generate_readmission_episodes(
    member_ids: List[str],
    member_risk: Dict[str, Dict[str, Any]] | None = None,
    inpatient_rate: float = 0.28,
    history_days: int = 365,
    observation_gap_days: int = 45,
    seed: int | None = None,
) -> List[Dict[str, Any]]:
    """Generate member-linked inpatient episodes with a feature-correlated readmission label.

    Args:
        member_ids: Full member pool.
        member_risk: Optional map member_id -> {"raf_score", "hcc_count", "age"}.
            When absent, neutral defaults are used (raf 1.0, 0 HCCs, age 55) so
            the generator still runs standalone.
        inpatient_rate: Fraction of members who have at least one index inpatient
            stay in the history window.
        history_days: Length of the historical window (ending observation_gap_days
            before today) that index discharges are drawn from.
        observation_gap_days: Buffer between the latest index discharge and today,
            guaranteeing a fully-observed 30-day readmission window for every stay.
        seed: Optional RNG seed for reproducible datasets.

    Returns:
        Flat ADT event dicts (index admit, index discharge, and — when the member
        readmits — a readmit admit/discharge), schema-compatible with the live feed.
    """
    if seed is not None:
        random.seed(seed)
    member_risk = member_risk or {}

    today = date.today()
    window_end = today - timedelta(days=observation_gap_days)
    window_start = window_end - timedelta(days=history_days)

    events: List[Dict[str, Any]] = []
    n_index = int(len(member_ids) * inpatient_rate)
    index_members = random.sample(member_ids, min(n_index, len(member_ids)))

    for member_id in index_members:
        risk = member_risk.get(member_id, {})
        raf_score = float(risk.get("raf_score") or 1.0)
        hcc_count = int(risk.get("hcc_count") or 0)
        age = int(risk.get("age") or 55)

        # High-risk members get more index stays (utilization begets utilization).
        stay_lambda = 1.0 + 0.4 * max(raf_score - 1.0, 0.0) + 0.15 * min(hcc_count, 4)
        n_stays = min(1 + _poisson_like(stay_lambda), 4)

        # Anchor the first index admit somewhere early in the window; subsequent
        # stays step forward in time.
        cursor = random_date_between(window_start, window_end - timedelta(days=40))
        prior_admits_6mo = 0

        for _ in range(n_stays):
            if cursor >= window_end:
                break
            facility = random.choice(FACILITIES)
            reason, dx_code, service_line, dx_readmit_w = random.choice(ADMIT_REASONS)
            physician_name, physician_npi = random.choice(ATTENDING_PHYSICIANS)
            patient_class = weighted_choice(["Inpatient", "Observation"], [0.82, 0.18])

            los_days = random.choices(
                [1, 2, 3, 4, 5, 7, 10, 14], weights=[6, 16, 20, 18, 14, 12, 9, 5], k=1
            )[0]
            disposition = weighted_choice(
                [d[0] for d in INDEX_DISCHARGE_DISPOSITIONS],
                [d[1] for d in INDEX_DISCHARGE_DISPOSITIONS],
            )

            admit_day = cursor
            discharge_day = admit_day + timedelta(days=los_days)
            if discharge_day >= window_end:
                break

            events.append(_build_episode_event(
                member_id=member_id, event_type="A01", event_day=admit_day,
                facility=facility, reason=reason, dx_code=dx_code, service_line=service_line,
                physician_name=physician_name, physician_npi=physician_npi,
                patient_class=patient_class, expected_los_days=los_days,
                is_readmission=False,  # index admits are the reference stay, never the readmit
            ))
            events.append(_build_episode_event(
                member_id=member_id, event_type="A03", event_day=discharge_day,
                facility=facility, reason=reason, dx_code=dx_code, service_line=service_line,
                physician_name=physician_name, physician_npi=physician_npi,
                patient_class=patient_class, discharge_disposition=disposition,
            ))

            # Decide readmission from the discharge-time feature vector.
            p_readmit = _sigmoid(_readmit_logit(
                dx_readmit_weight=dx_readmit_w, disposition=disposition, los_days=los_days,
                patient_class=patient_class, prior_admits_6mo=prior_admits_6mo,
                raf_score=raf_score, hcc_count=hcc_count, age=age,
            ))

            if random.random() < p_readmit:
                # Readmit lands 3–30 days after discharge (right-skewed toward early).
                days_to_readmit = random.choices(
                    [3, 5, 7, 10, 14, 21, 28], weights=[10, 16, 20, 18, 14, 12, 10], k=1
                )[0]
                readmit_day = discharge_day + timedelta(days=days_to_readmit)
                if readmit_day < today:
                    r_reason, r_dx, r_service, _ = random.choice(ADMIT_REASONS)
                    r_los = random.choices([2, 3, 4, 5, 7, 10], weights=[15, 22, 20, 18, 15, 10], k=1)[0]
                    events.append(_build_episode_event(
                        member_id=member_id, event_type="A01", event_day=readmit_day,
                        facility=facility, reason=r_reason, dx_code=r_dx, service_line=r_service,
                        physician_name=physician_name, physician_npi=physician_npi,
                        patient_class="Inpatient", expected_los_days=r_los,
                        is_readmission=True,
                    ))
                    r_discharge = readmit_day + timedelta(days=r_los)
                    if r_discharge < today:
                        events.append(_build_episode_event(
                            member_id=member_id, event_type="A03", event_day=r_discharge,
                            facility=facility, reason=r_reason, dx_code=r_dx, service_line=r_service,
                            physician_name=physician_name, physician_npi=physician_npi,
                            patient_class="Inpatient",
                            discharge_disposition=weighted_choice(
                                [d[0] for d in INDEX_DISCHARGE_DISPOSITIONS],
                                [d[1] for d in INDEX_DISCHARGE_DISPOSITIONS],
                            ),
                        ))
                    cursor = r_discharge + timedelta(days=random.randint(20, 90))
                else:
                    cursor = discharge_day + timedelta(days=random.randint(20, 90))
            else:
                cursor = discharge_day + timedelta(days=random.randint(30, 120))

            prior_admits_6mo += 1

    events.sort(key=lambda e: e["event_timestamp"])
    return events


def _poisson_like(lam: float) -> int:
    """Small-count Poisson draw (Knuth's algorithm) for extra-stay counts."""
    l = math.exp(-lam)
    k, p = 0, 1.0
    while True:
        k += 1
        p *= random.random()
        if p <= l:
            return k - 1
