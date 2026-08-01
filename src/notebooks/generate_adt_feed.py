# Databricks notebook source
# MAGIC %md
# MAGIC # ADT Feed Generator — Simulated Hospital Event Stream
# MAGIC
# MAGIC Generates a batch of synthetic ADT (Admit, Discharge, Transfer) events and writes
# MAGIC them as JSON to the raw volume. The ADT SDP pipeline (continuous mode) picks these
# MAGIC up via Autoloader within seconds.
# MAGIC
# MAGIC **Schedule:** Every 3 hours via Databricks Workflow. Each run drops 10-20 new events,
# MAGIC simulating a continuous feed from partner hospitals.
# MAGIC
# MAGIC **Alert integration:** After writing events, queries `gold_adt_alerts` for new
# MAGIC alert-triggering events and inserts them into the Lakebase alerts table for the
# MAGIC Population Health Command Center app.

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance_catalog", "Catalog")
dbutils.widgets.text("events_per_batch", "15", "Events per batch")
dbutils.widgets.text("lakebase_project_id", "red-bricks-insurance", "Lakebase Project ID")
# When "true", also emit a one-time historical backfill of member-linked
# inpatient episodes with a feature-correlated 30-day readmission label. This
# is the training corpus for the readmission risk model. The recurring 3-hour
# live feed leaves this "false" so episodes are generated exactly once at seed.
dbutils.widgets.dropdown("generate_episodes", "false", ["true", "false"], "Generate readmission episodes (one-time)")

catalog = dbutils.widgets.get("catalog")
events_per_batch = int(dbutils.widgets.get("events_per_batch"))
generate_episodes = dbutils.widgets.get("generate_episodes") == "true"
volume_base = f"/Volumes/{catalog}/raw/raw_sources"

print(f"Catalog: {catalog}")
print(f"Events per batch: {events_per_batch}")
print(f"Generate episodes: {generate_episodes}")
print(f"Volume: {volume_base}")

# COMMAND ----------

# NOTE: No `%pip install faker` / `restartPython()` here. The ADT generators
# (generate_adt_events / generate_readmission_episodes in adt.py) and their only
# import (helpers.py) are stdlib-only — faker is never used on this path. The
# in-notebook pip install + restartPython was removed because it hangs on
# serverless environment init and blocks the whole ADT branch of the demo job.

# COMMAND ----------

import json
import random
import uuid
from datetime import date, datetime, timedelta

catalog = dbutils.widgets.get("catalog")
events_per_batch = int(dbutils.widgets.get("events_per_batch"))
generate_episodes = dbutils.widgets.get("generate_episodes") == "true"
volume_base = f"/Volumes/{catalog}/raw/raw_sources"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate ADT Events

# COMMAND ----------

# Import the ADT generator — add bundle root to sys.path
import sys, os
try:
    _here = os.path.dirname(os.path.abspath(__file__))
    _repo_root = os.path.abspath(os.path.join(_here, "..", ".."))
except Exception:
    _nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    _ws_root = "/Workspace" + _nb_path.rsplit("/src/notebooks/", 1)[0] if not _nb_path.startswith("/Workspace") else _nb_path.rsplit("/src/notebooks/", 1)[0]
    _repo_root = _ws_root
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

from src.data_generation.domains.adt import (
    generate_adt_events,
    generate_readmission_episodes,
    ADT_EVENT_TYPES,
    ADT_EVENT_DESCRIPTIONS,
    FACILITIES,
    ADMIT_REASONS,
)

# Get member IDs from the raw parquet (available right after data_generation)
member_df = spark.read.parquet(f"{volume_base}/members/").select("member_id").distinct().limit(5000)
member_ids = [row.member_id for row in member_df.collect()]
print(f"Loaded {len(member_ids)} member IDs")

# Generate a batch
batch = generate_adt_events(
    member_ids=member_ids,
    start_date=date.today() - timedelta(hours=3),
    end_date=date.today(),
    events_per_batch=events_per_batch,
)

# Tag with batch metadata
batch_id = f"BATCH_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
for evt in batch:
    evt["batch_id"] = batch_id
    evt["batch_timestamp"] = datetime.now().isoformat()

print(f"Generated {len(batch)} ADT events in batch {batch_id}")

# Show sample
for evt in batch[:3]:
    print(f"  {evt['event_type']} ({evt['event_description']}) — {evt['member_id']} at {evt['facility_name']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Volume (JSON for Autoloader)

# COMMAND ----------

# Write as JSON — each batch gets a unique filename so Autoloader picks it up
output_dir = f"{volume_base}/adt_events"
dbutils.fs.mkdirs(output_dir)

# Write directly as JSON lines to the Volume FUSE mount path
# (serverless blocks dbutils.fs.cp from local /tmp, but direct FUSE writes work)
import json, os
file_name = f"adt_{batch_id}.json"
fuse_path = f"/Volumes/{catalog}/raw/raw_sources/adt_events/{file_name}"
os.makedirs(os.path.dirname(fuse_path), exist_ok=True)
with open(fuse_path, "w") as f:
    for evt in batch:
        f.write(json.dumps(evt, default=str) + "\n")

print(f"Written {len(batch)} events to {fuse_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## One-Time Readmission Episode Backfill (training corpus)
# MAGIC
# MAGIC When `generate_episodes=true` (seed run only), emit member-linked inpatient
# MAGIC episodes (index admit → discharge → optional 30-day readmit) whose readmission
# MAGIC outcome is a logistic function of index LOS, discharge disposition, primary
# MAGIC diagnosis, prior utilization, and member risk (RAF / HCC count / age). These
# MAGIC episodes are the labeled training data for the readmission risk model and flow
# MAGIC through the same Autoloader → bronze/silver ADT path as the live feed.

# COMMAND ----------

if not generate_episodes:
    print("generate_episodes=false — skipping one-time readmission episode backfill")
else:
    from datetime import date as _date

    # Build member risk context (RAF, HCC count, age) from the raw parquet so the
    # readmission label correlates with the features the Member 360 already shows.
    risk_pdf = (
        spark.read.parquet(f"{volume_base}/risk_adjustment_member/")
        .select("member_id", "raf_score", "hcc_codes")
        .toPandas()
    )
    members_pdf = (
        spark.read.parquet(f"{volume_base}/members/")
        .select("member_id", "date_of_birth")
        .toPandas()
    )

    def _hcc_count(codes):
        if not codes:
            return 0
        return len([c for c in str(codes).split(",") if c.strip()])

    def _age_from_dob(dob):
        try:
            d = datetime.fromisoformat(str(dob)[:10]).date()
            return max(0, int((_date.today() - d).days // 365.25))
        except Exception:
            return 55

    age_map = {r.member_id: _age_from_dob(r.date_of_birth) for r in members_pdf.itertuples()}
    member_risk = {}
    for r in risk_pdf.itertuples():
        member_risk[r.member_id] = {
            "raf_score": float(r.raf_score) if r.raf_score is not None else 1.0,
            "hcc_count": _hcc_count(r.hcc_codes),
            "age": age_map.get(r.member_id, 55),
        }
    # Members without a risk record still get episodes (neutral risk).
    for m in member_ids:
        member_risk.setdefault(m, {"raf_score": 1.0, "hcc_count": 0, "age": age_map.get(m, 55)})

    episodes = generate_readmission_episodes(
        member_ids=member_ids,
        member_risk=member_risk,
        seed=42,
    )
    n_readmit = sum(1 for e in episodes if e.get("is_readmission"))
    print(f"Generated {len(episodes)} episode events ({n_readmit} readmission admits) for {len(member_ids)} members")

    episode_file = f"/Volumes/{catalog}/raw/raw_sources/adt_events/adt_episodes_backfill.json"
    os.makedirs(os.path.dirname(episode_file), exist_ok=True)
    with open(episode_file, "w") as f:
        for evt in episodes:
            f.write(json.dumps(evt, default=str) + "\n")
    print(f"Written {len(episodes)} episode events to {episode_file}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Seed Lakebase Alerts from ADT Events
# MAGIC
# MAGIC For events that trigger care management alerts (readmissions, admissions,
# MAGIC discharges, high-acuity ED visits), insert directly into the Lakebase
# MAGIC `risk_stratification_alerts` table.

# COMMAND ----------

import time
import psycopg
from databricks.sdk import WorkspaceClient

LAKEBASE_PROJECT_ID = dbutils.widgets.get("lakebase_project_id")
LAKEBASE_BRANCH = "production"
DATABASE_NAME = "red_bricks_alerts"

# Hard wall-clock cap for the entire Lakebase alert-seeding step. This is a
# run-once demo seeder, alert seeding is a BONUS (the ADT pipeline also derives
# alerts from gold_adt_alerts, and the episode backfill JSON — the training
# corpus — is already written above before we reach this block). The Lakebase
# connect/wake path has been observed to block indefinitely against a
# scaled-to-zero endpoint on serverless, so we run the whole thing in a worker
# thread and abandon it if it exceeds the cap. Never let it hang the deploy job.
LAKEBASE_SEED_TIMEOUT_SEC = 120

w = WorkspaceClient()

def _ensure_endpoint_awake(endpoint_path: str) -> str:
    """Poll the Autoscaling endpoint until a host is live (handles scale-to-zero)."""
    max_attempts = 15
    for attempt in range(1, max_attempts + 1):
        ep = w.postgres.get_endpoint(name=endpoint_path)
        if ep.status and ep.status.hosts and ep.status.hosts.host:
            return ep.status.hosts.host
        if attempt < max_attempts:
            wait = min(5 * attempt, 30)
            print(f"  Lakebase endpoint not ready (attempt {attempt}/{max_attempts}), retrying in {wait}s...")
            time.sleep(wait)
    raise RuntimeError(f"Lakebase endpoint {endpoint_path} did not become ready after {max_attempts} attempts")


def get_lakebase_connection():
    """Get an authenticated psycopg connection to Lakebase (wake poll + connect_timeout)."""
    endpoint_path = f"projects/{LAKEBASE_PROJECT_ID}/branches/{LAKEBASE_BRANCH}/endpoints/primary"
    host = _ensure_endpoint_awake(endpoint_path)
    cred = w.postgres.generate_database_credential(endpoint=endpoint_path)
    return psycopg.connect(
        host=host,
        dbname=DATABASE_NAME,
        user=w.current_user.me().user_name,
        password=cred.token,
        sslmode="require",
        connect_timeout=60,
    )


def _should_trigger_alert(evt):
    """Determine if an ADT event should create a care management alert."""
    if evt.get("is_readmission"):
        return True
    if evt["event_type"] == "A01" and evt.get("patient_class") == "Inpatient":
        return True
    if evt["event_type"] == "A03":  # All discharges
        return True
    if evt["event_type"] == "A04" and evt.get("acuity_level") in ("1-Resuscitation", "2-Emergent", "3-Urgent"):
        return True
    return False

alert_events = [e for e in batch if _should_trigger_alert(e)]
print(f"{len(alert_events)} of {len(batch)} events trigger alerts")

# COMMAND ----------

# Insert alerts into Lakebase — under a hard wall-clock cap (see LAKEBASE_SEED_TIMEOUT_SEC).
def _seed_alerts_into_lakebase(alert_events):
    conn = get_lakebase_connection()
    try:
        with conn.cursor() as cur:
            count = 0
            for evt in alert_events:
                # Map ADT event to alert fields
                if evt.get("is_readmission"):
                    risk_tier = "Critical"
                    source = "Readmission Risk"
                elif evt["event_type"] == "A01":
                    risk_tier = "High"
                    source = "Readmission Risk"
                elif evt["event_type"] == "A03" and evt.get("discharge_disposition") in ("Against Medical Advice", "Skilled Nursing Facility"):
                    risk_tier = "High"
                    source = "Readmission Risk"
                elif evt["event_type"] == "A04":
                    risk_tier = "Elevated"
                    source = "ED High Utilizer"
                else:
                    risk_tier = "Moderate"
                    source = "Readmission Risk"

                primary = f"ADT {evt['event_description']}: {evt['admit_reason']} at {evt['facility_name']}"
                secondary = [
                    f"DX: {evt['primary_diagnosis_code']}",
                    f"Service: {evt['service_line']}",
                    f"Class: {evt['patient_class']}",
                ]
                if evt.get("discharge_disposition"):
                    secondary.append(f"Disposition: {evt['discharge_disposition']}")
                if evt.get("is_readmission"):
                    secondary.append("⚠️ READMISSION within 30 days")

                # Check for existing alert_source enum value — use closest match
                alert_source_sql = {
                    "Readmission Risk": "Readmission Risk",
                    "ED High Utilizer": "ED High Utilizer",
                }.get(source, "Manual")

                cur.execute(
                    """
                    INSERT INTO risk_stratification_alerts (
                        patient_id, mrn, member_id, risk_tier, risk_score,
                        primary_driver, secondary_drivers, alert_source,
                        last_facility, last_encounter_date,
                        notes, status
                    ) VALUES (
                        %s, %s, %s, %s::risk_tier, %s,
                        %s, %s, %s::alert_source,
                        %s, %s::timestamptz,
                        %s, 'Unassigned'::care_cycle_status
                    )
                    """,
                    (
                        evt["member_id"], evt["member_id"], evt["member_id"],
                        risk_tier, round(random.uniform(40, 95), 2),
                        primary, secondary,
                        alert_source_sql,
                        evt["facility_name"], evt["event_timestamp"],
                        f"ADT feed: {evt['event_description']} — {evt['admit_reason']} ({evt['primary_diagnosis_code']})",
                    ),
                )
                count += 1
            conn.commit()
        print(f"✅ {count} ADT-triggered alerts seeded into Lakebase")
    finally:
        try:
            conn.close()
        except Exception:
            pass


if alert_events:
    import threading

    _seed_result = {"error": None}

    def _worker():
        try:
            _seed_alerts_into_lakebase(alert_events)
        except Exception as e:  # noqa: BLE001 — best-effort seeding
            _seed_result["error"] = e

    # Daemon thread so an abandoned (hung) connect can't keep the notebook alive.
    _t = threading.Thread(target=_worker, daemon=True)
    _t.start()
    _t.join(timeout=LAKEBASE_SEED_TIMEOUT_SEC)

    if _t.is_alive():
        print(f"⚠️ Lakebase alert seeding exceeded {LAKEBASE_SEED_TIMEOUT_SEC}s — abandoning (non-fatal).")
        print("  The episode backfill JSON is already written; the ADT pipeline and bootstrap")
        print("  will derive/seed alerts from gold_adt_alerts. Continuing.")
    elif _seed_result["error"] is not None:
        print(f"⚠️ Lakebase alert seeding failed: {_seed_result['error']}")
        print("  Non-fatal — alerts will be available once the pipeline processes events.")
else:
    print("No alert-triggering events in this batch")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print(f"\n{'='*60}")
print(f"ADT Feed Batch Complete")
print(f"{'='*60}")
print(f"  Batch ID:       {batch_id}")
print(f"  Events:         {len(batch)}")
print(f"  Alert triggers: {len(alert_events)}")
print(f"  Output:         {fuse_path}")
print(f"{'='*60}")

# Event type breakdown
from collections import Counter
type_counts = Counter(e["event_type"] for e in batch)
for etype, count in sorted(type_counts.items()):
    print(f"  {etype} ({ADT_EVENT_DESCRIPTIONS.get(etype, '?')}): {count}")
