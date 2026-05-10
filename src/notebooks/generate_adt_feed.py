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

dbutils.widgets.text("catalog", "red_bricks_insurance", "Catalog")
dbutils.widgets.text("events_per_batch", "15", "Events per batch")
dbutils.widgets.text("lakebase_project_id", "red-bricks-insurance", "Lakebase Project ID")

catalog = dbutils.widgets.get("catalog")
events_per_batch = int(dbutils.widgets.get("events_per_batch"))
volume_base = f"/Volumes/{catalog}/raw/raw_sources"

print(f"Catalog: {catalog}")
print(f"Events per batch: {events_per_batch}")
print(f"Volume: {volume_base}")

# COMMAND ----------

# MAGIC %pip install faker --quiet --retries 10 --timeout 120

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import json
import random
import uuid
from datetime import date, datetime, timedelta

catalog = dbutils.widgets.get("catalog")
events_per_batch = int(dbutils.widgets.get("events_per_batch"))
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
# MAGIC ## Seed Lakebase Alerts from ADT Events
# MAGIC
# MAGIC For events that trigger care management alerts (readmissions, admissions,
# MAGIC discharges, high-acuity ED visits), insert directly into the Lakebase
# MAGIC `risk_stratification_alerts` table.

# COMMAND ----------

import psycopg
from databricks.sdk import WorkspaceClient

LAKEBASE_PROJECT_ID = dbutils.widgets.get("lakebase_project_id")
DATABASE_NAME = "red_bricks_alerts"

w = WorkspaceClient()

def get_lakebase_connection():
    """Get authenticated connection to Lakebase."""
    projects = list(w.database.list_databases(filter_by=f"projects/{LAKEBASE_PROJECT_ID}"))
    if not projects:
        raise RuntimeError(f"Lakebase project '{LAKEBASE_PROJECT_ID}' not found")
    db = projects[0]
    endpoint = db.endpoints[0] if db.endpoints else None
    if not endpoint:
        raise RuntimeError("No endpoint found for Lakebase project")

    cred = w.database.generate_database_credential(endpoint=endpoint.name)
    host = endpoint.host
    port = endpoint.port or 5432

    return psycopg.connect(
        host=host,
        port=port,
        dbname=DATABASE_NAME,
        user=cred.username,
        password=cred.token,
        sslmode="require",
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

# Insert alerts into Lakebase
if alert_events:
    try:
        conn = get_lakebase_connection()
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
        conn.close()
        print(f"✅ {count} ADT-triggered alerts seeded into Lakebase")
    except Exception as e:
        print(f"⚠️ Lakebase alert seeding failed: {e}")
        print("  Alerts will be available when pipeline processes events and bootstrap runs")
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
