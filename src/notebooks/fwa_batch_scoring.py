# Databricks notebook source
# MAGIC %md
# MAGIC # Red Bricks Insurance — FWA Batch Scoring via Serving Endpoint
# MAGIC
# MAGIC **Manual notebook** — run after creating the serving endpoint in the UI.
# MAGIC
# MAGIC Sends **500 claims** to the `fwa_scorer` serving endpoint in batches of 50 to:
# MAGIC 1. Demonstrate real-time model serving with the pyfunc probability model
# MAGIC 2. Populate the **inference tables** (must be enabled on the endpoint)
# MAGIC 3. Generate prediction data for the Lakehouse Monitor pipeline
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - `train_fwa_model.py` has run (model registered as `@champion`)
# MAGIC - Serving endpoint created manually in the UI with inference tables enabled

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance_catalog", "Catalog")
dbutils.widgets.text("endpoint_name", "fwa_scorer", "Serving Endpoint Name")

catalog = dbutils.widgets.get("catalog")
catalog_sql = f"`{catalog}`"
endpoint_name = dbutils.widgets.get("endpoint_name")

ANALYTICS_SCHEMA = "analytics"
INFERENCE_SCHEMA = "inference"
FEATURE_TABLE = f"{catalog}.{ANALYTICS_SCHEMA}.fwa_training_features"
BATCH_SIZE = 50
TOTAL_PREDICTIONS = 500

FEATURE_COLS = [
    "billed_amount", "allowed_amount", "paid_amount", "billed_to_allowed_ratio",
    "member_responsibility", "copay", "coinsurance", "deductible",
    "payment_lag_days", "service_day_of_week", "diagnosis_code_count",
    "provider_total_claims", "provider_avg_billed", "provider_e5_visit_pct",
    "provider_denial_rate", "provider_unique_members",
    "provider_billed_to_allowed_ratio", "provider_fwa_signal_count",
    "provider_composite_risk_score",
    "member_total_claims", "member_unique_providers", "member_unique_diagnoses",
    "member_risk_score",
]

print(f"Catalog:     {catalog}")
print(f"Endpoint:    {endpoint_name}")
print(f"Predictions: {TOTAL_PREDICTIONS} (batches of {BATCH_SIZE})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Claims & Get Features

# COMMAND ----------

import requests
import json
import time
import pandas as pd
from pyspark.sql import functions as F
from datetime import datetime

# Get workspace URL and token
host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# Sample 500 claims from the feature table
sample_df = spark.table(FEATURE_TABLE).select("claim_id").orderBy(F.rand(seed=42)).limit(TOTAL_PREDICTIONS)
claim_ids = [row.claim_id for row in sample_df.collect()]

# Get feature data for endpoint scoring
features_pdf = (
    spark.table(FEATURE_TABLE)
    .filter(F.col("claim_id").isin(claim_ids))
    .select(["claim_id"] + FEATURE_COLS)
    .toPandas()
)

print(f"Sampled {len(claim_ids)} claim IDs for scoring")
print(f"Feature columns: {len(FEATURE_COLS)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Score Endpoint (Populates Inference Tables)
# MAGIC
# MAGIC Sends real HTTP requests to the serving endpoint. Each request
# MAGIC is logged to the endpoint's inference table for downstream monitoring.

# COMMAND ----------

print(f"Checking endpoint readiness: {endpoint_name}")
endpoint_ready = False
for attempt in range(30):
    try:
        resp = requests.get(f"{host}/api/2.0/serving-endpoints/{endpoint_name}", headers=headers)
        if resp.status_code == 200:
            state = resp.json().get("state", {}).get("ready", "NOT_READY")
            config_update = resp.json().get("state", {}).get("config_update", "NOT_UPDATING")
            if state == "READY" and config_update != "IN_PROGRESS":
                print(f"  Endpoint READY.")
                endpoint_ready = True
                break
            if attempt % 3 == 0:
                print(f"  [{attempt*10}s] state={state}, config_update={config_update}")
        else:
            if attempt % 3 == 0:
                print(f"  [{attempt*10}s] HTTP {resp.status_code}")
    except Exception as e:
        if attempt % 3 == 0:
            print(f"  [{attempt*10}s] Error: {e}")
    time.sleep(10)

if not endpoint_ready:
    dbutils.notebook.exit("ERROR: Endpoint not ready after 5 minutes. Create it in the Serving UI first.")

# COMMAND ----------

# Score in batches
results = []
total_scored = 0
total_errors = 0
start_time = time.time()

for batch_start in range(0, len(features_pdf), BATCH_SIZE):
    batch = features_pdf.iloc[batch_start:batch_start + BATCH_SIZE]
    batch_claim_ids = batch["claim_id"].tolist()

    instances = batch[["claim_id"] + FEATURE_COLS].to_dict(orient="records")
    payload = {"dataframe_records": instances}

    try:
        resp = requests.post(
            f"{host}/serving-endpoints/{endpoint_name}/invocations",
            headers=headers,
            json=payload,
            timeout=60,
        )

        if resp.status_code == 200:
            predictions = resp.json().get("predictions", [])
            for cid, pred in zip(batch_claim_ids, predictions):
                score = pred if isinstance(pred, (int, float)) else pred.get("prediction", 0)
                results.append({"claim_id": cid, "fraud_probability": float(score)})
            total_scored += len(predictions)
        else:
            total_errors += len(batch)
            if batch_start == 0:
                print(f"  Batch 1: HTTP {resp.status_code} — {resp.text[:200]}")

    except Exception as e:
        total_errors += len(batch)
        if batch_start == 0:
            print(f"  Batch 1: Error — {str(e)[:200]}")

elapsed = time.time() - start_time
print(f"Scored:  {total_scored:,}")
print(f"Errors:  {total_errors:,}")
print(f"Time:    {elapsed:.1f}s ({total_scored/max(elapsed,1):.1f} predictions/sec)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Endpoint Results to Delta

# COMMAND ----------

if results:
    ep_pdf = pd.DataFrame(results)
    ep_pdf["source"] = "serving_endpoint"
    ep_pdf["endpoint_name"] = endpoint_name
    ep_pdf["scored_at"] = datetime.utcnow().isoformat()
    ep_pdf["risk_tier"] = ep_pdf["fraud_probability"].apply(
        lambda p: "High" if p >= 0.7 else ("Medium" if p >= 0.4 else "Low")
    )

    table_name = f"{catalog_sql}.{INFERENCE_SCHEMA}.{endpoint_name}_predictions"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_sql}.{INFERENCE_SCHEMA}")
    spark.createDataFrame(ep_pdf).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)

    high = (ep_pdf["risk_tier"] == "High").sum()
    med = (ep_pdf["risk_tier"] == "Medium").sum()
    print(f"\nPredictions written: {table_name}")
    print(f"  Total: {len(results):,}  High: {high:,}  Medium: {med:,}")
else:
    print("No results — endpoint may not have been ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 70)
print("BATCH SCORING COMPLETE")
print("=" * 70)
print(f"  Endpoint:       {endpoint_name}")
print(f"  Claims scored:  {total_scored:,}")
print(f"  Errors:         {total_errors:,}")
print()
print("  NOTE: Inference table data appears ~1hr after scoring")
print()
print("  Next steps:")
print("    1. Check Serving UI — verify inference tables are enabled")
print("    2. Wait ~1hr for inference table data to appear")
print(f"    3. Trigger fwa_monitoring_pipeline DLT pipeline")
print("    4. Run fwa_backfill_monitoring.py to seed multi-day data")
print("    5. Run fwa_model_monitoring.py for drift detection")
