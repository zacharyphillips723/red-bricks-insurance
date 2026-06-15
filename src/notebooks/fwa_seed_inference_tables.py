# Databricks notebook source
# MAGIC %md
# MAGIC # FWA — Seed Inference Tables
# MAGIC
# MAGIC After manually creating a serving endpoint with inference tables enabled,
# MAGIC this notebook sends a high volume of requests to populate the inference table
# MAGIC queue AND writes a batch inference table directly for **immediate** demo use.
# MAGIC
# MAGIC ### Why this notebook exists
# MAGIC Serving-side inference tables take **~1 hour** to materialize after the first request.
# MAGIC For demos, we can't wait — so this notebook:
# MAGIC 1. Sends 500+ requests to the endpoint (populates the real inference table queue)
# MAGIC 2. Writes `fwa_model_inference` directly from the responses (queryable NOW)
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - `train_fwa_model.py` (or AutoML variant) has run — feature table + model exist
# MAGIC - Serving endpoint created manually in UI with inference tables enabled

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance", "Catalog")
dbutils.widgets.text("endpoint_name", "fwa_scorer_demo", "Endpoint Name")
dbutils.widgets.text("num_requests", "500", "Number of Claims to Score")

catalog = dbutils.widgets.get("catalog")
catalog_sql = f"`{catalog}`"
endpoint_name = dbutils.widgets.get("endpoint_name")
num_requests = int(dbutils.widgets.get("num_requests"))

ANALYTICS_SCHEMA = "analytics"
CLAIMS_SCHEMA = "claims"
MEMBERS_SCHEMA = "members"
FEATURE_TABLE_NAME = f"{catalog}.{ANALYTICS_SCHEMA}.fwa_feature_store"

print(f"Catalog:         {catalog}")
print(f"Endpoint:        {endpoint_name}")
print(f"Claims to score: {num_requests}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Feature Table & Sample Claims

# COMMAND ----------

# DBTITLE 1,Cell 4
import time
import json
import requests as http_requests
import numpy as np
import pandas as pd
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

sample_df = spark.table(FEATURE_TABLE_NAME).limit(num_requests)
sample_pd = sample_df.toPandas()
claim_ids = sample_pd["claim_id"].tolist()
# Cast all numeric columns to float64 — model schema expects doubles
numeric_cols = sample_pd.select_dtypes(include="number").columns
sample_pd[numeric_cols] = sample_pd[numeric_cols].astype("float64")
sample_records = sample_pd.to_dict(orient="records")

print(f"Loaded {len(sample_records)} claims from {FEATURE_TABLE_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Send Batch Requests to Serving Endpoint

# COMMAND ----------

# DBTITLE 1,Cell 6
workspace_url = spark.conf.get("spark.databricks.workspaceUrl", "")
if not workspace_url.startswith("https://"):
    workspace_url = f"https://{workspace_url}"

token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
endpoint_url = f"{workspace_url}/serving-endpoints/{endpoint_name}/invocations"
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# Wait for endpoint to be ready
print(f"Checking endpoint '{endpoint_name}' status...")
for _ in range(60):
    ep = w.serving_endpoints.get(endpoint_name)
    state = ep.state
    if state and state.ready.value == "READY":
        print(f"  Endpoint is READY")
        break
    print(f"  State: {state} — waiting 15s...")
    time.sleep(15)
else:
    raise RuntimeError(f"Endpoint '{endpoint_name}' did not become READY within timeout")

# Send requests in batches of 20
batch_size = 20
latencies = []
all_predictions = []
errors = 0

print(f"\nSending {len(sample_records)} requests in batches of {batch_size}...")
for i in range(0, len(sample_records), batch_size):
    batch_claim_ids = claim_ids[i : i + batch_size]
    # Only send the lookup key — Feature Store wrapper fetches features from online store
    batch = [{"claim_id": cid} for cid in batch_claim_ids]
    payload = {"dataframe_records": batch}

    start_t = time.time()
    resp = http_requests.post(endpoint_url, json=payload, headers=headers, timeout=60)
    elapsed_ms = (time.time() - start_t) * 1000
    latencies.append(elapsed_ms)

    if resp.status_code == 200:
        resp_data = resp.json()
        preds = resp_data.get("predictions", [])
        for j, pred in enumerate(preds):
            prob = pred if isinstance(pred, (int, float)) else pred.get("prediction", pred)
            all_predictions.append({
                "claim_id": batch_claim_ids[j],
                "ml_fraud_probability": float(prob),
            })
    else:
        errors += 1
        if errors <= 3:
            print(f"  Batch {i//batch_size + 1}: HTTP {resp.status_code} — {resp.text[:200]}")

    if (i // batch_size + 1) % 5 == 0:
        print(f"  Sent {min(i + batch_size, len(sample_records))}/{len(sample_records)} requests...")

print(f"\nRequest Summary:")
print(f"  Total requests:     {len(sample_records)}")
print(f"  Successful batches: {len(latencies) - errors}/{len(latencies)}")
print(f"  Predictions:        {len(all_predictions)}")
print(f"  Avg latency:        {np.mean(latencies):.0f} ms")
print(f"  P95 latency:        {np.percentile(latencies, 95):.0f} ms")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Batch Inference Table Directly
# MAGIC
# MAGIC Build a DataFrame matching the inference table schema and write it immediately.
# MAGIC This gives us data for Lakehouse Monitor and dashboards without waiting for
# MAGIC the serving-side inference table to materialize.

# COMMAND ----------

if len(all_predictions) > 0:
    pred_pdf = pd.DataFrame(all_predictions)
    pred_pdf["ml_risk_tier"] = pred_pdf["ml_fraud_probability"].apply(
        lambda p: "High" if p >= 0.7 else ("Medium" if p >= 0.4 else "Low")
    )

    pred_spark_df = spark.createDataFrame(pred_pdf[["claim_id", "ml_fraud_probability", "ml_risk_tier"]])

    inference_table_name = f"{catalog_sql}.{ANALYTICS_SCHEMA}.fwa_model_inference"
    inference_with_context = pred_spark_df.join(
        spark.sql(f"""
            SELECT c.claim_id, c.member_id, c.rendering_provider_npi AS provider_npi,
                   c.claim_type, c.procedure_code, c.billed_amount, c.allowed_amount,
                   c.paid_amount, c.service_from_date, c.service_year_month,
                   e.line_of_business
            FROM {catalog_sql}.{CLAIMS_SCHEMA}.silver_claims_medical c
            LEFT JOIN {catalog_sql}.{MEMBERS_SCHEMA}.silver_enrollment e ON c.member_id = e.member_id
        """),
        on="claim_id",
        how="inner",
    )

    inference_with_context.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(inference_table_name)

    inf_count = spark.table(inference_table_name).count()
    high_risk = spark.sql(f"SELECT COUNT(*) FROM {inference_table_name} WHERE ml_risk_tier = 'High'").collect()[0][0]
    med_risk = spark.sql(f"SELECT COUNT(*) FROM {inference_table_name} WHERE ml_risk_tier = 'Medium'").collect()[0][0]
    low_risk = spark.sql(f"SELECT COUNT(*) FROM {inference_table_name} WHERE ml_risk_tier = 'Low'").collect()[0][0]

    print(f"Inference table written: {inference_table_name}")
    print(f"  Total:   {inf_count:,}")
    print(f"  High:    {high_risk:,}")
    print(f"  Medium:  {med_risk:,}")
    print(f"  Low:     {low_risk:,}")
else:
    print("WARNING: No predictions collected — check endpoint responses above.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

print("=" * 70)
print("FWA INFERENCE TABLE SEEDING — COMPLETE")
print("=" * 70)
print(f"  Endpoint:           {endpoint_name}")
print(f"  Requests sent:      {len(sample_records)}")
print(f"  Predictions:        {len(all_predictions)}")
print(f"  Avg latency:        {np.mean(latencies):.0f} ms")
print(f"  P95 latency:        {np.percentile(latencies, 95):.0f} ms")
print()
if len(all_predictions) > 0:
    print(f"  Inference Table:    {inference_table_name}")
    print(f"    Rows:             {inf_count:,}")
    print(f"    Risk distribution: High={high_risk:,} / Medium={med_risk:,} / Low={low_risk:,}")
print()
print("  The serving-side inference table will materialize in ~1 hour.")
print("  The batch inference table above is queryable NOW for monitoring.")
print()
print("  Next: Run fwa_model_monitoring.py for drift detection & API metrics")