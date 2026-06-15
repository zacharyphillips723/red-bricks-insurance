# Databricks notebook source
# MAGIC %md
# MAGIC # Red Bricks Insurance — Backfill FWA Monitoring Table
# MAGIC
# MAGIC **Manual notebook** — seeds multi-day synthetic prediction data so Lakehouse Monitor
# MAGIC has enough time windows for drift detection, profile metrics, and anomaly detection.
# MAGIC
# MAGIC Uses real feature distributions from the training feature table and simulates model
# MAGIC predictions with realistic probability scores spread across configurable days.
# MAGIC Introduces slight distribution drift over time to make monitoring more interesting.
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - `train_fwa_model.py` has run (feature table populated)
# MAGIC - `inference.fwa_xgboost_monitoring_input` table exists (from DLT pipeline or initial run)

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance_catalog", "Catalog")
dbutils.widgets.text("days_back", "7", "Days of history to generate")
dbutils.widgets.text("predictions_per_day", "500", "Predictions per day")

catalog = dbutils.widgets.get("catalog")
catalog_sql = f"`{catalog}`"
days_back = int(dbutils.widgets.get("days_back"))
predictions_per_day = int(dbutils.widgets.get("predictions_per_day"))

ANALYTICS_SCHEMA = "analytics"
FWA_SCHEMA = "fwa"
INFERENCE_SCHEMA = "inference"
FEATURE_TABLE = f"{catalog}.{ANALYTICS_SCHEMA}.fwa_training_features"
MONITORING_TABLE = f"{catalog_sql}.{INFERENCE_SCHEMA}.fwa_xgboost_monitoring_input"

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

print(f"Catalog:             {catalog}")
print(f"Days to backfill:    {days_back}")
print(f"Predictions per day: {predictions_per_day}")
print(f"Total rows:          {days_back * predictions_per_day:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Real Feature Data & Labels

# COMMAND ----------

from pyspark.sql import functions as F
import random

feature_df = spark.table(FEATURE_TABLE)

# Load all features with claim_id
features_pdf = feature_df.select(["claim_id"] + FEATURE_COLS).toPandas()

# Load ground truth labels from FWA signals
labels_pdf = spark.sql(f"""
    SELECT DISTINCT claim_id, 1 AS is_fraud
    FROM {catalog_sql}.{FWA_SCHEMA}.silver_fwa_signals
    WHERE fraud_score >= 0.5
""").toPandas()
label_map = dict(zip(labels_pdf["claim_id"], labels_pdf["is_fraud"]))

print(f"Feature rows available: {len(features_pdf):,}")
print(f"Fraud label rows:      {len(labels_pdf):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Backfill Data
# MAGIC
# MAGIC For each day, sample claims from the feature table and generate realistic
# MAGIC fraud probability scores. Introduces slight distribution shifts across days
# MAGIC to make drift detection more interesting.

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

random.seed(42)
np.random.seed(42)

rows = []
now = datetime.utcnow()

for day_offset in range(days_back, 0, -1):
    base_time = now - timedelta(days=day_offset)

    # Sample claims for this day (with replacement if needed)
    day_sample = features_pdf.sample(n=predictions_per_day, replace=True).reset_index(drop=True)

    # Simulate slight drift: shift fraud probability distribution over time
    drift_factor = 1.0 + (days_back - day_offset) * 0.03

    for i, row in day_sample.iterrows():
        ts = base_time + timedelta(
            hours=random.randint(8, 22),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59),
        )

        claim_id = row["claim_id"]
        is_fraud = float(label_map.get(claim_id, 0))

        # Generate realistic probability based on ground truth + noise
        if is_fraud == 1.0:
            base_prob = np.random.beta(5 * drift_factor, 2)
            prob = np.clip(base_prob, 0.01, 0.99)
        else:
            base_prob = np.random.beta(2, 5 * drift_factor)
            prob = np.clip(base_prob, 0.01, 0.99)

        risk_tier = "High" if prob >= 0.7 else ("Medium" if prob >= 0.4 else "Low")

        record = {
            "prediction_timestamp": ts,
            "claim_id": claim_id,
            "fraud_probability": round(float(prob), 6),
            "prediction": 1.0 if prob >= 0.5 else 0.0,
            "risk_tier": risk_tier,
            "model_name": "XGBoost",
            "is_fraud": is_fraud,
        }
        for col in FEATURE_COLS:
            record[col] = float(row[col])

        rows.append(record)

backfill_pdf = pd.DataFrame(rows)
print(f"Generated {len(backfill_pdf):,} backfill rows across {days_back} days")
print(f"\nPrediction timestamp range:")
print(f"  Earliest: {backfill_pdf['prediction_timestamp'].min()}")
print(f"  Latest:   {backfill_pdf['prediction_timestamp'].max()}")
print(f"\nFraud probability distribution:")
print(backfill_pdf["fraud_probability"].describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Append to Monitoring Table

# COMMAND ----------

# Ensure the inference schema exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_sql}.{INFERENCE_SCHEMA}")

backfill_sdf = spark.createDataFrame(backfill_pdf)

# Check if table exists and match column order, otherwise just write
try:
    existing_cols = [f.name for f in spark.table(MONITORING_TABLE).schema.fields]
    backfill_sdf = backfill_sdf.select(existing_cols)
    backfill_sdf.write.mode("append").saveAsTable(MONITORING_TABLE)
except Exception:
    # Table doesn't exist yet — create it
    backfill_sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(MONITORING_TABLE)

final_count = spark.table(MONITORING_TABLE).count()
print(f"Appended {len(backfill_pdf):,} rows to {MONITORING_TABLE}")
print(f"Total rows in monitoring table: {final_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Distribution by Day

# COMMAND ----------

daily_stats = spark.sql(f"""
    SELECT
        date_trunc('day', prediction_timestamp) as day,
        count(*) as predictions,
        round(avg(fraud_probability), 4) as avg_prob,
        round(stddev(fraud_probability), 4) as std_prob,
        sum(CASE WHEN risk_tier = 'High' THEN 1 ELSE 0 END) as high_risk,
        sum(CASE WHEN is_fraud = 1.0 THEN 1 ELSE 0 END) as actual_fraud
    FROM {MONITORING_TABLE}
    GROUP BY 1
    ORDER BY 1
""")
daily_stats.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 70)
print("MONITORING BACKFILL COMPLETE")
print("=" * 70)
print(f"  Days backfilled:      {days_back}")
print(f"  Predictions per day:  {predictions_per_day}")
print(f"  Total rows added:     {len(backfill_pdf):,}")
print(f"  Table:                {MONITORING_TABLE}")
print()
print("  Next steps:")
print("    1. Refresh the Lakehouse Monitor (Quality tab → Refresh)")
print("    2. Drift metrics will populate across daily time windows")
print("    3. Run fwa_model_monitoring.py to create the monitor + governance audit")
