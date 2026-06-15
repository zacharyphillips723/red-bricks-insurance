# Databricks notebook source
# MAGIC %md
# MAGIC # Red Bricks Insurance — FWA Monitoring Pipeline (DLT)
# MAGIC
# MAGIC **Spark Declarative Pipeline** that parses serving endpoint payload tables
# MAGIC into monitoring-ready tables with exploded features and ground truth labels.
# MAGIC
# MAGIC | Source (Payload Table) | Target (Monitoring Input) |
# MAGIC |------------------------|---------------------------|
# MAGIC | `inference.fwa_scorer_payload` | `inference.fwa_xgboost_monitoring_input` |
# MAGIC
# MAGIC Once deployed, new serving predictions automatically flow into the monitoring table.
# MAGIC Set up Lakehouse Monitors directly on the target table via the Catalog UI.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType, StructField, DoubleType, StringType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config

# COMMAND ----------

CATALOG = spark.conf.get("pipeline.catalog", "red_bricks_insurance_catalog")
FWA_SCHEMA = "fwa"
INFERENCE_SCHEMA = "inference"

# Feature columns (must match train_fwa_model.py — 22 Red Bricks features)
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

# Schema for parsing the request JSON
RECORD_SCHEMA = ArrayType(StructType(
    [StructField("claim_id", StringType(), True)] +
    [StructField(c, DoubleType(), True) for c in FEATURE_COLS]
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## XGBoost Monitoring Input

# COMMAND ----------

@dlt.table(
    name="fwa_xgboost_monitoring_input",
    comment="Parsed XGBoost serving predictions with features and ground truth for drift monitoring",
)
def fwa_xgboost_monitoring_input():
    payload_df = spark.readStream.table(f"`{CATALOG}`.{INFERENCE_SCHEMA}.fwa_scorer_payload")

    # Filter successful requests
    filtered = payload_df.filter(F.col("status_code") == 200)

    # Parse request/response JSON
    parsed = filtered.select(
        F.col("request_time").cast("timestamp").alias("prediction_timestamp"),
        F.from_json(
            F.get_json_object(F.col("request"), "$.dataframe_records"),
            RECORD_SCHEMA,
        ).alias("records"),
        F.from_json(
            F.get_json_object(F.col("response"), "$.predictions"),
            ArrayType(DoubleType()),
        ).alias("predictions"),
    )

    # Explode batches into individual rows
    exploded = parsed.select(
        "prediction_timestamp",
        F.explode(F.arrays_zip("records", "predictions")).alias("zipped"),
    ).select(
        "prediction_timestamp",
        F.col("zipped.records.claim_id").alias("claim_id"),
        *[F.col(f"zipped.records.{c}").alias(c) for c in FEATURE_COLS],
        F.col("zipped.predictions").alias("fraud_probability"),
    )

    # Add derived columns
    enriched = exploded.withColumn(
        "prediction",
        F.when(F.col("fraud_probability") >= 0.5, 1.0).otherwise(0.0),
    ).withColumn(
        "risk_tier",
        F.when(F.col("fraud_probability") >= 0.7, "High")
         .when(F.col("fraud_probability") >= 0.4, "Medium")
         .otherwise("Low"),
    ).withColumn("model_name", F.lit("XGBoost"))

    # Join ground truth labels from FWA signals
    labels = spark.table(f"`{CATALOG}`.{FWA_SCHEMA}.silver_fwa_signals").filter(
        F.col("fraud_score") >= 0.5
    ).select("claim_id").distinct().withColumn("is_fraud", F.lit(1.0))

    result = enriched.join(labels, on="claim_id", how="left")
    result = result.fillna({"is_fraud": 0.0})

    return result
