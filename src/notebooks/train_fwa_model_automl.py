# Databricks notebook source
# MAGIC %md
# MAGIC # Red Bricks Insurance — FWA Fraud Scoring Model (AutoML)
# MAGIC
# MAGIC **Standalone notebook** — same fraud scoring use case as `train_fwa_model.py`,
# MAGIC but uses **Databricks AutoML** for fully automated model selection and tuning.
# MAGIC
# MAGIC ### When to use this notebook
# MAGIC - Non-serverless environments (AutoML uses `PERSIST TABLE` internally)
# MAGIC - Demo audiences who want to see the "one API call" AutoML experience
# MAGIC - Comparing AutoML's algorithm selection vs the manual XGBoost approach
# MAGIC
# MAGIC ### What AutoML does automatically
# MAGIC - Tries multiple algorithms (XGBoost, LightGBM, sklearn RandomForest, LogisticRegression)
# MAGIC - Performs hyperparameter optimization
# MAGIC - Handles class imbalance (~7% fraud rate)
# MAGIC - Logs all trials to MLflow with metrics, feature importance, and SHAP plots
# MAGIC - Generates a reproducible notebook for the best model
# MAGIC
# MAGIC > **Note:** This notebook is NOT part of the automated DAB job pipeline.
# MAGIC > The primary pipeline uses `train_fwa_model.py` (manual XGBoost) which runs on serverless.
# MAGIC > Both notebooks produce the same outputs: a registered UC model, batch predictions, and a serving endpoint.

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance", "Catalog")

catalog = dbutils.widgets.get("catalog")
catalog_sql = f"`{catalog}`"  # SQL-safe quoting (handles hyphens in catalog names)

FWA_SCHEMA = "fwa"
CLAIMS_SCHEMA = "claims"
MEMBERS_SCHEMA = "members"
ANALYTICS_SCHEMA = "analytics"
EXPERIMENT_NAME = f"{catalog}_fwa_fraud_scorer_automl"
MODEL_NAME = f"{catalog}.{ANALYTICS_SCHEMA}.fwa_scoring_model"

print(f"Catalog:    {catalog}")
print(f"Experiment: {EXPERIMENT_NAME}")
print(f"Model:      {MODEL_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Engineering
# MAGIC
# MAGIC Same feature table as the XGBoost notebook — claim-level + provider-level + member-level features.
# MAGIC Label: binary — `1` if claim_id appears in silver_fwa_signals with fraud_score >= 0.5.

# COMMAND ----------

feature_df = spark.sql(f"""
WITH claim_features AS (
  SELECT
    c.claim_id,
    c.member_id,
    c.rendering_provider_npi,
    c.claim_type,
    c.procedure_code,
    c.place_of_service_code,
    c.billed_amount,
    c.allowed_amount,
    c.paid_amount,
    c.copay,
    c.coinsurance,
    c.deductible,
    c.member_responsibility,
    c.service_from_date,
    c.service_year_month,

    -- Derived claim features
    COALESCE(c.billed_amount / NULLIF(c.allowed_amount, 0), 0) AS billed_to_allowed_ratio,
    DATEDIFF(c.paid_date, c.service_from_date) AS payment_lag_days,
    DAYOFWEEK(c.service_from_date) AS service_day_of_week,
    CASE WHEN c.secondary_diagnosis_code_1 IS NOT NULL THEN 1 ELSE 0 END
    + CASE WHEN c.secondary_diagnosis_code_2 IS NOT NULL THEN 1 ELSE 0 END
    + CASE WHEN c.secondary_diagnosis_code_3 IS NOT NULL THEN 1 ELSE 0 END
    + 1 AS diagnosis_code_count

  FROM {catalog_sql}.{CLAIMS_SCHEMA}.silver_claims_medical c
),

provider_features AS (
  SELECT
    provider_npi,
    total_claims AS provider_total_claims,
    avg_billed_per_claim AS provider_avg_billed,
    e5_visit_pct AS provider_e5_visit_pct,
    denial_rate AS provider_denial_rate,
    unique_members AS provider_unique_members,
    billed_to_allowed_ratio AS provider_billed_to_allowed_ratio,
    fwa_signal_count AS provider_fwa_signal_count,
    fwa_score_avg AS provider_fwa_score_avg,
    composite_risk_score AS provider_composite_risk_score
  FROM {catalog_sql}.{FWA_SCHEMA}.silver_fwa_provider_profiles
),

member_features AS (
  SELECT
    m.member_id,
    e.line_of_business AS member_lob,
    e.risk_score AS member_risk_score,
    COUNT(DISTINCT c2.claim_id) AS member_total_claims,
    COUNT(DISTINCT c2.rendering_provider_npi) AS member_unique_providers,
    COUNT(DISTINCT c2.primary_diagnosis_code) AS member_unique_diagnoses
  FROM {catalog_sql}.{MEMBERS_SCHEMA}.silver_members m
  LEFT JOIN {catalog_sql}.{MEMBERS_SCHEMA}.silver_enrollment e ON m.member_id = e.member_id
  LEFT JOIN {catalog_sql}.{CLAIMS_SCHEMA}.silver_claims_medical c2 ON m.member_id = c2.member_id
  GROUP BY m.member_id, e.line_of_business, e.risk_score
),

labels AS (
  SELECT DISTINCT claim_id, 1 AS is_fraud
  FROM {catalog_sql}.{FWA_SCHEMA}.silver_fwa_signals
  WHERE fraud_score >= 0.5
)

SELECT
  cf.claim_id,
  cf.billed_amount,
  cf.allowed_amount,
  cf.paid_amount,
  cf.billed_to_allowed_ratio,
  cf.member_responsibility,
  cf.copay,
  cf.coinsurance,
  cf.deductible,
  cf.payment_lag_days,
  cf.service_day_of_week,
  cf.diagnosis_code_count,

  COALESCE(pf.provider_total_claims, 0) AS provider_total_claims,
  COALESCE(pf.provider_avg_billed, 0) AS provider_avg_billed,
  COALESCE(pf.provider_e5_visit_pct, 0) AS provider_e5_visit_pct,
  COALESCE(pf.provider_denial_rate, 0) AS provider_denial_rate,
  COALESCE(pf.provider_unique_members, 0) AS provider_unique_members,
  COALESCE(pf.provider_billed_to_allowed_ratio, 0) AS provider_billed_to_allowed_ratio,
  COALESCE(pf.provider_fwa_signal_count, 0) AS provider_fwa_signal_count,
  COALESCE(pf.provider_composite_risk_score, 0) AS provider_composite_risk_score,

  COALESCE(mf.member_total_claims, 0) AS member_total_claims,
  COALESCE(mf.member_unique_providers, 0) AS member_unique_providers,
  COALESCE(mf.member_unique_diagnoses, 0) AS member_unique_diagnoses,
  COALESCE(mf.member_risk_score, 1.0) AS member_risk_score,

  COALESCE(l.is_fraud, 0) AS is_fraud

FROM claim_features cf
LEFT JOIN provider_features pf ON cf.rendering_provider_npi = pf.provider_npi
LEFT JOIN member_features mf ON cf.member_id = mf.member_id
LEFT JOIN labels l ON cf.claim_id = l.claim_id
""")

total = feature_df.count()
fraud_count = feature_df.filter("is_fraud = 1").count()
print(f"Feature table: {total:,} rows, {fraud_count:,} fraud ({fraud_count/total:.2%})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Feature Table for AutoML

# COMMAND ----------

feature_table_name = f"{catalog}.{ANALYTICS_SCHEMA}.fwa_training_features"
feature_df.write.mode("overwrite").saveAsTable(feature_table_name)
print(f"Feature table written to: {feature_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run AutoML Classification
# MAGIC
# MAGIC One API call — AutoML handles algorithm selection, hyperparameter tuning,
# MAGIC cross-validation, class imbalance, and generates a best-model notebook.
# MAGIC
# MAGIC > **Requires non-serverless compute.** AutoML uses `PERSIST TABLE` internally.

# COMMAND ----------

import databricks.automl

# Resolve current user for experiment directory
_user = spark.conf.get("spark.databricks.workspaceUrl", "").split("/")[0]
try:
    _user = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .userName()
        .get()
    )
except Exception:
    pass
EXPERIMENT_DIR = f"/Users/{_user}"

summary = databricks.automl.classify(
    dataset=feature_table_name,
    target_col="is_fraud",
    primary_metric="f1",
    timeout_minutes=15,
    max_trials=20,
    exclude_cols=["claim_id"],
    experiment_dir=EXPERIMENT_DIR,
    pos_label=1,
)

print(f"\nAutoML complete!")
print(f"  Best trial metric (F1): {summary.best_trial.metrics.get('test_f1_score', 'N/A')}")
print(f"  Best trial AUC-ROC:     {summary.best_trial.metrics.get('test_roc_auc', 'N/A')}")
print(f"  Best model type:        {summary.best_trial.model_description}")
print(f"  MLflow Run ID:          {summary.best_trial.mlflow_run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register Best Model in Unity Catalog

# COMMAND ----------

import mlflow
from mlflow import MlflowClient

mlflow.set_registry_uri("databricks-uc")
client = MlflowClient()

best_run_id = summary.best_trial.mlflow_run_id
model_uri = f"runs:/{best_run_id}/model"

registered_model = mlflow.register_model(
    model_uri=model_uri,
    name=MODEL_NAME,
)

print(f"Registered model: {registered_model.name}")
print(f"Version:          {registered_model.version}")

# COMMAND ----------

# Set production alias
client.set_registered_model_alias(
    name=MODEL_NAME,
    alias="production",
    version=registered_model.version,
)

client.update_registered_model(
    name=MODEL_NAME,
    description=(
        "FWA fraud scoring model trained with Databricks AutoML. "
        "Binary classifier predicting fraud probability for medical claims. "
        f"Best model: {summary.best_trial.model_description}. "
        f"F1: {summary.best_trial.metrics.get('test_f1_score', 'N/A')}, "
        f"AUC-ROC: {summary.best_trial.metrics.get('test_roc_auc', 'N/A')}. "
        "Features include claim-level billing patterns, provider risk indicators, "
        "and member behavioral signals."
    ),
)

print(f"Set alias 'production' -> version {registered_model.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 70)
print("FWA FRAUD SCORING MODEL — AUTOML TRAINING COMPLETE")
print("=" * 70)
print(f"  Model Name:    {MODEL_NAME}")
print(f"  Version:       {registered_model.version}")
print(f"  Alias:         production -> v{registered_model.version}")
print(f"  Best Run ID:   {best_run_id}")
print()
print(f"  AutoML Results:")
print(f"    Algorithm:        {summary.best_trial.model_description}")
print(f"    F1 Score:         {summary.best_trial.metrics.get('test_f1_score', 'N/A')}")
print(f"    AUC-ROC:          {summary.best_trial.metrics.get('test_roc_auc', 'N/A')}")
print(f"    Precision:        {summary.best_trial.metrics.get('test_precision_score', 'N/A')}")
print(f"    Recall:           {summary.best_trial.metrics.get('test_recall_score', 'N/A')}")
print(f"    Trials:           {len(summary.trials)}")
print()
print(f"  Feature Table:  {feature_table_name}")
print(f"  Training Data:  {total:,} claims, {fraud_count:,} fraud ({fraud_count/total:.2%})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch Inference — Write Predictions Table
# MAGIC
# MAGIC Same output tables as the XGBoost notebook so either approach
# MAGIC feeds the same downstream agent, app, and gold analytics.

# COMMAND ----------

import mlflow.pyfunc
import numpy as np

prod_model = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}@production")

inference_df = spark.table(feature_table_name)
feature_cols = [c for c in inference_df.columns if c not in ("claim_id", "is_fraud")]
inference_pd = inference_df.select("claim_id", *feature_cols).toPandas()
inference_pd[feature_cols] = inference_pd[feature_cols].astype("float64")

try:
    probabilities = prod_model._model_impl.predict_proba(inference_pd[feature_cols])
    inference_pd["ml_fraud_probability"] = probabilities[:, 1]
    print("Using predict_proba for fraud probabilities")
except (AttributeError, Exception):
    predictions = prod_model.predict(inference_pd[feature_cols])
    inference_pd["ml_fraud_probability"] = predictions
    print("Using raw predict output (may be binary labels)")

inference_pd["ml_risk_tier"] = inference_pd["ml_fraud_probability"].apply(
    lambda p: "High" if p >= 0.7 else ("Medium" if p >= 0.4 else "Low")
)

# --- Predictions table (used by gold MV) ---
from datetime import datetime

predictions_table_name = f"{catalog}.{ANALYTICS_SCHEMA}.fwa_ml_predictions"
predictions_pd = inference_pd[["claim_id", "ml_fraud_probability", "ml_risk_tier"]].copy()
predictions_pd["model_version"] = str(registered_model.version)
predictions_pd["scored_at"] = datetime.utcnow().isoformat()
predictions_pd["ml_fraud_probability"] = predictions_pd["ml_fraud_probability"].astype(float)

predictions_spark_df = spark.createDataFrame(predictions_pd)
predictions_spark_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(predictions_table_name)

pred_count = spark.table(predictions_table_name).count()
high_risk_pred = spark.sql(f"SELECT COUNT(*) FROM {predictions_table_name} WHERE ml_risk_tier = 'High'").collect()[0][0]
med_risk_pred = spark.sql(f"SELECT COUNT(*) FROM {predictions_table_name} WHERE ml_risk_tier = 'Medium'").collect()[0][0]
print(f"\nPredictions table written: {predictions_table_name}")
print(f"  Total scored claims: {pred_count:,}")
print(f"  High risk (>=0.7):   {high_risk_pred:,}")
print(f"  Medium risk (0.4-0.7): {med_risk_pred:,}")

# --- Full inference table with claim context ---
inference_result_df = spark.createDataFrame(inference_pd[["claim_id", "ml_fraud_probability", "ml_risk_tier"]])

inference_table_name = f"{catalog}.{ANALYTICS_SCHEMA}.fwa_model_inference"
inference_with_context = inference_result_df.join(
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
print(f"\nInference table written: {inference_table_name}")
print(f"  Total scored claims: {inf_count:,}")
print(f"  High risk (>=0.7):   {high_risk:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy Model Serving Endpoint

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput,
    ServedEntityInput,
    AutoCaptureConfigInput,
)

w = WorkspaceClient()

endpoint_name = "fwa-fraud-scorer"
inference_log_table = f"{catalog}.{ANALYTICS_SCHEMA}.fwa_model_serving_logs"

try:
    w.serving_endpoints.create(
        name=endpoint_name,
        config=EndpointCoreConfigInput(
            served_entities=[
                ServedEntityInput(
                    entity_name=MODEL_NAME,
                    entity_version=registered_model.version,
                    workload_size="Small",
                    scale_to_zero_enabled=True,
                ),
            ],
            auto_capture_config=AutoCaptureConfigInput(
                catalog_name=catalog,
                schema_name=ANALYTICS_SCHEMA,
                table_name_prefix="fwa_model_serving",
                enabled=True,
            ),
        ),
    )
    print(f"Serving endpoint '{endpoint_name}' created with inference table logging.")
    print(f"  Inference logs will be written to: {inference_log_table}")
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"Serving endpoint '{endpoint_name}' already exists — updating...")
        w.serving_endpoints.update_config(
            name=endpoint_name,
            served_entities=[
                ServedEntityInput(
                    entity_name=MODEL_NAME,
                    entity_version=registered_model.version,
                    workload_size="Small",
                    scale_to_zero_enabled=True,
                ),
            ],
        )
        print(f"  Endpoint updated to model version {registered_model.version}")
    else:
        print(f"  Serving endpoint creation skipped: {e}")
        print("  Batch inference table is available as fallback.")
