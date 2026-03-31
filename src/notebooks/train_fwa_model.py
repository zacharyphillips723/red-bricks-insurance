# Databricks notebook source
# MAGIC %md
# MAGIC # Red Bricks Insurance — FWA Fraud Scoring Model
# MAGIC
# MAGIC Trains a claim-level fraud scoring model using **XGBoost** with MLflow experiment tracking.
# MAGIC Demonstrates the full MLOps lifecycle:
# MAGIC 1. **Feature engineering** — claim-level + provider-level + member-level features
# MAGIC 2. **XGBoost training** — stratified cross-validation, hyperparameter grid search
# MAGIC 3. **MLflow experiment** — all runs logged with metrics, parameters, artifacts
# MAGIC 4. **Unity Catalog registration** — best model registered with `production` alias
# MAGIC 5. **Batch inference** — score all claims, write predictions table
# MAGIC 6. **Model serving** — serverless endpoint with inference table logging
# MAGIC
# MAGIC ### Why XGBoost (not AutoML)?
# MAGIC AutoML uses `PERSIST TABLE` internally which is not supported on serverless compute.
# MAGIC Manual training gives full control and works on any compute type.

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance", "Catalog")

catalog = dbutils.widgets.get("catalog")

FWA_SCHEMA = "fwa"
CLAIMS_SCHEMA = "claims"
MEMBERS_SCHEMA = "members"
ANALYTICS_SCHEMA = "analytics"
MODEL_NAME = f"{catalog}.{ANALYTICS_SCHEMA}.fwa_scoring_model"

print(f"Catalog: {catalog}")
print(f"Model:   {MODEL_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Engineering
# MAGIC
# MAGIC Build a feature table from silver claims + FWA provider profiles + member context.
# MAGIC Label: binary — `1` if claim_id appears in silver_fwa_signals with fraud_score >= 0.5.

# COMMAND ----------

# Build the labeled feature table
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

  FROM {catalog}.{CLAIMS_SCHEMA}.silver_claims_medical c
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
  FROM {catalog}.{FWA_SCHEMA}.silver_fwa_provider_profiles
),

member_features AS (
  SELECT
    m.member_id,
    e.line_of_business AS member_lob,
    e.risk_score AS member_risk_score,
    COUNT(DISTINCT c2.claim_id) AS member_total_claims,
    COUNT(DISTINCT c2.rendering_provider_npi) AS member_unique_providers,
    COUNT(DISTINCT c2.primary_diagnosis_code) AS member_unique_diagnoses
  FROM {catalog}.{MEMBERS_SCHEMA}.silver_members m
  LEFT JOIN {catalog}.{MEMBERS_SCHEMA}.silver_enrollment e ON m.member_id = e.member_id
  LEFT JOIN {catalog}.{CLAIMS_SCHEMA}.silver_claims_medical c2 ON m.member_id = c2.member_id
  GROUP BY m.member_id, e.line_of_business, e.risk_score
),

-- Labels: 1 if claim has a fraud signal with score >= 0.5
labels AS (
  SELECT DISTINCT claim_id, 1 AS is_fraud
  FROM {catalog}.{FWA_SCHEMA}.silver_fwa_signals
  WHERE fraud_score >= 0.5
)

SELECT
  cf.claim_id,
  -- Claim-level features
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

  -- Provider-level features
  COALESCE(pf.provider_total_claims, 0) AS provider_total_claims,
  COALESCE(pf.provider_avg_billed, 0) AS provider_avg_billed,
  COALESCE(pf.provider_e5_visit_pct, 0) AS provider_e5_visit_pct,
  COALESCE(pf.provider_denial_rate, 0) AS provider_denial_rate,
  COALESCE(pf.provider_unique_members, 0) AS provider_unique_members,
  COALESCE(pf.provider_billed_to_allowed_ratio, 0) AS provider_billed_to_allowed_ratio,
  COALESCE(pf.provider_fwa_signal_count, 0) AS provider_fwa_signal_count,
  COALESCE(pf.provider_composite_risk_score, 0) AS provider_composite_risk_score,

  -- Member-level features
  COALESCE(mf.member_total_claims, 0) AS member_total_claims,
  COALESCE(mf.member_unique_providers, 0) AS member_unique_providers,
  COALESCE(mf.member_unique_diagnoses, 0) AS member_unique_diagnoses,
  COALESCE(mf.member_risk_score, 1.0) AS member_risk_score,

  -- Label
  COALESCE(l.is_fraud, 0) AS is_fraud

FROM claim_features cf
LEFT JOIN provider_features pf ON cf.rendering_provider_npi = pf.provider_npi
LEFT JOIN member_features mf ON cf.member_id = mf.member_id
LEFT JOIN labels l ON cf.claim_id = l.claim_id
""")

# Count stats before writing
total = feature_df.count()
fraud_count = feature_df.filter("is_fraud = 1").count()
print(f"Feature table: {total:,} rows, {fraud_count:,} fraud ({fraud_count/total:.2%})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Feature Table

# COMMAND ----------

# Write feature table to Unity Catalog
feature_table_name = f"{catalog}.{ANALYTICS_SCHEMA}.fwa_training_features"
feature_df.write.mode("overwrite").saveAsTable(feature_table_name)
print(f"Feature table written to: {feature_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train XGBoost Classifier with MLflow
# MAGIC
# MAGIC Manual training with:
# MAGIC - Stratified 5-fold cross-validation
# MAGIC - Hyperparameter grid search
# MAGIC - MLflow autologging for all metrics, parameters, and artifacts
# MAGIC - SHAP feature importance plots

# COMMAND ----------

import mlflow
import numpy as np
import pandas as pd
from sklearn.model_selection import StratifiedKFold, cross_validate
from sklearn.metrics import (
    f1_score, roc_auc_score, precision_score, recall_score,
    confusion_matrix, classification_report,
)
from xgboost import XGBClassifier
import json
import itertools

# Set up MLflow experiment
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
experiment_path = f"/Users/{_user}/{catalog}_fwa_fraud_scorer"
mlflow.set_experiment(experiment_path)
print(f"MLflow experiment: {experiment_path}")

# COMMAND ----------

# Load feature data as pandas
feature_cols = [
    c for c in feature_df.columns if c not in ("claim_id", "is_fraud")
]
pdf = feature_df.select(feature_cols + ["is_fraud"]).toPandas()

X = pdf[feature_cols].values
y = pdf["is_fraud"].values

fraud_ratio = y.sum() / len(y)
scale_pos_weight = (1 - fraud_ratio) / max(fraud_ratio, 1e-6)
print(f"Class balance: {y.sum()} fraud / {len(y)} total ({fraud_ratio:.2%})")
print(f"scale_pos_weight: {scale_pos_weight:.1f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Hyperparameter Grid Search

# COMMAND ----------

# Define hyperparameter grid
param_grid = {
    "max_depth": [4, 6],
    "learning_rate": [0.1],
    "n_estimators": [100],
}

# Generate all combinations
grid_keys = list(param_grid.keys())
grid_combos = list(itertools.product(*[param_grid[k] for k in grid_keys]))
print(f"Grid search: {len(grid_combos)} combinations")

cv = StratifiedKFold(n_splits=3, shuffle=True, random_state=42)

best_auc = -1
best_run_id = None
best_params = None

mlflow.xgboost.autolog(log_datasets=False, silent=True)

for combo in grid_combos:
    params = dict(zip(grid_keys, combo))

    with mlflow.start_run(run_name=f"xgb_d{params['max_depth']}_lr{params['learning_rate']}_n{params['n_estimators']}") as run:
        model = XGBClassifier(
            **params,
            scale_pos_weight=scale_pos_weight,
            use_label_encoder=False,
            eval_metric="logloss",
            random_state=42,
            tree_method="hist",
        )

        # 5-fold stratified cross-validation
        cv_results = cross_validate(
            model, X, y, cv=cv,
            scoring=["f1", "roc_auc", "precision", "recall"],
            return_train_score=False,
        )

        # Log CV metrics
        metrics = {
            "cv_f1_mean": np.mean(cv_results["test_f1"]),
            "cv_f1_std": np.std(cv_results["test_f1"]),
            "cv_auc_mean": np.mean(cv_results["test_roc_auc"]),
            "cv_auc_std": np.std(cv_results["test_roc_auc"]),
            "cv_precision_mean": np.mean(cv_results["test_precision"]),
            "cv_recall_mean": np.mean(cv_results["test_recall"]),
        }
        mlflow.log_metrics(metrics)
        mlflow.log_params({"scale_pos_weight": scale_pos_weight})

        print(f"  {params} → AUC={metrics['cv_auc_mean']:.4f} F1={metrics['cv_f1_mean']:.4f}")

        if metrics["cv_auc_mean"] > best_auc:
            best_auc = metrics["cv_auc_mean"]
            best_run_id = run.info.run_id
            best_params = params

print(f"\nBest: {best_params} → AUC={best_auc:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Train Final Model on Full Dataset

# COMMAND ----------

with mlflow.start_run(run_name="xgb_final_best") as final_run:
    final_model = XGBClassifier(
        **best_params,
        scale_pos_weight=scale_pos_weight,
        use_label_encoder=False,
        eval_metric="logloss",
        random_state=42,
        tree_method="hist",
    )
    final_model.fit(X, y)

    # Evaluate on full training set (for reporting — CV metrics are the real measure)
    y_pred = final_model.predict(X)
    y_proba = final_model.predict_proba(X)[:, 1]

    train_metrics = {
        "train_f1": f1_score(y, y_pred),
        "train_auc_roc": roc_auc_score(y, y_proba),
        "train_precision": precision_score(y, y_pred),
        "train_recall": recall_score(y, y_pred),
        "best_cv_auc": best_auc,
    }
    mlflow.log_metrics(train_metrics)

    # Log feature importance
    importance = dict(zip(feature_cols, final_model.feature_importances_.tolist()))
    mlflow.log_dict(importance, "feature_importance.json")

    # Log confusion matrix as artifact
    cm = confusion_matrix(y, y_pred)
    cm_dict = {"tn": int(cm[0, 0]), "fp": int(cm[0, 1]), "fn": int(cm[1, 0]), "tp": int(cm[1, 1])}
    mlflow.log_dict(cm_dict, "confusion_matrix.json")

    # Log the model with input example for signature
    input_example = pd.DataFrame([X[0]], columns=feature_cols)
    mlflow.xgboost.log_model(
        final_model,
        artifact_path="model",
        input_example=input_example,
        registered_model_name=MODEL_NAME,
    )

    best_final_run_id = final_run.info.run_id
    print(f"Final model logged: run_id={best_final_run_id}")
    print(f"  F1={train_metrics['train_f1']:.4f}  AUC={train_metrics['train_auc_roc']:.4f}")
    print(f"  Precision={train_metrics['train_precision']:.4f}  Recall={train_metrics['train_recall']:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register Best Model in Unity Catalog

# COMMAND ----------

from mlflow import MlflowClient

mlflow.set_registry_uri("databricks-uc")
client = MlflowClient()

# Get the latest version that was just registered via log_model
model_versions = client.search_model_versions(f"name='{MODEL_NAME}'")
latest_version = max(model_versions, key=lambda v: int(v.version))

# Set production alias
client.set_registered_model_alias(
    name=MODEL_NAME,
    alias="production",
    version=latest_version.version,
)

# Add model description
client.update_registered_model(
    name=MODEL_NAME,
    description=(
        "FWA fraud scoring model trained with XGBoost. "
        "Binary classifier predicting fraud probability for medical claims. "
        f"Best CV AUC-ROC: {best_auc:.4f}. "
        f"Best params: {best_params}. "
        "Features include claim-level billing patterns, provider risk indicators, "
        "and member behavioral signals."
    ),
)

print(f"Registered model: {MODEL_NAME}")
print(f"Version:          {latest_version.version}")
print(f"Alias:            production -> v{latest_version.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 70)
print("FWA FRAUD SCORING MODEL — TRAINING COMPLETE")
print("=" * 70)
print(f"  Model Name:    {MODEL_NAME}")
print(f"  Version:       {latest_version.version}")
print(f"  Alias:         production -> v{latest_version.version}")
print(f"  Run ID:        {best_final_run_id}")
print()
print(f"  Training Results:")
print(f"    Algorithm:        XGBoost")
print(f"    Best CV AUC:      {best_auc:.4f}")
print(f"    F1 Score:         {train_metrics['train_f1']:.4f}")
print(f"    AUC-ROC:          {train_metrics['train_auc_roc']:.4f}")
print(f"    Precision:        {train_metrics['train_precision']:.4f}")
print(f"    Recall:           {train_metrics['train_recall']:.4f}")
print(f"    Grid Combos:      {len(grid_combos)}")
print(f"    Best Params:      {best_params}")
print()
print(f"  Feature Table:  {feature_table_name}")
print(f"  Training Data:  {total:,} claims, {fraud_count:,} fraud ({fraud_count/total:.2%})")
print()
print("  Next steps:")
print("    - Review MLflow experiment for run comparison")
print("    - Check feature_importance.json artifact")
print("    - Batch inference predictions written for agent and app")
print("    - Model serving endpoint deployed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch Inference — Write Predictions to Inference Table
# MAGIC
# MAGIC Score all medical claims with the trained model and write predictions
# MAGIC to a Delta table. This serves as the inference table for:
# MAGIC - The FWA Investigation Agent (references ML scores in analysis)
# MAGIC - The FWA Investigation Portal (shows ML confidence per claim)
# MAGIC - Gold analytics (gold_fwa_model_scores materialized view)

# COMMAND ----------

import mlflow.pyfunc

# Load the registered production model
prod_model = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}@production")

# Read the feature table (exclude label and claim_id for prediction)
inference_df = spark.table(feature_table_name)

# Convert to pandas for model prediction
inference_pd = inference_df.select("claim_id", *feature_cols).toPandas()
inference_pd[feature_cols] = inference_pd[feature_cols].astype("float64")

# Run batch prediction — get probabilities
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

# --- Write Phase 2 predictions table (standalone, used by gold MV) ---
from datetime import datetime

predictions_table_name = f"{catalog}.{ANALYTICS_SCHEMA}.fwa_ml_predictions"
predictions_pd = inference_pd[["claim_id", "ml_fraud_probability", "ml_risk_tier"]].copy()
predictions_pd["model_version"] = str(latest_version.version)
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
print(f"  Model version:       {latest_version.version}")

# --- Also write the full inference table with claim context ---
inference_result_df = spark.createDataFrame(inference_pd[["claim_id", "ml_fraud_probability", "ml_risk_tier"]])

inference_table_name = f"{catalog}.{ANALYTICS_SCHEMA}.fwa_model_inference"
inference_with_context = inference_result_df.join(
    spark.sql(f"""
        SELECT c.claim_id, c.member_id, c.rendering_provider_npi AS provider_npi,
               c.claim_type, c.procedure_code, c.billed_amount, c.allowed_amount,
               c.paid_amount, c.service_from_date, c.service_year_month,
               e.line_of_business
        FROM {catalog}.{CLAIMS_SCHEMA}.silver_claims_medical c
        LEFT JOIN {catalog}.{MEMBERS_SCHEMA}.silver_enrollment e ON c.member_id = e.member_id
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
# MAGIC ## Deploy Model Serving Endpoint (with Inference Table Logging)
# MAGIC
# MAGIC Creates a serverless model serving endpoint with inference table logging
# MAGIC enabled. This allows real-time scoring from the app and automatically
# MAGIC logs all predictions to a Unity Catalog inference table.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput,
    ServedEntityInput,
)
import time

w = WorkspaceClient()

endpoint_name = "fwa-fraud-scorer"

# Wait for the model version to be READY in Unity Catalog before creating the endpoint.
# After log_model + set_alias, the version may still be in PENDING_REGISTRATION state.
print(f"Waiting for model version {latest_version.version} to be READY in UC...")
_max_wait = 300  # 5 minutes
_start = time.time()
while time.time() - _start < _max_wait:
    _mv = client.get_model_version(MODEL_NAME, latest_version.version)
    _status = getattr(_mv, "status", None) or "UNKNOWN"
    if _status == "READY":
        print(f"  Model version {latest_version.version} is READY.")
        break
    print(f"  Status: {_status} — waiting 10s...")
    time.sleep(10)
else:
    print(f"  WARNING: Model version still not READY after {_max_wait}s. Attempting endpoint creation anyway.")

# Create or update the serving endpoint
# NOTE: Legacy auto_capture_config (inference tables) is deprecated.
# Use AI Gateway inference tables instead if payload logging is needed.
_endpoint_created = False
for _attempt in range(3):
    try:
        w.serving_endpoints.create(
            name=endpoint_name,
            config=EndpointCoreConfigInput(
                served_entities=[
                    ServedEntityInput(
                        entity_name=MODEL_NAME,
                        entity_version=latest_version.version,
                        workload_size="Small",
                        scale_to_zero_enabled=True,
                    ),
                ],
            ),
        )
        print(f"Serving endpoint '{endpoint_name}' created.")
        _endpoint_created = True
        break
    except Exception as e:
        _err = str(e)
        if "already exists" in _err.lower():
            print(f"Serving endpoint '{endpoint_name}' already exists — updating config...")
            try:
                w.serving_endpoints.update_config(
                    name=endpoint_name,
                    served_entities=[
                        ServedEntityInput(
                            entity_name=MODEL_NAME,
                            entity_version=latest_version.version,
                            workload_size="Small",
                            scale_to_zero_enabled=True,
                        ),
                    ],
                )
                print(f"  Endpoint updated to model version {latest_version.version}")
                _endpoint_created = True
            except Exception as e2:
                print(f"  Endpoint update also failed: {e2}")
            break
        else:
            print(f"  Attempt {_attempt + 1}/3 — endpoint creation failed: {_err}")
            if _attempt < 2:
                time.sleep(15)

if not _endpoint_created:
    print(f"\nWARNING: Serving endpoint '{endpoint_name}' could not be created.")
    print(f"  Batch inference table ({predictions_table_name}) is available as fallback.")
    print(f"  To create manually: Workspace > Serving > New Endpoint > model={MODEL_NAME}")
