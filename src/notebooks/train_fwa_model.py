# Databricks notebook source
# MAGIC %md
# MAGIC # Red Bricks Insurance — FWA Fraud Scoring Model
# MAGIC
# MAGIC Trains a claim-level fraud scoring model using **XGBoost** with MLflow experiment tracking
# MAGIC and **pyfunc model** for probability-based serving.
# MAGIC
# MAGIC ### MLOps Lifecycle
# MAGIC 1. **Feature engineering** — claim-level + provider-level + member-level features
# MAGIC 2. **Feature Store registration** — `fe.create_table()` with primary key for lineage tracking
# MAGIC 3. **XGBoost training** — stratified cross-validation, hyperparameter grid search
# MAGIC 4. **SHAP** — feature importance visualization logged to MLflow
# MAGIC 5. **`mlflow.pyfunc.log_model()`** — registers pyfunc wrapper returning probabilities
# MAGIC 6. **Batch scoring** — `predict_proba()` writes to analytics tables for downstream consumers
# MAGIC 7. **Model alias** — `@champion` alias in Unity Catalog
# MAGIC
# MAGIC ### Post-Training Manual Steps
# MAGIC After this notebook completes (as part of the automated job), follow these steps:
# MAGIC 1. **Create serving endpoint** in the Serving UI:
# MAGIC    - Entity: `{catalog}.analytics.fwa_scoring_model@champion`
# MAGIC    - Workload size: Small, scale-to-zero enabled
# MAGIC    - **Enable inference tables** (required for monitoring pipeline)
# MAGIC 2. **Run `fwa_batch_scoring.py`** — sends 500 claims to the endpoint
# MAGIC 3. **Wait ~1hr** for inference table materialization
# MAGIC 4. **Trigger `fwa_monitoring_pipeline`** DLT pipeline (parses payload → monitoring input)
# MAGIC 5. **Run `fwa_backfill_monitoring.py`** — seeds 7 days of synthetic prediction data
# MAGIC 6. **Run `fwa_model_monitoring.py`** — creates Lakehouse Monitor + governance audit

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance_catalog", "Catalog")

catalog = dbutils.widgets.get("catalog")
catalog_sql = f"`{catalog}`"  # SQL-safe quoting (handles hyphens in catalog names)

FWA_SCHEMA = "fwa"
CLAIMS_SCHEMA = "claims"
MEMBERS_SCHEMA = "members"
ANALYTICS_SCHEMA = "analytics"
MODEL_NAME = f"{catalog}.{ANALYTICS_SCHEMA}.fwa_scoring_model"

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
    MAX(e.risk_score) AS member_risk_score,
    COUNT(DISTINCT c2.claim_id) AS member_total_claims,
    COUNT(DISTINCT c2.rendering_provider_npi) AS member_unique_providers,
    COUNT(DISTINCT c2.primary_diagnosis_code) AS member_unique_diagnoses
  FROM {catalog_sql}.{MEMBERS_SCHEMA}.silver_members m
  LEFT JOIN {catalog_sql}.{MEMBERS_SCHEMA}.silver_enrollment e ON m.member_id = e.member_id
  LEFT JOIN {catalog_sql}.{CLAIMS_SCHEMA}.silver_claims_medical c2 ON m.member_id = c2.member_id
  GROUP BY m.member_id
),

-- Labels: 1 if claim has a fraud signal with score >= 0.5
labels AS (
  SELECT DISTINCT claim_id, 1 AS is_fraud
  FROM {catalog_sql}.{FWA_SCHEMA}.silver_fwa_signals
  WHERE fraud_score >= 0.5
)

SELECT
  cf.claim_id,
  -- Claim-level features (all DOUBLE to match XGBoost/MLflow model signature)
  CAST(cf.billed_amount AS DOUBLE) AS billed_amount,
  CAST(cf.allowed_amount AS DOUBLE) AS allowed_amount,
  CAST(cf.paid_amount AS DOUBLE) AS paid_amount,
  CAST(cf.billed_to_allowed_ratio AS DOUBLE) AS billed_to_allowed_ratio,
  CAST(cf.member_responsibility AS DOUBLE) AS member_responsibility,
  CAST(cf.copay AS DOUBLE) AS copay,
  CAST(cf.coinsurance AS DOUBLE) AS coinsurance,
  CAST(cf.deductible AS DOUBLE) AS deductible,
  CAST(cf.payment_lag_days AS DOUBLE) AS payment_lag_days,
  CAST(cf.service_day_of_week AS DOUBLE) AS service_day_of_week,
  CAST(cf.diagnosis_code_count AS DOUBLE) AS diagnosis_code_count,

  -- Provider-level features
  CAST(COALESCE(pf.provider_total_claims, 0) AS DOUBLE) AS provider_total_claims,
  CAST(COALESCE(pf.provider_avg_billed, 0) AS DOUBLE) AS provider_avg_billed,
  CAST(COALESCE(pf.provider_e5_visit_pct, 0) AS DOUBLE) AS provider_e5_visit_pct,
  CAST(COALESCE(pf.provider_denial_rate, 0) AS DOUBLE) AS provider_denial_rate,
  CAST(COALESCE(pf.provider_unique_members, 0) AS DOUBLE) AS provider_unique_members,
  CAST(COALESCE(pf.provider_billed_to_allowed_ratio, 0) AS DOUBLE) AS provider_billed_to_allowed_ratio,
  CAST(COALESCE(pf.provider_fwa_signal_count, 0) AS DOUBLE) AS provider_fwa_signal_count,
  CAST(COALESCE(pf.provider_composite_risk_score, 0) AS DOUBLE) AS provider_composite_risk_score,

  -- Member-level features
  CAST(COALESCE(mf.member_total_claims, 0) AS DOUBLE) AS member_total_claims,
  CAST(COALESCE(mf.member_unique_providers, 0) AS DOUBLE) AS member_unique_providers,
  CAST(COALESCE(mf.member_unique_diagnoses, 0) AS DOUBLE) AS member_unique_diagnoses,
  CAST(COALESCE(mf.member_risk_score, 1.0) AS DOUBLE) AS member_risk_score,

  -- Label
  CAST(COALESCE(l.is_fraud, 0) AS DOUBLE) AS is_fraud

FROM claim_features cf
LEFT JOIN provider_features pf ON cf.rendering_provider_npi = pf.provider_npi
LEFT JOIN member_features mf ON cf.member_id = mf.member_id
LEFT JOIN labels l ON cf.claim_id = l.claim_id
""")

# Dedup to one row per claim_id (service-line fan-out from silver_claims_medical)
feature_df = feature_df.dropDuplicates(["claim_id"])

# Count stats before writing
total = feature_df.count()
fraud_count = feature_df.filter("is_fraud = 1").count()
print(f"Feature table: {total:,} rows, {fraud_count:,} fraud ({fraud_count/total:.2%})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register Feature Table in Unity Catalog
# MAGIC
# MAGIC Use the **Feature Engineering client** to register the feature table in Unity Catalog.
# MAGIC This gives us:
# MAGIC - **Feature lineage** — UC tracks which models consume which features
# MAGIC - **Feature discovery** — table is tagged and searchable as a feature table in UC

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()

feature_table_name = f"{catalog}.{ANALYTICS_SCHEMA}.fwa_training_features"

# Drop and recreate to ensure clean PK constraint (idempotent for demo reruns)
spark.sql(f"DROP TABLE IF EXISTS {feature_table_name}")

fe.create_table(
    name=feature_table_name,
    primary_keys=["claim_id"],
    df=feature_df,
    description=(
        "FWA fraud scoring features: claim-level billing patterns, "
        "provider risk indicators, and member behavioral signals. "
        "Primary key: claim_id. Label: is_fraud."
    ),
)
print(f"Feature table created: {feature_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train XGBoost Classifier with MLflow
# MAGIC
# MAGIC Manual training with:
# MAGIC - Stratified 3-fold cross-validation
# MAGIC - Hyperparameter grid search (2 combos for job speed)
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
mlflow.set_registry_uri("databricks-uc")
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

        # 3-fold stratified cross-validation
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

with mlflow.start_run(run_name="xgb_final_champion") as final_run:
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

    # SHAP feature importance plot
    try:
        import shap
        explainer = shap.TreeExplainer(final_model)
        shap_sample = pd.DataFrame(X[:500], columns=feature_cols)
        shap_values = explainer.shap_values(shap_sample)
        fig = shap.summary_plot(shap_values, shap_sample, show=False)
        import matplotlib.pyplot as plt
        plt.tight_layout()
        plt.savefig("/tmp/shap_summary.png", dpi=150, bbox_inches="tight")
        mlflow.log_artifact("/tmp/shap_summary.png")
        plt.close()
        print("SHAP summary plot logged to MLflow")
    except Exception as e:
        print(f"SHAP plot skipped: {e}")

    # Log model with a pyfunc wrapper that returns probabilities (not class labels)
    input_example = pd.DataFrame([X[0]], columns=feature_cols)

    class FraudProbaModel(mlflow.pyfunc.PythonModel):
        def __init__(self, xgb_model):
            self.xgb_model = xgb_model

        def predict(self, context, model_input, params=None):
            if isinstance(model_input, pd.DataFrame):
                numeric_cols = model_input.select_dtypes(include="number").columns
                proba = self.xgb_model.predict_proba(model_input[numeric_cols].values)[:, 1]
            else:
                proba = self.xgb_model.predict_proba(model_input)[:, 1]
            return proba.tolist()

    mlflow.pyfunc.log_model(
        artifact_path="model",
        python_model=FraudProbaModel(final_model),
        input_example=input_example,
        registered_model_name=MODEL_NAME,
        pip_requirements=["xgboost==3.1.1", "pandas>=2.0,<3.0", "numpy", "scikit-learn"],
    )

    best_final_run_id = final_run.info.run_id
    print(f"\nFinal model logged: run_id={best_final_run_id}")
    for k, v in train_metrics.items():
        print(f"  {k}: {v:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register Best Model — `@champion` Alias

# COMMAND ----------

from mlflow import MlflowClient

client = MlflowClient()

# Get the latest version that was just registered via log_model
model_versions = client.search_model_versions(f"name='{MODEL_NAME}'")
latest_version = max(model_versions, key=lambda v: int(v.version))

# Set champion alias
client.set_registered_model_alias(
    name=MODEL_NAME,
    alias="champion",
    version=latest_version.version,
)

# Add model description
client.update_registered_model(
    name=MODEL_NAME,
    description=(
        "FWA fraud scoring model (XGBoost pyfunc). "
        "Binary classifier returning fraud probability for medical claims. "
        f"Best CV AUC-ROC: {best_auc:.4f}. "
        f"Best params: {best_params}. "
        "Features include claim-level billing patterns, provider risk indicators, "
        "and member behavioral signals."
    ),
)

print(f"Registered model: {MODEL_NAME}")
print(f"Version:          {latest_version.version}")
print(f"Alias:            @champion -> v{latest_version.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch Inference — Write Predictions to Analytics Tables
# MAGIC
# MAGIC Score all medical claims with the trained model and write predictions
# MAGIC to Delta tables. These serve as downstream consumers:
# MAGIC - `analytics.fwa_ml_predictions` → consumed by `gold_fwa_model_scores` MV
# MAGIC - `analytics.fwa_model_inference` → consumed by FWA Investigation Agent + dashboard

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

# Score using the in-memory final_model directly (no fe.score_batch needed)
features_pdf = feature_df.select(["claim_id"] + feature_cols).toPandas()
probabilities = final_model.predict_proba(features_pdf[feature_cols].values)[:, 1]

# Build scored pandas DataFrame
scored_pdf = pd.DataFrame({
    "claim_id": features_pdf["claim_id"],
    "ml_fraud_probability": probabilities,
})
scored_pdf["ml_risk_tier"] = scored_pdf["ml_fraud_probability"].apply(
    lambda p: "High" if p >= 0.7 else ("Medium" if p >= 0.4 else "Low")
)
scored_pdf["model_version"] = str(latest_version.version)
scored_pdf["scored_at"] = datetime.utcnow().isoformat()

scored_sdf = spark.createDataFrame(scored_pdf)

# --- Write predictions table (standalone, used by gold MV) ---
predictions_table_name = f"{catalog_sql}.{ANALYTICS_SCHEMA}.fwa_ml_predictions"

predictions_df = scored_sdf.select(
    "claim_id", "ml_fraud_probability", "ml_risk_tier", "model_version", "scored_at"
)
predictions_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(predictions_table_name)

pred_count = spark.table(predictions_table_name).count()
high_risk_pred = spark.sql(f"SELECT COUNT(*) FROM {predictions_table_name} WHERE ml_risk_tier = 'High'").collect()[0][0]
med_risk_pred = spark.sql(f"SELECT COUNT(*) FROM {predictions_table_name} WHERE ml_risk_tier = 'Medium'").collect()[0][0]
print(f"Predictions table written: {predictions_table_name}")
print(f"  Total scored claims: {pred_count:,}")
print(f"  High risk (>=0.7):   {high_risk_pred:,}")
print(f"  Medium risk (0.4-0.7): {med_risk_pred:,}")
print(f"  Model version:       {latest_version.version}")

# --- Also write the full inference table with claim context ---
inference_table_name = f"{catalog_sql}.{ANALYTICS_SCHEMA}.fwa_model_inference"
inference_result_sdf = scored_sdf.select("claim_id", "ml_fraud_probability", "ml_risk_tier")

inference_with_context = inference_result_sdf.join(
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
# MAGIC ## Summary

# COMMAND ----------

print("=" * 70)
print("FWA FRAUD SCORING MODEL — TRAINING COMPLETE")
print("=" * 70)
print(f"  Model Name:    {MODEL_NAME}")
print(f"  Version:       {latest_version.version}")
print(f"  Alias:         @champion -> v{latest_version.version}")
print(f"  Run ID:        {best_final_run_id}")
print()
print(f"  Training Results:")
print(f"    Algorithm:        XGBoost (pyfunc wrapper)")
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
print("  Batch Predictions:")
print(f"    {predictions_table_name}")
print(f"    {inference_table_name}")
print()
print("  Next steps (MANUAL):")
print("    1. Create serving endpoint in UI:")
print(f"       Entity: {MODEL_NAME}@champion")
print("       Size: Small, scale-to-zero, enable inference tables")
print("    2. Run fwa_batch_scoring.py (500 claims → endpoint)")
print("    3. Wait ~1hr for inference table materialization")
print("    4. Trigger fwa_monitoring_pipeline DLT pipeline")
print("    5. Run fwa_backfill_monitoring.py (seed 7 days)")
print("    6. Run fwa_model_monitoring.py (Lakehouse Monitor + governance)")
