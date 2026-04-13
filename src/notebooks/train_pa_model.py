# Databricks notebook source
# MAGIC %md
# MAGIC # Red Bricks Insurance — PA Auto-Adjudication Model
# MAGIC
# MAGIC Trains a **prior authorization determination model** using XGBoost with MLflow tracking.
# MAGIC Predicts whether a PA request will be approved, denied, or pended based on:
# MAGIC - Service type, procedure code, urgency
# MAGIC - Estimated cost, clinical summary length
# MAGIC - Provider historical approval rates
# MAGIC - Policy characteristics
# MAGIC
# MAGIC ### Pipeline
# MAGIC 1. Feature engineering from silver PA tables
# MAGIC 2. XGBoost multiclass training (approved / denied / pended)
# MAGIC 3. MLflow experiment logging + UC model registration
# MAGIC 4. Batch inference → predictions table

# COMMAND ----------

# MAGIC %pip install shap --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance", "Catalog")

catalog = dbutils.widgets.get("catalog")
catalog_sql = f"`{catalog}`"
PA_SCHEMA = "prior_auth"
MODEL_NAME = f"{catalog}.{PA_SCHEMA}.pa_adjudication_model"

# SQL-safe three-part name for saveAsTable / spark.table (needs backticks for hyphenated catalog)
def _tbl(schema: str, table: str) -> str:
    return f"`{catalog}`.{schema}.{table}"

print(f"Catalog: {catalog}")
print(f"Model:   {MODEL_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Engineering

# COMMAND ----------

feature_df = spark.sql(f"""
WITH provider_stats AS (
  SELECT
    requesting_provider_npi,
    COUNT(*) AS provider_total_requests,
    SUM(CASE WHEN determination = 'approved' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS provider_approval_rate,
    SUM(CASE WHEN determination = 'denied' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS provider_denial_rate,
    AVG(turnaround_hours) AS provider_avg_turnaround,
    AVG(estimated_cost) AS provider_avg_cost
  FROM {catalog_sql}.{PA_SCHEMA}.silver_pa_requests
  GROUP BY requesting_provider_npi
),

policy_stats AS (
  SELECT
    policy_id,
    COUNT(*) AS policy_total_requests,
    SUM(CASE WHEN determination = 'approved' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS policy_approval_rate
  FROM {catalog_sql}.{PA_SCHEMA}.silver_pa_requests
  GROUP BY policy_id
)

SELECT
  r.auth_request_id,
  r.member_id,
  r.requesting_provider_npi,

  -- Categorical features (will be encoded)
  r.service_type,
  r.urgency,
  r.line_of_business,
  r.determination_tier,

  -- Numeric features
  CAST(r.estimated_cost AS DOUBLE) AS estimated_cost,
  CAST(r.turnaround_hours AS DOUBLE) AS turnaround_hours,
  CAST(LENGTH(COALESCE(r.clinical_summary, '')) AS DOUBLE) AS clinical_summary_length,
  CAST(SIZE(SPLIT(COALESCE(r.diagnosis_codes, ''), '\\|')) AS DOUBLE) AS diagnosis_code_count,

  -- Provider context
  CAST(COALESCE(ps.provider_total_requests, 0) AS DOUBLE) AS provider_total_requests,
  CAST(COALESCE(ps.provider_approval_rate, 0.5) AS DOUBLE) AS provider_approval_rate,
  CAST(COALESCE(ps.provider_denial_rate, 0.1) AS DOUBLE) AS provider_denial_rate,
  CAST(COALESCE(ps.provider_avg_turnaround, 72) AS DOUBLE) AS provider_avg_turnaround,
  CAST(COALESCE(ps.provider_avg_cost, 1000) AS DOUBLE) AS provider_avg_cost,

  -- Policy context
  CAST(COALESCE(pol.policy_total_requests, 0) AS DOUBLE) AS policy_total_requests,
  CAST(COALESCE(pol.policy_approval_rate, 0.5) AS DOUBLE) AS policy_approval_rate,

  -- Label
  CASE
    WHEN r.determination = 'approved' THEN 0
    WHEN r.determination = 'denied' THEN 1
    WHEN r.determination = 'pended' THEN 2
  END AS label

FROM {catalog_sql}.{PA_SCHEMA}.silver_pa_requests r
LEFT JOIN provider_stats ps ON r.requesting_provider_npi = ps.requesting_provider_npi
LEFT JOIN policy_stats pol ON r.policy_id = pol.policy_id
WHERE r.determination IN ('approved', 'denied', 'pended')
""")

# Save feature table
# Drop existing table if schema changed (CAST to DOUBLE may conflict)
spark.sql(f"DROP TABLE IF EXISTS {_tbl(PA_SCHEMA, 'pa_training_features')}")
feature_df.write.mode("overwrite").saveAsTable(_tbl(PA_SCHEMA, "pa_training_features"))
print(f"Feature table: {feature_df.count()} rows")
feature_df.groupBy("label").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model Training

# COMMAND ----------

import mlflow
import mlflow.xgboost
import xgboost as xgb
import numpy as np
import pandas as pd
from sklearn.model_selection import StratifiedKFold, cross_val_score
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import f1_score, accuracy_score, classification_report

mlflow.set_registry_uri("databricks-uc")

# Get current user for experiment path
user = spark.sql("SELECT current_user()").first()[0]
experiment_path = f"/Users/{user}/{catalog}_pa_adjudication"
mlflow.set_experiment(experiment_path)
print(f"MLflow experiment: {experiment_path}")

# COMMAND ----------

# Prepare features
pdf = feature_df.toPandas()

# Encode categorical features
categorical_cols = ["service_type", "urgency", "line_of_business", "determination_tier"]
label_encoders = {}
for col in categorical_cols:
    le = LabelEncoder()
    pdf[f"{col}_encoded"] = le.fit_transform(pdf[col].astype(str))
    label_encoders[col] = le

numeric_cols = [
    "estimated_cost", "turnaround_hours", "clinical_summary_length",
    "diagnosis_code_count", "provider_total_requests", "provider_approval_rate",
    "provider_denial_rate", "provider_avg_turnaround", "provider_avg_cost",
    "policy_total_requests", "policy_approval_rate",
]
encoded_cols = [f"{c}_encoded" for c in categorical_cols]
feature_cols = numeric_cols + encoded_cols

X = pdf[feature_cols].values
y = pdf["label"].values

print(f"Features: {X.shape[1]}, Samples: {X.shape[0]}")
print(f"Class distribution: {np.bincount(y)} (approved/denied/pended)")

# COMMAND ----------

# Train with cross-validation
PARAMS = {
    "max_depth": 5,
    "learning_rate": 0.1,
    "n_estimators": 100,
    "objective": "multi:softprob",
    "num_class": 3,
    "eval_metric": "mlogloss",
    "use_label_encoder": False,
    "random_state": 42,
}

with mlflow.start_run(run_name="pa_adjudication_xgboost") as run:
    mlflow.log_params(PARAMS)
    mlflow.log_param("feature_count", len(feature_cols))
    mlflow.log_param("sample_count", len(y))
    mlflow.log_param("feature_columns", feature_cols)

    # Cross-validation
    skf = StratifiedKFold(n_splits=3, shuffle=True, random_state=42)
    cv_f1_scores = []
    cv_acc_scores = []

    for fold, (train_idx, val_idx) in enumerate(skf.split(X, y)):
        model = xgb.XGBClassifier(**PARAMS)
        model.fit(X[train_idx], y[train_idx])
        preds = model.predict(X[val_idx])
        f1 = f1_score(y[val_idx], preds, average="weighted")
        acc = accuracy_score(y[val_idx], preds)
        cv_f1_scores.append(f1)
        cv_acc_scores.append(acc)
        print(f"  Fold {fold+1}: F1={f1:.4f}, Accuracy={acc:.4f}")

    mlflow.log_metric("cv_f1_mean", np.mean(cv_f1_scores))
    mlflow.log_metric("cv_f1_std", np.std(cv_f1_scores))
    mlflow.log_metric("cv_accuracy_mean", np.mean(cv_acc_scores))

    # Train final model on full dataset
    final_model = xgb.XGBClassifier(**PARAMS)
    final_model.fit(X, y)

    final_preds = final_model.predict(X)
    mlflow.log_metric("train_f1", f1_score(y, final_preds, average="weighted"))
    mlflow.log_metric("train_accuracy", accuracy_score(y, final_preds))

    # Log feature importance
    importance = dict(zip(feature_cols, final_model.feature_importances_.tolist()))
    mlflow.log_dict(importance, "feature_importance.json")

    # Log classification report
    report = classification_report(y, final_preds, target_names=["approved", "denied", "pended"])
    print(report)
    mlflow.log_text(report, "classification_report.txt")

    # Register model
    input_example = pd.DataFrame([X[0]], columns=feature_cols)
    model_info = mlflow.xgboost.log_model(
        final_model,
        artifact_path="model",
        input_example=input_example,
        registered_model_name=MODEL_NAME,
    )

    print(f"\nModel registered: {MODEL_NAME}")
    print(f"Run ID: {run.info.run_id}")

# COMMAND ----------

# Set production alias
client = mlflow.MlflowClient()
versions = client.search_model_versions(f"name='{MODEL_NAME}'")
latest = max(versions, key=lambda v: int(v.version))
client.set_registered_model_alias(MODEL_NAME, "production", latest.version)
print(f"Set 'production' alias → version {latest.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch Inference — Score All PA Requests

# COMMAND ----------

import json
from datetime import datetime

# Load model
loaded_model = mlflow.xgboost.load_model(f"models:/{MODEL_NAME}@production")

# Score all requests — re-encode categoricals from saved table
all_features = spark.table(_tbl(PA_SCHEMA, "pa_training_features")).toPandas()
for col in categorical_cols:
    all_features[f"{col}_encoded"] = label_encoders[col].transform(all_features[col].astype(str))
X_all = all_features[feature_cols].values
probas = loaded_model.predict_proba(X_all)
preds = loaded_model.predict(X_all)

label_map = {0: "approved", 1: "denied", 2: "pended"}
all_features["predicted_determination"] = [label_map[p] for p in preds]
all_features["prob_approved"] = probas[:, 0]
all_features["prob_denied"] = probas[:, 1]
all_features["prob_pended"] = probas[:, 2]
all_features["confidence"] = probas.max(axis=1)
all_features["model_version"] = str(latest.version)
all_features["scored_at"] = datetime.now().isoformat()

# Write predictions
predictions_df = spark.createDataFrame(all_features[[
    "auth_request_id", "member_id", "requesting_provider_npi",
    "service_type", "urgency", "line_of_business", "estimated_cost",
    "predicted_determination", "prob_approved", "prob_denied", "prob_pended",
    "confidence", "model_version", "scored_at",
]])

predictions_df.write.mode("overwrite").saveAsTable(_tbl(PA_SCHEMA, "pa_ml_predictions"))
print(f"Wrote {predictions_df.count()} predictions to {catalog}.{PA_SCHEMA}.pa_ml_predictions")

# COMMAND ----------

# Quick summary
spark.sql(f"""
SELECT
  predicted_determination,
  COUNT(*) AS count,
  ROUND(AVG(confidence), 3) AS avg_confidence,
  ROUND(AVG(prob_approved), 3) AS avg_prob_approved,
  ROUND(AVG(prob_denied), 3) AS avg_prob_denied
FROM {catalog_sql}.{PA_SCHEMA}.pa_ml_predictions
GROUP BY predicted_determination
ORDER BY count DESC
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## SHAP Explainability
# MAGIC
# MAGIC Global and per-class feature importance using SHAP TreeExplainer.
# MAGIC Logged as artifacts to the MLflow run for governance review.

# COMMAND ----------

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

label_names = ["approved", "denied", "pended"]

# Attempt SHAP explainability — fall back to XGBoost native importance on any error
try:
    import shap
    explainer = shap.TreeExplainer(final_model, feature_names=feature_cols)
    shap_values = explainer.shap_values(X)

    with mlflow.start_run(run_id=run.info.run_id):
        for cls_idx, cls_name in enumerate(label_names):
            fig, ax = plt.subplots(figsize=(10, 6))
            shap.summary_plot(
                shap_values[cls_idx], X,
                feature_names=feature_cols,
                show=False, plot_type="bar",
            )
            plt.title(f"SHAP Feature Importance — {cls_name}")
            plt.tight_layout()
            path = f"/tmp/shap_summary_{cls_name}.png"
            plt.savefig(path, dpi=150)
            plt.close()
            mlflow.log_artifact(path, artifact_path="shap")
            print(f"  Logged SHAP summary for class: {cls_name}")

        shap_global = np.mean([np.abs(shap_values[i]).mean(axis=0) for i in range(3)], axis=0)
        shap_importance = dict(sorted(zip(feature_cols, shap_global.tolist()), key=lambda x: -x[1]))
        mlflow.log_dict(shap_importance, "shap/global_shap_importance.json")
        print(f"  Top 5 SHAP features: {list(shap_importance.keys())[:5]}")

except Exception as shap_err:
    print(f"  SHAP unavailable ({type(shap_err).__name__}: {shap_err})")
    print("  Using XGBoost native feature importance as fallback")

    with mlflow.start_run(run_id=run.info.run_id):
        importance = dict(sorted(zip(feature_cols, final_model.feature_importances_.tolist()), key=lambda x: -x[1]))
        mlflow.log_dict(importance, "shap/xgboost_feature_importance.json")

        fig, ax = plt.subplots(figsize=(10, 6))
        top_n = dict(list(importance.items())[:15])
        ax.barh(list(reversed(top_n.keys())), list(reversed(top_n.values())))
        ax.set_title("Feature Importance (XGBoost gain)")
        ax.set_xlabel("Importance")
        plt.tight_layout()
        for cls_name in label_names:
            path = f"/tmp/shap_summary_{cls_name}.png"
            plt.savefig(path, dpi=150)
            mlflow.log_artifact(path, artifact_path="shap")
        plt.close()
        print(f"  Top 5 features: {list(importance.keys())[:5]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bias Monitoring — Fairness Analysis
# MAGIC
# MAGIC Evaluate model performance across protected/operational slices:
# MAGIC - **Line of Business** (Commercial, Medicare Advantage, Medicaid, etc.)
# MAGIC - **Urgency** (standard vs. expedited)
# MAGIC - **Service Type** (behavioral health, specialty pharmacy, etc.)
# MAGIC
# MAGIC Flags disparate impact if any slice's approval rate deviates >15% from the overall rate.

# COMMAND ----------

bias_slices = {
    "line_of_business": pdf["line_of_business"],
    "urgency": pdf["urgency"],
    "service_type": pdf["service_type"],
}

bias_results = []

with mlflow.start_run(run_id=run.info.run_id):
    overall_approval_rate = (final_preds == 0).mean()
    overall_denial_rate = (final_preds == 1).mean()
    overall_f1 = f1_score(y, final_preds, average="weighted")

    mlflow.log_metric("bias_overall_approval_rate", overall_approval_rate)
    mlflow.log_metric("bias_overall_denial_rate", overall_denial_rate)

    for slice_name, slice_values in bias_slices.items():
        unique_vals = slice_values.unique()
        print(f"\n--- Bias Analysis: {slice_name} ({len(unique_vals)} groups) ---")

        for val in sorted(unique_vals, key=str):
            mask = slice_values == val
            n = mask.sum()
            if n < 10:
                continue

            slice_preds = final_preds[mask]
            slice_actual = y[mask]
            slice_approval_rate = (slice_preds == 0).mean()
            slice_denial_rate = (slice_preds == 1).mean()
            slice_f1 = f1_score(slice_actual, slice_preds, average="weighted")

            # Disparate impact ratio (slice approval rate / overall approval rate)
            di_ratio = slice_approval_rate / overall_approval_rate if overall_approval_rate > 0 else 0
            flagged = abs(1 - di_ratio) > 0.15

            bias_results.append({
                "slice": slice_name,
                "value": str(val),
                "n": int(n),
                "approval_rate": round(float(slice_approval_rate), 4),
                "denial_rate": round(float(slice_denial_rate), 4),
                "f1": round(float(slice_f1), 4),
                "disparate_impact_ratio": round(float(di_ratio), 4),
                "flagged": flagged,
            })

            flag_str = " *** FLAGGED ***" if flagged else ""
            print(f"  {val}: n={n}, approval={slice_approval_rate:.1%}, denial={slice_denial_rate:.1%}, F1={slice_f1:.3f}, DI={di_ratio:.3f}{flag_str}")

            # Log per-slice metrics
            safe_val = str(val).replace(" ", "_").replace("/", "_")[:30]
            mlflow.log_metric(f"bias_{slice_name}_{safe_val}_f1", slice_f1)
            mlflow.log_metric(f"bias_{slice_name}_{safe_val}_approval", slice_approval_rate)
            mlflow.log_metric(f"bias_{slice_name}_{safe_val}_di_ratio", di_ratio)

    mlflow.log_dict(bias_results, "bias/fairness_analysis.json")

    flagged_count = sum(1 for r in bias_results if r["flagged"])
    mlflow.log_metric("bias_flagged_slices", flagged_count)
    print(f"\nBias analysis complete: {flagged_count} slices flagged for review (>15% DI deviation)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drift Detection Baseline
# MAGIC
# MAGIC Captures training data distribution statistics as a baseline for future drift monitoring.
# MAGIC Compares feature distributions between training data and current inference predictions.
# MAGIC Uses Population Stability Index (PSI) to quantify drift.

# COMMAND ----------

from scipy import stats as scipy_stats

def compute_psi(expected, actual, bins=10):
    """Population Stability Index — measures distribution shift."""
    eps = 1e-4
    breakpoints = np.linspace(
        min(expected.min(), actual.min()),
        max(expected.max(), actual.max()),
        bins + 1,
    )
    expected_pcts = np.histogram(expected, bins=breakpoints)[0] / len(expected) + eps
    actual_pcts = np.histogram(actual, bins=breakpoints)[0] / len(actual) + eps
    return np.sum((actual_pcts - expected_pcts) * np.log(actual_pcts / expected_pcts))

# Training distribution baseline
training_stats = {}
for i, col in enumerate(feature_cols):
    vals = X[:, i]
    training_stats[col] = {
        "mean": float(np.mean(vals)),
        "std": float(np.std(vals)),
        "min": float(np.min(vals)),
        "max": float(np.max(vals)),
        "p25": float(np.percentile(vals, 25)),
        "p50": float(np.percentile(vals, 50)),
        "p75": float(np.percentile(vals, 75)),
    }

# Compare training vs. inference (current batch)
drift_results = []
for i, col in enumerate(feature_cols):
    train_vals = X[:, i]
    infer_vals = X_all[:, i]

    psi = compute_psi(train_vals, infer_vals)
    ks_stat, ks_pval = scipy_stats.ks_2samp(train_vals, infer_vals)

    drift_flag = psi > 0.2  # PSI > 0.2 = significant drift
    drift_results.append({
        "feature": col,
        "psi": round(float(psi), 6),
        "ks_statistic": round(float(ks_stat), 6),
        "ks_pvalue": round(float(ks_pval), 6),
        "drift_detected": drift_flag,
    })

    if drift_flag:
        print(f"  DRIFT detected: {col} (PSI={psi:.4f})")

with mlflow.start_run(run_id=run.info.run_id):
    mlflow.log_dict(training_stats, "drift/training_distribution_baseline.json")
    mlflow.log_dict(drift_results, "drift/drift_analysis.json")

    drifted = sum(1 for r in drift_results if r["drift_detected"])
    mlflow.log_metric("drift_features_flagged", drifted)
    mlflow.log_metric("drift_max_psi", max(r["psi"] for r in drift_results))
    print(f"\nDrift analysis: {drifted}/{len(feature_cols)} features flagged (PSI > 0.2)")

# Save bias + drift results as Delta tables for dashboard consumption
bias_df = spark.createDataFrame(pd.DataFrame(bias_results))
bias_df.write.mode("overwrite").saveAsTable(_tbl(PA_SCHEMA, "pa_model_bias_analysis"))

drift_df = spark.createDataFrame(pd.DataFrame(drift_results))
drift_df.write.mode("overwrite").saveAsTable(_tbl(PA_SCHEMA, "pa_model_drift_analysis"))

print(f"\nGovernance tables written:")
print(f"  {catalog}.{PA_SCHEMA}.pa_model_bias_analysis ({len(bias_results)} rows)")
print(f"  {catalog}.{PA_SCHEMA}.pa_model_drift_analysis ({len(drift_results)} rows)")

# COMMAND ----------

print("PA auto-adjudication model training complete — with SHAP, bias monitoring, and drift detection.")
