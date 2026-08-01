# Databricks notebook source
# MAGIC %md
# MAGIC # Red Bricks Insurance — 30-Day Inpatient Readmission Risk Model
# MAGIC
# MAGIC Trains an index-stay-level readmission scoring model using **XGBoost** with MLflow
# MAGIC experiment tracking and a **pyfunc wrapper** that returns readmission probability.
# MAGIC
# MAGIC ### MLOps Lifecycle
# MAGIC 1. **Feature engineering** — joins the ADT `gold_readmission_features` index-stay table
# MAGIC    (LOS, disposition, diagnosis, prior utilization) with member risk (RAF / HCC / SDOH / age).
# MAGIC 2. **Feature Store registration** — `fe.create_table()` with primary key `index_admit_id`.
# MAGIC 3. **XGBoost training** — stratified cross-validation + small hyperparameter grid.
# MAGIC 4. **SHAP** — global summary plot logged to MLflow AND per-member top-factor extraction
# MAGIC    written to the gold table (drives the Member 360 "why" explanation).
# MAGIC 5. **`mlflow.pyfunc.log_model()`** — registers the probability wrapper to Unity Catalog.
# MAGIC 6. **`@champion` alias** in Unity Catalog.
# MAGIC 7. **Batch scoring** — writes `analytics.gold_member_readmission_risk` (one row per member
# MAGIC    with a recent index stay) for the Population Health Command Center app.
# MAGIC 8. **Serving endpoint** — `readmission-scorer` created programmatically (zero-intervention,
# MAGIC    inference tables enabled) for the app's optional live "re-score now" call.

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance_catalog", "Catalog")

catalog = dbutils.widgets.get("catalog")
catalog_sql = f"`{catalog}`"  # SQL-safe quoting (handles hyphens in catalog names)

ADT_SCHEMA = "adt"
RISK_SCHEMA = "risk_adjustment"
MEMBERS_SCHEMA = "members"
CARE_SCHEMA = "care_management"
ANALYTICS_SCHEMA = "analytics"
MODEL_NAME = f"{catalog}.{ANALYTICS_SCHEMA}.readmission_scorer"
# The serving endpoint (readmission-scorer) is created by the dedicated
# deploy_readmission_endpoint task, not this notebook — see the note near the end.

print(f"Catalog:  {catalog}")
print(f"Model:    {MODEL_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Engineering
# MAGIC
# MAGIC Grain: one row per index inpatient stay (from ADT `gold_readmission_features`).
# MAGIC ADT-derived features are enriched with member risk context (RAF, HCC count, SDOH
# MAGIC composite, age) so the model — and its SHAP explanation — reflect the same risk
# MAGIC signals a care manager sees on the Member 360. Label: `readmitted_30d`.

# COMMAND ----------

feature_df = spark.sql(f"""
WITH stays AS (
  SELECT * FROM {catalog_sql}.{ADT_SCHEMA}.gold_readmission_features
),

risk AS (
  SELECT
    member_id,
    MAX(raf_score) AS raf_score,
    MAX(
      CASE WHEN hcc_codes IS NULL OR hcc_codes = '' THEN 0
           ELSE SIZE(SPLIT(hcc_codes, ',')) END
    ) AS hcc_count
  FROM {catalog_sql}.{RISK_SCHEMA}.silver_risk_adjustment_member
  GROUP BY member_id
),

demo AS (
  SELECT
    member_id,
    MAX(FLOOR(DATEDIFF(CURRENT_DATE(), date_of_birth) / 365.25)) AS age
  FROM {catalog_sql}.{MEMBERS_SCHEMA}.silver_members
  GROUP BY member_id
),

sdoh AS (
  SELECT
    member_id,
    MAX(composite_sdoh_risk_score) AS composite_sdoh_risk_score
  FROM {catalog_sql}.{CARE_SCHEMA}.silver_member_sdoh
  GROUP BY member_id
)

SELECT
  s.index_admit_id,
  s.member_id,
  s.admit_timestamp,
  s.discharge_timestamp,
  s.admit_reason,
  s.primary_diagnosis_code,

  -- ADT-derived features
  CAST(s.length_of_stay_days AS DOUBLE)      AS length_of_stay_days,
  CAST(s.is_inpatient AS DOUBLE)             AS is_inpatient,
  CAST(s.discharged_to_post_acute AS DOUBLE) AS discharged_to_post_acute,
  CAST(s.discharged_ama AS DOUBLE)           AS discharged_ama,
  CAST(s.prior_admits_180d AS DOUBLE)        AS prior_admits_180d,

  -- Member risk context
  CAST(COALESCE(r.raf_score, 1.0) AS DOUBLE)                 AS raf_score,
  CAST(COALESCE(r.hcc_count, 0) AS DOUBLE)                   AS hcc_count,
  CAST(COALESCE(sd.composite_sdoh_risk_score, 0.0) AS DOUBLE) AS composite_sdoh_risk_score,
  CAST(COALESCE(d.age, 55) AS DOUBLE)                        AS age,

  -- Label
  CAST(s.readmitted_30d AS DOUBLE) AS readmitted_30d

FROM stays s
LEFT JOIN risk r  ON s.member_id = r.member_id
LEFT JOIN demo d  ON s.member_id = d.member_id
LEFT JOIN sdoh sd ON s.member_id = sd.member_id
""")

feature_df = feature_df.dropDuplicates(["index_admit_id"])

total = feature_df.count()
readmit_count = feature_df.filter("readmitted_30d = 1").count()
print(f"Feature table: {total:,} index stays, {readmit_count:,} readmitted ({readmit_count/max(total,1):.2%})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register Feature Table in Unity Catalog
# MAGIC
# MAGIC Registered via the **Feature Engineering client** for lineage and discovery —
# MAGIC UC tracks that the readmission model consumes these features.

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()

feature_table_name = f"{catalog}.{ANALYTICS_SCHEMA}.readmission_training_features"

spark.sql(f"DROP TABLE IF EXISTS {feature_table_name}")

fe.create_table(
    name=feature_table_name,
    primary_keys=["index_admit_id"],
    df=feature_df,
    description=(
        "30-day inpatient readmission features: index-stay length of stay, discharge "
        "disposition, prior utilization, and member risk context (RAF, HCC count, SDOH, "
        "age). Primary key: index_admit_id. Label: readmitted_30d."
    ),
)
print(f"Feature table created: {feature_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train XGBoost Classifier with MLflow

# COMMAND ----------

import mlflow
import numpy as np
import pandas as pd
from sklearn.model_selection import StratifiedKFold, cross_validate
from sklearn.metrics import (
    f1_score, roc_auc_score, precision_score, recall_score,
    confusion_matrix,
)
from xgboost import XGBClassifier
import itertools

_user = spark.conf.get("spark.databricks.workspaceUrl", "").split("/")[0]
try:
    _user = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook().getContext().userName().get()
    )
except Exception:
    pass
experiment_path = f"/Users/{_user}/{catalog}_readmission_scorer"
mlflow.set_experiment(experiment_path)
mlflow.set_registry_uri("databricks-uc")
print(f"MLflow experiment: {experiment_path}")

# COMMAND ----------

# Ordered feature columns — this list is the model signature and is reused verbatim
# by the backend when calling the serving endpoint, so keep it stable.
FEATURE_COLS = [
    "length_of_stay_days",
    "is_inpatient",
    "discharged_to_post_acute",
    "discharged_ama",
    "prior_admits_180d",
    "raf_score",
    "hcc_count",
    "composite_sdoh_risk_score",
    "age",
]

# Include stay-metadata columns used by the batch-scoring/gold-write block below
# (admit_reason + timestamps) alongside the model feature columns.
_META_COLS = ["admit_timestamp", "discharge_timestamp", "admit_reason"]
pdf = feature_df.select(
    ["index_admit_id", "member_id"] + FEATURE_COLS + _META_COLS + ["readmitted_30d"]
).toPandas()
X = pdf[FEATURE_COLS].values
y = pdf["readmitted_30d"].values

readmit_ratio = y.sum() / max(len(y), 1)
print(f"Class balance: {int(y.sum())} readmit / {len(y)} total ({readmit_ratio:.2%})")
# NOTE: unlike the FWA fraud model (~2% positive → needs scale_pos_weight), the
# readmission base rate (~20%) is only mildly imbalanced. Rebalancing here would
# de-calibrate predicted probabilities (inflating everyone toward High/Very High),
# and the risk-tier thresholds below assume calibrated output. So we train on the
# natural distribution and let the probabilities mean what they say.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Hyperparameter Grid Search (stratified 3-fold CV)

# COMMAND ----------

param_grid = {
    "max_depth": [3, 5],
    "learning_rate": [0.1],
    "n_estimators": [200],
}
grid_keys = list(param_grid.keys())
grid_combos = list(itertools.product(*[param_grid[k] for k in grid_keys]))
print(f"Grid search: {len(grid_combos)} combinations")

cv = StratifiedKFold(n_splits=3, shuffle=True, random_state=42)
best_auc, best_params = -1, None

mlflow.xgboost.autolog(log_datasets=False, silent=True)

for combo in grid_combos:
    params = dict(zip(grid_keys, combo))
    with mlflow.start_run(run_name=f"xgb_d{params['max_depth']}_lr{params['learning_rate']}_n{params['n_estimators']}"):
        model = XGBClassifier(
            **params,
            eval_metric="logloss", random_state=42, tree_method="hist",
        )
        cv_results = cross_validate(
            model, X, y, cv=cv,
            scoring=["f1", "roc_auc", "precision", "recall"],
        )
        metrics = {
            "cv_f1_mean": np.mean(cv_results["test_f1"]),
            "cv_auc_mean": np.mean(cv_results["test_roc_auc"]),
            "cv_auc_std": np.std(cv_results["test_roc_auc"]),
            "cv_precision_mean": np.mean(cv_results["test_precision"]),
            "cv_recall_mean": np.mean(cv_results["test_recall"]),
        }
        mlflow.log_metrics(metrics)
        print(f"  {params} → AUC={metrics['cv_auc_mean']:.4f} F1={metrics['cv_f1_mean']:.4f}")
        if metrics["cv_auc_mean"] > best_auc:
            best_auc, best_params = metrics["cv_auc_mean"], params

print(f"\nBest: {best_params} → CV AUC={best_auc:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Train Final Model + SHAP + Register to Unity Catalog

# COMMAND ----------

with mlflow.start_run(run_name="xgb_final_champion") as final_run:
    final_model = XGBClassifier(
        **best_params,
        eval_metric="logloss", random_state=42, tree_method="hist",
    )
    final_model.fit(X, y)

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

    importance = dict(zip(FEATURE_COLS, final_model.feature_importances_.tolist()))
    mlflow.log_dict(importance, "feature_importance.json")

    cm = confusion_matrix(y, y_pred)
    mlflow.log_dict(
        {"tn": int(cm[0, 0]), "fp": int(cm[0, 1]), "fn": int(cm[1, 0]), "tp": int(cm[1, 1])},
        "confusion_matrix.json",
    )

    # SHAP global summary plot
    try:
        import shap
        import matplotlib.pyplot as plt
        explainer = shap.TreeExplainer(final_model)
        shap_sample = pd.DataFrame(X[:1000], columns=FEATURE_COLS)
        shap_values = explainer.shap_values(shap_sample)
        shap.summary_plot(shap_values, shap_sample, show=False)
        plt.tight_layout()
        plt.savefig("/tmp/readmission_shap_summary.png", dpi=150, bbox_inches="tight")
        mlflow.log_artifact("/tmp/readmission_shap_summary.png")
        plt.close()
        print("SHAP summary plot logged to MLflow")
    except Exception as e:
        print(f"SHAP plot skipped: {e}")

    # pyfunc wrapper returning readmission probability
    input_example = pd.DataFrame([X[0]], columns=FEATURE_COLS)

    class ReadmissionProbaModel(mlflow.pyfunc.PythonModel):
        def __init__(self, xgb_model, feature_cols):
            self.xgb_model = xgb_model
            self.feature_cols = feature_cols

        def predict(self, context, model_input, params=None):
            if isinstance(model_input, pd.DataFrame):
                cols = [c for c in self.feature_cols if c in model_input.columns]
                arr = model_input[cols].values
            else:
                arr = model_input
            return self.xgb_model.predict_proba(arr)[:, 1].tolist()

    mlflow.pyfunc.log_model(
        artifact_path="model",
        python_model=ReadmissionProbaModel(final_model, FEATURE_COLS),
        input_example=input_example,
        registered_model_name=MODEL_NAME,
        pip_requirements=["xgboost==3.1.1", "pandas>=2.0,<3.0", "numpy", "scikit-learn"],
    )
    best_final_run_id = final_run.info.run_id
    print(f"Final model logged: run_id={best_final_run_id}")
    for k, v in train_metrics.items():
        print(f"  {k}: {v:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register `@champion` Alias

# COMMAND ----------

from mlflow import MlflowClient

client = MlflowClient()
model_versions = client.search_model_versions(f"name='{MODEL_NAME}'")
latest_version = max(model_versions, key=lambda v: int(v.version))

client.set_registered_model_alias(name=MODEL_NAME, alias="champion", version=latest_version.version)
client.update_registered_model(
    name=MODEL_NAME,
    description=(
        "30-day inpatient readmission risk model (XGBoost pyfunc). Returns the probability "
        "that a member is readmitted within 30 days of an index inpatient discharge. "
        f"Best CV AUC-ROC: {best_auc:.4f}. Best params: {best_params}. Features: index LOS, "
        "discharge disposition, prior 180-day admissions, RAF, HCC count, SDOH composite, age."
    ),
)
print(f"Registered model: {MODEL_NAME}")
print(f"Alias: @champion -> v{latest_version.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch Scoring → `gold_member_readmission_risk`
# MAGIC
# MAGIC Score every index stay, then keep the **most recent** stay per member (30-day
# MAGIC readmission risk is defined relative to a discharge). Per-member SHAP contributions
# MAGIC are distilled into human-readable `top_risk_factors` for the Member 360 explanation.

# COMMAND ----------

from datetime import datetime, timezone

score_pdf = pdf.copy()
score_pdf["readmission_risk_score"] = final_model.predict_proba(score_pdf[FEATURE_COLS].values)[:, 1]

# Per-row SHAP → top positive contributors, mapped to friendly labels.
FEATURE_LABELS = {
    "length_of_stay_days": "Long length of stay",
    "is_inpatient": "Inpatient admission",
    "discharged_to_post_acute": "Discharged to post-acute facility",
    "discharged_ama": "Left against medical advice",
    "prior_admits_180d": "Multiple recent admissions",
    "raf_score": "High RAF / clinical risk",
    "hcc_count": "Multiple chronic conditions (HCCs)",
    "composite_sdoh_risk_score": "Social determinant barriers",
    "age": "Advanced age",
}

try:
    import shap
    explainer = shap.TreeExplainer(final_model)
    shap_matrix = explainer.shap_values(score_pdf[FEATURE_COLS])

    def _top_factors(row_idx, k=3):
        contribs = list(zip(FEATURE_COLS, shap_matrix[row_idx]))
        positive = [(f, v) for f, v in contribs if v > 0]
        positive.sort(key=lambda x: x[1], reverse=True)
        return [FEATURE_LABELS.get(f, f) for f, _ in positive[:k]]

    score_pdf["top_risk_factors"] = [_top_factors(i) for i in range(len(score_pdf))]
    print("Per-member SHAP top-factors computed")
except Exception as e:
    print(f"SHAP top-factors skipped ({e}) — falling back to global importance")
    _fallback = [FEATURE_LABELS.get(f, f) for f, _ in
                 sorted(importance.items(), key=lambda x: x[1], reverse=True)[:3]]
    score_pdf["top_risk_factors"] = [list(_fallback) for _ in range(len(score_pdf))]

def _tier(p):
    if p >= 0.40:
        return "Very High"
    if p >= 0.25:
        return "High"
    if p >= 0.12:
        return "Moderate"
    return "Low"

score_pdf["readmission_risk_tier"] = score_pdf["readmission_risk_score"].apply(_tier)

# Keep the most recent index stay per member.
score_pdf = score_pdf.sort_values("discharge_timestamp").groupby("member_id", as_index=False).tail(1)

member_scored = score_pdf[[
    "member_id", "index_admit_id", "admit_timestamp", "discharge_timestamp",
    "admit_reason", "length_of_stay_days", "prior_admits_180d",
    "readmission_risk_score", "readmission_risk_tier", "top_risk_factors",
]].copy()
member_scored["model_version"] = str(latest_version.version)
member_scored["scored_at"] = datetime.now(timezone.utc).isoformat()

from pyspark.sql import types as T

schema = T.StructType([
    T.StructField("member_id", T.StringType()),
    T.StructField("index_admit_id", T.StringType()),
    T.StructField("admit_timestamp", T.StringType()),
    T.StructField("discharge_timestamp", T.StringType()),
    T.StructField("admit_reason", T.StringType()),
    T.StructField("length_of_stay_days", T.DoubleType()),
    T.StructField("prior_admits_180d", T.DoubleType()),
    T.StructField("readmission_risk_score", T.DoubleType()),
    T.StructField("readmission_risk_tier", T.StringType()),
    T.StructField("top_risk_factors", T.ArrayType(T.StringType())),
    T.StructField("model_version", T.StringType()),
    T.StructField("scored_at", T.StringType()),
])

member_scored["admit_timestamp"] = member_scored["admit_timestamp"].astype(str)
member_scored["discharge_timestamp"] = member_scored["discharge_timestamp"].astype(str)

scored_sdf = spark.createDataFrame(member_scored, schema=schema)
gold_table = f"{catalog_sql}.{ANALYTICS_SCHEMA}.gold_member_readmission_risk"
scored_sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold_table)

scored_count = spark.table(gold_table).count()
dist = spark.sql(f"""
    SELECT readmission_risk_tier, COUNT(*) AS n
    FROM {gold_table} GROUP BY readmission_risk_tier ORDER BY n DESC
""").collect()
print(f"Gold table written: {gold_table} ({scored_count:,} members)")
for r in dist:
    print(f"  {r['readmission_risk_tier']}: {r['n']:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Serving endpoint — deployed by a separate task
# MAGIC
# MAGIC The `readmission-scorer` serving endpoint is created and **waited on** by the
# MAGIC dedicated `deploy_readmission_endpoint` job task (see `deploy_readmission_endpoint.py`),
# MAGIC not here. Endpoint provisioning takes 10-20 min; isolating it keeps the fast training
# MAGIC path off that critical wait and lets the deploy task's completion honestly mean
# MAGIC "the endpoint is READY and queryable." The batch-scored `gold_member_readmission_risk`
# MAGIC table written above is the app's default read path and does not depend on the endpoint.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 70)
print("READMISSION RISK MODEL — TRAINING COMPLETE")
print("=" * 70)
print(f"  Model Name:   {MODEL_NAME}")
print(f"  Version:      {latest_version.version}  (@champion)")
print(f"  Run ID:       {best_final_run_id}")
print()
print(f"  Training Results:")
print(f"    Algorithm:   XGBoost (pyfunc wrapper)")
print(f"    Best CV AUC: {best_auc:.4f}")
print(f"    AUC-ROC:     {train_metrics['train_auc_roc']:.4f}")
print(f"    F1:          {train_metrics['train_f1']:.4f}")
print(f"    Precision:   {train_metrics['train_precision']:.4f}")
print(f"    Recall:      {train_metrics['train_recall']:.4f}")
print(f"    Best Params: {best_params}")
print()
print(f"  Feature Table: {feature_table_name}")
print(f"  Training Data: {total:,} index stays, {readmit_count:,} readmitted ({readmit_count/max(total,1):.2%})")
print(f"  Gold Table:    {catalog}.{ANALYTICS_SCHEMA}.gold_member_readmission_risk ({scored_count:,} members)")
print(f"  Endpoint:      readmission-scorer (created by deploy_readmission_endpoint task)")
