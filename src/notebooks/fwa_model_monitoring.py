# Databricks notebook source
# MAGIC %md
# MAGIC # Red Bricks Insurance — FWA Model Monitoring & Governance
# MAGIC
# MAGIC **Manual notebook** — run after the monitoring pipeline and backfill have populated
# MAGIC `inference.fwa_xgboost_monitoring_input`.
# MAGIC
# MAGIC 1. **Parse inference table payload** (if DLT pipeline hasn't run yet)
# MAGIC 2. **Enrich with ground truth** from `fwa.silver_fwa_signals`
# MAGIC 3. **Create Lakehouse Monitor** via REST API
# MAGIC 4. **Refresh monitor** & query profile/drift metrics
# MAGIC 5. **Manual PSI/KS drift analysis**
# MAGIC 6. **Governance audit trail** → MLflow + `analytics.fwa_governance_audit`
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - `train_fwa_model.py` has run (model registered)
# MAGIC - `fwa_batch_scoring.py` has run (or backfill has populated monitoring table)
# MAGIC - `inference.fwa_xgboost_monitoring_input` has data

# COMMAND ----------

# MAGIC %pip install scipy --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance_catalog", "Catalog")
dbutils.widgets.text("warehouse_id", "", "SQL Warehouse ID (optional)")

catalog = dbutils.widgets.get("catalog")
catalog_sql = f"`{catalog}`"
warehouse_id = dbutils.widgets.get("warehouse_id")

ANALYTICS_SCHEMA = "analytics"
FWA_SCHEMA = "fwa"
INFERENCE_SCHEMA = "inference"
MODEL_NAME = f"{catalog}.{ANALYTICS_SCHEMA}.fwa_scoring_model"
MONITORING_TABLE = f"{catalog}.{INFERENCE_SCHEMA}.fwa_xgboost_monitoring_input"
MONITORING_TABLE_SQL = f"{catalog_sql}.{INFERENCE_SCHEMA}.fwa_xgboost_monitoring_input"

print(f"Catalog:          {catalog}")
print(f"Model:            {MODEL_NAME}")
print(f"Monitoring Table: {MONITORING_TABLE}")

# COMMAND ----------

import mlflow
from mlflow import MlflowClient
import numpy as np
import pandas as pd
import time
import json
import requests
from datetime import datetime

mlflow.set_registry_uri("databricks-uc")
client = MlflowClient()

try:
    _user = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook().getContext().userName().get()
    )
except Exception:
    _user = spark.sql("SELECT current_user()").first()[0]

_host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
_headers = {"Authorization": f"Bearer {_token}", "Content-Type": "application/json"}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Monitoring Data Exists

# COMMAND ----------

try:
    monitor_count = spark.table(MONITORING_TABLE_SQL).count()
    print(f"Monitoring table: {monitor_count:,} rows")
    if monitor_count == 0:
        print("WARNING: Monitoring table is empty. Run fwa_backfill_monitoring.py first.")
except Exception as e:
    print(f"ERROR: Monitoring table not found: {e}")
    print("Run the fwa_monitoring_pipeline DLT pipeline or fwa_backfill_monitoring.py first.")
    dbutils.notebook.exit("Monitoring table not ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Lakehouse Monitor
# MAGIC
# MAGIC Creates a Lakehouse Monitor via REST API on the monitoring input table.
# MAGIC Uses `fraud_probability` as the prediction column and `is_fraud` as ground truth.

# COMMAND ----------

monitor_assets_schema = f"{catalog}.{INFERENCE_SCHEMA}"
monitor_assets_dir = f"/Shared/{catalog}_fwa_monitor"

# Get a SQL warehouse if not provided
if not warehouse_id:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    _warehouses = w.warehouses.list()
    warehouse_id = next((wh.id for wh in _warehouses), None)
    if warehouse_id:
        print(f"  Using warehouse: {warehouse_id}")

# Delete existing monitor if present
monitor_url = f"{_host}/api/2.1/unity-catalog/tables/{MONITORING_TABLE}/monitor"
del_resp = requests.delete(monitor_url, headers=_headers)
if del_resp.status_code == 200:
    print(f"  Deleted existing monitor on {MONITORING_TABLE}")
    time.sleep(5)

# Create monitor
payload = {
    "inference_log": {
        "problem_type": "PROBLEM_TYPE_CLASSIFICATION",
        "prediction_col": "fraud_probability",
        "timestamp_col": "prediction_timestamp",
        "label_col": "is_fraud",
        "model_id_col": "model_name",
        "granularities": ["1 day", "1 week"],
    },
    "assets_dir": monitor_assets_dir,
    "output_schema_name": monitor_assets_schema,
}
if warehouse_id:
    payload["warehouse_id"] = warehouse_id

resp = requests.post(monitor_url, headers=_headers, json=payload)

if resp.status_code in (200, 201):
    print(f"Lakehouse Monitor created on: {MONITORING_TABLE}")
    print(f"  Assets dir: {monitor_assets_dir}")
else:
    print(f"Monitor creation: HTTP {resp.status_code} — {resp.text[:300]}")
    if "already being monitored" in resp.text.lower():
        print("  Monitor already exists — will refresh.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Refresh Monitor & Query Metrics

# COMMAND ----------

print("Triggering monitor refresh...")
refresh_body = {}
if warehouse_id:
    refresh_body["warehouse_id"] = warehouse_id

resp = requests.post(f"{monitor_url}/refreshes", headers=_headers, json=refresh_body)
if resp.status_code not in (200, 201):
    resp = requests.post(f"{monitor_url}/run-refresh", headers=_headers, json=refresh_body)
    if resp.status_code not in (200, 201):
        print(f"  Refresh: HTTP {resp.status_code} — {resp.text[:200]}")
        print(f"  You can trigger the refresh manually from the monitor UI.")

# Wait for refresh
for attempt in range(30):
    try:
        list_resp = requests.get(f"{monitor_url}/refreshes", headers=_headers)
        if list_resp.status_code == 200:
            refreshes = list_resp.json().get("refreshes", [])
            if refreshes:
                latest = refreshes[-1]
                state = latest.get("state", "UNKNOWN")
                if attempt % 6 == 0:
                    print(f"  [{attempt*10}s] Refresh state: {state}")
                if state in ("SUCCESS", "SUCCEEDED", "COMPLETED"):
                    print(f"  Monitor refresh complete!")
                    break
                if state in ("FAILED", "CANCELED"):
                    print(f"  Monitor refresh {state}.")
                    break
    except Exception:
        pass
    time.sleep(10)

# Query metrics
profile_table = f"{catalog_sql}.{INFERENCE_SCHEMA}.fwa_xgboost_monitoring_input_profile_metrics"
drift_table = f"{catalog_sql}.{INFERENCE_SCHEMA}.fwa_xgboost_monitoring_input_drift_metrics"

try:
    print("\n--- Top Feature Profiles ---")
    display(spark.sql(f"""
        SELECT column_name, data_type, num_nulls, avg, stddev, min, max, distinct_count
        FROM {profile_table}
        WHERE column_name NOT IN ('claim_id', 'model_name', 'risk_tier')
        ORDER BY column_name
        LIMIT 15
    """))
except Exception as e:
    print(f"  Profile metrics not ready yet: {e}")

try:
    print("\n--- Drift Metrics (Top Drifted Features) ---")
    display(spark.sql(f"""
        SELECT column_name, drift_type,
            ks_test.statistic AS ks_statistic,
            ks_test.pvalue AS ks_pvalue,
            js_distance,
            population_stability_index AS psi
        FROM {drift_table}
        WHERE drift_type IS NOT NULL
        ORDER BY js_distance DESC
        LIMIT 15
    """))
except Exception as e:
    print(f"  Drift metrics not ready yet: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manual PSI & KS Drift Analysis
# MAGIC
# MAGIC Two-window split on the monitoring table to detect distribution shift.
# MAGIC - **PSI (Population Stability Index):** < 0.1 = stable, 0.1-0.2 = warning, > 0.2 = drift
# MAGIC - **KS (Kolmogorov-Smirnov):** non-parametric test for distribution equality

# COMMAND ----------

from scipy import stats as scipy_stats

monitor_pdf = spark.table(MONITORING_TABLE_SQL).select(
    "prediction_timestamp", "fraud_probability",
    "billed_amount", "provider_composite_risk_score",
).toPandas()

monitor_pdf["prediction_timestamp"] = pd.to_datetime(monitor_pdf["prediction_timestamp"])
median_date = monitor_pdf["prediction_timestamp"].median()

window_a = monitor_pdf[monitor_pdf["prediction_timestamp"] <= median_date]
window_b = monitor_pdf[monitor_pdf["prediction_timestamp"] > median_date]

print(f"Window A: {len(window_a):,} rows (up to {median_date})")
print(f"Window B: {len(window_b):,} rows (after {median_date})")

def compute_psi(expected, actual, bins=10):
    breakpoints = np.linspace(
        min(expected.min(), actual.min()),
        max(expected.max(), actual.max()) + 1e-6,
        bins + 1,
    )
    expected_counts = np.histogram(expected, bins=breakpoints)[0] + 1
    actual_counts = np.histogram(actual, bins=breakpoints)[0] + 1
    expected_pct = expected_counts / expected_counts.sum()
    actual_pct = actual_counts / actual_counts.sum()
    psi = np.sum((actual_pct - expected_pct) * np.log(actual_pct / expected_pct))
    return psi

psi_results = {}
ks_results = {}

if len(window_a) > 0 and len(window_b) > 0:
    for col in ["fraud_probability", "billed_amount", "provider_composite_risk_score"]:
        a_vals = window_a[col].dropna().values
        b_vals = window_b[col].dropna().values

        psi = compute_psi(a_vals, b_vals)
        ks_stat, ks_pvalue = scipy_stats.ks_2samp(a_vals, b_vals)

        psi_results[col] = psi
        ks_results[col] = {"statistic": ks_stat, "pvalue": ks_pvalue}

        drift_flag = "DRIFT DETECTED" if psi > 0.2 else ("WARNING" if psi > 0.1 else "STABLE")
        print(f"\n  {col}:")
        print(f"    PSI:     {psi:.4f} ({drift_flag})")
        print(f"    KS stat: {ks_stat:.4f}  p-value: {ks_pvalue:.4e}")
else:
    print("  Insufficient data for two-window analysis.")

print("\n  PSI thresholds: < 0.1 = stable, 0.1-0.2 = warning, > 0.2 = drift detected")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model Quality Metrics (vs Ground Truth)

# COMMAND ----------

from sklearn.metrics import f1_score, roc_auc_score, precision_score, recall_score, confusion_matrix

quality_pdf = spark.table(MONITORING_TABLE_SQL).select("fraud_probability", "is_fraud").toPandas()

y_true = quality_pdf["is_fraud"].values
y_prob = quality_pdf["fraud_probability"].values
y_pred = (y_prob >= 0.5).astype(int)

print("MODEL QUALITY METRICS (Monitoring Data)")
print("=" * 50)

quality_metrics = {
    "AUC-ROC": roc_auc_score(y_true, y_prob),
    "F1 Score": f1_score(y_true, y_pred),
    "Precision": precision_score(y_true, y_pred),
    "Recall": recall_score(y_true, y_pred),
}

for name, val in quality_metrics.items():
    print(f"  {name:<20} {val:.4f}")

cm = confusion_matrix(y_true, y_pred)
print(f"\n  Confusion Matrix:")
print(f"    TN={cm[0,0]:,}  FP={cm[0,1]:,}")
print(f"    FN={cm[1,0]:,}  TP={cm[1,1]:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Governance Audit Trail
# MAGIC
# MAGIC Log all monitoring metrics to an MLflow experiment and append an audit record.

# COMMAND ----------

governance_experiment = f"/Users/{_user}/{catalog}_fwa_governance"
mlflow.set_experiment(governance_experiment)

# Get model version info
model_versions = client.search_model_versions(f"name='{MODEL_NAME}'")
if model_versions:
    sorted_versions = sorted(model_versions, key=lambda v: int(v.version), reverse=True)
    champion_ver = sorted_versions[0].version
    challenger_ver = sorted_versions[1].version if len(sorted_versions) > 1 else None
else:
    champion_ver = "unknown"
    challenger_ver = None

governance_metrics = {
    "champion_version": int(champion_ver) if champion_ver != "unknown" else 0,
    "monitoring_rows": int(monitor_count),
}

alerts = []

# Add PSI drift alerts
for col, psi_val in psi_results.items():
    governance_metrics[f"psi_{col}"] = psi_val
    if psi_val > 0.2:
        alerts.append(f"DRIFT: {col} PSI={psi_val:.4f} (threshold: 0.2)")
    elif psi_val > 0.1:
        alerts.append(f"WARNING: {col} PSI={psi_val:.4f} (threshold: 0.1)")

# Add quality metrics
for name, val in quality_metrics.items():
    governance_metrics[f"quality_{name.lower().replace('-', '_').replace(' ', '_')}"] = val

# Add Lakehouse Monitor drift alerts
try:
    drift_rows = spark.sql(f"""
        SELECT column_name, js_distance
        FROM {drift_table}
        WHERE drift_type IS NOT NULL
        ORDER BY js_distance DESC
        LIMIT 5
    """).collect()

    if drift_rows:
        max_drift = max(float(r["js_distance"]) for r in drift_rows if r["js_distance"] is not None)
        governance_metrics["lakehouse_drift_max"] = max_drift
        if max_drift > 0.3:
            alerts.append(f"HIGH_DRIFT: Lakehouse Monitor max JS distance {max_drift:.4f}")
except Exception:
    pass

with mlflow.start_run(run_name=f"fwa_governance_{datetime.now().strftime('%Y%m%d_%H%M')}") as gov_run:
    mlflow.log_metrics(governance_metrics)
    mlflow.log_dict(
        {"alerts": alerts, "timestamp": datetime.now().isoformat(), "model": MODEL_NAME},
        "governance_alerts.json",
    )
    mlflow.log_dict(psi_results, "psi_results.json")
    mlflow.log_dict(
        {k: {"statistic": v["statistic"], "pvalue": v["pvalue"]} for k, v in ks_results.items()},
        "ks_results.json",
    )
    mlflow.set_tag("governance_type", "fwa_model_monitoring")
    mlflow.set_tag("model_name", MODEL_NAME)
    mlflow.set_tag("alert_count", len(alerts))

    if alerts:
        print("\nGOVERNANCE ALERTS:")
        for a in alerts:
            print(f"  - {a}")
    else:
        print("\nAll governance checks passed.")
    print(f"MLflow governance run: {gov_run.info.run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Audit Table

# COMMAND ----------

audit_table = f"{catalog_sql}.{ANALYTICS_SCHEMA}.fwa_governance_audit"

audit_data = [{
    "check_timestamp": datetime.now().isoformat(),
    "model_name": MODEL_NAME,
    "model_version_champion": str(champion_ver),
    "model_version_challenger": str(challenger_ver) if challenger_ver else None,
    "monitoring_rows": int(monitor_count),
    "psi_fraud_probability": float(psi_results.get("fraud_probability", 0)),
    "psi_billed_amount": float(psi_results.get("billed_amount", 0)),
    "quality_auc_roc": float(quality_metrics.get("AUC-ROC", 0)),
    "quality_f1": float(quality_metrics.get("F1 Score", 0)),
    "alert_count": len(alerts),
    "alerts_json": json.dumps(alerts),
    "mlflow_run_id": gov_run.info.run_id,
}]

audit_df = spark.createDataFrame(audit_data)
audit_df.write.mode("append").option("mergeSchema", "true").saveAsTable(audit_table)
print(f"Appended governance audit record to {audit_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 72)
print("FWA MODEL MONITORING — COMPLETE")
print("=" * 72)
print()
print(f"  Model:              {MODEL_NAME}")
print(f"    Champion:         v{champion_ver}")
if challenger_ver:
    print(f"    Challenger:       v{challenger_ver}")
print()
print(f"  Monitoring Table:   {MONITORING_TABLE}")
print(f"    Rows:             {monitor_count:,}")
print(f"    Profile Table:    {profile_table}")
print(f"    Drift Table:      {drift_table}")
print()
print(f"  Drift Analysis:")
for col, psi_val in psi_results.items():
    flag = "DRIFT" if psi_val > 0.2 else ("WARNING" if psi_val > 0.1 else "STABLE")
    print(f"    {col}: PSI={psi_val:.4f} ({flag})")
print()
print(f"  Model Quality:")
for name, val in quality_metrics.items():
    print(f"    {name:<20} {val:.4f}")
print()
print(f"  Governance Audit:   {audit_table}")
print(f"  MLflow Run:         {gov_run.info.run_id}")
print(f"  Alerts:             {len(alerts)}")
