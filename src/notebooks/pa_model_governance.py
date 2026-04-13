# Databricks notebook source
# MAGIC %md
# MAGIC # Red Bricks Insurance — PA Model Governance & Bias Monitoring
# MAGIC
# MAGIC Implements MLflow governance for the PA auto-adjudication pipeline:
# MAGIC 1. **Accuracy tracking** — Model predictions vs. actual determinations
# MAGIC 2. **Bias monitoring** — Approval/denial rates by demographics (LOB, urgency, service type)
# MAGIC 3. **Drift detection** — Feature distribution shifts over time
# MAGIC 4. **Audit trail** — Log every governance check to MLflow

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance", "Catalog")

catalog = dbutils.widgets.get("catalog")
catalog_sql = f"`{catalog}`"
PA_SCHEMA = "prior_auth"
MODEL_NAME = f"{catalog}.{PA_SCHEMA}.pa_adjudication_model"

def _tbl(schema: str, table: str) -> str:
    return f"`{catalog}`.{schema}.{table}"

print(f"Catalog: {catalog}")
print(f"Model:   {MODEL_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Model Accuracy vs. Actual Determinations

# COMMAND ----------

accuracy_df = spark.sql(f"""
SELECT
  p.predicted_determination,
  r.determination AS actual_determination,
  COUNT(*) AS count,
  CASE WHEN p.predicted_determination = r.determination THEN 'correct' ELSE 'incorrect' END AS accuracy
FROM {_tbl(PA_SCHEMA, 'pa_ml_predictions')} p
JOIN {catalog_sql}.{PA_SCHEMA}.silver_pa_requests r ON p.auth_request_id = r.auth_request_id
GROUP BY p.predicted_determination, r.determination
ORDER BY count DESC
""")

accuracy_df.show(20)

# Overall accuracy
overall = spark.sql(f"""
SELECT
  COUNT(*) AS total,
  SUM(CASE WHEN p.predicted_determination = r.determination THEN 1 ELSE 0 END) AS correct,
  ROUND(SUM(CASE WHEN p.predicted_determination = r.determination THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS accuracy_pct
FROM {_tbl(PA_SCHEMA, 'pa_ml_predictions')} p
JOIN {catalog_sql}.{PA_SCHEMA}.silver_pa_requests r ON p.auth_request_id = r.auth_request_id
""")
overall.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Bias Monitoring — Approval Rates by Segment

# COMMAND ----------

# Bias check: ML approval rate by line of business
lob_bias = spark.sql(f"""
SELECT
  p.line_of_business,
  COUNT(*) AS total_predictions,
  SUM(CASE WHEN p.predicted_determination = 'approved' THEN 1 ELSE 0 END) AS ml_approved,
  ROUND(SUM(CASE WHEN p.predicted_determination = 'approved' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS ml_approval_rate,
  SUM(CASE WHEN r.determination = 'approved' THEN 1 ELSE 0 END) AS actual_approved,
  ROUND(SUM(CASE WHEN r.determination = 'approved' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS actual_approval_rate,
  ROUND(
    ABS(SUM(CASE WHEN p.predicted_determination = 'approved' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)
    - SUM(CASE WHEN r.determination = 'approved' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2
  ) AS rate_delta
FROM {_tbl(PA_SCHEMA, 'pa_ml_predictions')} p
JOIN {catalog_sql}.{PA_SCHEMA}.silver_pa_requests r ON p.auth_request_id = r.auth_request_id
GROUP BY p.line_of_business
ORDER BY rate_delta DESC
""")
print("Bias Check — Approval Rate by Line of Business:")
lob_bias.show()

# COMMAND ----------

# Bias check: by service type
service_bias = spark.sql(f"""
SELECT
  p.service_type,
  COUNT(*) AS total,
  ROUND(SUM(CASE WHEN p.predicted_determination = 'approved' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS ml_approval_rate,
  ROUND(SUM(CASE WHEN r.determination = 'approved' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS actual_approval_rate,
  ROUND(SUM(CASE WHEN p.predicted_determination = 'denied' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS ml_denial_rate,
  ROUND(SUM(CASE WHEN r.determination = 'denied' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS actual_denial_rate
FROM {_tbl(PA_SCHEMA, 'pa_ml_predictions')} p
JOIN {catalog_sql}.{PA_SCHEMA}.silver_pa_requests r ON p.auth_request_id = r.auth_request_id
GROUP BY p.service_type
ORDER BY total DESC
""")
print("Bias Check — Rates by Service Type:")
service_bias.show(truncate=False)

# COMMAND ----------

# Bias check: by urgency (expedited should not have lower approval)
urgency_bias = spark.sql(f"""
SELECT
  p.urgency,
  COUNT(*) AS total,
  ROUND(SUM(CASE WHEN p.predicted_determination = 'approved' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS ml_approval_rate,
  ROUND(SUM(CASE WHEN r.determination = 'approved' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS actual_approval_rate,
  ROUND(AVG(p.confidence), 3) AS avg_confidence
FROM {_tbl(PA_SCHEMA, 'pa_ml_predictions')} p
JOIN {catalog_sql}.{PA_SCHEMA}.silver_pa_requests r ON p.auth_request_id = r.auth_request_id
GROUP BY p.urgency
""")
print("Bias Check — Rates by Urgency:")
urgency_bias.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Confidence Distribution & Drift Detection

# COMMAND ----------

confidence_dist = spark.sql(f"""
SELECT
  predicted_determination,
  COUNT(*) AS count,
  ROUND(AVG(confidence), 3) AS avg_confidence,
  ROUND(MIN(confidence), 3) AS min_confidence,
  ROUND(PERCENTILE(confidence, 0.25), 3) AS p25_confidence,
  ROUND(PERCENTILE(confidence, 0.50), 3) AS median_confidence,
  ROUND(PERCENTILE(confidence, 0.75), 3) AS p75_confidence,
  ROUND(MAX(confidence), 3) AS max_confidence
FROM {_tbl(PA_SCHEMA, 'pa_ml_predictions')}
GROUP BY predicted_determination
ORDER BY count DESC
""")
print("Model Confidence Distribution:")
confidence_dist.show()

# Low-confidence predictions (potential drift)
low_confidence = spark.sql(f"""
SELECT
  service_type,
  predicted_determination,
  COUNT(*) AS count,
  ROUND(AVG(confidence), 3) AS avg_confidence
FROM {_tbl(PA_SCHEMA, 'pa_ml_predictions')}
WHERE confidence < 0.6
GROUP BY service_type, predicted_determination
ORDER BY count DESC
""")
print("Low-Confidence Predictions (< 0.6) — Potential Drift Indicators:")
low_confidence.show(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Log Governance Metrics to MLflow

# COMMAND ----------

import mlflow
import json
from datetime import datetime

mlflow.set_registry_uri("databricks-uc")
user = spark.sql("SELECT current_user()").first()[0]
experiment_path = f"/Users/{user}/{catalog}_pa_governance"
mlflow.set_experiment(experiment_path)

# Collect metrics
overall_row = overall.first()
accuracy_pct = float(overall_row["accuracy_pct"])

lob_rows = lob_bias.collect()
max_lob_delta = max(float(r["rate_delta"]) for r in lob_rows) if lob_rows else 0

urgency_rows = urgency_bias.collect()
confidence_rows = confidence_dist.collect()

with mlflow.start_run(run_name=f"governance_check_{datetime.now().strftime('%Y%m%d_%H%M')}") as run:
    # Accuracy
    mlflow.log_metric("overall_accuracy_pct", accuracy_pct)
    mlflow.log_metric("total_predictions", int(overall_row["total"]))

    # Bias metrics
    mlflow.log_metric("max_lob_approval_rate_delta", max_lob_delta)
    for r in lob_rows:
        mlflow.log_metric(f"ml_approval_rate_{r['line_of_business']}", float(r["ml_approval_rate"]))

    for r in urgency_rows:
        mlflow.log_metric(f"ml_approval_rate_{r['urgency']}", float(r["ml_approval_rate"]))
        mlflow.log_metric(f"avg_confidence_{r['urgency']}", float(r["avg_confidence"]))

    # Confidence distribution
    for r in confidence_rows:
        mlflow.log_metric(f"avg_confidence_{r['predicted_determination']}", float(r["avg_confidence"]))

    # Flag alerts
    alerts = []
    if accuracy_pct < 80:
        alerts.append(f"LOW_ACCURACY: {accuracy_pct}% (threshold: 80%)")
    if max_lob_delta > 10:
        alerts.append(f"HIGH_BIAS: LOB approval rate delta {max_lob_delta}% (threshold: 10%)")

    low_conf_count = spark.sql(f"""
        SELECT COUNT(*) AS cnt FROM {_tbl(PA_SCHEMA, 'pa_ml_predictions')} WHERE confidence < 0.5
    """).first()["cnt"]
    total_count = int(overall_row["total"])
    low_conf_pct = round(low_conf_count * 100.0 / total_count, 2) if total_count > 0 else 0
    mlflow.log_metric("low_confidence_pct", low_conf_pct)
    if low_conf_pct > 20:
        alerts.append(f"HIGH_UNCERTAINTY: {low_conf_pct}% predictions below 0.5 confidence")

    mlflow.log_dict({"alerts": alerts, "timestamp": datetime.now().isoformat()}, "governance_alerts.json")

    # Tags
    mlflow.set_tag("governance_type", "scheduled_check")
    mlflow.set_tag("model_name", MODEL_NAME)
    mlflow.set_tag("alert_count", len(alerts))

    if alerts:
        print("\n⚠ GOVERNANCE ALERTS:")
        for a in alerts:
            print(f"  - {a}")
    else:
        print("\n✓ All governance checks passed.")

    print(f"\nMLflow run: {run.info.run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Write Governance Audit Table

# COMMAND ----------

# Create audit record
from pyspark.sql import Row

audit_data = [{
    "check_timestamp": datetime.now().isoformat(),
    "model_name": MODEL_NAME,
    "model_version": str(mlflow.MlflowClient().get_model_version_by_alias(MODEL_NAME, "production").version),
    "overall_accuracy_pct": accuracy_pct,
    "max_lob_bias_delta": max_lob_delta,
    "low_confidence_pct": low_conf_pct,
    "alert_count": len(alerts),
    "alerts": json.dumps(alerts),
    "mlflow_run_id": run.info.run_id,
}]

audit_df = spark.createDataFrame(audit_data)
audit_df.write.mode("append").saveAsTable(_tbl(PA_SCHEMA, "pa_governance_audit"))
print(f"Appended governance audit record to {catalog}.{PA_SCHEMA}.pa_governance_audit")

# COMMAND ----------

print("PA model governance check complete.")
