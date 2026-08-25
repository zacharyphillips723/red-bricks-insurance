# Databricks notebook source
# MAGIC %md
# MAGIC # Red Bricks Insurance — Denial Risk Model Governance & Drift Monitoring
# MAGIC
# MAGIC MLflow governance + Lakehouse Monitoring for the denial-risk model:
# MAGIC 1. **Accuracy proxy** — batch scores vs. actual claim outcomes (confusion matrix)
# MAGIC 2. **Bias monitoring** — predicted vs. actual denial rate by line of business
# MAGIC 3. **Drift/confidence** — denial-prob distribution + uncertain-band share
# MAGIC 4. **Audit trail** — log every governance check to MLflow + a Delta audit table
# MAGIC 5. **Lakehouse Monitor** — InferenceLog monitor on the serving inference table

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance_catalog", "Catalog")

catalog = dbutils.widgets.get("catalog")
catalog_sql = f"`{catalog}`"
CLAIMS_SCHEMA = "claims"
MEMBERS_SCHEMA = "members"
MODEL_NAME = f"{catalog}.{CLAIMS_SCHEMA}.denial_risk_model"
INFERENCE_TABLE = f"{catalog}.{CLAIMS_SCHEMA}.denial_risk_scorer_payload"

def _tbl(schema: str, table: str) -> str:
    return f"`{catalog}`.{schema}.{table}"

print(f"Catalog: {catalog}")
print(f"Model:   {MODEL_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Accuracy Proxy — Batch Scores vs. Actual Outcomes
# MAGIC
# MAGIC Joins `gold_denial_risk_scores` to `silver_claims_medical` on `claim_id`,
# MAGIC treats `denial_prob >= 0.5` as predicted-denied, and compares to the actual
# MAGIC status (`LOWER(claim_status) = 'denied'`).

# COMMAND ----------

confusion_df = spark.sql(f"""
SELECT
  CASE WHEN s.denial_prob >= 0.5 THEN 1 ELSE 0 END AS predicted_denied,
  CASE WHEN LOWER(c.claim_status) = 'denied' THEN 1 ELSE 0 END AS actual_denied,
  COUNT(*) AS count
FROM {_tbl(CLAIMS_SCHEMA, 'gold_denial_risk_scores')} s
JOIN {_tbl(CLAIMS_SCHEMA, 'silver_claims_medical')} c ON s.claim_id = c.claim_id
GROUP BY 1, 2
ORDER BY predicted_denied, actual_denied
""")
print("Confusion matrix (predicted_denied x actual_denied):")
confusion_df.show()

metrics_row = spark.sql(f"""
SELECT
  COUNT(*) AS total,
  SUM(CASE WHEN (s.denial_prob >= 0.5) = (LOWER(c.claim_status) = 'denied') THEN 1 ELSE 0 END) AS correct,
  SUM(CASE WHEN s.denial_prob >= 0.5 AND LOWER(c.claim_status) = 'denied' THEN 1 ELSE 0 END) AS tp,
  SUM(CASE WHEN s.denial_prob >= 0.5 AND LOWER(c.claim_status) <> 'denied' THEN 1 ELSE 0 END) AS fp,
  SUM(CASE WHEN s.denial_prob < 0.5 AND LOWER(c.claim_status) = 'denied' THEN 1 ELSE 0 END) AS fn
FROM {_tbl(CLAIMS_SCHEMA, 'gold_denial_risk_scores')} s
JOIN {_tbl(CLAIMS_SCHEMA, 'silver_claims_medical')} c ON s.claim_id = c.claim_id
""").first()

total = int(metrics_row["total"] or 0)
tp = int(metrics_row["tp"] or 0)
fp = int(metrics_row["fp"] or 0)
fn = int(metrics_row["fn"] or 0)
overall_accuracy = round((metrics_row["correct"] or 0) / total, 4) if total else 0.0
precision = round(tp / (tp + fp), 4) if (tp + fp) else 0.0
recall = round(tp / (tp + fn), 4) if (tp + fn) else 0.0
print(f"Total scored+matched: {total:,}")
print(f"Accuracy: {overall_accuracy} | Precision: {precision} | Recall: {recall}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Bias Monitoring — Predicted vs. Actual Denial Rate by Line of Business

# COMMAND ----------

lob_bias = spark.sql(f"""
SELECT
  COALESCE(e.line_of_business, 'Unknown') AS line_of_business,
  COUNT(*) AS total,
  ROUND(AVG(CASE WHEN s.denial_prob >= 0.5 THEN 1.0 ELSE 0 END), 4) AS predicted_denial_rate,
  ROUND(AVG(CASE WHEN LOWER(c.claim_status) = 'denied' THEN 1.0 ELSE 0 END), 4) AS actual_denial_rate,
  ROUND(ABS(AVG(CASE WHEN s.denial_prob >= 0.5 THEN 1.0 ELSE 0 END)
          - AVG(CASE WHEN LOWER(c.claim_status) = 'denied' THEN 1.0 ELSE 0 END)), 4) AS rate_delta
FROM {_tbl(CLAIMS_SCHEMA, 'gold_denial_risk_scores')} s
JOIN {_tbl(CLAIMS_SCHEMA, 'silver_claims_medical')} c ON s.claim_id = c.claim_id
LEFT JOIN (
    SELECT member_id, MAX(line_of_business) AS line_of_business
    FROM {_tbl(MEMBERS_SCHEMA, 'silver_enrollment')} GROUP BY member_id
) e ON c.member_id = e.member_id
GROUP BY 1
ORDER BY rate_delta DESC
""")
print("Bias Check — Denial Rate by Line of Business:")
lob_bias.show(truncate=False)

lob_rows = lob_bias.collect()
max_lob_delta = max((float(r["rate_delta"]) for r in lob_rows), default=0.0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Drift / Confidence — Denial-Probability Distribution

# COMMAND ----------

dist_row = spark.sql(f"""
SELECT
  ROUND(AVG(denial_prob), 4) AS avg_prob,
  ROUND(PERCENTILE(denial_prob, 0.50), 4) AS p50_prob,
  ROUND(PERCENTILE(denial_prob, 0.90), 4) AS p90_prob,
  ROUND(AVG(CASE WHEN denial_prob BETWEEN 0.4 AND 0.6 THEN 1.0 ELSE 0 END), 4) AS uncertain_band_pct,
  COUNT(*) AS total
FROM {_tbl(CLAIMS_SCHEMA, 'gold_denial_risk_scores')}
""").first()

avg_denial_prob = float(dist_row["avg_prob"] or 0.0)
uncertain_band_pct = float(dist_row["uncertain_band_pct"] or 0.0)
print(f"Denial-prob distribution — avg {avg_denial_prob}, p50 {dist_row['p50_prob']}, "
      f"p90 {dist_row['p90_prob']}, uncertain-band(0.4-0.6) {uncertain_band_pct}")

# Compare average predicted propensity to the historical actual denial rate as a
# calibration / drift indicator.
actual_rate = spark.sql(f"""
SELECT ROUND(AVG(CASE WHEN LOWER(claim_status) = 'denied' THEN 1.0 ELSE 0 END), 4) AS r
FROM {_tbl(CLAIMS_SCHEMA, 'silver_claims_medical')}
""").first()["r"]
actual_denial_rate = float(actual_rate or 0.0)
calibration_gap = round(abs(avg_denial_prob - actual_denial_rate), 4)
print(f"Actual historical denial rate: {actual_denial_rate} | calibration gap: {calibration_gap}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Log Governance Metrics to MLflow

# COMMAND ----------

import mlflow
import json
from datetime import datetime

mlflow.set_registry_uri("databricks-uc")
user = spark.sql("SELECT current_user()").first()[0]
mlflow.set_experiment(f"/Users/{user}/{catalog}_denial_governance")

with mlflow.start_run(run_name=f"governance_check_{datetime.now().strftime('%Y%m%d_%H%M')}") as run:
    mlflow.log_metric("overall_accuracy", overall_accuracy)
    mlflow.log_metric("precision", precision)
    mlflow.log_metric("recall", recall)
    mlflow.log_metric("total_scored", total)
    mlflow.log_metric("max_lob_bias_delta", max_lob_delta)
    mlflow.log_metric("uncertain_band_pct", uncertain_band_pct)
    mlflow.log_metric("avg_denial_prob", avg_denial_prob)
    mlflow.log_metric("actual_denial_rate", actual_denial_rate)
    mlflow.log_metric("calibration_gap", calibration_gap)
    for r in lob_rows:
        mlflow.log_metric(f"predicted_denial_rate_{r['line_of_business']}", float(r["predicted_denial_rate"]))

    alerts = []
    if overall_accuracy < 0.75:
        alerts.append(f"LOW_ACCURACY: {overall_accuracy} (threshold: 0.75)")
    if max_lob_delta > 0.10:
        alerts.append(f"HIGH_BIAS: LOB denial-rate delta {max_lob_delta} (threshold: 0.10)")
    if uncertain_band_pct > 0.30:
        alerts.append(f"HIGH_UNCERTAINTY: {uncertain_band_pct} of scores in 0.4-0.6 band (threshold: 0.30)")

    mlflow.log_dict({"alerts": alerts, "timestamp": datetime.now().isoformat()}, "governance_alerts.json")
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

try:
    model_version = str(mlflow.MlflowClient().get_model_version_by_alias(MODEL_NAME, "production").version)
except Exception as e:
    print(f"Could not resolve @production version ({e})")
    model_version = "unknown"

audit_data = [{
    "check_timestamp": datetime.now().isoformat(),
    "model_name": MODEL_NAME,
    "model_version": model_version,
    "overall_accuracy": overall_accuracy,
    "precision": precision,
    "recall": recall,
    "max_lob_bias_delta": max_lob_delta,
    "uncertain_band_pct": uncertain_band_pct,
    "avg_denial_prob": avg_denial_prob,
    "calibration_gap": calibration_gap,
    "alert_count": len(alerts),
    "alerts": json.dumps(alerts),
    "mlflow_run_id": run.info.run_id,
}]
spark.createDataFrame(audit_data).write.mode("append").saveAsTable(_tbl(CLAIMS_SCHEMA, "denial_governance_audit"))
print(f"Appended governance audit record to {catalog}.{CLAIMS_SCHEMA}.denial_governance_audit")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Lakehouse Monitor — InferenceLog on the Serving Inference Table
# MAGIC
# MAGIC Creates (once) an InferenceLog monitor on `claims.denial_risk_scorer_payload`
# MAGIC so drift/quality metrics + a dashboard are generated from live serving traffic,
# MAGIC then triggers a refresh.
# MAGIC
# MAGIC NOTE: the inference table only has rows **after the app has queried the endpoint**.
# MAGIC The databricks-sdk monitor API surface is version-sensitive, so this block is
# MAGIC best-effort: on any failure it prints a clear message and continues — the
# MAGIC governance audit table above is the always-on fallback artifact.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
_monitor_output_schema = f"{catalog}.{CLAIMS_SCHEMA}"
_assets_dir = f"/Workspace/Users/{user}/lakehouse_monitoring/denial_risk_scorer_payload"


def _inference_table_exists() -> bool:
    try:
        spark.sql(f"SELECT 1 FROM {INFERENCE_TABLE} LIMIT 1")
        return True
    except Exception as e:
        print(f"Inference table {INFERENCE_TABLE} not queryable yet ({e}).")
        print("  It populates after the app calls the endpoint — skipping monitor creation this run.")
        return False


def _create_monitor_legacy() -> bool:
    """Older SDK surface: w.quality_monitors.create + MonitorInferenceLog."""
    from databricks.sdk.service.catalog import (
        MonitorInferenceLog,
        MonitorInferenceLogProblemType,
    )
    try:
        w.quality_monitors.get(table_name=INFERENCE_TABLE)
        print("Lakehouse Monitor already exists (quality_monitors) — refreshing.")
    except Exception:
        w.quality_monitors.create(
            table_name=INFERENCE_TABLE,
            assets_dir=_assets_dir,
            output_schema_name=_monitor_output_schema,
            inference_log=MonitorInferenceLog(
                granularities=["1 day"],
                timestamp_col="__db_request_time",
                prediction_col="denial_prob",
                model_id_col="served_entity_id",
                problem_type=MonitorInferenceLogProblemType.PROBLEM_TYPE_CLASSIFICATION,
            ),
        )
        print("Created Lakehouse Monitor (quality_monitors) on the inference table.")
    w.quality_monitors.run_refresh(table_name=INFERENCE_TABLE)
    return True


def _create_monitor_new() -> bool:
    """Newer SDK surface: w.data_quality.create_monitor + InferenceLogConfig."""
    from databricks.sdk.service.dataquality import (
        Monitor,
        DataProfilingConfig,
        InferenceLogConfig,
        InferenceProblemType,
        AggregationGranularity,
    )
    schema = w.schemas.get(full_name=_monitor_output_schema)
    table = w.tables.get(full_name=INFERENCE_TABLE)
    config = DataProfilingConfig(
        output_schema_id=schema.schema_id,
        assets_dir=_assets_dir,
        inference_log=InferenceLogConfig(
            problem_type=InferenceProblemType.INFERENCE_PROBLEM_TYPE_CLASSIFICATION,
            prediction_column="denial_prob",
            model_id_column="served_entity_id",
            timestamp_column="__db_request_time",
            granularities=[AggregationGranularity.AGGREGATION_GRANULARITY_1_DAY],
        ),
    )
    w.data_quality.create_monitor(
        monitor=Monitor(object_type="table", object_id=table.table_id, data_profiling_config=config)
    )
    print("Created Lakehouse Monitor (data_quality) on the inference table.")
    return True


if _inference_table_exists():
    _created = False
    for _attempt in (_create_monitor_legacy, _create_monitor_new):
        try:
            _created = _attempt()
            break
        except Exception as e:
            print(f"  {_attempt.__name__} unavailable: {type(e).__name__}: {e}")
    if not _created:
        print("Lakehouse Monitor SDK surface unavailable on this runtime — "
              "governance audit table is the fallback artifact.")

# COMMAND ----------

print("Denial model governance check complete.")
