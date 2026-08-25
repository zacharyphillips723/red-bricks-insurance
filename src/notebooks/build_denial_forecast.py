# Databricks notebook source
# MAGIC %md
# MAGIC # Red Bricks Insurance — Denial Trend Forecast
# MAGIC
# MAGIC Forecasts the next 6 months of denial trends and writes
# MAGIC `claims.gold_denial_forecast` — the source for the **Denial Intelligence**
# MAGIC tab's forecast chart in the provider Claim Scrubber app.
# MAGIC
# MAGIC Three monthly series are projected: `denial_rate`, `denied_count`, and
# MAGIC `denied_amount`, built from `claims.silver_claims_medical`.
# MAGIC
# MAGIC ### Forecast engine
# MAGIC Primary path is Databricks' native **`ai_forecast`** SQL table-valued
# MAGIC function (Public Preview as of 2025; requires a **Pro or Serverless SQL
# MAGIC warehouse**), run through the SQL Statement Execution API. If `ai_forecast`
# MAGIC is unavailable (preview not enabled, no suitable warehouse), the notebook
# MAGIC falls back to **Prophet**, then to a naive trend+seasonal forecast — so the
# MAGIC output table is always populated. The engine used is recorded per row in a
# MAGIC `method` column.

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance_catalog", "Catalog")
catalog = dbutils.widgets.get("catalog")
catalog_sql = f"`{catalog}`"

CLAIMS_SCHEMA = "claims"
SOURCE_TABLE = f"{catalog_sql}.{CLAIMS_SCHEMA}.silver_claims_medical"
FORECAST_TABLE = f"{catalog}.{CLAIMS_SCHEMA}.gold_denial_forecast"
HORIZON = 6
METRICS = ["denial_rate", "denied_count", "denied_amount"]

print(f"Source:   {SOURCE_TABLE}")
print(f"Forecast: {FORECAST_TABLE}")
print(f"Horizon:  {HORIZON} months | metrics: {METRICS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monthly history
# MAGIC
# MAGIC Long-form (ds, metric, y) so each metric forecasts independently.

# COMMAND ----------

from datetime import datetime

hist_wide = spark.sql(f"""
    SELECT
        service_year_month AS ds,
        COUNT(*) AS claim_count,
        SUM(CASE WHEN LOWER(claim_status) = 'denied' THEN 1 ELSE 0 END) AS denied_count,
        SUM(CASE WHEN LOWER(claim_status) = 'denied' THEN billed_amount ELSE 0 END) AS denied_amount
    FROM {SOURCE_TABLE}
    WHERE service_year_month IS NOT NULL
    GROUP BY service_year_month
    ORDER BY service_year_month
""").toPandas()

# Derive the rate and melt to long form.
hist_wide["denial_rate"] = hist_wide["denied_count"] / hist_wide["claim_count"].replace(0, 1)
hist_wide["ds"] = hist_wide["ds"].astype("datetime64[ns]")

history = {
    m: hist_wide[["ds", m]].rename(columns={m: "y"}).dropna().sort_values("ds").reset_index(drop=True)
    for m in METRICS
}
print(f"History months: {len(hist_wide)} "
      f"({hist_wide['ds'].min().date()} → {hist_wide['ds'].max().date()})")
display(hist_wide)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers — SQL warehouse resolution + Statement Execution

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()


def _resolve_warehouse() -> str:
    """Prefer a RUNNING serverless/pro warehouse; else any warehouse."""
    whs = list(w.warehouses.list())
    running = [x for x in whs if x.state and x.state.value == "RUNNING"]
    for pool in (running, whs):
        for x in pool:
            # Serverless / Pro warehouses support ai_forecast.
            if getattr(x, "enable_serverless_compute", False) or (x.warehouse_type and "PRO" in str(x.warehouse_type)):
                return x.id
        if pool:
            return pool[0].id
    raise RuntimeError("No SQL warehouse available for ai_forecast.")


def _run_sql(sql: str, warehouse_id: str, timeout_s: int = 300) -> list[dict]:
    """Execute a statement on the SQL warehouse; return rows as dicts."""
    import time as _t
    from databricks.sdk.service.sql import StatementState

    stmt = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id, statement=sql, wait_timeout="30s",
    )
    sid = stmt.statement_id
    deadline = _t.monotonic() + timeout_s
    while stmt.status and stmt.status.state in (StatementState.PENDING, StatementState.RUNNING):
        if _t.monotonic() > deadline:
            raise TimeoutError(f"ai_forecast statement {sid} timed out")
        _t.sleep(2)
        stmt = w.statement_execution.get_statement(sid)
    if stmt.status and stmt.status.state != StatementState.SUCCEEDED:
        err = stmt.status.error.message if stmt.status.error else "unknown"
        raise RuntimeError(f"ai_forecast failed: {err}")
    if not stmt.result or not stmt.result.data_array:
        return []
    cols = [c.name for c in stmt.manifest.schema.columns]
    return [dict(zip(cols, row)) for row in stmt.result.data_array]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Forecast — ai_forecast (primary) → Prophet → naive (fallbacks)

# COMMAND ----------

import pandas as pd
from dateutil.relativedelta import relativedelta


def _metric_agg_sql(metric: str) -> str:
    """The (ds, y) subquery feeding ai_forecast, per metric."""
    y_expr = {
        "denied_count": "SUM(CASE WHEN LOWER(claim_status)='denied' THEN 1 ELSE 0 END)",
        "denied_amount": "SUM(CASE WHEN LOWER(claim_status)='denied' THEN billed_amount ELSE 0 END)",
        "denial_rate": ("SUM(CASE WHEN LOWER(claim_status)='denied' THEN 1 ELSE 0 END) "
                        "/ NULLIF(COUNT(*),0)"),
    }[metric]
    return (f"SELECT service_year_month AS ds, CAST({y_expr} AS DOUBLE) AS y "
            f"FROM {SOURCE_TABLE} WHERE service_year_month IS NOT NULL "
            f"GROUP BY service_year_month")


def forecast_ai(metric: str, warehouse_id: str) -> pd.DataFrame:
    """ai_forecast TVF → DataFrame(ds, forecast, lower, upper)."""
    sql = f"""
        SELECT ds, y_forecast AS forecast, y_lower AS lower, y_upper AS upper
        FROM ai_forecast(
            TABLE({_metric_agg_sql(metric)}),
            horizon => {HORIZON},
            time_col => 'ds',
            value_col => 'y'
        )
    """
    rows = _run_sql(sql, warehouse_id)
    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError(f"ai_forecast returned no rows for {metric}")
    df["ds"] = pd.to_datetime(df["ds"])
    for c in ("forecast", "lower", "upper"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.sort_values("ds").reset_index(drop=True)


def forecast_prophet(metric: str) -> pd.DataFrame:
    from prophet import Prophet  # noqa: F401 (import guarded by caller)

    h = history[metric].rename(columns={"ds": "ds", "y": "y"})
    m = Prophet(interval_width=0.8, seasonality_mode="additive",
                yearly_seasonality=True, weekly_seasonality=False, daily_seasonality=False)
    m.fit(h)
    future = m.make_future_dataframe(periods=HORIZON, freq="MS")
    fc = m.predict(future).tail(HORIZON)
    return pd.DataFrame({
        "ds": pd.to_datetime(fc["ds"]),
        "forecast": fc["yhat"].values,
        "lower": fc["yhat_lower"].values,
        "upper": fc["yhat_upper"].values,
    }).reset_index(drop=True)


def forecast_naive(metric: str) -> pd.DataFrame:
    """Linear trend on the last 12 months + ±1σ band."""
    import numpy as np

    h = history[metric].tail(12).reset_index(drop=True)
    y = h["y"].values.astype(float)
    x = np.arange(len(y))
    slope, intercept = np.polyfit(x, y, 1) if len(y) >= 2 else (0.0, float(y.mean()))
    resid_std = float(np.std(y - (slope * x + intercept))) if len(y) >= 2 else float(np.std(y) or 1.0)
    last_ds = history[metric]["ds"].max()
    rows = []
    for k in range(1, HORIZON + 1):
        yhat = intercept + slope * (len(y) - 1 + k)
        rows.append({
            "ds": pd.Timestamp(last_ds) + relativedelta(months=k),
            "forecast": yhat, "lower": yhat - 1.96 * resid_std, "upper": yhat + 1.96 * resid_std,
        })
    return pd.DataFrame(rows)


def _ensure_prophet() -> bool:
    try:
        import prophet  # noqa: F401
        return True
    except Exception:
        try:
            import subprocess, sys
            subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "prophet"])
            import prophet  # noqa: F401
            return True
        except Exception as e:
            print(f"  Prophet unavailable: {e}")
            return False

# COMMAND ----------

# Try ai_forecast for all metrics first; fall back as a whole so `method` is consistent.
method = None
forecasts: dict[str, pd.DataFrame] = {}

try:
    wid = _resolve_warehouse()
    print(f"Trying ai_forecast on warehouse {wid} ...")
    forecasts = {m: forecast_ai(m, wid) for m in METRICS}
    method = "ai_forecast"
    print("ai_forecast succeeded for all metrics.")
except Exception as e:
    print(f"ai_forecast path failed ({e}); trying Prophet ...")
    if _ensure_prophet():
        try:
            forecasts = {m: forecast_prophet(m) for m in METRICS}
            method = "prophet"
            print("Prophet succeeded for all metrics.")
        except Exception as pe:
            print(f"Prophet failed ({pe}); using naive fallback.")
    if method is None:
        forecasts = {m: forecast_naive(m) for m in METRICS}
        method = "naive_trend"
        print("Naive trend fallback used.")

print(f"Forecast method: {method}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Assemble actuals + forecast and write the table

# COMMAND ----------

generated_at = datetime.utcnow()
records = []

for m in METRICS:
    # Historical actuals (continuous line up to the last observed month).
    for _, r in history[m].iterrows():
        records.append({
            "ds": pd.Timestamp(r["ds"]).date(), "metric": m,
            "actual": float(r["y"]), "forecast": float(r["y"]),
            "lower": None, "upper": None, "is_forecast": False,
            "method": method, "generated_at": generated_at,
        })
    # Future forecast months.
    for _, r in forecasts[m].iterrows():
        records.append({
            "ds": pd.Timestamp(r["ds"]).date(), "metric": m,
            "actual": None, "forecast": float(r["forecast"]),
            "lower": None if pd.isna(r["lower"]) else float(r["lower"]),
            "upper": None if pd.isna(r["upper"]) else float(r["upper"]),
            "is_forecast": True, "method": method, "generated_at": generated_at,
        })

from pyspark.sql.types import (
    StructType, StructField, DateType, StringType, DoubleType, BooleanType, TimestampType,
)

schema = StructType([
    StructField("ds", DateType(), False),
    StructField("metric", StringType(), False),
    StructField("actual", DoubleType(), True),
    StructField("forecast", DoubleType(), True),
    StructField("lower", DoubleType(), True),
    StructField("upper", DoubleType(), True),
    StructField("is_forecast", BooleanType(), False),
    StructField("method", StringType(), True),
    StructField("generated_at", TimestampType(), True),
])

fc_df = spark.createDataFrame(records, schema=schema)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_sql}.{CLAIMS_SCHEMA}")
(fc_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(FORECAST_TABLE))

spark.sql(f"""
    COMMENT ON TABLE {FORECAST_TABLE} IS
    'Monthly denial-trend forecast (denial_rate, denied_count, denied_amount) for the provider Claim Scrubber Denial Intelligence tab. Includes historical actuals + a 6-month forecast with confidence bounds. Produced by build_denial_forecast (ai_forecast primary; Prophet/naive fallback).'
""")

print(f"Wrote {fc_df.count()} rows to {FORECAST_TABLE} (method={method})")

# COMMAND ----------

display(spark.sql(f"""
    SELECT metric, ds, actual, forecast, lower, upper, is_forecast
    FROM {FORECAST_TABLE}
    WHERE is_forecast = true
    ORDER BY metric, ds
"""))

# COMMAND ----------

print(f"Denial forecast build complete — method={method}, horizon={HORIZON} months.")
