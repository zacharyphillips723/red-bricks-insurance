# Databricks notebook source
# MAGIC %md
# MAGIC # Red Bricks Insurance — Denial Model Experimentation (Champion / Challenger)
# MAGIC
# MAGIC Operationalizes experimentation for the binary denial-risk model behind the
# MAGIC **Claim Scrubber**. Rather than blindly re-registering, it trains a *challenger*
# MAGIC and only promotes it to `@production` if it beats the current *champion* on a
# MAGIC held-out, time-based evaluation set — without regressing fairness.
# MAGIC
# MAGIC ### Pipeline
# MAGIC 1. **Versioned holdout** — time split on `service_year_month`; the most recent
# MAGIC    `HOLDOUT_MONTHS` become the eval set (persisted to `claims.denial_eval_holdout`).
# MAGIC 2. **Train challenger** — XGBoost binary with different hyperparameters; register
# MAGIC    a new version of `claims.denial_risk_model` aliased `@challenger`.
# MAGIC 3. **Champion vs challenger** — evaluate both `@production` and `@challenger` on the
# MAGIC    same holdout (AUC / F1 / precision / recall + LOB bias delta).
# MAGIC 4. **Gated promotion** — promote challenger → `@production` only if it clears the
# MAGIC    AUC-gain gate and does not worsen fairness.
# MAGIC 5. **Audit** — append the decision to `claims.denial_model_experiments`.
# MAGIC
# MAGIC Reuses the exact feature engineering + encoders from `train_denial_model.py`.

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance_catalog", "Catalog")
dbutils.widgets.text("promote_min_auc_gain", "0.005", "Min AUC gain to promote")
dbutils.widgets.text("holdout_months", "3", "Most-recent months held out for eval")

catalog = dbutils.widgets.get("catalog")
catalog_sql = f"`{catalog}`"
CLAIMS_SCHEMA = "claims"
MEMBERS_SCHEMA = "members"

PROMOTE_MIN_AUC_GAIN = float(dbutils.widgets.get("promote_min_auc_gain"))
HOLDOUT_MONTHS = int(dbutils.widgets.get("holdout_months"))
# Fairness guard: challenger's LOB bias delta may not exceed champion's by more than this.
BIAS_TOLERANCE = 0.02

RISK_MODEL_NAME = f"{catalog}.{CLAIMS_SCHEMA}.denial_risk_model"


def _tbl(schema: str, table: str) -> str:
    return f"`{catalog}`.{schema}.{table}"


# Engineered feature vector — MUST match train_denial_model.py (authoritative order).
FEATURE_COLS = [
    "billed_amount", "allowed_amount", "billed_allowed_ratio",
    "proc_denial_rate", "dx_denial_rate", "provider_denial_rate", "pxdx_denial_rate",
    "claim_type_code", "place_of_service_code", "line_of_business_code",
]

print(f"Catalog: {catalog}")
print(f"Risk model: {RISK_MODEL_NAME}")
print(f"Promote gate: challenger AUC must beat champion by >= {PROMOTE_MIN_AUC_GAIN}")
print(f"Holdout: most recent {HOLDOUT_MONTHS} month(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load labeled claims (with service_year_month for the time split)

# COMMAND ----------

raw_df = spark.sql(f"""
SELECT
  c.member_id,
  c.rendering_provider_npi,
  c.claim_type,
  CAST(c.place_of_service_code AS STRING)   AS place_of_service_code,
  c.procedure_code,
  c.primary_diagnosis_code,
  CAST(c.billed_amount AS DOUBLE)           AS billed_amount,
  CAST(c.allowed_amount AS DOUBLE)          AS allowed_amount,
  c.service_year_month,
  COALESCE(e.line_of_business, 'Unknown')   AS line_of_business,
  CASE WHEN LOWER(c.claim_status) = 'denied' THEN 1 ELSE 0 END AS is_denied
FROM {catalog_sql}.{CLAIMS_SCHEMA}.silver_claims_medical c
LEFT JOIN (
    SELECT member_id, MAX(line_of_business) AS line_of_business
    FROM {catalog_sql}.{MEMBERS_SCHEMA}.silver_enrollment
    GROUP BY member_id
) e ON c.member_id = e.member_id
WHERE c.procedure_code IS NOT NULL
  AND c.primary_diagnosis_code IS NOT NULL
  AND c.service_year_month IS NOT NULL
""")

pdf = raw_df.toPandas()
print(f"Loaded {len(pdf):,} claims; overall denial rate {pdf['is_denied'].mean():.3f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Versioned time-based holdout
# MAGIC
# MAGIC Train on everything up to the cutoff; evaluate on the most recent
# MAGIC `HOLDOUT_MONTHS` — a realistic "score the future" split rather than a random one.

# COMMAND ----------

import numpy as np
import pandas as pd

# service_year_month is a month-truncated date; rank the distinct months.
pdf["_ym"] = pd.to_datetime(pdf["service_year_month"]).dt.strftime("%Y-%m")
months = sorted(pdf["_ym"].unique())
holdout_months = set(months[-HOLDOUT_MONTHS:]) if len(months) > HOLDOUT_MONTHS else set(months[-1:])
cutoff = min(holdout_months)

is_holdout = pdf["_ym"].isin(holdout_months)
train_pdf = pdf[~is_holdout].reset_index(drop=True)
eval_pdf = pdf[is_holdout].reset_index(drop=True)
print(f"Months: {months[0]}..{months[-1]} | holdout = {sorted(holdout_months)} (cutoff {cutoff})")
print(f"Train rows: {len(train_pdf):,} | Holdout rows: {len(eval_pdf):,} "
      f"(holdout denial rate {eval_pdf['is_denied'].mean():.3f})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fit encoders on TRAIN only + engineer features
# MAGIC
# MAGIC Encoders are fit strictly on the training window so the holdout stays a true
# MAGIC out-of-time evaluation. Logic is copied verbatim from `train_denial_model.py`.

# COMMAND ----------

overall_rate = float(train_pdf["is_denied"].mean())


def _rate_map(frame: pd.DataFrame, key, min_n: int = 20) -> dict:
    """Denial-rate target encoding; categories below min_n fall back to overall."""
    grp = frame.groupby(key)["is_denied"].agg(["mean", "count"])
    grp = grp[grp["count"] >= min_n]
    if isinstance(key, list):
        return {"|".join(map(str, k)): float(v) for k, v in grp["mean"].items()}
    return {str(k): float(v) for k, v in grp["mean"].items()}


def _cat_map(frame: pd.DataFrame, col: str) -> dict:
    return {str(v): i for i, v in enumerate(sorted(frame[col].astype(str).unique()))}


_pxdx = train_pdf.assign(_px=train_pdf["procedure_code"].astype(str),
                         _dx=train_pdf["primary_diagnosis_code"].astype(str))

ENC = {
    "feature_cols": FEATURE_COLS,
    "overall_rate": overall_rate,
    "proc_rate": _rate_map(train_pdf, "procedure_code"),
    "dx_rate": _rate_map(train_pdf, "primary_diagnosis_code"),
    "provider_rate": _rate_map(train_pdf, "rendering_provider_npi"),
    "pxdx_rate": _rate_map(_pxdx, ["_px", "_dx"]),
    "claim_type_map": _cat_map(train_pdf, "claim_type"),
    "place_of_service_map": _cat_map(train_pdf, "place_of_service_code"),
    "line_of_business_map": _cat_map(train_pdf, "line_of_business"),
}


def engineer_features(frame: pd.DataFrame, enc: dict) -> np.ndarray:
    """Build the FEATURE_COLS matrix from raw claim fields (train_denial_model parity)."""
    f = frame.copy()
    px = f["procedure_code"].astype(str)
    dx = f["primary_diagnosis_code"].astype(str)
    npi = f["rendering_provider_npi"].astype(str)
    o = enc["overall_rate"]
    billed = pd.to_numeric(f["billed_amount"], errors="coerce").fillna(0.0)
    allowed = pd.to_numeric(f["allowed_amount"], errors="coerce").fillna(0.0)
    out = pd.DataFrame({
        "billed_amount": billed,
        "allowed_amount": allowed,
        "billed_allowed_ratio": billed / (allowed + 1.0),
        "proc_denial_rate": px.map(enc["proc_rate"]).fillna(o),
        "dx_denial_rate": dx.map(enc["dx_rate"]).fillna(o),
        "provider_denial_rate": npi.map(enc["provider_rate"]).fillna(o),
        "pxdx_denial_rate": (px + "|" + dx).map(enc["pxdx_rate"]).fillna(o),
        "claim_type_code": f["claim_type"].astype(str).map(enc["claim_type_map"]).fillna(-1),
        "place_of_service_code": f["place_of_service_code"].astype(str).map(enc["place_of_service_map"]).fillna(-1),
        "line_of_business_code": f["line_of_business"].astype(str).map(enc["line_of_business_map"]).fillna(-1),
    })
    return out[enc["feature_cols"]].astype(float).values


X_train = engineer_features(train_pdf, ENC)
y_train = train_pdf["is_denied"].values
X_eval = engineer_features(eval_pdf, ENC)
y_eval = eval_pdf["is_denied"].values
print(f"Train X {X_train.shape} | Eval X {X_eval.shape}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persist the versioned holdout eval dataset

# COMMAND ----------

_eval_out = pd.DataFrame(X_eval, columns=FEATURE_COLS)
_eval_out["is_denied"] = y_eval
_eval_out["line_of_business"] = eval_pdf["line_of_business"].astype(str).values
_eval_out["service_year_month"] = eval_pdf["_ym"].values

spark.sql(f"DROP TABLE IF EXISTS {_tbl(CLAIMS_SCHEMA, 'denial_eval_holdout')}")
(spark.createDataFrame(_eval_out)
    .write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(_tbl(CLAIMS_SCHEMA, "denial_eval_holdout")))
print(f"Wrote {len(_eval_out):,} rows to {catalog}.{CLAIMS_SCHEMA}.denial_eval_holdout")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train the challenger
# MAGIC
# MAGIC Deliberately different hyperparameters than the champion (`max_depth` 5→7,
# MAGIC `learning_rate` 0.1→0.05, `n_estimators` 200→350, plus row/col subsampling).

# COMMAND ----------

import mlflow
import mlflow.xgboost
import xgboost as xgb
from sklearn.metrics import (
    roc_auc_score, f1_score, precision_score, recall_score,
)

mlflow.set_registry_uri("databricks-uc")
_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
EXPERIMENT = f"/Users/{_user}/{catalog}_denial_experiments"
mlflow.set_experiment(EXPERIMENT)
client = mlflow.MlflowClient()

CHALLENGER_PARAMS = {
    "max_depth": 7,
    "learning_rate": 0.05,
    "n_estimators": 350,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "objective": "binary:logistic",
    "eval_metric": "logloss",
    "scale_pos_weight": float((y_train == 0).sum() / max((y_train == 1).sum(), 1)),
    "random_state": 42,
}

with mlflow.start_run(run_name="challenger_train") as challenger_run:
    mlflow.log_params(CHALLENGER_PARAMS)
    mlflow.log_param("holdout_months", sorted(holdout_months))
    mlflow.log_param("train_rows", len(train_pdf))
    try:
        _ds = mlflow.data.from_pandas(
            _eval_out, name="denial_eval_holdout", targets="is_denied")
        mlflow.log_input(_ds, context="evaluation")
    except Exception as _de:
        print(f"(dataset logging skipped: {_de})")

    challenger = xgb.XGBClassifier(**CHALLENGER_PARAMS)
    challenger.fit(X_train, y_train)
    mlflow.xgboost.log_model(
        challenger,
        artifact_path="model",
        input_example=pd.DataFrame([X_train[0]], columns=FEATURE_COLS),
        registered_model_name=RISK_MODEL_NAME,
    )

challenger_v = max(client.search_model_versions(f"name='{RISK_MODEL_NAME}'"),
                   key=lambda v: int(v.version))
client.set_registered_model_alias(RISK_MODEL_NAME, "challenger", challenger_v.version)
print(f"Challenger registered as {RISK_MODEL_NAME} v{challenger_v.version} (@challenger)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluate champion vs challenger on the same holdout

# COMMAND ----------

champion_mv = client.get_model_version_by_alias(RISK_MODEL_NAME, "production")
champion_v = champion_mv.version
print(f"Champion @production = v{champion_v} | Challenger @challenger = v{challenger_v.version}")

champion_model = mlflow.xgboost.load_model(f"models:/{RISK_MODEL_NAME}@production")
challenger_model = mlflow.xgboost.load_model(f"models:/{RISK_MODEL_NAME}@challenger")


def _proba(model, X: np.ndarray) -> np.ndarray:
    """P(denied) that tolerates either an sklearn wrapper or a raw Booster."""
    if hasattr(model, "predict_proba"):
        return model.predict_proba(X)[:, 1]
    return np.asarray(model.predict(xgb.DMatrix(X, feature_names=FEATURE_COLS)))


def _lob_bias_delta(prob: np.ndarray) -> float:
    """Max abs spread in predicted-denial rate across LOB groups (n>=10)."""
    lob = eval_pdf["line_of_business"].astype(str).values
    rates = []
    for val in np.unique(lob):
        mask = lob == val
        if int(mask.sum()) >= 10:
            rates.append(float((prob[mask] >= 0.5).mean()))
    return float(max(rates) - min(rates)) if len(rates) >= 2 else 0.0


def _metrics(model) -> dict:
    prob = _proba(model, X_eval)
    pred = (prob >= 0.5).astype(int)
    single_class = len(np.unique(y_eval)) < 2
    return {
        "auc": (float("nan") if single_class else float(roc_auc_score(y_eval, prob))),
        "f1": float(f1_score(y_eval, pred, zero_division=0)),
        "precision": float(precision_score(y_eval, pred, zero_division=0)),
        "recall": float(recall_score(y_eval, pred, zero_division=0)),
        "bias_delta": _lob_bias_delta(prob),
    }


champ = _metrics(champion_model)
chall = _metrics(challenger_model)

for name, run_name, m, ver in [
    ("Champion", "champion_eval", champ, champion_v),
    ("Challenger", "challenger_eval", chall, challenger_v.version),
]:
    with mlflow.start_run(run_name=run_name):
        mlflow.set_tag("model_version", str(ver))
        for k, v in m.items():
            if v == v:  # skip NaN
                mlflow.log_metric(k, v)
    print(f"{name} v{ver}: AUC={m['auc']:.4f} F1={m['f1']:.4f} "
          f"P={m['precision']:.4f} R={m['recall']:.4f} bias_delta={m['bias_delta']:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gated promotion decision

# COMMAND ----------

import json
from datetime import datetime

champ_auc = champ["auc"] if champ["auc"] == champ["auc"] else 0.0
chall_auc = chall["auc"] if chall["auc"] == chall["auc"] else 0.0
auc_gain = chall_auc - champ_auc
bias_ok = chall["bias_delta"] <= champ["bias_delta"] + BIAS_TOLERANCE
auc_ok = auc_gain >= PROMOTE_MIN_AUC_GAIN
promoted = bool(auc_ok and bias_ok)

if promoted:
    reason = (f"AUC gain {auc_gain:+.4f} >= {PROMOTE_MIN_AUC_GAIN} and fairness preserved "
              f"(bias {chall['bias_delta']:.4f} <= {champ['bias_delta'] + BIAS_TOLERANCE:.4f}).")
elif not auc_ok:
    reason = f"AUC gain {auc_gain:+.4f} below promotion threshold {PROMOTE_MIN_AUC_GAIN}."
else:
    reason = (f"Fairness regressed: challenger bias {chall['bias_delta']:.4f} exceeds "
              f"champion {champ['bias_delta']:.4f} + tolerance {BIAS_TOLERANCE}.")

if promoted:
    client.set_registered_model_alias(RISK_MODEL_NAME, "production", challenger_v.version)
    print(f"PROMOTED challenger v{challenger_v.version} → @production. {reason}")
    print("ACTION REQUIRED: re-run deploy_denial_endpoint to serve the promoted model.")
else:
    print(f"NOT promoted; @production stays v{champion_v}. {reason}")
print(f"Challenger remains @challenger = v{challenger_v.version}")

# Decision artifact on the challenger run.
decision = {
    "experiment_ts": datetime.now().isoformat(),
    "champion_version": str(champion_v),
    "challenger_version": str(challenger_v.version),
    "champion_auc": champ_auc,
    "challenger_auc": chall_auc,
    "auc_gain": auc_gain,
    "champion_bias_delta": champ["bias_delta"],
    "challenger_bias_delta": chall["bias_delta"],
    "promoted": promoted,
    "decision_reason": reason,
}
with mlflow.start_run(run_id=challenger_run.info.run_id):
    mlflow.log_dict(decision, "promotion_decision.json")
    mlflow.set_tag("governance_type", "experiment")
    mlflow.log_metric("promoted", int(promoted))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Append to the experiment audit table

# COMMAND ----------

audit_row = {
    "experiment_ts": decision["experiment_ts"],
    "champion_version": decision["champion_version"],
    "challenger_version": decision["challenger_version"],
    "champion_auc": round(champ_auc, 5),
    "challenger_auc": round(chall_auc, 5),
    "champion_f1": round(champ["f1"], 5),
    "challenger_f1": round(chall["f1"], 5),
    "champion_bias_delta": round(champ["bias_delta"], 5),
    "challenger_bias_delta": round(chall["bias_delta"], 5),
    "auc_gain": round(auc_gain, 5),
    "promoted": promoted,
    "decision_reason": reason,
    "mlflow_experiment": EXPERIMENT,
}
(spark.createDataFrame(pd.DataFrame([audit_row]))
    .write.mode("append").option("mergeSchema", "true")
    .saveAsTable(_tbl(CLAIMS_SCHEMA, "denial_model_experiments")))
print(f"Appended decision to {catalog}.{CLAIMS_SCHEMA}.denial_model_experiments")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 68)
print("DENIAL MODEL EXPERIMENT — CHAMPION vs CHALLENGER")
print("=" * 68)
print(f"  {'metric':<20}{'champion':>14}{'challenger':>14}")
print(f"  {'-'*20}{'-'*14:>14}{'-'*14:>14}")
print(f"  {'version':<20}{('v'+str(champion_v)):>14}{('v'+str(challenger_v.version)):>14}")
print(f"  {'AUC':<20}{champ_auc:>14.4f}{chall_auc:>14.4f}")
print(f"  {'F1':<20}{champ['f1']:>14.4f}{chall['f1']:>14.4f}")
print(f"  {'precision':<20}{champ['precision']:>14.4f}{chall['precision']:>14.4f}")
print(f"  {'recall':<20}{champ['recall']:>14.4f}{chall['recall']:>14.4f}")
print(f"  {'LOB bias delta':<20}{champ['bias_delta']:>14.4f}{chall['bias_delta']:>14.4f}")
print("-" * 68)
print(f"  AUC gain: {auc_gain:+.4f} (gate {PROMOTE_MIN_AUC_GAIN})")
print(f"  DECISION: {'PROMOTED' if promoted else 'NOT PROMOTED'} — {reason}")
print("=" * 68)
