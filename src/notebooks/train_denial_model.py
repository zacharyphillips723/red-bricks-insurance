# Databricks notebook source
# MAGIC %md
# MAGIC # Red Bricks Insurance — Claim Denial Risk Model
# MAGIC
# MAGIC Trains the models behind the provider-facing **Denial Risk Predictor /
# MAGIC Claim Scrubber**. From historical adjudicated claims it learns:
# MAGIC
# MAGIC 1. **`claims.denial_risk_model`** — binary XGBoost: P(claim is denied).
# MAGIC 2. **`claims.denial_reason_model`** — multiclass XGBoost trained on denied
# MAGIC    claims: which CARC `reason_category` is most likely.
# MAGIC 3. **`claims.denial_scorer`** — an `mlflow.pyfunc` wrapper that bundles both
# MAGIC    models + the feature encoders and, given a single draft claim, returns
# MAGIC    `{"denial_prob": float, "reason_probs": {category: prob, ...}}`. This is
# MAGIC    what the `denial-risk-scorer` serving endpoint hosts.
# MAGIC
# MAGIC ### Pipeline
# MAGIC 1. Feature engineering from `claims.silver_claims_medical` (+ enrollment LOB)
# MAGIC 2. Encoder fit (frequency/target encodings + categorical maps)
# MAGIC 3. XGBoost training (binary + multiclass) with SHAP / bias / drift artifacts
# MAGIC 4. UC registration (both boosters + the pyfunc wrapper) with @production
# MAGIC 5. Batch inference → `claims.gold_denial_risk_scores`

# COMMAND ----------

# MAGIC %pip install shap --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance_catalog", "Catalog")

catalog = dbutils.widgets.get("catalog")
catalog_sql = f"`{catalog}`"
CLAIMS_SCHEMA = "claims"
MEMBERS_SCHEMA = "members"

RISK_MODEL_NAME = f"{catalog}.{CLAIMS_SCHEMA}.denial_risk_model"
REASON_MODEL_NAME = f"{catalog}.{CLAIMS_SCHEMA}.denial_reason_model"
SCORER_MODEL_NAME = f"{catalog}.{CLAIMS_SCHEMA}.denial_scorer"


def _tbl(schema: str, table: str) -> str:
    return f"`{catalog}`.{schema}.{table}"


# Canonical reason-category label space (matches claims.carc_reference).
CANONICAL_REASONS = [
    "no_auth", "not_medically_necessary", "experimental", "missing_info",
    "coding_mismatch", "eligibility", "frequency_limit", "other",
]

# Raw record fields the served scorer accepts.
RAW_FIELDS = [
    "procedure_code", "primary_diagnosis_code", "claim_type",
    "place_of_service_code", "billed_amount", "allowed_amount",
    "line_of_business", "rendering_provider_npi",
]

# Engineered feature vector (order is authoritative — shared by both models).
FEATURE_COLS = [
    "billed_amount", "allowed_amount", "billed_allowed_ratio",
    "proc_denial_rate", "dx_denial_rate", "provider_denial_rate", "pxdx_denial_rate",
    "claim_type_code", "place_of_service_code", "line_of_business_code",
]

print(f"Catalog: {catalog}")
print(f"Risk model:   {RISK_MODEL_NAME}")
print(f"Reason model: {REASON_MODEL_NAME}")
print(f"Scorer:       {SCORER_MODEL_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load labeled claims
# MAGIC
# MAGIC `reason_category` comes from joining `denial_reason_code` to
# MAGIC `claims.carc_reference`; denied claims whose code isn't in the reference
# MAGIC map to `other`. Non-denied claims have `reason_category = NULL`.

# COMMAND ----------

raw_df = spark.sql(f"""
SELECT
  c.claim_id,
  c.member_id,
  c.rendering_provider_npi,
  c.claim_type,
  CAST(c.place_of_service_code AS STRING)   AS place_of_service_code,
  c.procedure_code,
  c.primary_diagnosis_code,
  CAST(c.billed_amount AS DOUBLE)           AS billed_amount,
  CAST(c.allowed_amount AS DOUBLE)          AS allowed_amount,
  c.claim_status,
  c.denial_reason_code,
  COALESCE(e.line_of_business, 'Unknown')   AS line_of_business,
  CASE WHEN LOWER(c.claim_status) = 'denied' THEN 1 ELSE 0 END AS is_denied,
  CASE
    WHEN LOWER(c.claim_status) = 'denied'
      THEN COALESCE(ref.reason_category, 'other')
    ELSE NULL
  END AS reason_category
FROM {catalog_sql}.{CLAIMS_SCHEMA}.silver_claims_medical c
LEFT JOIN (
    SELECT member_id, MAX(line_of_business) AS line_of_business
    FROM {catalog_sql}.{MEMBERS_SCHEMA}.silver_enrollment
    GROUP BY member_id
) e ON c.member_id = e.member_id
LEFT JOIN {catalog_sql}.{CLAIMS_SCHEMA}.carc_reference ref
    ON c.denial_reason_code = ref.carc_code
WHERE c.procedure_code IS NOT NULL
  AND c.primary_diagnosis_code IS NOT NULL
""")

pdf = raw_df.toPandas()
print(f"Loaded {len(pdf):,} claims")
print(f"Denial rate: {pdf['is_denied'].mean():.3f}")
print("Reason distribution (denied only):")
print(pdf.loc[pdf.is_denied == 1, "reason_category"].value_counts())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fit encoders
# MAGIC
# MAGIC Frequency/target encodings (denial rate per procedure, diagnosis, provider,
# MAGIC and dx↔px pair) plus integer maps for the categorical columns. Everything is
# MAGIC captured in a plain-dict `ENC` so it can be serialized and reproduced
# MAGIC identically at serving time inside the pyfunc wrapper.

# COMMAND ----------

import numpy as np
import pandas as pd

overall_rate = float(pdf["is_denied"].mean())


def _rate_map(frame: pd.DataFrame, key, min_n: int = 20) -> dict:
    """Denial-rate target encoding; categories below min_n fall back to overall."""
    grp = frame.groupby(key)["is_denied"].agg(["mean", "count"])
    grp = grp[grp["count"] >= min_n]
    if isinstance(key, list):
        return {"|".join(map(str, k)): float(v) for k, v in grp["mean"].items()}
    return {str(k): float(v) for k, v in grp["mean"].items()}


def _cat_map(frame: pd.DataFrame, col: str) -> dict:
    return {str(v): i for i, v in enumerate(sorted(frame[col].astype(str).unique()))}


pxdx_key = pdf.assign(_px=pdf["procedure_code"].astype(str),
                      _dx=pdf["primary_diagnosis_code"].astype(str))

ENC = {
    "feature_cols": FEATURE_COLS,
    "raw_fields": RAW_FIELDS,
    "overall_rate": overall_rate,
    "proc_rate": _rate_map(pdf, "procedure_code"),
    "dx_rate": _rate_map(pdf, "primary_diagnosis_code"),
    "provider_rate": _rate_map(pdf, "rendering_provider_npi"),
    "pxdx_rate": _rate_map(pxdx_key, ["_px", "_dx"]),
    "claim_type_map": _cat_map(pdf, "claim_type"),
    "place_of_service_map": _cat_map(pdf, "place_of_service_code"),
    "line_of_business_map": _cat_map(pdf, "line_of_business"),
}
print(f"Encoders fit — overall denial rate {overall_rate:.3f}; "
      f"{len(ENC['proc_rate'])} procedures, {len(ENC['provider_rate'])} providers encoded")

# COMMAND ----------


def engineer_features(frame: pd.DataFrame, enc: dict) -> np.ndarray:
    """Build the FEATURE_COLS matrix from raw claim fields using fitted encoders.

    IDENTICAL logic is reproduced inside DenialScorer._engineer for serving —
    keep the two in sync.
    """
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


X = engineer_features(pdf, ENC)
y_binary = pdf["is_denied"].values
print(f"Feature matrix: {X.shape}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train binary denial-risk model

# COMMAND ----------

import mlflow
import mlflow.xgboost
import xgboost as xgb
from sklearn.model_selection import StratifiedKFold
from sklearn.metrics import f1_score, accuracy_score, roc_auc_score, classification_report

mlflow.set_registry_uri("databricks-uc")
user = spark.sql("SELECT current_user()").first()[0]
mlflow.set_experiment(f"/Users/{user}/{catalog}_denial_risk")

BINARY_PARAMS = {
    "max_depth": 5,
    "learning_rate": 0.1,
    "n_estimators": 200,
    "objective": "binary:logistic",
    "eval_metric": "logloss",
    "scale_pos_weight": float((y_binary == 0).sum() / max((y_binary == 1).sum(), 1)),
    "random_state": 42,
}

with mlflow.start_run(run_name="denial_risk_binary") as risk_run:
    mlflow.log_params(BINARY_PARAMS)
    mlflow.log_param("feature_columns", FEATURE_COLS)
    mlflow.log_param("sample_count", len(y_binary))

    skf = StratifiedKFold(n_splits=3, shuffle=True, random_state=42)
    cv_auc = []
    for train_idx, val_idx in skf.split(X, y_binary):
        m = xgb.XGBClassifier(**BINARY_PARAMS)
        m.fit(X[train_idx], y_binary[train_idx])
        proba = m.predict_proba(X[val_idx])[:, 1]
        cv_auc.append(roc_auc_score(y_binary[val_idx], proba))
    mlflow.log_metric("cv_auc_mean", float(np.mean(cv_auc)))
    print(f"Binary CV AUC: {np.mean(cv_auc):.4f}")

    risk_model = xgb.XGBClassifier(**BINARY_PARAMS)
    risk_model.fit(X, y_binary)
    mlflow.log_metric("train_auc", roc_auc_score(y_binary, risk_model.predict_proba(X)[:, 1]))
    mlflow.log_dict(dict(zip(FEATURE_COLS, risk_model.feature_importances_.tolist())),
                    "feature_importance.json")

    mlflow.xgboost.log_model(
        risk_model,
        artifact_path="model",
        input_example=pd.DataFrame([X[0]], columns=FEATURE_COLS),
        registered_model_name=RISK_MODEL_NAME,
    )

client = mlflow.MlflowClient()
risk_v = max(client.search_model_versions(f"name='{RISK_MODEL_NAME}'"), key=lambda v: int(v.version))
client.set_registered_model_alias(RISK_MODEL_NAME, "production", risk_v.version)
print(f"Registered {RISK_MODEL_NAME} v{risk_v.version} (@production)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train multiclass reason model (denied claims only)

# COMMAND ----------

from sklearn.preprocessing import LabelEncoder

denied = pdf[pdf.is_denied == 1].reset_index(drop=True)
X_denied = engineer_features(denied, ENC)

reason_le = LabelEncoder()
y_reason = reason_le.fit_transform(denied["reason_category"].astype(str))
REASON_CLASSES = list(reason_le.classes_)
print(f"Reason classes ({len(REASON_CLASSES)}): {REASON_CLASSES}")

REASON_PARAMS = {
    "max_depth": 5,
    "learning_rate": 0.1,
    "n_estimators": 200,
    "objective": "multi:softprob",
    "num_class": len(REASON_CLASSES),
    "eval_metric": "mlogloss",
    "random_state": 42,
}

with mlflow.start_run(run_name="denial_reason_multiclass") as reason_run:
    mlflow.log_params(REASON_PARAMS)
    mlflow.log_param("reason_classes", REASON_CLASSES)

    reason_model = xgb.XGBClassifier(**REASON_PARAMS)
    reason_model.fit(X_denied, y_reason)
    reason_preds = reason_model.predict(X_denied)
    mlflow.log_metric("train_f1", f1_score(y_reason, reason_preds, average="weighted"))
    mlflow.log_metric("train_accuracy", accuracy_score(y_reason, reason_preds))
    report = classification_report(y_reason, reason_preds, target_names=REASON_CLASSES)
    print(report)
    mlflow.log_text(report, "reason_classification_report.txt")

    mlflow.xgboost.log_model(
        reason_model,
        artifact_path="model",
        input_example=pd.DataFrame([X_denied[0]], columns=FEATURE_COLS),
        registered_model_name=REASON_MODEL_NAME,
    )

reason_v = max(client.search_model_versions(f"name='{REASON_MODEL_NAME}'"), key=lambda v: int(v.version))
client.set_registered_model_alias(REASON_MODEL_NAME, "production", reason_v.version)
print(f"Registered {REASON_MODEL_NAME} v{reason_v.version} (@production)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Package the pyfunc scorer
# MAGIC
# MAGIC Bundles both boosters + the encoders + the reason-class ordering. Given a
# MAGIC batch of raw draft-claim records it returns, per row,
# MAGIC `{"denial_prob", "reason_probs"}` over the canonical reason categories
# MAGIC (categories the reason model never saw are reported as 0.0). This is the
# MAGIC single object the serving endpoint hosts.

# COMMAND ----------

import json
import os
import tempfile

import mlflow.pyfunc


class DenialScorer(mlflow.pyfunc.PythonModel):
    """Serves denial probability + reason distribution for a draft claim."""

    def load_context(self, context):
        import json as _json
        import xgboost as _xgb

        with open(context.artifacts["encoders"]) as fh:
            self.enc = _json.load(fh)
        self.reason_classes = self.enc["reason_classes"]
        self.canonical = self.enc["canonical_reasons"]
        self.feature_cols = self.enc["feature_cols"]
        self.feature_labels = self.enc.get("feature_labels", {})
        self.risk = _xgb.XGBClassifier()
        self.risk.load_model(context.artifacts["risk_model"])
        self.reason = _xgb.XGBClassifier()
        self.reason.load_model(context.artifacts["reason_model"])
        # Per-claim local explanations use XGBoost's native SHAP (booster
        # pred_contribs) — no extra library, always available in the serving image.
        self._booster = self.risk.get_booster()

    def _engineer(self, frame):
        import pandas as _pd

        enc = self.enc
        f = frame.copy()
        px = f["procedure_code"].astype(str)
        dx = f["primary_diagnosis_code"].astype(str)
        npi = f["rendering_provider_npi"].astype(str)
        o = enc["overall_rate"]
        billed = _pd.to_numeric(f["billed_amount"], errors="coerce").fillna(0.0)
        allowed = _pd.to_numeric(f["allowed_amount"], errors="coerce").fillna(0.0)
        out = _pd.DataFrame({
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

    def predict(self, context, model_input):
        import pandas as _pd
        import xgboost as _xgb

        if not isinstance(model_input, _pd.DataFrame):
            model_input = _pd.DataFrame(model_input)
        for col in self.enc["raw_fields"]:
            if col not in model_input.columns:
                model_input[col] = None
        feats = self._engineer(model_input)

        denial_prob = self.risk.predict_proba(feats)[:, 1]
        reason_proba = self.reason.predict_proba(feats)

        # Per-row SHAP contributions via XGBoost's native pred_contribs. Returns
        # (n_rows, n_features + 1); the trailing column is the bias term, dropped.
        shap_vals = None
        try:
            _contribs = self._booster.predict(_xgb.DMatrix(feats), pred_contribs=True)
            shap_vals = _contribs[:, :-1]
        except Exception as _e:
            print(f"[DenialScorer] pred_contribs failed ({_e}).")

        results = []
        for i in range(len(model_input)):
            probs = {r: 0.0 for r in self.canonical}
            for cls_idx, cls_name in enumerate(self.reason_classes):
                if cls_name in probs:
                    probs[cls_name] = float(reason_proba[i][cls_idx])

            contributions = []
            if shap_vals is not None:
                row = shap_vals[i]
                ranked = sorted(
                    range(len(self.feature_cols)),
                    key=lambda j: abs(float(row[j])),
                    reverse=True,
                )[:7]
                for j in ranked:
                    feat = self.feature_cols[j]
                    contributions.append({
                        "feature": feat,
                        "label": self.feature_labels.get(feat, feat),
                        "value": float(feats[i][j]),
                        "contribution": round(float(row[j]), 4),
                    })

            results.append({
                "denial_prob": float(denial_prob[i]),
                "reason_probs": probs,
                "feature_contributions": contributions,
            })
        return results


# Persist artifacts to disk for logging.
_art_dir = tempfile.mkdtemp()
_risk_path = os.path.join(_art_dir, "risk_model.json")
_reason_path = os.path.join(_art_dir, "reason_model.json")
_enc_path = os.path.join(_art_dir, "encoders.json")

risk_model.save_model(_risk_path)
reason_model.save_model(_reason_path)
_enc_payload = dict(ENC)
_enc_payload["reason_classes"] = REASON_CLASSES
_enc_payload["canonical_reasons"] = CANONICAL_REASONS
# Human-readable labels so per-claim SHAP contributions render nicely in the app.
_enc_payload["feature_labels"] = {
    "billed_amount": "Billed amount",
    "allowed_amount": "Expected allowed amount",
    "billed_allowed_ratio": "Billed-to-allowed ratio",
    "proc_denial_rate": "Procedure historical denial rate",
    "dx_denial_rate": "Diagnosis historical denial rate",
    "provider_denial_rate": "Provider historical denial rate",
    "pxdx_denial_rate": "Procedure–diagnosis pair denial rate",
    "claim_type_code": "Claim type",
    "place_of_service_code": "Place of service",
    "line_of_business_code": "Line of business",
}
with open(_enc_path, "w") as fh:
    json.dump(_enc_payload, fh)

_input_example = pd.DataFrame([{
    "procedure_code": str(pdf.iloc[0]["procedure_code"]),
    "primary_diagnosis_code": str(pdf.iloc[0]["primary_diagnosis_code"]),
    "claim_type": str(pdf.iloc[0]["claim_type"]),
    "place_of_service_code": str(pdf.iloc[0]["place_of_service_code"]),
    "billed_amount": float(pdf.iloc[0]["billed_amount"]),
    "allowed_amount": float(pdf.iloc[0]["allowed_amount"]),
    "line_of_business": str(pdf.iloc[0]["line_of_business"]),
    "rendering_provider_npi": str(pdf.iloc[0]["rendering_provider_npi"]),
}])

with mlflow.start_run(run_name="denial_scorer_pyfunc"):
    mlflow.pyfunc.log_model(
        artifact_path="model",
        python_model=DenialScorer(),
        artifacts={
            "risk_model": _risk_path,
            "reason_model": _reason_path,
            "encoders": _enc_path,
        },
        input_example=_input_example,
        pip_requirements=["xgboost", "pandas", "scikit-learn", "mlflow"],
        registered_model_name=SCORER_MODEL_NAME,
    )

scorer_v = max(client.search_model_versions(f"name='{SCORER_MODEL_NAME}'"), key=lambda v: int(v.version))
client.set_registered_model_alias(SCORER_MODEL_NAME, "production", scorer_v.version)
print(f"Registered {SCORER_MODEL_NAME} v{scorer_v.version} (@production)")

# COMMAND ----------

# Local sanity check of the wrapper before it is served.
_scorer = mlflow.pyfunc.load_model(f"models:/{SCORER_MODEL_NAME}@production")
print("Sample scorer output:", _scorer.predict(_input_example))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch inference → gold_denial_risk_scores

# COMMAND ----------

from datetime import datetime

denial_prob_all = risk_model.predict_proba(X)[:, 1]
reason_proba_all = reason_model.predict_proba(X)
reason_idx = reason_proba_all.argmax(axis=1)

scores = pd.DataFrame({
    "claim_id": pdf["claim_id"].astype(str),
    "member_id": pdf["member_id"].astype(str),
    "procedure_code": pdf["procedure_code"].astype(str),
    "primary_diagnosis_code": pdf["primary_diagnosis_code"].astype(str),
    "denial_prob": denial_prob_all,
    "predicted_reason_category": [REASON_CLASSES[i] for i in reason_idx],
})
scores["reason_probs_json"] = [
    json.dumps({REASON_CLASSES[j]: float(reason_proba_all[i][j]) for j in range(len(REASON_CLASSES))})
    for i in range(len(scores))
]
scores["model_version"] = str(scorer_v.version)
scores["scored_at"] = datetime.now().isoformat()

spark.sql(f"DROP TABLE IF EXISTS {_tbl(CLAIMS_SCHEMA, 'gold_denial_risk_scores')}")
(spark.createDataFrame(scores)
   .write.mode("overwrite").option("overwriteSchema", "true")
   .saveAsTable(_tbl(CLAIMS_SCHEMA, "gold_denial_risk_scores")))
print(f"Wrote {len(scores):,} rows to {catalog}.{CLAIMS_SCHEMA}.gold_denial_risk_scores")

# COMMAND ----------

# MAGIC %md
# MAGIC ## SHAP explainability (binary risk model)

# COMMAND ----------

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

try:
    import shap

    explainer = shap.TreeExplainer(risk_model, feature_names=FEATURE_COLS)
    shap_values = explainer.shap_values(X)
    with mlflow.start_run(run_id=risk_run.info.run_id):
        fig, _ = plt.subplots(figsize=(10, 6))
        shap.summary_plot(shap_values, X, feature_names=FEATURE_COLS, show=False, plot_type="bar")
        plt.title("SHAP Feature Importance — Denial Risk")
        plt.tight_layout()
        _p = "/tmp/shap_denial_risk.png"
        plt.savefig(_p, dpi=150)
        plt.close()
        mlflow.log_artifact(_p, artifact_path="shap")
        shap_global = np.abs(shap_values).mean(axis=0)
        mlflow.log_dict(
            dict(sorted(zip(FEATURE_COLS, shap_global.tolist()), key=lambda x: -x[1])),
            "shap/global_shap_importance.json",
        )
        print("Logged SHAP importance for denial-risk model")
except Exception as shap_err:
    print(f"SHAP unavailable ({type(shap_err).__name__}: {shap_err}) — using native importance")
    with mlflow.start_run(run_id=risk_run.info.run_id):
        mlflow.log_dict(
            dict(sorted(zip(FEATURE_COLS, risk_model.feature_importances_.tolist()), key=lambda x: -x[1])),
            "shap/xgboost_feature_importance.json",
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persist denial drivers to a governed table (Denial Intelligence tab)

# COMMAND ----------

# Surface the model's global feature importance as a queryable UC table so the
# Claim Scrubber app renders "top denial drivers" without reading MLflow artifacts.
try:
    _driver_imp = shap_global.tolist()
    _driver_method = "shap"
except NameError:
    _driver_imp = risk_model.feature_importances_.tolist()
    _driver_method = "xgboost_gain"

_DRIVER_LABELS = {
    "billed_amount": "Billed amount",
    "allowed_amount": "Expected allowed amount",
    "billed_allowed_ratio": "Billed-to-allowed ratio",
    "proc_denial_rate": "Procedure historical denial rate",
    "dx_denial_rate": "Diagnosis historical denial rate",
    "provider_denial_rate": "Provider historical denial rate",
    "pxdx_denial_rate": "Procedure–diagnosis pair denial rate",
    "claim_type_code": "Claim type",
    "place_of_service_code": "Place of service",
    "line_of_business_code": "Line of business",
}
_imp_total = float(sum(_driver_imp)) or 1.0
_drivers = sorted(
    [
        {
            "feature": f,
            "label": _DRIVER_LABELS.get(f, f),
            "importance": float(v),
            "importance_pct": round(float(v) / _imp_total * 100, 2),
            "method": _driver_method,
        }
        for f, v in zip(FEATURE_COLS, _driver_imp)
    ],
    key=lambda d: -d["importance"],
)
for _i, _d in enumerate(_drivers, start=1):
    _d["rank"] = _i

spark.sql(f"DROP TABLE IF EXISTS {_tbl(CLAIMS_SCHEMA, 'denial_model_drivers')}")
(spark.createDataFrame(pd.DataFrame(_drivers))
    .write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(_tbl(CLAIMS_SCHEMA, "denial_model_drivers")))
print(f"Wrote {len(_drivers)} denial drivers to "
      f"{catalog}.{CLAIMS_SCHEMA}.denial_model_drivers (method={_driver_method})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bias monitoring — denial rate by line of business

# COMMAND ----------

bias_results = []
with mlflow.start_run(run_id=risk_run.info.run_id):
    overall_pred_rate = float((denial_prob_all >= 0.5).mean())
    mlflow.log_metric("bias_overall_pred_denial_rate", overall_pred_rate)
    for val in sorted(pdf["line_of_business"].astype(str).unique()):
        mask = pdf["line_of_business"].astype(str).values == val
        n = int(mask.sum())
        if n < 10:
            continue
        slice_rate = float((denial_prob_all[mask] >= 0.5).mean())
        di_ratio = slice_rate / overall_pred_rate if overall_pred_rate > 0 else 0.0
        flagged = abs(1 - di_ratio) > 0.15
        bias_results.append({
            "slice": "line_of_business", "value": val, "n": n,
            "pred_denial_rate": round(slice_rate, 4),
            "disparate_impact_ratio": round(di_ratio, 4), "flagged": flagged,
        })
        print(f"  {val}: n={n}, pred_denial={slice_rate:.1%}, DI={di_ratio:.3f}"
              f"{' *** FLAGGED ***' if flagged else ''}")
    mlflow.log_dict(bias_results, "bias/fairness_analysis.json")
    mlflow.log_metric("bias_flagged_slices", sum(1 for r in bias_results if r["flagged"]))

if bias_results:
    (spark.createDataFrame(pd.DataFrame(bias_results))
        .write.mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(_tbl(CLAIMS_SCHEMA, "denial_model_bias_analysis")))
    print(f"Wrote {catalog}.{CLAIMS_SCHEMA}.denial_model_bias_analysis")

# COMMAND ----------

print("Denial risk model training complete — binary + multiclass + pyfunc scorer, "
      "with SHAP and bias artifacts.")
