# Databricks notebook source
# MAGIC %md
# MAGIC # Red Bricks Insurance — Deploy Readmission Scorer Serving Endpoint
# MAGIC
# MAGIC Creates (or updates) the `readmission-scorer` Model Serving endpoint from the
# MAGIC `@champion` readmission model and **waits until it is READY**.
# MAGIC
# MAGIC ### Why this is a separate task
# MAGIC Endpoint provisioning (container build + model load) takes ~10-20 min. Splitting it
# MAGIC out of `train_readmission_model` keeps the fast training/scoring path off that wait,
# MAGIC and — critically — makes this task's completion *honest*: when it finishes, the
# MAGIC endpoint is genuinely queryable. `bootstrap_workspace` depends on this task, so the
# MAGIC app service principal's `CAN_QUERY` grant lands on a ready endpoint and the Member 360
# MAGIC "re-score now" button works the moment the job completes.
# MAGIC
# MAGIC The batch-scored `gold_member_readmission_risk` table (written by training) is the
# MAGIC app's default read path, so the app is fully functional even if `deploy_endpoint=false`.

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance_catalog", "Catalog")
dbutils.widgets.dropdown("deploy_endpoint", "true", ["true", "false"], "Create serving endpoint")
dbutils.widgets.text("wait_minutes", "25", "Max minutes to wait for READY")

catalog = dbutils.widgets.get("catalog")
deploy_endpoint = dbutils.widgets.get("deploy_endpoint") == "true"
wait_minutes = int(dbutils.widgets.get("wait_minutes"))

ANALYTICS_SCHEMA = "analytics"
MODEL_NAME = f"{catalog}.{ANALYTICS_SCHEMA}.readmission_scorer"
ENDPOINT_NAME = "readmission-scorer"

print(f"Catalog:  {catalog}")
print(f"Model:    {MODEL_NAME}@champion")
print(f"Endpoint: {ENDPOINT_NAME}")
print(f"Deploy:   {deploy_endpoint} (max wait {wait_minutes} min)")

# COMMAND ----------

if not deploy_endpoint:
    dbutils.notebook.exit("SKIPPED: deploy_endpoint=false — gold table is the read path")

# COMMAND ----------

import time
import requests
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput,
    ServedEntityInput,
)
from mlflow import MlflowClient

mlflow_client = MlflowClient(registry_uri="databricks-uc")

# Resolve the concrete version behind @champion (serving wants a version, not an alias).
champion = mlflow_client.get_model_version_by_alias(MODEL_NAME, "champion")
model_version = champion.version
print(f"Resolved {MODEL_NAME}@champion -> v{model_version}")

# Model-version registration delay: a freshly registered version can still be in
# PENDING_REGISTRATION, which makes endpoint creation fail. Poll up to 5 min for READY.
for _ in range(30):
    mv = mlflow_client.get_model_version(MODEL_NAME, model_version)
    status = getattr(mv, "status", "READY")
    if status == "READY":
        break
    print(f"  model version v{model_version} status={status} — waiting 10s...")
    time.sleep(10)
else:
    raise RuntimeError(f"Model version v{model_version} not READY after 5 min (status={status})")

w = WorkspaceClient()

served_entities = [
    ServedEntityInput(
        entity_name=MODEL_NAME,
        entity_version=model_version,
        workload_size="Small",
        scale_to_zero_enabled=True,
    ),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create or Update the Endpoint
# MAGIC
# MAGIC NOTE: `AutoCaptureConfigInput` (legacy inference tables) now blocks the create call
# MAGIC entirely — see the README "AutoCaptureConfigInput Is Deprecated" note. The endpoint
# MAGIC is created WITHOUT it; inference-table logging is enabled via the AI Gateway PUT
# MAGIC after the endpoint is READY (same pattern as `deploy_fwa_supervisor_agent.py`).

# COMMAND ----------

# Determine whether the endpoint already exists (idempotent across re-runs).
_exists = False
try:
    w.serving_endpoints.get(name=ENDPOINT_NAME)
    _exists = True
except Exception as e:
    if "not found" not in str(e).lower() and "does not exist" not in str(e).lower():
        print(f"  (get returned: {e})")

if _exists:
    print(f"Endpoint '{ENDPOINT_NAME}' exists — updating served entity to v{model_version}...")
    w.serving_endpoints.update_config(
        name=ENDPOINT_NAME,
        served_entities=served_entities,
    )
else:
    print(f"Creating endpoint '{ENDPOINT_NAME}' (model v{model_version})...")
    w.serving_endpoints.create(
        name=ENDPOINT_NAME,
        config=EndpointCoreConfigInput(name=ENDPOINT_NAME, served_entities=served_entities),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wait Until READY
# MAGIC
# MAGIC Polls endpoint state until both readiness is `READY` and the config update
# MAGIC finishes (`config_update` back to `NOT_UPDATING`). Fails loudly on timeout or a
# MAGIC `*_FAILED` state so the job surfaces a real problem rather than a silent false success.

# COMMAND ----------

deadline = time.time() + wait_minutes * 60
poll_interval = 20
ready = False

while time.time() < deadline:
    ep = w.serving_endpoints.get(name=ENDPOINT_NAME)
    state = ep.state
    ready_val = state.ready.value if state and state.ready else "UNKNOWN"
    cfg_val = state.config_update.value if state and state.config_update else "UNKNOWN"
    print(f"  ready={ready_val} config_update={cfg_val} — waiting {poll_interval}s...")

    if "FAILED" in cfg_val:
        raise RuntimeError(
            f"Endpoint '{ENDPOINT_NAME}' config update failed (state={cfg_val}). "
            "Check the serving endpoint events/logs in the workspace."
        )
    if ready_val == "READY" and cfg_val in ("NOT_UPDATING", "UNKNOWN"):
        ready = True
        break
    time.sleep(poll_interval)

if not ready:
    raise RuntimeError(
        f"Endpoint '{ENDPOINT_NAME}' did not reach READY within {wait_minutes} min. "
        "It may still be provisioning — re-run this task or check the workspace."
    )

print(f"\nEndpoint '{ENDPOINT_NAME}' is READY and serving model v{model_version}.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enable Inference Table Logging (AI Gateway)
# MAGIC
# MAGIC Enabled AFTER the endpoint is READY, via the AI Gateway API — the legacy
# MAGIC `AutoCaptureConfigInput` create-time path is deprecated and blocks endpoint
# MAGIC creation. Payloads land in `analytics.readmission_scorer_payload`.

# COMMAND ----------

try:
    _host = spark.conf.get("spark.databricks.workspaceUrl")
    _token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    _resp = requests.put(
        f"https://{_host}/api/2.0/serving-endpoints/{ENDPOINT_NAME}/ai-gateway",
        headers={"Authorization": f"Bearer {_token}", "Content-Type": "application/json"},
        json={
            "inference_table_config": {
                "catalog_name": catalog,
                "schema_name": ANALYTICS_SCHEMA,
                "table_name_prefix": "readmission_scorer",
                "enabled": True,
            }
        },
    )
    if _resp.status_code == 200:
        print(f"Inference tables enabled → {catalog}.{ANALYTICS_SCHEMA}.readmission_scorer_payload")
    else:
        print(f"Inference table enablement returned {_resp.status_code}: {_resp.text[:200]}")
        print("  (non-fatal — endpoint is READY and queryable without logging)")
except Exception as e:
    print(f"Inference table enablement skipped ({e}) — endpoint is READY and queryable.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Smoke Test — one scoring call

# COMMAND ----------

# Feature order MUST match FEATURE_COLS in train_readmission_model.py.
from databricks.sdk.service.serving import DataframeSplitInput

_smoke = DataframeSplitInput(
    columns=[
        "length_of_stay_days", "is_inpatient", "discharged_to_post_acute",
        "discharged_ama", "prior_admits_180d", "raf_score", "hcc_count",
        "composite_sdoh_risk_score", "age",
    ],
    data=[[7.0, 1.0, 1.0, 0.0, 2.0, 2.4, 3.0, 6.5, 74.0]],
)
try:
    resp = w.serving_endpoints.query(name=ENDPOINT_NAME, dataframe_split=_smoke)
    print(f"Smoke test OK — predictions: {resp.predictions}")
except Exception as e:
    # Non-fatal: readiness already confirmed above. A cold scale-to-zero endpoint can
    # reject the very first call while the container warms; the app retries on wake.
    print(f"Smoke test call did not return a prediction ({e}) — endpoint is READY; "
          "first call may be a scale-to-zero cold start.")

# COMMAND ----------

print(f"Readmission scorer endpoint deployment complete: {ENDPOINT_NAME} (model v{model_version})")
