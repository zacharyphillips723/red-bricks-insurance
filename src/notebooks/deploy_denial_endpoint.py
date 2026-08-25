# Databricks notebook source
# MAGIC %md
# MAGIC # Red Bricks Insurance — Deploy Denial Risk Scorer Serving Endpoint
# MAGIC
# MAGIC Creates (or updates) the `denial-risk-scorer` Model Serving endpoint from the
# MAGIC `claims.denial_scorer@production` pyfunc wrapper and **waits until it is READY**.
# MAGIC
# MAGIC ### Why this is a separate task
# MAGIC Endpoint provisioning (container build + model load) takes ~10-20 min. Splitting it
# MAGIC out of `train_denial_model` keeps the fast training/scoring path off that wait, and
# MAGIC makes this task's completion *honest*: when it finishes, the endpoint is genuinely
# MAGIC queryable. `bootstrap_workspace` depends on this task so the provider-scrub app's
# MAGIC service principal `CAN_QUERY` grant lands on a ready endpoint.
# MAGIC
# MAGIC The batch-scored `claims.gold_denial_risk_scores` table (written by training) is a
# MAGIC read fallback, so the scrubber's ML layer degrades gracefully if the endpoint is cold.

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance_catalog", "Catalog")
dbutils.widgets.dropdown("deploy_endpoint", "true", ["true", "false"], "Create serving endpoint")
dbutils.widgets.text("wait_minutes", "25", "Max minutes to wait for READY")

catalog = dbutils.widgets.get("catalog")
deploy_endpoint = dbutils.widgets.get("deploy_endpoint") == "true"
wait_minutes = int(dbutils.widgets.get("wait_minutes"))

CLAIMS_SCHEMA = "claims"
MODEL_NAME = f"{catalog}.{CLAIMS_SCHEMA}.denial_scorer"
ENDPOINT_NAME = "denial-risk-scorer"

print(f"Catalog:  {catalog}")
print(f"Model:    {MODEL_NAME}@production")
print(f"Endpoint: {ENDPOINT_NAME}")
print(f"Deploy:   {deploy_endpoint} (max wait {wait_minutes} min)")

# COMMAND ----------

if not deploy_endpoint:
    dbutils.notebook.exit("SKIPPED: deploy_endpoint=false — gold_denial_risk_scores is the read fallback")

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

# Resolve the concrete version behind @production (serving wants a version, not an alias).
prod = mlflow_client.get_model_version_by_alias(MODEL_NAME, "production")
model_version = prod.version
print(f"Resolved {MODEL_NAME}@production -> v{model_version}")

# A freshly registered version can still be PENDING_REGISTRATION, which makes
# endpoint creation fail. Poll up to 5 min for READY.
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
# MAGIC Created WITHOUT the deprecated `AutoCaptureConfigInput`; inference-table
# MAGIC logging is enabled via the AI Gateway PUT after the endpoint is READY.

# COMMAND ----------

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
# MAGIC Payloads land in `claims.denial_risk_scorer_payload`.

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
                "schema_name": CLAIMS_SCHEMA,
                "table_name_prefix": "denial_risk_scorer",
                "enabled": True,
            }
        },
    )
    if _resp.status_code == 200:
        print(f"Inference tables enabled → {catalog}.{CLAIMS_SCHEMA}.denial_risk_scorer_payload")
    else:
        print(f"Inference table enablement returned {_resp.status_code}: {_resp.text[:200]}")
        print("  (non-fatal — endpoint is READY and queryable without logging)")
except Exception as e:
    print(f"Inference table enablement skipped ({e}) — endpoint is READY and queryable.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Smoke Test — one scoring call

# COMMAND ----------

_record = {
    "procedure_code": "27447",
    "primary_diagnosis_code": "M17.11",
    "claim_type": "professional",
    "place_of_service_code": "11",
    "billed_amount": 42000.0,
    "allowed_amount": 0.0,
    "line_of_business": "Commercial",
    "rendering_provider_npi": "1548302976",
}
try:
    resp = w.serving_endpoints.query(name=ENDPOINT_NAME, dataframe_records=[_record])
    preds = resp.predictions
    print(f"Smoke test OK — predictions: {preds}")
    first = preds[0] if isinstance(preds, list) and preds else preds
    print(f"  denial_prob: {first.get('denial_prob') if isinstance(first, dict) else 'n/a'}")
    print(f"  reason_probs: {first.get('reason_probs') if isinstance(first, dict) else 'n/a'}")
except Exception as e:
    print(f"Smoke test call did not return a prediction ({e}) — endpoint is READY; "
          "first call may be a scale-to-zero cold start.")

# COMMAND ----------

print(f"Denial risk scorer endpoint deployment complete: {ENDPOINT_NAME} (model v{model_version})")
