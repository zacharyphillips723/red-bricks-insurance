# Databricks notebook source
# MAGIC %md
# MAGIC # Red Bricks Insurance — Deploy FWA Supervisor Agent as Serving Endpoint
# MAGIC
# MAGIC This notebook:
# MAGIC 1. **Validates** prerequisites (gold tables, FMAPI endpoints, Genie space, Vector Search)
# MAGIC 2. **Logs** the FWA Supervisor Agent as an MLflow ChatModel using "models from code"
# MAGIC 3. **Registers** it in Unity Catalog with `production` alias
# MAGIC 4. **Deploys** as a Model Serving endpoint with **inference tables** enabled
# MAGIC
# MAGIC Inference tables automatically capture every request, response, token count,
# MAGIC and latency in Unity Catalog — zero instrumentation code required.

# COMMAND ----------

import os
import time

dbutils.widgets.text("catalog", "red_bricks_insurance_catalog", "Catalog")
dbutils.widgets.text("warehouse_id", "", "SQL Warehouse ID (auto-detect if empty)")
dbutils.widgets.text("lakebase_project_id", "red-bricks-insurance", "Lakebase Project ID")
dbutils.widgets.text("genie_space_id", "auto", "Genie Space ID (auto-detect if 'auto')")

catalog = dbutils.widgets.get("catalog")
warehouse_id = dbutils.widgets.get("warehouse_id")
lakebase_project_id = dbutils.widgets.get("lakebase_project_id")
genie_space_id = dbutils.widgets.get("genie_space_id")

# Auto-detect warehouse if not provided
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

if not warehouse_id.strip():
    warehouses = [wh for wh in w.warehouses.list() if wh.state and wh.state.value == "RUNNING"]
    if warehouses:
        warehouse_id = warehouses[0].id
        print(f"Auto-detected warehouse: {warehouse_id} ({warehouses[0].name})")
    else:
        raise ValueError("No running SQL warehouse found — required for agent deployment.")

# Auto-detect Genie space
if genie_space_id.strip().lower() in ("auto", ""):
    try:
        resp = w.api_client.do("GET", "/api/2.0/genie/spaces")
        spaces = resp.get("spaces", [])
        target_title = "Red Bricks Insurance — FWA Analytics"
        for s in spaces:
            if s.get("title") == target_title:
                genie_space_id = s["space_id"]
                break
        else:
            if spaces:
                genie_space_id = spaces[0]["space_id"]
            else:
                genie_space_id = ""
        print(f"Auto-detected Genie space: {genie_space_id}")
    except Exception as e:
        print(f"Genie space auto-detection failed: {e}")
        genie_space_id = ""

LLM_ENDPOINT = os.environ.get("LLM_ENDPOINT", "databricks-llama-4-maverick")
GEMINI_ENDPOINT = os.environ.get("GEMINI_ENDPOINT", "databricks-gemini-2-5-pro")
VS_INDEX_NAME = f"{catalog}.prior_auth.medical_policy_vs_index"
MODEL_NAME = f"{catalog}.analytics.fwa_supervisor_agent"
ENDPOINT_NAME = "fwa-supervisor-agent"

print(f"Catalog:         {catalog}")
print(f"Warehouse:       {warehouse_id}")
print(f"Genie Space:     {genie_space_id}")
print(f"LLM Endpoint:    {LLM_ENDPOINT}")
print(f"Gemini Endpoint: {GEMINI_ENDPOINT}")
print(f"VS Index:        {VS_INDEX_NAME}")
print(f"UC Model Name:   {MODEL_NAME}")
print(f"Serving Endpoint:{ENDPOINT_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import json
import requests

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
host = ctx.apiUrl().get()
token = ctx.apiToken().get()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate 1: FWA Provider Risk Table

# COMMAND ----------

PROVIDER_RISK_TABLE = f"{catalog}.fwa.gold_fwa_provider_risk"

pr_resp = requests.post(
    f"{host}/api/2.0/sql/statements",
    headers=headers,
    json={
        "warehouse_id": warehouse_id,
        "statement": f"SELECT COUNT(*) AS cnt, COUNT(DISTINCT provider_npi) AS providers FROM {PROVIDER_RISK_TABLE}",
        "wait_timeout": "30s",
    },
).json()

pr_data = pr_resp.get("result", {}).get("data_array", [[0, 0]])[0]
print(f"Provider Risk: {pr_data[0]} rows, {pr_data[1]} unique providers")
if int(pr_data[1]) > 0:
    print("PASS: Provider risk table populated")
else:
    print("WARNING: Provider risk table is empty — agent will be registered anyway.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate 2: Foundation Model APIs

# COMMAND ----------

for endpoint_name in [LLM_ENDPOINT, GEMINI_ENDPOINT]:
    resp = requests.post(
        f"{host}/serving-endpoints/{endpoint_name}/invocations",
        headers=headers,
        json={
            "messages": [
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": "Say 'validation ok' in exactly those words."},
            ],
            "max_tokens": 20,
            "temperature": 0.0,
        },
    )
    resp.raise_for_status()
    text = resp.json().get("choices", [{}])[0].get("message", {}).get("content", "")
    print(f"PASS: {endpoint_name} → {text}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate 3: Vector Search Index

# COMMAND ----------

try:
    vs_resp = requests.post(
        f"{host}/api/2.0/vector-search/indexes/{VS_INDEX_NAME}/query",
        headers=headers,
        json={"query_text": "upcoding E/M visits", "columns": ["chunk_id"], "num_results": 1},
    )
    vs_resp.raise_for_status()
    count = len(vs_resp.json().get("result", {}).get("data_array", []))
    print(f"PASS: Vector Search index responds ({count} result)")
except Exception as e:
    print(f"WARNING: Vector Search validation failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log Agent Model with MLflow

# COMMAND ----------

import mlflow

mlflow.set_registry_uri("databricks-uc")

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
parts = notebook_path.split("/")
src_idx = len(parts) - 1
for i in range(len(parts) - 1, -1, -1):
    if parts[i] == "src":
        src_idx = i
        break
src_root = "/Workspace" + "/".join(parts[: src_idx + 1])
agent_code_path = os.path.join(src_root, "agents", "fwa_supervisor_agent.py")
genie_code_path = os.path.join(src_root, "..", "app-fwa", "backend", "genie.py")

print(f"Notebook path:  {notebook_path}")
print(f"Agent code:     {agent_code_path}")
print(f"MLflow version: {mlflow.__version__}")
assert os.path.exists(agent_code_path), f"FWA supervisor agent code not found at {agent_code_path}"

# COMMAND ----------

model_config = {
    "UC_CATALOG": catalog,
    "SQL_WAREHOUSE_ID": warehouse_id,
    "GENIE_SPACE_ID": genie_space_id,
    "LLM_ENDPOINT": LLM_ENDPOINT,
    "GEMINI_ENDPOINT": GEMINI_ENDPOINT,
    "VS_INDEX_NAME": VS_INDEX_NAME,
    "LAKEBASE_PROJECT_ID": lakebase_project_id,
    "LAKEBASE_BRANCH": "production",
    "LAKEBASE_DATABASE_NAME": "fwa_cases",
}

# Declare resources for automatic dependency tracking
from mlflow.models.resources import (
    DatabricksServingEndpoint,
    DatabricksVectorSearchIndex,
)

resources = [
    DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT),
    DatabricksServingEndpoint(endpoint_name=GEMINI_ENDPOINT),
    DatabricksVectorSearchIndex(index_name=VS_INDEX_NAME),
]

with mlflow.start_run(run_name="fwa_supervisor_agent") as run:
    model_info = mlflow.pyfunc.log_model(
        name="fwa_supervisor_agent",
        python_model=agent_code_path,
        pip_requirements=[
            "databricks-sdk>=0.30.0",
            "mlflow>=2.14.0",
            "psycopg[binary]>=3.1",
        ],
        model_config=model_config,
        resources=resources,
    )

    mlflow.set_tags({
        "agent_type": "fwa_supervisor",
        "domain": "fraud_waste_abuse",
        "architecture": "supervisor_agent",
        "sub_agents": "genie,gemini",
        "llm_endpoint": LLM_ENDPOINT,
        "gemini_endpoint": GEMINI_ENDPOINT,
        "version_notes": "v1 — Supervisor agent with Genie + Gemini sub-agents, inference tables",
    })

    print(f"Run ID:    {run.info.run_id}")
    print(f"Model URI: {model_info.model_uri}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register in Unity Catalog

# COMMAND ----------

registered_model = mlflow.register_model(
    model_uri=model_info.model_uri,
    name=MODEL_NAME,
)

print(f"Registered model: {registered_model.name}")
print(f"Version:          {registered_model.version}")

# COMMAND ----------

from mlflow import MlflowClient

client = MlflowClient()
client.set_registered_model_alias(
    name=MODEL_NAME,
    alias="production",
    version=registered_model.version,
)

client.update_registered_model(
    name=MODEL_NAME,
    description=(
        "FWA Supervisor Agent for Red Bricks Insurance SIU analysts. "
        "Orchestrates two sub-agents (Genie for structured claims data, "
        "Gemini for medical policy RAG + compliance analysis) and synthesizes "
        "unified investigation briefings. Deployed as a Model Serving endpoint "
        "with inference tables for automatic request/response logging."
    ),
)

print(f"Set alias 'production' -> version {registered_model.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wait for Model Version to be READY

# COMMAND ----------

print(f"Waiting for model version {registered_model.version} to be READY in UC...")
_max_wait = 300
_start = time.time()
while time.time() - _start < _max_wait:
    _mv = client.get_model_version(MODEL_NAME, registered_model.version)
    _status = getattr(_mv, "status", None) or "UNKNOWN"
    if _status == "READY":
        print(f"  Model version {registered_model.version} is READY.")
        break
    print(f"  Status: {_status} — waiting 10s...")
    time.sleep(10)
else:
    print(f"  WARNING: Model version still not READY after {_max_wait}s. Attempting endpoint creation anyway.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy Serving Endpoint with AI Gateway Inference Tables
# MAGIC
# MAGIC Creates a serverless serving endpoint, then enables AI Gateway inference tables
# MAGIC for automatic request/response logging. Legacy `AutoCaptureConfigInput` is
# MAGIC deprecated — use the AI Gateway `/ai-gateway` API instead.
# MAGIC
# MAGIC Inference tables land at:
# MAGIC - `{catalog}.analytics.fwa_supervisor_payload` — request/response logs
# MAGIC - `{catalog}.analytics.fwa_supervisor_payload_assessment` — optional assessments

# COMMAND ----------

from databricks.sdk.service.serving import (
    EndpointCoreConfigInput,
    ServedEntityInput,
)

_env_vars = {
    "UC_CATALOG": catalog,
    "SQL_WAREHOUSE_ID": warehouse_id,
    "GENIE_SPACE_ID": genie_space_id,
    "LLM_ENDPOINT": LLM_ENDPOINT,
    "GEMINI_ENDPOINT": GEMINI_ENDPOINT,
    "VS_INDEX_NAME": VS_INDEX_NAME,
    "LAKEBASE_PROJECT_ID": lakebase_project_id,
    "LAKEBASE_BRANCH": "production",
    "LAKEBASE_DATABASE_NAME": "fwa_cases",
}

_served_entities = [
    ServedEntityInput(
        entity_name=MODEL_NAME,
        entity_version=registered_model.version,
        workload_size="Small",
        scale_to_zero_enabled=True,
        environment_vars=_env_vars,
    ),
]

# Check if the endpoint already exists (created via UI with inference tables).
# If it exists, update the served entity to the new model version.
# If it doesn't exist, print instructions — first-time creation MUST be done
# via the UC Model page UI with "Enable inference tables" checked, because the
# SDK-only path does not provision the DLT pipeline that populates inference tables.
_endpoint_created = False
try:
    _existing = w.serving_endpoints.get(name=ENDPOINT_NAME)
    print(f"Serving endpoint '{ENDPOINT_NAME}' exists — updating to model v{registered_model.version}...")
    w.serving_endpoints.update_config(
        name=ENDPOINT_NAME,
        served_entities=_served_entities,
    )
    print(f"  Endpoint updated successfully.")
    _endpoint_created = True
except Exception as e:
    if "not found" in str(e).lower() or "does not exist" in str(e).lower():
        print(f"\nEndpoint '{ENDPOINT_NAME}' does not exist.")
        print(f"  Model {MODEL_NAME} v{registered_model.version} is registered in UC.")
        print(f"  CREATE VIA UI (required for inference tables):")
        print(f"    1. Go to UC Model page > 'Serve this model'")
        print(f"    2. Enable inference tables (catalog: {catalog}, schema: analytics, prefix: fwa_supervisor)")
        print(f"    3. Add env vars: UC_CATALOG, SQL_WAREHOUSE_ID, GENIE_SPACE_ID, LLM_ENDPOINT, etc.")
        print(f"  The SDK-only path does NOT provision the DLT pipeline for inference tables.")
    else:
        print(f"\nWARNING: Could not update endpoint: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enable AI Gateway Inference Tables

# COMMAND ----------

# Enable AI Gateway inference tables on the endpoint.
# The endpoint must already exist (created via UI with inference tables checked).
# This API call ensures inference table logging stays enabled after model version updates.
import requests as _gw_requests

if _endpoint_created:
    _gw_host = spark.conf.get("spark.databricks.workspaceUrl")
    _gw_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

    _gw_resp = _gw_requests.put(
        f"https://{_gw_host}/api/2.0/serving-endpoints/{ENDPOINT_NAME}/ai-gateway",
        headers={"Authorization": f"Bearer {_gw_token}", "Content-Type": "application/json"},
        json={
            "inference_table_config": {
                "catalog_name": catalog,
                "schema_name": "analytics",
                "table_name_prefix": "fwa_supervisor",
                "enabled": True,
            }
        },
    )
    if _gw_resp.status_code == 200:
        print(f"AI Gateway inference tables confirmed on '{ENDPOINT_NAME}'")
        print(f"  Payload table: {catalog}.analytics.fwa_supervisor_payload")
    else:
        print(f"WARNING: Could not enable inference tables ({_gw_resp.status_code}): {_gw_resp.text[:300]}")
else:
    print(f"Endpoint not found — create via UI first with inference tables enabled.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 70)
print("FWA SUPERVISOR AGENT — DEPLOYED AS SERVING ENDPOINT")
print("=" * 70)
print(f"  Model Name:       {MODEL_NAME}")
print(f"  Version:          {registered_model.version}")
print(f"  Alias:            production -> v{registered_model.version}")
print(f"  Run ID:           {run.info.run_id}")
print(f"  Serving Endpoint: {ENDPOINT_NAME}")
print()
print(f"  Architecture:")
print(f"    Supervisor:     {LLM_ENDPOINT}")
print(f"    Gemini Analyst: {GEMINI_ENDPOINT}")
print(f"    Genie Space:    {genie_space_id}")
print(f"    Vector Search:  {VS_INDEX_NAME}")
print(f"    Lakebase:       {lakebase_project_id}/fwa_cases")
print()
print(f"  Inference Tables:")
print(f"    Payload:        {catalog}.analytics.fwa_supervisor_payload")
print(f"    Assessment:     {catalog}.analytics.fwa_supervisor_payload_assessment")
print()
print(f"  Input format:")
print(f"    [INV-XXXX] <question>       — Investigation briefing")
print(f"    [PRV-XXXXXXXXXX] <question>  — Provider analysis")
print(f"    [MBR-XXXX] <question>       — Member fraud history")
print()
print(f"  Test command:")
print(f"    w.serving_endpoints.query(")
print(f"        name='{ENDPOINT_NAME}',")
print(f"        messages=[{{'role': 'user', 'content': '[PRV-1234567890] Full investigation briefing'}}]")
print(f"    )")
print()
print(f"  Verify inference tables:")
print(f"    SELECT * FROM {catalog}.analytics.fwa_supervisor_payload ORDER BY timestamp_ms DESC LIMIT 5")
