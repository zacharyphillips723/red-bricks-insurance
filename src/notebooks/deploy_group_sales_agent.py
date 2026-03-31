# Databricks notebook source
# MAGIC %md
# MAGIC # Red Bricks Insurance — Register Group Sales Coach Agent in Unity Catalog
# MAGIC
# MAGIC This notebook:
# MAGIC 1. **Validates** gold_group_report_card is populated
# MAGIC 2. **Validates** Foundation Model API connectivity
# MAGIC 3. **Logs** the Group Sales Coach Agent as an MLflow ChatModel using "models from code"
# MAGIC 4. **Registers** it in Unity Catalog with `production` alias

# COMMAND ----------

import os

dbutils.widgets.text("catalog", "red_bricks_insurance", "Catalog")
dbutils.widgets.text("warehouse_id", "", "SQL Warehouse ID (auto-detect if empty)")

catalog = dbutils.widgets.get("catalog")
warehouse_id = dbutils.widgets.get("warehouse_id")

# Auto-detect warehouse if not provided
if not warehouse_id.strip():
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    warehouses = [wh for wh in w.warehouses.list() if wh.state and wh.state.value == "RUNNING"]
    if warehouses:
        warehouse_id = warehouses[0].id
        print(f"Auto-detected warehouse: {warehouse_id} ({warehouses[0].name})")
    else:
        print("WARNING: No running SQL warehouse found. Data validation will be skipped.")

REPORT_CARD_TABLE = f"{catalog}.analytics.gold_group_report_card"
LLM_ENDPOINT = os.environ.get("LLM_ENDPOINT", "databricks-llama-4-maverick")
MODEL_NAME = f"{catalog}.analytics.group_sales_coach_agent"

print(f"Report Card:     {REPORT_CARD_TABLE}")
print(f"LLM Endpoint:    {LLM_ENDPOINT}")
print(f"UC Model Name:   {MODEL_NAME}")

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
# MAGIC ## Validate 1: Gold Group Report Card

# COMMAND ----------

rc_resp = requests.post(
    f"{host}/api/2.0/sql/statements",
    headers=headers,
    json={
        "warehouse_id": warehouse_id,
        "statement": f"SELECT COUNT(*) AS cnt, COUNT(DISTINCT group_id) AS groups FROM {REPORT_CARD_TABLE}",
        "wait_timeout": "30s",
    },
).json()

rc_data = rc_resp.get("result", {}).get("data_array", [[0, 0]])[0]
print(f"Report Card: {rc_data[0]} rows, {rc_data[1]} unique groups")
if int(rc_data[1]) > 0:
    print("PASS: Gold group report card populated")
else:
    print("WARNING: Report card table is empty — MV may still be materializing. Agent will be registered anyway.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate 2: Foundation Model API (Llama 4 Maverick)

# COMMAND ----------

llm_resp = requests.post(
    f"{host}/serving-endpoints/{LLM_ENDPOINT}/invocations",
    headers=headers,
    json={
        "messages": [
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": "Say 'Sales Coach validation successful' in exactly those words."},
        ],
        "max_tokens": 50,
        "temperature": 0.0,
    },
)
llm_resp.raise_for_status()
llm_text = llm_resp.json().get("choices", [{}])[0].get("message", {}).get("content", "")
print(f"LLM response: {llm_text}")
print("PASS: Foundation Model API works")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log Agent Model with MLflow

# COMMAND ----------

import mlflow
import os

# Set the Unity Catalog model registry
mlflow.set_registry_uri("databricks-uc")

# Resolve the agent code file path
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
parts = notebook_path.split("/")
src_idx = len(parts) - 1
for i in range(len(parts) - 1, -1, -1):
    if parts[i] == "src":
        src_idx = i
        break
src_root = "/Workspace" + "/".join(parts[: src_idx + 1])
agent_code_path = os.path.join(src_root, "agents", "group_sales_coach_agent.py")

print(f"Notebook path:  {notebook_path}")
print(f"Agent code:     {agent_code_path}")
print(f"MLflow version: {mlflow.__version__}")
assert os.path.exists(agent_code_path), f"Agent code not found at {agent_code_path}"

# COMMAND ----------

model_config = {
    "UC_CATALOG": catalog,
    "SQL_WAREHOUSE_ID": warehouse_id,
    "LLM_ENDPOINT": LLM_ENDPOINT,
    "report_card_table": REPORT_CARD_TABLE,
}

with mlflow.start_run(run_name="group_sales_coach_agent") as run:
    model_info = mlflow.pyfunc.log_model(
        name="group_sales_coach_agent",
        python_model=agent_code_path,
        pip_requirements=[
            "databricks-sdk>=0.30.0",
            "mlflow>=2.14.0",
        ],
        model_config=model_config,
    )

    mlflow.set_tags({
        "agent_type": "group_sales_coach",
        "domain": "sales_enablement",
        "llm_endpoint": LLM_ENDPOINT,
        "prompt_strategy": "structured_briefing",
        "version_notes": "v1 — Group renewal prep, peer benchmarks, objection handling",
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
print(f"Source:           {registered_model.source}")

# COMMAND ----------

# Set version alias
from mlflow import MlflowClient

client = MlflowClient()
client.set_registered_model_alias(
    name=MODEL_NAME,
    alias="production",
    version=registered_model.version,
)
print(f"Set alias 'production' -> version {registered_model.version}")

# Add model description
client.update_registered_model(
    name=MODEL_NAME,
    description=(
        "Group Sales Coach Agent for Red Bricks Insurance. "
        "Generates structured renewal meeting briefings with talking points, "
        "risk areas, competitive positioning, and objection handling. "
        "Uses group report card, experience, stop-loss, renewal, and TCOC data."
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 70)
print("GROUP SALES COACH AGENT — REGISTERED IN UNITY CATALOG")
print("=" * 70)
print(f"  Model Name:    {MODEL_NAME}")
print(f"  Version:       {registered_model.version}")
print(f"  Alias:         production -> v{registered_model.version}")
print(f"  Run ID:        {run.info.run_id}")
print()
print("  Components:")
print(f"    Report Card:      {REPORT_CARD_TABLE}")
print(f"    Foundation Model: {LLM_ENDPOINT}")
print()
print("  Test command:")
print(f"    [GRP-0001] Prepare me for the renewal meeting")
