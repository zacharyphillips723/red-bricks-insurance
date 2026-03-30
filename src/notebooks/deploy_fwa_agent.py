# Databricks notebook source
# MAGIC %md
# MAGIC # Red Bricks Insurance — Register FWA Investigation Agent in Unity Catalog
# MAGIC
# MAGIC This notebook:
# MAGIC 1. **Validates** FWA gold tables populated, Foundation Model API responding
# MAGIC 2. **Logs** the FWA Investigation Agent as an MLflow ChatModel using "models from code"
# MAGIC 3. **Registers** it in Unity Catalog with `production` alias

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")

catalog = dbutils.widgets.get("catalog")

PROVIDER_RISK_TABLE = f"{catalog}.fwa.gold_fwa_provider_risk"
CLAIM_FLAGS_TABLE = f"{catalog}.fwa.gold_fwa_claim_flags"
LLM_ENDPOINT = "databricks-llama-4-maverick"
MODEL_NAME = f"{catalog}.analytics.fwa_investigation_agent"

print(f"Provider Risk:   {PROVIDER_RISK_TABLE}")
print(f"Claim Flags:     {CLAIM_FLAGS_TABLE}")
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
# MAGIC ## Validate 1: FWA Provider Risk Table

# COMMAND ----------

pr_resp = requests.post(
    f"{host}/api/2.0/sql/statements",
    headers=headers,
    json={
        "warehouse_id": "781064a3466c0984",
        "statement": f"SELECT COUNT(*) AS cnt, COUNT(DISTINCT provider_npi) AS providers FROM {PROVIDER_RISK_TABLE}",
        "wait_timeout": "30s",
    },
).json()

pr_data = pr_resp.get("result", {}).get("data_array", [[0, 0]])[0]
print(f"Provider Risk: {pr_data[0]} rows, {pr_data[1]} unique providers")
assert int(pr_data[1]) > 0, "Provider risk table is empty"
print("PASS: Provider risk table populated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate 2: FWA Claim Flags Table

# COMMAND ----------

cf_resp = requests.post(
    f"{host}/api/2.0/sql/statements",
    headers=headers,
    json={
        "warehouse_id": "781064a3466c0984",
        "statement": f"SELECT COUNT(*) AS cnt, COUNT(DISTINCT claim_id) AS claims FROM {CLAIM_FLAGS_TABLE}",
        "wait_timeout": "30s",
    },
).json()

cf_data = cf_resp.get("result", {}).get("data_array", [[0, 0]])[0]
print(f"Claim Flags: {cf_data[0]} rows, {cf_data[1]} unique claims")
assert int(cf_data[1]) > 0, "Claim flags table is empty"
print("PASS: Claim flags table populated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate 3: Foundation Model API (Llama 4 Maverick)

# COMMAND ----------

llm_resp = requests.post(
    f"{host}/serving-endpoints/{LLM_ENDPOINT}/invocations",
    headers=headers,
    json={
        "messages": [
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": "Say 'FWA agent validation successful' in exactly those words."},
        ],
        "max_tokens": 50,
        "temperature": 0.0,
    },
)
llm_resp.raise_for_status()
llm_text = llm_resp.json().get("choices", [{}])[0].get("message", {}).get("content", "")
print(f"LLM response: {llm_text}")
print("PASS: Foundation Model API works (Llama 4 Maverick)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log Agent Model with MLflow

# COMMAND ----------

import mlflow
import os

mlflow.set_registry_uri("databricks-uc")

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
parts = notebook_path.split("/")
src_idx = len(parts) - 1
for i in range(len(parts) - 1, -1, -1):
    if parts[i] == "src":
        src_idx = i
        break
src_root = "/Workspace" + "/".join(parts[: src_idx + 1])
agent_code_path = os.path.join(src_root, "agents", "fwa_investigation_agent.py")

print(f"Notebook path:  {notebook_path}")
print(f"Agent code:     {agent_code_path}")
print(f"MLflow version: {mlflow.__version__}")
assert os.path.exists(agent_code_path), f"FWA agent code not found at {agent_code_path}"

# COMMAND ----------

model_config = {
    "UC_CATALOG": catalog,
    "SQL_WAREHOUSE_ID": "781064a3466c0984",
    "LLM_ENDPOINT": LLM_ENDPOINT,
    "provider_risk_table": PROVIDER_RISK_TABLE,
    "claim_flags_table": CLAIM_FLAGS_TABLE,
}

with mlflow.start_run(run_name="fwa_investigation_agent") as run:
    model_info = mlflow.pyfunc.log_model(
        name="fwa_investigation_agent",
        python_model=agent_code_path,
        pip_requirements=[
            "databricks-sdk>=0.30.0",
            "mlflow>=2.14.0",
        ],
        model_config=model_config,
    )

    mlflow.set_tags({
        "agent_type": "fwa_investigation",
        "domain": "fraud_waste_abuse",
        "llm_endpoint": LLM_ENDPOINT,
        "prompt_strategy": "structured_briefing",
        "version_notes": "v1 — SQL-based retrieval, structured investigation briefings",
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
        "FWA Investigation Agent for Red Bricks Insurance SIU analysts. "
        "Provides structured investigation briefings with case summary, key findings, "
        "evidence analysis, risk assessment, and recommended actions. "
        "Retrieves data from gold FWA tables via Statement Execution API "
        "and generates analysis using Llama 4 Maverick."
    ),
)

print(f"Set alias 'production' -> version {registered_model.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 70)
print("FWA INVESTIGATION AGENT — REGISTERED IN UNITY CATALOG")
print("=" * 70)
print(f"  Model Name:    {MODEL_NAME}")
print(f"  Version:       {registered_model.version}")
print(f"  Alias:         production -> v{registered_model.version}")
print(f"  Run ID:        {run.info.run_id}")
print()
print(f"  Components:")
print(f"    Provider Risk:    {PROVIDER_RISK_TABLE}")
print(f"    Claim Flags:      {CLAIM_FLAGS_TABLE}")
print(f"    Foundation Model: {LLM_ENDPOINT}")
print()
print("  Input format:")
print("    [INV-XXXX] <question>    — Investigation briefing")
print("    [PRV-XXXXXXXXXX] <question> — Provider analysis")
print("    [MBR-XXXX] <question>    — Member fraud history")
print()
print("  Next steps:")
print("    - Test: '[INV-0001] Full briefing' in the FWA Investigation Portal")
print("    - Review investigation narratives in the app")
