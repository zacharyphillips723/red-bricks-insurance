# Databricks notebook source
# MAGIC %md
# MAGIC # Red Bricks Insurance — Register Care Intelligence Agent v2 in Unity Catalog
# MAGIC
# MAGIC This notebook:
# MAGIC 1. **Validates** all RAG agent components (Vector Search, Member 360, Benefit Utilization, Foundation Model API)
# MAGIC 2. **Logs** the v2 Care Intelligence Agent as an MLflow ChatModel using "models from code"
# MAGIC 3. **Registers** it in Unity Catalog with `champion` alias for A/B evaluation against v1 (`production`)
# MAGIC
# MAGIC ### v2 Changes from v1
# MAGIC | Dimension | v1 | v2 |
# MAGIC |-----------|----|----|
# MAGIC | Prompt | Narrative style | SOAP format with severity ratings |
# MAGIC | LLM | Llama 3.3 70B | Llama 4 Maverick |
# MAGIC | Temperature | 0.1 | 0.05 |
# MAGIC | Retrieval chunks | 5 | 10 |
# MAGIC | Context | Member 360 only | Member 360 + Benefit Utilization |
# MAGIC | Max tokens | 1500 | 2000 |

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

VS_INDEX_NAME = f"{catalog}.documents.case_notes_vs_index"
MEMBER_360_TABLE = f"{catalog}.analytics.gold_member_360"
BENEFIT_UTIL_TABLE = f"{catalog}.benefits.gold_member_benefit_utilization"
LLM_ENDPOINT = os.environ.get("LLM_ENDPOINT", "databricks-llama-4-maverick")
MODEL_NAME = f"{catalog}.analytics.care_intelligence_agent_v2"

print(f"VS Index:        {VS_INDEX_NAME}")
print(f"Member 360:      {MEMBER_360_TABLE}")
print(f"Benefit Util:    {BENEFIT_UTIL_TABLE}")
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
# MAGIC ## Validate 1: Vector Search Index

# COMMAND ----------

idx_info = requests.get(
    f"{host}/api/2.0/vector-search/indexes/{VS_INDEX_NAME}", headers=headers
).json()
idx_status = idx_info.get("status", {})
print(f"Index: {VS_INDEX_NAME}")
print(f"  State: {idx_status.get('detailed_state')}")
print(f"  Ready: {idx_status.get('ready')}")
print(f"  Rows:  {idx_status.get('indexed_row_count')}")

assert idx_status.get("ready"), f"Index not ready: {idx_status}"
print("PASS: Vector Search index is ONLINE")

# COMMAND ----------

# Test similarity search
test_query = requests.post(
    f"{host}/api/2.0/vector-search/indexes/{VS_INDEX_NAME}/query",
    headers=headers,
    json={
        "columns": ["chunk_id", "document_id", "member_id", "document_type", "chunk_text"],
        "query_text": "diabetes management and blood glucose control",
        "num_results": 3,
    },
).json()

rows = test_query.get("result", {}).get("data_array", [])
print(f"Test query returned {len(rows)} chunks:")
for row in rows:
    print(f"  - {row[0]} | {row[2]} | {row[3]} | {str(row[4])[:80]}...")

assert len(rows) > 0, "Vector Search returned no results"
print("PASS: Vector Search query works")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate 2: Member 360 Table

# COMMAND ----------

m360_resp = requests.post(
    f"{host}/api/2.0/sql/statements",
    headers=headers,
    json={
        "warehouse_id": warehouse_id,
        "statement": f"SELECT COUNT(*) AS cnt, COUNT(DISTINCT member_id) AS members FROM {MEMBER_360_TABLE}",
        "wait_timeout": "30s",
    },
).json()

m360_data = m360_resp.get("result", {}).get("data_array", [[0, 0]])[0]
print(f"Member 360: {m360_data[0]} rows, {m360_data[1]} unique members")
if int(m360_data[1]) > 0:
    print("PASS: Member 360 table populated")
else:
    print("WARNING: Member 360 table is empty — MV may still be materializing. Agent will be registered anyway.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate 3: Benefit Utilization Table

# COMMAND ----------

bu_resp = requests.post(
    f"{host}/api/2.0/sql/statements",
    headers=headers,
    json={
        "warehouse_id": warehouse_id,
        "statement": (
            f"SELECT COUNT(*) AS cnt, COUNT(DISTINCT member_id) AS members, "
            f"COUNT(DISTINCT benefit_category) AS categories "
            f"FROM {BENEFIT_UTIL_TABLE}"
        ),
        "wait_timeout": "30s",
    },
).json()

bu_data = bu_resp.get("result", {}).get("data_array", [[0, 0, 0]])[0]
print(f"Benefit Utilization: {bu_data[0]} rows, {bu_data[1]} unique members, {bu_data[2]} categories")
if int(bu_data[1]) > 0:
    print("PASS: Benefit utilization table populated")
else:
    print("WARNING: Benefit utilization table is empty — MV may still be materializing. Agent will be registered anyway.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate 4: Foundation Model API (Llama 4 Maverick)

# COMMAND ----------

llm_resp = requests.post(
    f"{host}/serving-endpoints/{LLM_ENDPOINT}/invocations",
    headers=headers,
    json={
        "messages": [
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": "Say 'Agent v2 validation successful' in exactly those words."},
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
# MAGIC ## Log Agent v2 Model with MLflow

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
agent_code_path = os.path.join(src_root, "agents", "care_intelligence_agent_v2.py")

print(f"Notebook path:  {notebook_path}")
print(f"Agent code:     {agent_code_path}")
print(f"MLflow version: {mlflow.__version__}")
assert os.path.exists(agent_code_path), f"Agent v2 code not found at {agent_code_path}"

# COMMAND ----------

model_config = {
    "UC_CATALOG": catalog,
    "SQL_WAREHOUSE_ID": warehouse_id,
    "LLM_ENDPOINT": LLM_ENDPOINT,
    "vs_index": VS_INDEX_NAME,
    "member_360_table": MEMBER_360_TABLE,
    "benefit_util_table": BENEFIT_UTIL_TABLE,
}

with mlflow.start_run(run_name="care_intelligence_agent_v2") as run:
    model_info = mlflow.pyfunc.log_model(
        name="care_intelligence_agent_v2",
        python_model=agent_code_path,
        pip_requirements=[
            "databricks-sdk>=0.30.0",
            "mlflow>=2.14.0",
        ],
        model_config=model_config,
    )

    mlflow.set_tags({
        "agent_type": "care_intelligence_rag",
        "agent_version": "v2",
        "domain": "care_management",
        "llm_endpoint": LLM_ENDPOINT,
        "vs_index": VS_INDEX_NAME,
        "prompt_strategy": "soap_structured",
        "retrieval_chunks": "10",
        "version_notes": "v2 — SOAP prompt, Llama 4 Maverick, 10 chunks, benefit utilization context",
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

# Set version alias — v2 gets "champion" for A/B evaluation
from mlflow import MlflowClient

client = MlflowClient()
client.set_registered_model_alias(
    name=MODEL_NAME,
    alias="champion",
    version=registered_model.version,
)
print(f"Set alias 'champion' -> version {registered_model.version}")

# Add model description
client.update_registered_model(
    name=MODEL_NAME,
    description=(
        "Care Intelligence RAG Agent v2 for Red Bricks Insurance. "
        "Enhanced with SOAP-format structured responses, Llama 4 Maverick, "
        "10 retrieval chunks, and benefit utilization context. "
        "Designed for A/B comparison against v1 (production alias)."
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 70)
print("CARE INTELLIGENCE AGENT v2 — REGISTERED IN UNITY CATALOG")
print("=" * 70)
print(f"  Model Name:    {MODEL_NAME}")
print(f"  Version:       {registered_model.version}")
print(f"  Alias:         champion -> v{registered_model.version}")
print(f"  Run ID:        {run.info.run_id}")
print()
print("  v2 Enhancements:")
print(f"    Prompt:           SOAP structured (Subjective/Objective/Assessment/Plan)")
print(f"    LLM:              {LLM_ENDPOINT}")
print(f"    Temperature:      0.05")
print(f"    Retrieval chunks: 10")
print(f"    Benefit util:     {BENEFIT_UTIL_TABLE}")
print(f"    Max tokens:       2000")
print()
print("  Components:")
print(f"    Vector Search:    {VS_INDEX_NAME}")
print(f"    Member 360:       {MEMBER_360_TABLE}")
print(f"    Benefit Util:     {BENEFIT_UTIL_TABLE}")
print(f"    Foundation Model: {LLM_ENDPOINT}")
print()
print("  Next steps:")
print("    - Run evaluate_agents.py to compare v1 (production) vs v2 (champion)")
print("    - Review A/B comparison dashboard")
print("    - Promote v2 to production if metrics improve")
