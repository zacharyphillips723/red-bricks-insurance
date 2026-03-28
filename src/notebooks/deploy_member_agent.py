# Databricks notebook source
# MAGIC %md
# MAGIC # Red Bricks Insurance — Register Care Intelligence Agent in Unity Catalog
# MAGIC
# MAGIC This notebook:
# MAGIC 1. **Validates** all RAG agent components (Vector Search, Member 360, Foundation Model API)
# MAGIC 2. **Logs** the Care Intelligence Agent as an MLflow ChatModel using "models from code"
# MAGIC 3. **Registers** it in Unity Catalog for governance, versioning, evaluation, and A/B testing
# MAGIC
# MAGIC The agent runs in the FastAPI backend for the live app, but the UC-registered version
# MAGIC enables MLflow evaluation, model comparison, and future Model Serving deployment.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")

catalog = dbutils.widgets.get("catalog")

VS_INDEX_NAME = f"{catalog}.documents.case_notes_vs_index"
MEMBER_360_TABLE = f"{catalog}.analytics.gold_member_360"
LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"
MODEL_NAME = f"{catalog}.analytics.care_intelligence_agent"

print(f"VS Index:       {VS_INDEX_NAME}")
print(f"Member 360:     {MEMBER_360_TABLE}")
print(f"LLM Endpoint:   {LLM_ENDPOINT}")
print(f"UC Model Name:  {MODEL_NAME}")

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
        "warehouse_id": "781064a3466c0984",
        "statement": f"SELECT COUNT(*) AS cnt, COUNT(DISTINCT member_id) AS members FROM {MEMBER_360_TABLE}",
        "wait_timeout": "30s",
    },
).json()

m360_data = m360_resp.get("result", {}).get("data_array", [[0, 0]])[0]
print(f"Member 360: {m360_data[0]} rows, {m360_data[1]} unique members")
assert int(m360_data[1]) > 0, "Member 360 table is empty"
print("PASS: Member 360 table populated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate 3: Foundation Model API

# COMMAND ----------

llm_resp = requests.post(
    f"{host}/serving-endpoints/{LLM_ENDPOINT}/invocations",
    headers=headers,
    json={
        "messages": [
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": "Say 'Agent validation successful' in exactly those words."},
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
agent_code_path = os.path.join(src_root, "agents", "care_intelligence_agent.py")

print(f"Notebook path:  {notebook_path}")
print(f"Agent code:     {agent_code_path}")
print(f"MLflow version: {mlflow.__version__}")
assert os.path.exists(agent_code_path), f"Agent code not found at {agent_code_path}"

# COMMAND ----------

model_config = {
    "UC_CATALOG": catalog,
    "SQL_WAREHOUSE_ID": "781064a3466c0984",
    "LLM_ENDPOINT": LLM_ENDPOINT,
    "vs_index": VS_INDEX_NAME,
    "member_360_table": MEMBER_360_TABLE,
}

with mlflow.start_run(run_name="care_intelligence_agent_v1") as run:
    # Code-based logging: pass the .py file path as python_model.
    # The agent file has mlflow.models.set_model(CareIntelligenceAgent) at module level.
    model_info = mlflow.pyfunc.log_model(
        name="care_intelligence_agent",
        python_model=agent_code_path,
        pip_requirements=[
            "databricks-sdk>=0.30.0",
            "mlflow>=2.14.0",
        ],
        model_config=model_config,
    )

    mlflow.set_tags({
        "agent_type": "care_intelligence_rag",
        "domain": "care_management",
        "llm_endpoint": LLM_ENDPOINT,
        "vs_index": VS_INDEX_NAME,
        "version_notes": "v1 — baseline RAG agent with profile retrieval + case note search",
    })

    print(f"Run ID:    {run.info.run_id}")
    print(f"Model URI: {model_info.model_uri}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register in Unity Catalog

# COMMAND ----------

# Register the logged model in Unity Catalog
registered_model = mlflow.register_model(
    model_uri=model_info.model_uri,
    name=MODEL_NAME,
)

print(f"Registered model: {registered_model.name}")
print(f"Version:          {registered_model.version}")
print(f"Source:           {registered_model.source}")

# COMMAND ----------

# Set version alias for easy reference
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
        "Care Intelligence RAG Agent for Red Bricks Insurance. "
        "Synthesizes member profiles (gold_member_360), case notes (Vector Search), "
        "and clinical documents to support care manager outreach. "
        "Uses Foundation Model API (Llama 3.3 70B) for response generation."
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 70)
print("CARE INTELLIGENCE AGENT — REGISTERED IN UNITY CATALOG")
print("=" * 70)
print(f"  Model Name:    {MODEL_NAME}")
print(f"  Version:       {registered_model.version}")
print(f"  Alias:         production -> v{registered_model.version}")
print(f"  Run ID:        {run.info.run_id}")
print()
print("  Components:")
print(f"    Vector Search:    {VS_INDEX_NAME}")
print(f"    Member 360:       {MEMBER_360_TABLE}")
print(f"    Foundation Model: {LLM_ENDPOINT}")
print()
print("  Next steps:")
print("    - Run mlflow.evaluate() to benchmark agent quality")
print("    - Log v2 with different prompts/models for A/B comparison")
print("    - Deploy to Model Serving endpoint for production use")
print("    - Use UC lineage to trace data dependencies")
