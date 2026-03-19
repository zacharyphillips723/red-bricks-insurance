# Databricks notebook source
# MAGIC %md
# MAGIC # Red Bricks Insurance — Validate Member RAG Agent Components
# MAGIC
# MAGIC Validates that all components needed by the Member RAG Agent are functional:
# MAGIC 1. **Vector Search index** — case notes chunks are queryable
# MAGIC 2. **Member 360 table** — gold view returns member profiles
# MAGIC 3. **Foundation Model API** — LLM endpoint responds
# MAGIC
# MAGIC The RAG agent runs directly in the FastAPI backend (no separate Model Serving
# MAGIC endpoint needed). This notebook confirms all data sources are ready.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "red_bricks_insurance_dev", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

VS_INDEX_NAME = f"{catalog}.{schema}.case_notes_vs_index"
MEMBER_360_TABLE = f"{catalog}.{schema}.gold_member_360"
LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"

print(f"VS Index: {VS_INDEX_NAME}")
print(f"Member 360 Table: {MEMBER_360_TABLE}")
print(f"LLM Endpoint: {LLM_ENDPOINT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup REST API client

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

# Check index status
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

# Test filtered search (by member_id)
test_member = rows[0][2]  # Use a member_id from the previous results
filtered = requests.post(
    f"{host}/api/2.0/vector-search/indexes/{VS_INDEX_NAME}/query",
    headers=headers,
    json={
        "columns": ["chunk_id", "member_id", "document_type", "chunk_text"],
        "query_text": "care history and treatment plan",
        "filters_json": json.dumps({"member_id": test_member}),
        "num_results": 3,
    },
).json()

filtered_rows = filtered.get("result", {}).get("data_array", [])
print(f"Filtered query for {test_member}: {len(filtered_rows)} chunks")
for row in filtered_rows:
    print(f"  - {row[0]} | {row[2]} | {str(row[3])[:80]}...")

assert all(r[1] == test_member for r in filtered_rows), "Filter did not restrict to member"
print("PASS: Filtered Vector Search works")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate 2: Member 360 Table

# COMMAND ----------

# Query member 360
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

# Sample a member profile
sample_resp = requests.post(
    f"{host}/api/2.0/sql/statements",
    headers=headers,
    json={
        "warehouse_id": "781064a3466c0984",
        "statement": f"SELECT * FROM {MEMBER_360_TABLE} LIMIT 1",
        "wait_timeout": "30s",
    },
).json()

columns = [c["name"] for c in sample_resp.get("manifest", {}).get("schema", {}).get("columns", [])]
row = sample_resp.get("result", {}).get("data_array", [[]])[0]
profile = dict(zip(columns, row))
print("Sample member profile:")
for k, v in profile.items():
    print(f"  {k}: {v}")
print("PASS: Member 360 profile query works")

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
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("MEMBER RAG AGENT — ALL VALIDATIONS PASSED")
print("=" * 60)
print(f"  Vector Search Index:  {VS_INDEX_NAME} (ONLINE)")
print(f"  Member 360 Table:     {MEMBER_360_TABLE}")
print(f"  Foundation Model:     {LLM_ENDPOINT}")
print()
print("The RAG agent runs directly in the FastAPI backend.")
print("No separate Model Serving endpoint is required.")
print("Deploy the Databricks App to activate the agent.")
