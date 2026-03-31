# Databricks notebook source
# MAGIC %md
# MAGIC # Red Bricks Insurance — Vector Search Index Setup
# MAGIC
# MAGIC Creates a Vector Search endpoint and Delta Sync index on `silver_case_notes_chunks`
# MAGIC for the Member RAG Agent. Uses **managed embeddings** (`databricks-bge-large-en`)
# MAGIC so no separate embedding model endpoint is needed.

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance", "Catalog")

catalog = dbutils.widgets.get("catalog")

VS_ENDPOINT_NAME = "red-bricks-vs-endpoint"
VS_INDEX_NAME = f"{catalog}.documents.case_notes_vs_index"
SOURCE_TABLE = f"{catalog}.documents.silver_case_notes_chunks"

print(f"Catalog: {catalog}")
print(f"VS Endpoint: {VS_ENDPOINT_NAME}")
print(f"VS Index: {VS_INDEX_NAME}")
print(f"Source Table: {SOURCE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper: REST API client
# MAGIC
# MAGIC The Databricks Python SDK has deserialization issues with certain Vector Search
# MAGIC responses. We use direct REST API calls as a reliable fallback alongside SDK calls.

# COMMAND ----------

import requests
import time
import json

# Get workspace host and token from the notebook context
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
host = ctx.apiUrl().get()
token = ctx.apiToken().get()

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json",
}


def vs_api_get(path: str) -> dict:
    """GET request to Vector Search API."""
    resp = requests.get(f"{host}/api/2.0/vector-search/{path}", headers=headers)
    resp.raise_for_status()
    return resp.json()


def vs_api_post(path: str, body: dict) -> dict:
    """POST request to Vector Search API."""
    resp = requests.post(
        f"{host}/api/2.0/vector-search/{path}", headers=headers, json=body
    )
    resp.raise_for_status()
    return resp.json()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create or reuse Vector Search endpoint

# COMMAND ----------

# Check for existing endpoint
endpoints = vs_api_get("endpoints")
endpoint_names = [ep["name"] for ep in endpoints.get("endpoints", [])]

if VS_ENDPOINT_NAME in endpoint_names:
    print(f"Vector Search endpoint '{VS_ENDPOINT_NAME}' already exists — reusing.")
else:
    print(f"Creating Vector Search endpoint '{VS_ENDPOINT_NAME}'...")
    vs_api_post("endpoints", {"name": VS_ENDPOINT_NAME, "endpoint_type": "STANDARD"})

    # Wait for endpoint to be ready
    for i in range(60):
        ep = vs_api_get(f"endpoints/{VS_ENDPOINT_NAME}")
        state = ep.get("endpoint_status", {}).get("state", "UNKNOWN")
        print(f"  Endpoint status: {state} ({i*10}s)")
        if state == "ONLINE":
            break
        time.sleep(10)
    print(f"Endpoint '{VS_ENDPOINT_NAME}' is ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Delta Sync index with managed embeddings

# COMMAND ----------

# Check if index already exists via REST API
try:
    idx_info = vs_api_get(f"indexes/{VS_INDEX_NAME}")
    idx_status = idx_info.get("status", {})
    print(f"Index '{VS_INDEX_NAME}' already exists.")
    print(f"  Status: {idx_status.get('detailed_state', 'UNKNOWN')}")
    print(f"  Indexed rows: {idx_status.get('indexed_row_count', 'N/A')}")
    print(f"  Ready: {idx_status.get('ready', False)}")

    # Trigger sync if index is online
    if idx_status.get("ready"):
        print("Index is ready. Triggering sync...")
        vs_api_post(f"indexes/{VS_INDEX_NAME}/sync", {})
    else:
        print("Index exists but not ready yet. Will wait...")
except requests.exceptions.HTTPError as e:
    if e.response.status_code == 404:
        print(f"Index '{VS_INDEX_NAME}' does not exist. Creating...")
        vs_api_post(
            "indexes",
            {
                "name": VS_INDEX_NAME,
                "endpoint_name": VS_ENDPOINT_NAME,
                "primary_key": "chunk_id",
                "index_type": "DELTA_SYNC",
                "delta_sync_index_spec": {
                    "source_table": SOURCE_TABLE,
                    "pipeline_type": "TRIGGERED",
                    "embedding_source_columns": [
                        {
                            "name": "chunk_text",
                            "embedding_model_endpoint_name": "databricks-bge-large-en",
                        }
                    ],
                },
            },
        )
        print(f"Index '{VS_INDEX_NAME}' creation initiated.")
    else:
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wait for index to be ready

# COMMAND ----------

index_ready = False
for i in range(120):  # Up to 20 minutes
    try:
        idx_info = vs_api_get(f"indexes/{VS_INDEX_NAME}")
        idx_status = idx_info.get("status", {})
        ready = idx_status.get("ready", False)
        detailed = idx_status.get("detailed_state", "UNKNOWN")
        row_count = idx_status.get("indexed_row_count", 0)

        if i % 6 == 0 or ready:
            print(f"  [{i*10}s] state={detailed}, ready={ready}, rows={row_count}")

        if ready:
            index_ready = True
            break
    except Exception as e:
        print(f"  [{i*10}s] Waiting... ({e})")
    time.sleep(10)

if index_ready:
    print(f"\nVector Search index '{VS_INDEX_NAME}' is ONLINE and ready for queries.")
else:
    print(f"\nWarning: Index not ready after 20 min. Attempting test query anyway...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test: similarity search

# COMMAND ----------

# Query via REST API (SDK query_index can also have deserialization issues)
test_results = None
for attempt in range(6):
    try:
        result = vs_api_post(
            f"indexes/{VS_INDEX_NAME}/query",
            {
                "columns": [
                    "chunk_id",
                    "document_id",
                    "member_id",
                    "document_type",
                    "chunk_text",
                ],
                "query_text": "diabetes management and blood glucose control",
                "num_results": 3,
            },
        )
        test_results = result
        break
    except Exception as e:
        print(f"  Test query attempt {attempt+1} failed: {e}")
        if attempt < 5:
            time.sleep(30)

if test_results and test_results.get("result", {}).get("data_array"):
    data = test_results["result"]["data_array"]
    print("Test query: 'diabetes management and blood glucose control'")
    print(f"Results: {len(data)} chunks returned")
    for row in data:
        print(f"  - {row[0]} | {row[2]} | {row[3]} | {str(row[4])[:80]}...")
else:
    print("Warning: Test query returned no results. Index may still be syncing.")
    print(f"Raw response: {test_results}")
