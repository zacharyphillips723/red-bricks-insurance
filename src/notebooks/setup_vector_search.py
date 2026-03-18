# Databricks notebook source
# MAGIC %md
# MAGIC # Red Bricks Insurance — Vector Search Index Setup
# MAGIC
# MAGIC Creates a Vector Search endpoint and Delta Sync index on `silver_case_notes_chunks`
# MAGIC for the Member RAG Agent. Uses **managed embeddings** (`databricks-bge-large-en`)
# MAGIC so no separate embedding model endpoint is needed.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "red_bricks_insurance_dev", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

VS_ENDPOINT_NAME = "red-bricks-vs-endpoint"
VS_INDEX_NAME = f"{catalog}.{schema}.case_notes_vs_index"
SOURCE_TABLE = f"{catalog}.{schema}.silver_case_notes_chunks"

print(f"Catalog: {catalog}, Schema: {schema}")
print(f"VS Endpoint: {VS_ENDPOINT_NAME}")
print(f"VS Index: {VS_INDEX_NAME}")
print(f"Source Table: {SOURCE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create or reuse Vector Search endpoint

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import time

w = WorkspaceClient()

# Check for existing endpoint
existing_endpoints = list(w.vector_search_endpoints.list_endpoints())
endpoint_exists = any(ep.name == VS_ENDPOINT_NAME for ep in existing_endpoints)

if endpoint_exists:
    print(f"Vector Search endpoint '{VS_ENDPOINT_NAME}' already exists — reusing.")
else:
    print(f"Creating Vector Search endpoint '{VS_ENDPOINT_NAME}'...")
    w.vector_search_endpoints.create_endpoint(
        name=VS_ENDPOINT_NAME,
        endpoint_type="STANDARD",
    )
    # Wait for endpoint to be ready
    for i in range(60):
        ep = w.vector_search_endpoints.get_endpoint(VS_ENDPOINT_NAME)
        status = getattr(ep, "endpoint_status", None)
        state = getattr(status, "state", None) if status else None
        state_str = getattr(state, "value", str(state)) if state else "UNKNOWN"
        print(f"  Endpoint status: {state_str} ({i*10}s)")
        if state_str == "ONLINE":
            break
        time.sleep(10)
    print(f"Endpoint '{VS_ENDPOINT_NAME}' is ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Delta Sync index with managed embeddings

# COMMAND ----------

# Check if index already exists
try:
    existing_index = w.vector_search_indexes.get_index(VS_INDEX_NAME)
    print(f"Index '{VS_INDEX_NAME}' already exists. Triggering sync...")
    w.vector_search_indexes.sync_index(VS_INDEX_NAME)
except Exception:
    print(f"Creating Delta Sync index '{VS_INDEX_NAME}'...")
    w.vector_search_indexes.create_index(
        name=VS_INDEX_NAME,
        endpoint_name=VS_ENDPOINT_NAME,
        primary_key="chunk_id",
        index_type="DELTA_SYNC",
        delta_sync_index_spec={
            "source_table": SOURCE_TABLE,
            "pipeline_type": "TRIGGERED",
            "embedding_source_columns": [
                {
                    "name": "chunk_text",
                    "embedding_model_endpoint_name": "databricks-bge-large-en",
                }
            ],
        },
    )
    print(f"Index '{VS_INDEX_NAME}' creation initiated.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wait for index to be ready

# COMMAND ----------

for i in range(90):
    try:
        idx = w.vector_search_indexes.get_index(VS_INDEX_NAME)
        status = getattr(idx, "status", None)
        state = getattr(status, "ready", None) if status else None
        print(f"  Index status: ready={state} ({i*10}s)")
        if state:
            break
    except Exception as e:
        print(f"  Waiting... ({e})")
    time.sleep(10)

print(f"\nVector Search index '{VS_INDEX_NAME}' is ONLINE and ready for queries.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test: similarity search

# COMMAND ----------

test_results = w.vector_search_indexes.query_index(
    index_name=VS_INDEX_NAME,
    columns=["chunk_id", "document_id", "member_id", "document_type", "chunk_text"],
    query_text="diabetes management and blood glucose control",
    num_results=3,
)

print("Test query: 'diabetes management and blood glucose control'")
print(f"Results: {len(test_results.result.data_array)} chunks returned")
for row in test_results.result.data_array:
    print(f"  - {row[0]} | {row[2]} | {row[3]} | {row[4][:80]}...")
