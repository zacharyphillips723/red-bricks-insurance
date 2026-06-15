# Databricks notebook source
# MAGIC %md
# MAGIC # Red Bricks Insurance — Medical Policy Vector Search Index
# MAGIC
# MAGIC Parses medical policy **PDFs** from the Unity Catalog volume, chunks the text
# MAGIC content, and builds a **Delta Sync Vector Search index** for the FWA agent's
# MAGIC RAG pipeline. Uses **managed embeddings** (`databricks-bge-large-en`).
# MAGIC
# MAGIC **PDF source:** `Volumes/{catalog}/prior_auth/medical_policies_pdfs/`
# MAGIC **Chunks table:** `prior_auth.medical_policy_chunks`
# MAGIC **VS Index:** `prior_auth.medical_policy_vs_index`

# COMMAND ----------

# MAGIC %pip install pymupdf --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance_catalog", "Catalog")

catalog = dbutils.widgets.get("catalog")

VS_ENDPOINT_NAME = "red-bricks-vs-endpoint"
VS_INDEX_NAME = f"{catalog}.prior_auth.medical_policy_vs_index"
SOURCE_TABLE = f"{catalog}.prior_auth.medical_policy_chunks"
PDF_VOLUME_PATH = f"/Volumes/{catalog}/raw/raw_sources/medical_policies_pdfs"

print(f"Catalog: {catalog}")
print(f"VS Endpoint: {VS_ENDPOINT_NAME}")
print(f"VS Index: {VS_INDEX_NAME}")
print(f"Source Table: {SOURCE_TABLE}")
print(f"PDF Volume: {PDF_VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper: REST API client

# COMMAND ----------

import requests
import time
import json

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
host = ctx.apiUrl().get()
token = ctx.apiToken().get()

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json",
}


def vs_api_get(path: str) -> dict:
    resp = requests.get(f"{host}/api/2.0/vector-search/{path}", headers=headers)
    resp.raise_for_status()
    return resp.json()


def vs_api_post(path: str, body: dict) -> dict:
    resp = requests.post(
        f"{host}/api/2.0/vector-search/{path}", headers=headers, json=body
    )
    resp.raise_for_status()
    return resp.json()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse PDFs and build chunks table
# MAGIC
# MAGIC Extracts text from each medical policy PDF using PyMuPDF, then splits into
# MAGIC section-based chunks keyed by the section headings in the PDF (Purpose, Covered
# MAGIC Services, Clinical Criteria, etc.). Each chunk gets a unique `chunk_id` and
# MAGIC carries the `policy_id`, `policy_name`, and `service_category` metadata.

# COMMAND ----------

import fitz  # PyMuPDF
import os
import re

pdf_dir = PDF_VOLUME_PATH
pdf_files = [f for f in os.listdir(pdf_dir) if f.endswith(".pdf")]
print(f"Found {len(pdf_files)} PDF files in {pdf_dir}")

# Section headers used in the generated PDFs
SECTION_PATTERN = re.compile(
    r"^(\d+)\.\s+(Purpose and Scope|Covered Services|Applicable Diagnosis Codes|"
    r"Clinical Criteria for Authorization|Step Therapy Requirements|"
    r"Required Documentation|Exclusions and Limitations|Appeal Process|References)",
    re.MULTILINE,
)

chunks = []

for pdf_file in sorted(pdf_files):
    pdf_path = os.path.join(pdf_dir, pdf_file)
    policy_id = pdf_file.replace(".pdf", "")

    doc = fitz.open(pdf_path)
    full_text = ""
    for page in doc:
        full_text += page.get_text()
    doc.close()

    # Extract policy name (first substantial line after the header)
    lines = full_text.split("\n")
    policy_name = ""
    service_category = ""
    for line in lines:
        line = line.strip()
        if line and "RED BRICKS" not in line and "CONFIDENTIAL" not in line and "Page " not in line:
            if not policy_name and len(line) > 10:
                policy_name = line
                break

    # Try to extract service_category from policy_id -> lookup
    # We'll also parse it from the metadata line if present
    for line in lines:
        if "Policy ID:" in line:
            # Extract effective date etc. — we mainly need service_category
            break

    # Split text into sections based on numbered headings
    sections = SECTION_PATTERN.split(full_text)

    # sections[0] = preamble (title + metadata)
    # Then groups of (section_number, section_title, section_body)
    preamble = sections[0].strip() if sections else ""

    # Create a chunk for the preamble/overview
    chunk_id_counter = 0
    chunk_id_counter += 1
    chunks.append({
        "chunk_id": f"{policy_id}_overview",
        "policy_id": policy_id,
        "policy_name": policy_name,
        "service_category": "",  # will be filled from policy metadata table
        "section": "overview",
        "chunk_text": preamble[:2000],  # cap preamble
    })

    # Process each section
    i = 1
    while i < len(sections) - 2:
        section_num = sections[i].strip()
        section_title = sections[i + 1].strip()
        section_body = sections[i + 2].strip() if i + 2 < len(sections) else ""
        i += 3

        # For long sections (e.g. Covered Services tables), split into sub-chunks
        # Target ~500-800 tokens per chunk
        if len(section_body) > 1500:
            # Split on double newlines or bullet points
            sub_parts = re.split(r"\n\n+", section_body)
            sub_chunk_text = ""
            sub_idx = 0
            for part in sub_parts:
                if len(sub_chunk_text) + len(part) > 1200 and sub_chunk_text:
                    sub_idx += 1
                    chunks.append({
                        "chunk_id": f"{policy_id}_s{section_num}_{sub_idx}",
                        "policy_id": policy_id,
                        "policy_name": policy_name,
                        "service_category": "",
                        "section": section_title,
                        "chunk_text": f"{policy_name} | {section_title}\n\n{sub_chunk_text}",
                    })
                    sub_chunk_text = part
                else:
                    sub_chunk_text = f"{sub_chunk_text}\n\n{part}" if sub_chunk_text else part

            if sub_chunk_text.strip():
                sub_idx += 1
                chunks.append({
                    "chunk_id": f"{policy_id}_s{section_num}_{sub_idx}",
                    "policy_id": policy_id,
                    "policy_name": policy_name,
                    "service_category": "",
                    "section": section_title,
                    "chunk_text": f"{policy_name} | {section_title}\n\n{sub_chunk_text}",
                })
        else:
            chunks.append({
                "chunk_id": f"{policy_id}_s{section_num}",
                "policy_id": policy_id,
                "policy_name": policy_name,
                "service_category": "",
                "section": section_title,
                "chunk_text": f"{policy_name} | {section_title}\n\n{section_body}",
            })

print(f"Generated {len(chunks)} chunks from {len(pdf_files)} PDFs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enrich chunks with service_category from policy metadata
# MAGIC
# MAGIC Join against `bronze_medical_policy_rules` or `policy_metadata` to get
# MAGIC the `service_category` for each `policy_id`.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# Create chunks DataFrame
schema = StructType([
    StructField("chunk_id", StringType(), False),
    StructField("policy_id", StringType(), False),
    StructField("policy_name", StringType(), True),
    StructField("service_category", StringType(), True),
    StructField("section", StringType(), True),
    StructField("chunk_text", StringType(), True),
])

chunks_df = spark.createDataFrame(chunks, schema)

# Try to get service_category from existing policy metadata table
try:
    policy_meta = (
        spark.table(f"{catalog}.prior_auth.bronze_medical_policy_rules")
        .select("policy_id", "service_category")
        .distinct()
    )
    chunks_df = (
        chunks_df.drop("service_category")
        .join(policy_meta, on="policy_id", how="left")
    )
    print("Enriched chunks with service_category from bronze_medical_policy_rules")
except Exception as e:
    print(f"Could not enrich service_category (will use empty): {e}")

# Write to Delta with CDF enabled
(chunks_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .option("delta.enableChangeDataFeed", "true")
    .saveAsTable(SOURCE_TABLE))

row_count = spark.table(SOURCE_TABLE).count()
print(f"Wrote {row_count} policy chunks to {SOURCE_TABLE}")
display(spark.table(SOURCE_TABLE).limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create or reuse Vector Search endpoint

# COMMAND ----------

endpoints = vs_api_get("endpoints")
endpoint_names = [ep["name"] for ep in endpoints.get("endpoints", [])]

if VS_ENDPOINT_NAME in endpoint_names:
    print(f"Vector Search endpoint '{VS_ENDPOINT_NAME}' already exists -- reusing.")
else:
    print(f"Creating Vector Search endpoint '{VS_ENDPOINT_NAME}'...")
    vs_api_post("endpoints", {"name": VS_ENDPOINT_NAME, "endpoint_type": "STANDARD"})
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

try:
    idx_info = vs_api_get(f"indexes/{VS_INDEX_NAME}")
    idx_status = idx_info.get("status", {})
    print(f"Index '{VS_INDEX_NAME}' already exists.")
    print(f"  Status: {idx_status.get('detailed_state', 'UNKNOWN')}")
    print(f"  Indexed rows: {idx_status.get('indexed_row_count', 'N/A')}")
    print(f"  Ready: {idx_status.get('ready', False)}")

    if idx_status.get("ready"):
        try:
            print("Index is ready. Triggering sync to pick up new PDF chunks...")
            vs_api_post(f"indexes/{VS_INDEX_NAME}/sync", {})
        except requests.exceptions.HTTPError as sync_err:
            print(f"Sync request returned {sync_err.response.status_code} -- may already be syncing.")
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
for i in range(120):
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
# MAGIC ## Test: similarity search on PDF-sourced chunks

# COMMAND ----------

test_queries = [
    "Does our policy cover CGM devices for patients with HbA1c above 8?",
    "What are the step therapy requirements for biologic medications in rheumatoid arthritis?",
    "When is modifier 25 appropriately used with E/M services?",
    "What constitutes duplicate billing under our claims policy?",
]

for query in test_queries:
    try:
        result = vs_api_post(
            f"indexes/{VS_INDEX_NAME}/query",
            {
                "columns": [
                    "chunk_id",
                    "policy_name",
                    "service_category",
                    "chunk_text",
                ],
                "query_text": query,
                "num_results": 3,
            },
        )
        data = result.get("result", {}).get("data_array", [])
        print(f"\nQuery: '{query}'")
        print(f"Results: {len(data)} chunks")
        for row in data:
            print(f"  - {row[0]} | {row[1]} | {row[2]} | section={row[3]}")
            print(f"    {str(row[4])[:120]}...")
    except Exception as e:
        print(f"\nQuery failed: '{query}' -- {e}")

print("\nSetup complete. VS index is ready for the FWA agent RAG pipeline.")
