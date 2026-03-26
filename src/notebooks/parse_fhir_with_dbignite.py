# Databricks notebook source
# MAGIC %md
# MAGIC # Parse FHIR R4 Bundles with dbignite
# MAGIC
# MAGIC Reads FHIR R4 JSON Bundles from the UC Volume and uses **dbignite** to parse them
# MAGIC into clinical domain tables in Unity Catalog:
# MAGIC
# MAGIC | FHIR Resource | Target Table | Description |
# MAGIC |---|---|---|
# MAGIC | Patient | `Patient` | Demographics, identifiers, address |
# MAGIC | Encounter | `Encounter` | Visit records with class, type, period |
# MAGIC | Condition | `Condition` | Diagnoses (ICD-10) linked to encounters |
# MAGIC | Observation | `Observation` | Labs (LOINC) and vitals with reference ranges |
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Run `run_synthea_generation` first to produce FHIR bundles in `synthea_raw/fhir/`
# MAGIC - Run `run_data_generation` to produce providers (needed for practitioner NPI crosswalk)
# MAGIC - dbignite is installed via `%pip install` below

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "red_bricks_insurance_dev", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume_base = f"/Volumes/{catalog}/{schema}/raw_sources"
fhir_path = f"{volume_base}/synthea_raw/fhir"

print(f"Catalog: {catalog}")
print(f"Schema: {schema}")
print(f"FHIR bundles path: {fhir_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install dbignite

# COMMAND ----------

# MAGIC %pip install git+https://github.com/databrickslabs/dbignite.git --quiet
dbutils.library.restartPython()

# COMMAND ----------

# Re-read widgets after Python restart
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume_base = f"/Volumes/{catalog}/{schema}/raw_sources"
fhir_path = f"{volume_base}/synthea_raw/fhir"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read FHIR Bundles and Write Domain Tables
# MAGIC
# MAGIC dbignite reads FHIR JSON bundles and writes each resource type as a Delta table
# MAGIC directly into Unity Catalog using `bulk_table_write()`.

# COMMAND ----------

from dbignite.readers import read_from_directory
from dbignite.fhir_mapping_model import FhirSchemaModel

# Read all FHIR R4 JSON bundles from the volume
fhir_data = read_from_directory(fhir_path)

# Use R4 schema
fhir_schema = FhirSchemaModel(schema_version="r4")

# Parse entries with R4 schemas first — this caches the result internally.
# bulk_table_write() will then reuse this cached DataFrame.
entries_df = fhir_data.entry(schemas=fhir_schema)

print(f"Loaded FHIR bundles from: {fhir_path}")
print(f"Schema version: R4")
print(f"Columns available: {[c for c in entries_df.columns if c not in ('id','timestamp','bundleUUID')]}")

# COMMAND ----------

# Write the four clinical domain tables to Unity Catalog
target_location = f"{catalog}.{schema}"

fhir_data.bulk_table_write(
    location=target_location,
    write_mode="overwrite",
    columns=["Patient", "Encounter", "Condition", "Observation"],
)

print(f"Domain tables written to {target_location}:")
for t in ["Patient", "Encounter", "Condition", "Observation"]:
    print(f"  - {t}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Crosswalk Delta Tables
# MAGIC
# MAGIC **Patient crosswalk:** Maps Synthea Patient UUIDs to Red Bricks MBR IDs.
# MAGIC Used by `bronze.sql` to resolve member_id in encounter/observation queries.
# MAGIC
# MAGIC **Practitioner crosswalk:** Maps Synthea Practitioner UUIDs to provider NPIs.
# MAGIC Used by `bronze.sql` to resolve provider_npi in encounter queries.

# COMMAND ----------

import pyspark.sql.functions as F
import random

# --- Patient crosswalk ---
df_crosswalk = spark.read.parquet(f"{volume_base}/synthea_demographics/crosswalk.parquet")
df_crosswalk.select("synthea_uuid", "member_id").write.mode("overwrite").saveAsTable(
    f"{catalog}.{schema}.synthea_crosswalk"
)
crosswalk_count = spark.table(f"{catalog}.{schema}.synthea_crosswalk").count()
print(f"synthea_crosswalk: {crosswalk_count:,} rows")

# --- Practitioner crosswalk ---
# Extract unique practitioner UUIDs from Synthea FHIR bundles.
# Check practitionerInformation files first, then fall back to scanning patient bundles.
import os
import json

practitioner_uuids = set()
fhir_dir = f"{volume_base}/synthea_raw/fhir"

# Source 1: dedicated practitionerInformation files
for fname in os.listdir(fhir_dir):
    if fname.startswith("practitionerInformation") and fname.endswith(".json"):
        with open(os.path.join(fhir_dir, fname), "r") as f:
            prac_bundle = json.load(f)
        for entry in prac_bundle.get("entry", []):
            resource = entry.get("resource", {})
            if resource.get("resourceType") == "Practitioner":
                practitioner_uuids.add(resource.get("id"))

# Source 2: if no dedicated files found, scan patient bundles for Practitioner resources
if not practitioner_uuids:
    print("No practitionerInformation files found, scanning patient bundles...")
    for fname in os.listdir(fhir_dir):
        if not fname.endswith(".json") or fname.startswith(("hospitalInformation", "practitionerInformation")):
            continue
        with open(os.path.join(fhir_dir, fname), "r") as f:
            bundle = json.load(f)
        for entry in bundle.get("entry", []):
            resource = entry.get("resource", {})
            if resource.get("resourceType") == "Practitioner":
                practitioner_uuids.add(resource.get("id"))

practitioner_uuids = [u for u in practitioner_uuids if u]
print(f"Found {len(practitioner_uuids)} unique Synthea practitioner UUIDs")

# Read providers Parquet (generated by run_data_generation)
df_providers = spark.read.parquet(f"{volume_base}/providers/")
provider_npis = [row["npi"] for row in df_providers.select("npi").collect() if row["npi"]]

# Randomly assign NPIs to Synthea practitioner UUIDs
random.seed(42)
prac_crosswalk_rows = []
for uuid in practitioner_uuids:
    npi = random.choice(provider_npis)
    prac_crosswalk_rows.append({"synthea_practitioner_uuid": uuid, "provider_npi": npi})

if prac_crosswalk_rows:
    df_prac_crosswalk = spark.createDataFrame(prac_crosswalk_rows)
else:
    # Empty crosswalk — create with explicit schema to avoid inference error
    from pyspark.sql.types import StructType, StructField, StringType
    prac_schema = StructType([
        StructField("synthea_practitioner_uuid", StringType()),
        StructField("provider_npi", StringType()),
    ])
    df_prac_crosswalk = spark.createDataFrame([], schema=prac_schema)
    print("WARNING: No practitioner UUIDs found — writing empty crosswalk table")

df_prac_crosswalk.write.mode("overwrite").saveAsTable(
    f"{catalog}.{schema}.synthea_practitioner_crosswalk"
)
prac_count = spark.table(f"{catalog}.{schema}.synthea_practitioner_crosswalk").count()
print(f"synthea_practitioner_crosswalk: {prac_count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Domain Tables

# COMMAND ----------

for table in ["Patient", "Encounter", "Condition", "Observation",
              "synthea_crosswalk", "synthea_practitioner_crosswalk"]:
    try:
        df = spark.table(f"{catalog}.{schema}.{table}")
        count = df.count()
        cols = len(df.columns)
        print(f"  {table}: {count:,} rows, {cols} columns")
    except Exception as e:
        print(f"  {table}: ERROR - {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview: Patient Table

# COMMAND ----------

display(spark.table(f"{catalog}.{schema}.Patient").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview: Condition Table (ICD-10 Diagnoses)

# COMMAND ----------

display(spark.table(f"{catalog}.{schema}.Condition").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview: Observation Table (Labs + Vitals with LOINC)

# COMMAND ----------

display(spark.table(f"{catalog}.{schema}.Observation").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview: Encounter Table

# COMMAND ----------

display(spark.table(f"{catalog}.{schema}.Encounter").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Analytics

# COMMAND ----------

# Explore the schema of each domain table
for table in ["Patient", "Encounter", "Condition", "Observation"]:
    print(f"\n--- {table} schema ---")
    df = spark.table(f"{catalog}.{schema}.{table}")
    df.printSchema()

# COMMAND ----------

# Top diagnoses by frequency (navigate the nested FHIR struct)
# Use SQL for reliable nested struct navigation — PySpark bracket notation
# can conflict with dbignite's array-typed FHIR fields.
try:
    spark.sql(f"""
        SELECT
            Condition.code.coding[0].code   AS icd10_code,
            Condition.code.coding[0].display AS diagnosis,
            COUNT(*)                         AS condition_count,
            COUNT(DISTINCT Condition.subject.reference) AS unique_patients
        FROM {catalog}.{schema}.Condition
        GROUP BY 1, 2
        ORDER BY condition_count DESC
        LIMIT 15
    """).display()
except Exception as e:
    print(f"Condition summary skipped (schema exploration): {e}")

# COMMAND ----------

# Observation distribution (labs + vitals)
try:
    spark.sql(f"""
        SELECT
            Observation.code.coding[0].code    AS loinc_code,
            Observation.code.coding[0].display  AS observation_name,
            Observation.category[0].coding[0].code AS category,
            COUNT(*)                            AS observation_count,
            ROUND(AVG(Observation.valueQuantity.value), 2) AS avg_value,
            ROUND(MIN(Observation.valueQuantity.value), 2) AS min_value,
            ROUND(MAX(Observation.valueQuantity.value), 2) AS max_value
        FROM {catalog}.{schema}.Observation
        GROUP BY 1, 2, 3
        ORDER BY observation_count DESC
        LIMIT 20
    """).display()
except Exception as e:
    print(f"Observation summary skipped (schema exploration): {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done!
# MAGIC
# MAGIC Clinical domain tables are now available in Unity Catalog:
# MAGIC - `Patient` — Member demographics in FHIR format
# MAGIC - `Encounter` — Clinical visits with SNOMED types
# MAGIC - `Condition` — ICD-10 diagnoses linked to encounters
# MAGIC - `Observation` — Labs + vitals with LOINC codes and reference ranges
# MAGIC
# MAGIC These tables join back to insurance claims via `member_id` (Patient.id).
