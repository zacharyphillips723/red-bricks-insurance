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

dbutils.widgets.text("catalog", "red_bricks_insurance", "Catalog")

catalog = dbutils.widgets.get("catalog")
volume_base = f"/Volumes/{catalog}/raw/raw_sources"
fhir_path = f"{volume_base}/synthea_raw/fhir"
clinical_schema = f"{catalog}.clinical"

print(f"Catalog: {catalog}")
print(f"Clinical schema: {clinical_schema}")
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
volume_base = f"/Volumes/{catalog}/raw/raw_sources"
fhir_path = f"{volume_base}/synthea_raw/fhir"
clinical_schema = f"{catalog}.clinical"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read FHIR Bundles and Write Domain Tables
# MAGIC
# MAGIC dbignite reads FHIR JSON bundles and writes each resource type as a Delta table
# MAGIC directly into Unity Catalog using `bulk_table_write()`.

# COMMAND ----------

from dbignite.readers import read_from_directory
from dbignite.fhir_mapping_model import FhirSchemaModel
import time

# Read all FHIR R4 JSON bundles from the volume
fhir_data = read_from_directory(fhir_path)

# Use R4 schema
fhir_schema = FhirSchemaModel(schema_version="r4")

# Parse entries with R4 schemas. On serverless, persist() is not supported,
# but serverless Spark has aggressive disk caching and auto-scaling that
# compensate. The count() below forces materialization of the lazy parse plan.
entries_df = fhir_data.entry(schemas=fhir_schema)

t0 = time.time()
row_count = entries_df.count()
print(f"Parsed {row_count:,} entry rows in {time.time() - t0:.0f}s")
print(f"Loaded FHIR bundles from: {fhir_path}")
print(f"Schema version: R4")
print(f"Columns available: {[c for c in entries_df.columns if c not in ('id','timestamp','bundleUUID')]}")

# COMMAND ----------

# Write the four clinical domain tables to Unity Catalog.
# bulk_table_write uses a ThreadPool to write tables in parallel.
target_location = clinical_schema

t0 = time.time()
fhir_data.bulk_table_write(
    location=target_location,
    write_mode="overwrite",
    columns=["Patient", "Encounter", "Condition", "Observation"],
)
print(f"Domain tables written to {target_location} in {time.time() - t0:.0f}s:")
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
import time

t0 = time.time()

# --- Patient crosswalk ---
df_crosswalk = spark.read.parquet(f"{volume_base}/synthea_demographics/crosswalk.parquet")
df_crosswalk.select("synthea_uuid", "member_id").write.mode("overwrite").saveAsTable(
    f"{clinical_schema}.synthea_crosswalk"
)
crosswalk_count = spark.table(f"{clinical_schema}.synthea_crosswalk").count()
print(f"synthea_crosswalk: {crosswalk_count:,} rows ({time.time() - t0:.0f}s)")

# --- Practitioner crosswalk ---
# Use Spark to read all FHIR bundles in parallel and extract Practitioner UUIDs.
# Much faster than serial Python file I/O over 5K+ JSON files.
t1 = time.time()
fhir_dir = f"{volume_base}/synthea_raw/fhir"

df_bundles = spark.read.option("multiLine", True).json(f"{fhir_dir}/*.json")
df_practitioners = (
    df_bundles
    .select(F.explode("entry").alias("entry"))
    .filter(F.col("entry.resource.resourceType") == "Practitioner")
    .select(F.col("entry.resource.id").alias("synthea_practitioner_uuid"))
    .distinct()
    .filter(F.col("synthea_practitioner_uuid").isNotNull())
)

prac_count = df_practitioners.count()
print(f"Found {prac_count} unique Synthea practitioner UUIDs ({time.time() - t1:.0f}s)")

# Read providers and randomly assign NPIs via a deterministic cross-join + row_number
df_providers = (
    spark.read.parquet(f"{volume_base}/providers/")
    .select("npi")
    .filter(F.col("npi").isNotNull())
    .distinct()
)

# Assign each practitioner a deterministic NPI using hash-based modular mapping.
# Collect providers to driver (small table, ~500 rows) and broadcast for the join.
provider_npis = [row["npi"] for row in df_providers.collect()]
provider_count = len(provider_npis)

# Build a small lookup DataFrame with index → NPI
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
prov_rows = [(i, npi) for i, npi in enumerate(provider_npis)]
df_prov_lookup = spark.createDataFrame(prov_rows, ["_prov_idx", "npi"])

df_prac_numbered = df_practitioners.withColumn(
    "_prac_hash", F.abs(F.hash("synthea_practitioner_uuid")) % F.lit(provider_count)
)

df_prac_crosswalk = (
    df_prac_numbered
    .join(F.broadcast(df_prov_lookup), df_prac_numbered["_prac_hash"] == df_prov_lookup["_prov_idx"])
    .select(
        F.col("synthea_practitioner_uuid"),
        F.col("npi").alias("provider_npi")
    )
)

df_prac_crosswalk.write.mode("overwrite").saveAsTable(
    f"{clinical_schema}.synthea_practitioner_crosswalk"
)
final_count = spark.table(f"{clinical_schema}.synthea_practitioner_crosswalk").count()
print(f"synthea_practitioner_crosswalk: {final_count:,} rows ({time.time() - t1:.0f}s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Domain Tables

# COMMAND ----------

for table in ["Patient", "Encounter", "Condition", "Observation",
              "synthea_crosswalk", "synthea_practitioner_crosswalk"]:
    try:
        df = spark.table(f"{clinical_schema}.{table}")
        count = df.count()
        cols = len(df.columns)
        print(f"  {table}: {count:,} rows, {cols} columns")
    except Exception as e:
        print(f"  {table}: ERROR - {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview: Row Counts
# MAGIC
# MAGIC Previews are skipped in the pipeline (deeply nested FHIR structs exceed
# MAGIC display limits). Query the tables directly or use the clinical pipeline
# MAGIC silver views for flattened, queryable columns.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Summary analytics (schema exploration, top diagnoses, observation distributions)
# MAGIC are omitted from the pipeline run for speed. Use the preview tables above or
# MAGIC query the gold analytics views for detailed breakdowns.

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
