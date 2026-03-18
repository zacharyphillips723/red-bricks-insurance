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
# MAGIC - Run `run_data_generation` first to produce FHIR bundles
# MAGIC - dbignite is installed via `%pip install` below

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "red_bricks_insurance_dev", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume_base = f"/Volumes/{catalog}/{schema}/raw_sources"
fhir_path = f"{volume_base}/clinical/fhir_bundles"

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
fhir_path = f"{volume_base}/clinical/fhir_bundles"

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
# MAGIC ## Verify Domain Tables

# COMMAND ----------

for table in ["Patient", "Encounter", "Condition", "Observation"]:
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
df_cond = spark.table(f"{catalog}.{schema}.Condition")
# dbignite wraps the resource in a column named after the resource type
from pyspark.sql import functions as F

if "Condition" in df_cond.columns:
    # Nested struct: Condition.code.coding[0].code
    df_cond.select(
        F.col("Condition.code.coding")[0]["code"].alias("icd10_code"),
        F.col("Condition.code.coding")[0]["display"].alias("diagnosis"),
        F.col("Condition.subject.reference").alias("patient_ref"),
    ).groupBy("icd10_code", "diagnosis").agg(
        F.count("*").alias("condition_count"),
        F.countDistinct("patient_ref").alias("unique_patients"),
    ).orderBy(F.desc("condition_count")).limit(15).display()
else:
    # Flat schema — columns are top-level
    display(df_cond.limit(15))

# COMMAND ----------

# Observation distribution (labs + vitals)
df_obs = spark.table(f"{catalog}.{schema}.Observation")

if "Observation" in df_obs.columns:
    df_obs.select(
        F.col("Observation.code.coding")[0]["code"].alias("loinc_code"),
        F.col("Observation.code.coding")[0]["display"].alias("observation_name"),
        F.col("Observation.category")[0]["coding"][0]["code"].alias("category"),
        F.col("Observation.valueQuantity.value").alias("value"),
    ).groupBy("loinc_code", "observation_name", "category").agg(
        F.count("*").alias("observation_count"),
        F.round(F.avg("value"), 2).alias("avg_value"),
        F.round(F.min("value"), 2).alias("min_value"),
        F.round(F.max("value"), 2).alias("max_value"),
    ).orderBy(F.desc("observation_count")).limit(20).display()
else:
    display(df_obs.limit(20))

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
