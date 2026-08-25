# Databricks notebook source
# MAGIC %md
# MAGIC # Standards-Based PA Intake — X12 278 + Da Vinci FHIR PAS
# MAGIC
# MAGIC Demonstrates multi-channel, standards-based Prior Authorization intake
# MAGIC (RFI: Intake + Integration & Interoperability):
# MAGIC 1. Emits raw **X12 278** (EDI) and **Da Vinci PAS** (FHIR Claim) documents
# MAGIC    from a sample of PA requests → landed in UC Volumes (the "wire format").
# MAGIC 2. Parses both channels back into one normalized shape (Python — the same
# MAGIC    role the **X12 EDI Ember** accelerator and **dbignite** play in prod).
# MAGIC 3. Writes governed UC tables `prior_auth.bronze_pa_intake` (raw-parsed,
# MAGIC    channel-tagged) and `prior_auth.silver_pa_intake` (validated).
# MAGIC
# MAGIC Runs as a job task after `data_generation`.

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance_catalog")
catalog = dbutils.widgets.get("catalog")
catalog_sql = f"`{catalog}`"
volume_base = f"/Volumes/{catalog}/raw/raw_sources"
print(f"Catalog: {catalog}")

# COMMAND ----------

# Import the pure-Python standards codec from the repo (works whether the
# notebook runs from the workspace files sync or a local checkout).
import os, sys, json, glob

def _repo_root() -> str:
    try:
        here = os.path.dirname(os.path.abspath(__file__))
        return os.path.abspath(os.path.join(here, "..", ".."))
    except NameError:
        nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        root = nb.rsplit("/src/notebooks/", 1)[0]
        return ("/Workspace" + root) if not root.startswith("/Workspace") else root

sys.path.insert(0, _repo_root())
from src.data_generation.domains.pa_intake import (
    build_x12_278, parse_x12_278, build_fhir_pas_claim, parse_fhir_pas_claim,
)

# COMMAND ----------

# MAGIC %md ## 1. Emit raw X12 278 + FHIR PAS documents from a sample of PA requests

# COMMAND ----------

N_SAMPLES = 250
src_requests = (
    spark.read.parquet(f"{volume_base}/prior_auth_requests/")
    .select("auth_request_id", "member_id", "requesting_provider_npi",
            "service_type", "procedure_code", "diagnosis_codes", "urgency", "request_date")
    .limit(N_SAMPLES)
    .collect()
)
print(f"Loaded {len(src_requests)} PA requests to render as X12 278 / FHIR PAS")

x12_dir = f"{volume_base}/intake_x12_278"
fhir_dir = f"{volume_base}/intake_fhir_pas"
os.makedirs(x12_dir, exist_ok=True)
os.makedirs(fhir_dir, exist_ok=True)

# Split the sample across the two channels (some providers submit EDI, some FHIR).
for i, r in enumerate(src_requests):
    req = r.asDict()
    # diagnosis_codes stored as pipe-delimited string in the parquet
    if isinstance(req.get("diagnosis_codes"), str):
        req["diagnosis_codes"] = req["diagnosis_codes"]
    if i % 2 == 0:
        with open(f"{x12_dir}/{req['auth_request_id']}.edi", "w") as f:
            f.write(build_x12_278(req))
    else:
        with open(f"{fhir_dir}/{req['auth_request_id']}.json", "w") as f:
            json.dump(build_fhir_pas_claim(req), f)

n_x12 = len(glob.glob(f"{x12_dir}/*.edi"))
n_fhir = len(glob.glob(f"{fhir_dir}/*.json"))
print(f"Wrote {n_x12} X12 278 files and {n_fhir} FHIR PAS files to the volume")

# COMMAND ----------

# MAGIC %md ## 2. Parse both channels back into one normalized shape

# COMMAND ----------

normalized = []
for path in glob.glob(f"{x12_dir}/*.edi"):
    with open(path) as f:
        rec = parse_x12_278(f.read())
    rec["source_file"] = os.path.basename(path)
    normalized.append(rec)
for path in glob.glob(f"{fhir_dir}/*.json"):
    with open(path) as f:
        rec = parse_fhir_pas_claim(json.load(f))
    rec["source_file"] = os.path.basename(path)
    normalized.append(rec)

print(f"Parsed {len(normalized)} intake documents into normalized records")

# COMMAND ----------

# MAGIC %md ## 3. Write governed UC tables (bronze + silver)

# COMMAND ----------

from pyspark.sql import functions as F, types as T

schema = T.StructType([
    T.StructField("auth_request_id", T.StringType()),
    T.StructField("member_id", T.StringType()),
    T.StructField("requesting_provider_npi", T.StringType()),
    T.StructField("service_type", T.StringType()),
    T.StructField("procedure_code", T.StringType()),
    T.StructField("diagnosis_codes", T.StringType()),
    T.StructField("urgency", T.StringType()),
    T.StructField("request_date", T.StringType()),
    T.StructField("source_channel", T.StringType()),
    T.StructField("source_file", T.StringType()),
])
rows = [{k: rec.get(k) for k in schema.fieldNames()} for rec in normalized]

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_sql}.prior_auth")
bronze = (
    spark.createDataFrame(rows, schema)
    .withColumn("ingestion_timestamp", F.current_timestamp())
)
bronze.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog_sql}.prior_auth.bronze_pa_intake"
)
print("Wrote prior_auth.bronze_pa_intake:", bronze.count(), "rows")

# Silver: validated + typed; drop records missing the identifiers a review needs.
silver = (
    bronze
    .filter(F.col("auth_request_id").isNotNull() & F.col("member_id").isNotNull())
    .filter(F.col("procedure_code").isNotNull())
    .withColumn("request_date", F.to_date("request_date"))
    .withColumn("urgency", F.when(F.col("urgency").isin("standard", "expedited"), F.col("urgency"))
                .otherwise(F.lit("standard")))
    .dropDuplicates(["auth_request_id"])
)
silver.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog_sql}.prior_auth.silver_pa_intake"
)
print("Wrote prior_auth.silver_pa_intake:", silver.count(), "rows")

# COMMAND ----------

display(spark.sql(f"""
    SELECT source_channel, COUNT(*) AS documents,
           COUNT(DISTINCT procedure_code) AS distinct_procedures
    FROM {catalog_sql}.prior_auth.silver_pa_intake
    GROUP BY source_channel ORDER BY source_channel
"""))

# COMMAND ----------

dbutils.notebook.exit("OK")
