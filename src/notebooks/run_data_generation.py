# Databricks notebook source
# MAGIC %md
# MAGIC # Red Bricks Insurance — Synthetic Data Generation
# MAGIC
# MAGIC Generates all domains for the healthcare insurance snapshot. Run this job to refresh
# MAGIC raw data before pipelines. Outputs Parquet (and JSON for clinical) under the bundle volume.
# MAGIC
# MAGIC **Domains:** Providers, Members, Enrollment, Claims (medical + pharmacy), Clinical (encounters/labs/vitals), Underwriting, Risk Adjustment.
# MAGIC **Data quality:** ~2% bad data (nulls, invalid codes, out-of-range dates) for SDP expectations.
# MAGIC **Correlation:** Diagnosis-driven labs, risk-tier vs risk_score, member–provider linkage.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "red_bricks_insurance_dev", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume_base = f"/Volumes/{catalog}/{schema}/raw_sources"

print(f"Catalog: {catalog}, Schema: {schema}")
print(f"Volume base: {volume_base}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Seed and imports

# COMMAND ----------

import random
from pyspark.sql import SparkSession
import pyspark.sql.types as T

random.seed(42)
Faker = None  # lazy import in domains that need it

spark = SparkSession.builder.getOrCreate()

# Add bundle/project root to path so we can import src.data_generation
import os
import sys
try:
    _here = os.path.dirname(os.path.abspath(__file__))
    _repo_root = os.path.abspath(os.path.join(_here, "..", ".."))
except NameError:
    # In Databricks notebook context, derive bundle root from the notebook path.
    _nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    # Notebook path is like /Workspace/Users/.../files/src/notebooks/run_data_generation
    # Bundle files root is two directories above src/notebooks/
    _ws_root = "/Workspace" + _nb_path.rsplit("/src/notebooks/", 1)[0] if not _nb_path.startswith("/Workspace") else _nb_path.rsplit("/src/notebooks/", 1)[0]
    _repo_root = _ws_root
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)
from src.data_generation.domains import providers as dom_providers
from src.data_generation.domains import members as dom_members
from src.data_generation.domains import enrollment as dom_enrollment
from src.data_generation.domains import claims as dom_claims
from src.data_generation.domains import clinical as dom_clinical
from src.data_generation.domains import underwriting as dom_underwriting
from src.data_generation.domains import risk_adjustment as dom_risk
from src.data_generation.domains import fhir_bundles as dom_fhir
from src.data_generation.domains import documents as dom_documents

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create catalog/schema/volume and generate master data

# COMMAND ----------

# Catalog may already exist (e.g. FEVM Default Storage); skip creation errors gracefully
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
except Exception as e:
    print(f"Catalog creation skipped (may already exist): {e}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.raw_sources")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Providers

# COMMAND ----------

providers_data = dom_providers.generate_providers(500)
provider_schema = T.StructType([
    T.StructField("npi", T.StringType()),
    T.StructField("provider_first_name", T.StringType()),
    T.StructField("provider_last_name", T.StringType()),
    T.StructField("provider_name", T.StringType()),
    T.StructField("credential", T.StringType()),
    T.StructField("specialty", T.StringType()),
    T.StructField("taxonomy_code", T.StringType()),
    T.StructField("tax_id", T.StringType()),
    T.StructField("group_name", T.StringType()),
    T.StructField("network_status", T.StringType()),
    T.StructField("effective_date", T.StringType()),
    T.StructField("termination_date", T.StringType()),
    T.StructField("address_line_1", T.StringType()),
    T.StructField("city", T.StringType()),
    T.StructField("state", T.StringType()),
    T.StructField("zip_code", T.StringType()),
    T.StructField("county", T.StringType()),
    T.StructField("phone", T.StringType()),
])
df_providers = spark.createDataFrame(providers_data, schema=provider_schema)
df_providers.write.mode("overwrite").parquet(f"{volume_base}/providers/")
print("Providers:", df_providers.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Members (demographics)

# COMMAND ----------

members_data = dom_members.generate_members(5000)
member_schema = T.StructType([
    T.StructField("member_id", T.StringType()),
    T.StructField("last_name", T.StringType()),
    T.StructField("first_name", T.StringType()),
    T.StructField("date_of_birth", T.StringType()),
    T.StructField("gender", T.StringType()),
    T.StructField("ssn_last_4", T.StringType()),
    T.StructField("address_line_1", T.StringType()),
    T.StructField("city", T.StringType()),
    T.StructField("state", T.StringType()),
    T.StructField("zip_code", T.StringType()),
    T.StructField("county", T.StringType()),
    T.StructField("phone", T.StringType()),
    T.StructField("email", T.StringType()),
])
df_members = spark.createDataFrame(members_data, schema=member_schema)
df_members.write.mode("overwrite").parquet(f"{volume_base}/members/")
member_ids = [m["member_id"] for m in members_data]
print("Members:", len(member_ids))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enrollment (plan / LOB)

# COMMAND ----------

enrollment_data = dom_enrollment.generate_enrollment(member_ids)
enrollment_schema = T.StructType([
    T.StructField("member_id", T.StringType()),
    T.StructField("subscriber_id", T.StringType()),
    T.StructField("relationship", T.StringType()),
    T.StructField("line_of_business", T.StringType()),
    T.StructField("plan_type", T.StringType()),
    T.StructField("plan_id", T.StringType()),
    T.StructField("group_number", T.StringType()),
    T.StructField("group_name", T.StringType()),
    T.StructField("eligibility_start_date", T.StringType()),
    T.StructField("eligibility_end_date", T.StringType()),
    T.StructField("monthly_premium", T.DoubleType()),
    T.StructField("rating_area", T.StringType()),
    T.StructField("risk_score", T.DoubleType()),
    T.StructField("metal_level", T.StringType()),
])
df_enrollment = spark.createDataFrame(enrollment_data, schema=enrollment_schema)
df_enrollment.write.mode("overwrite").parquet(f"{volume_base}/enrollment/")
print("Enrollment:", df_enrollment.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Claims (medical + pharmacy)

# COMMAND ----------

medical_claims_data = dom_claims.generate_medical_claims(enrollment_data, providers_data, 40000)
medical_schema = T.StructType([
    T.StructField("claim_id", T.StringType()),
    T.StructField("claim_line_number", T.IntegerType()),
    T.StructField("claim_type", T.StringType()),
    T.StructField("member_id", T.StringType()),
    T.StructField("rendering_provider_npi", T.StringType()),
    T.StructField("billing_provider_npi", T.StringType()),
    T.StructField("service_from_date", T.StringType()),
    T.StructField("service_to_date", T.StringType()),
    T.StructField("paid_date", T.StringType()),
    T.StructField("admission_date", T.StringType()),
    T.StructField("discharge_date", T.StringType()),
    T.StructField("admission_type", T.StringType()),
    T.StructField("discharge_status", T.StringType()),
    T.StructField("bill_type", T.StringType()),
    T.StructField("place_of_service_code", T.StringType()),
    T.StructField("place_of_service_desc", T.StringType()),
    T.StructField("procedure_code", T.StringType()),
    T.StructField("procedure_desc", T.StringType()),
    T.StructField("revenue_code", T.StringType()),
    T.StructField("revenue_desc", T.StringType()),
    T.StructField("drg_code", T.StringType()),
    T.StructField("drg_desc", T.StringType()),
    T.StructField("primary_diagnosis_code", T.StringType()),
    T.StructField("primary_diagnosis_desc", T.StringType()),
    T.StructField("secondary_diagnosis_code_1", T.StringType()),
    T.StructField("secondary_diagnosis_code_2", T.StringType()),
    T.StructField("secondary_diagnosis_code_3", T.StringType()),
    T.StructField("billed_amount", T.DoubleType()),
    T.StructField("allowed_amount", T.DoubleType()),
    T.StructField("paid_amount", T.DoubleType()),
    T.StructField("copay", T.DoubleType()),
    T.StructField("coinsurance", T.DoubleType()),
    T.StructField("deductible", T.DoubleType()),
    T.StructField("member_responsibility", T.DoubleType()),
    T.StructField("claim_status", T.StringType()),
    T.StructField("denial_reason_code", T.StringType()),
    T.StructField("adjustment_reason", T.StringType()),
    T.StructField("source_system", T.StringType()),
])
df_medical = spark.createDataFrame(medical_claims_data, schema=medical_schema)
df_medical.write.mode("overwrite").parquet(f"{volume_base}/claims_medical/")

pharmacy_claims_data = dom_claims.generate_pharmacy_claims(enrollment_data, providers_data, 15000)
pharmacy_schema = T.StructType([
    T.StructField("claim_id", T.StringType()),
    T.StructField("member_id", T.StringType()),
    T.StructField("prescriber_npi", T.StringType()),
    T.StructField("pharmacy_npi", T.StringType()),
    T.StructField("pharmacy_name", T.StringType()),
    T.StructField("fill_date", T.StringType()),
    T.StructField("paid_date", T.StringType()),
    T.StructField("ndc", T.StringType()),
    T.StructField("drug_name", T.StringType()),
    T.StructField("therapeutic_class", T.StringType()),
    T.StructField("is_specialty", T.BooleanType()),
    T.StructField("days_supply", T.IntegerType()),
    T.StructField("quantity", T.IntegerType()),
    T.StructField("ingredient_cost", T.DoubleType()),
    T.StructField("dispensing_fee", T.DoubleType()),
    T.StructField("total_cost", T.DoubleType()),
    T.StructField("member_copay", T.DoubleType()),
    T.StructField("plan_paid", T.DoubleType()),
    T.StructField("claim_status", T.StringType()),
    T.StructField("formulary_tier", T.StringType()),
    T.StructField("mail_order_flag", T.StringType()),
])
df_pharmacy = spark.createDataFrame(pharmacy_claims_data, schema=pharmacy_schema)
df_pharmacy.write.mode("overwrite").parquet(f"{volume_base}/claims_pharmacy/")
print("Medical claims:", df_medical.count(), "Pharmacy claims:", df_pharmacy.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clinical (encounters, labs, vitals) — JSON for dbignite

# COMMAND ----------

# Correlate primary diagnosis to member for lab/value correlation
primary_dx_by_member = {}
for c in medical_claims_data:
    mid = c.get("member_id")
    dx = c.get("primary_diagnosis_code")
    if mid and dx and dx != "INVALID":
        primary_dx_by_member[mid] = dx

provider_npis = [p["npi"] for p in providers_data if p.get("npi")]

encounters, labs, vitals = dom_clinical.generate_clinical_events(
    member_ids, primary_dx_by_member, provider_npis,
    n_encounters=6000, n_lab_results=12000, n_vitals=10000,
)

enc_schema = T.StructType([
    T.StructField("encounter_id", T.StringType()),
    T.StructField("member_id", T.StringType()),
    T.StructField("provider_npi", T.StringType()),
    T.StructField("date_of_service", T.StringType()),
    T.StructField("encounter_type", T.StringType()),
    T.StructField("visit_type", T.StringType()),
])
lab_schema = T.StructType([
    T.StructField("lab_result_id", T.StringType()),
    T.StructField("member_id", T.StringType()),
    T.StructField("lab_name", T.StringType()),
    T.StructField("value", T.DoubleType()),
    T.StructField("unit", T.StringType()),
    T.StructField("reference_range_low", T.DoubleType()),
    T.StructField("reference_range_high", T.DoubleType()),
    T.StructField("collection_date", T.StringType()),
])
vit_schema = T.StructType([
    T.StructField("vital_id", T.StringType()),
    T.StructField("member_id", T.StringType()),
    T.StructField("vital_name", T.StringType()),
    T.StructField("value", T.DoubleType()),
    T.StructField("measurement_date", T.StringType()),
])

df_enc = spark.createDataFrame(encounters, schema=enc_schema)
df_labs = spark.createDataFrame(labs, schema=lab_schema)
df_vitals = spark.createDataFrame(vitals, schema=vit_schema)

# Write as JSON for dbignite parsing
df_enc.write.mode("overwrite").json(f"{volume_base}/clinical/encounters/")
df_labs.write.mode("overwrite").json(f"{volume_base}/clinical/lab_results/")
df_vitals.write.mode("overwrite").json(f"{volume_base}/clinical/vitals/")
print("Clinical: encounters", df_enc.count(), "labs", df_labs.count(), "vitals", df_vitals.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### FHIR R4 Bundles (for dbignite parsing)
# MAGIC
# MAGIC Generates one FHIR R4 Bundle per member containing Patient, Encounter,
# MAGIC Condition, and Observation (labs + vitals) resources. These are written
# MAGIC as individual JSON files and parsed by dbignite into clinical domain tables.

# COMMAND ----------

# Build secondary diagnosis mapping for richer Condition resources
secondary_dx_by_member = {}
for c in medical_claims_data:
    mid = c.get("member_id")
    if not mid:
        continue
    for key in ("secondary_diagnosis_code_1", "secondary_diagnosis_code_2", "secondary_diagnosis_code_3"):
        dx = c.get(key)
        if dx and dx != "INVALID":
            secondary_dx_by_member.setdefault(mid, []).append(dx)

fhir_bundle_strings = dom_fhir.generate_fhir_bundles(
    members_data=members_data,
    encounters=encounters,
    labs=labs,
    vitals=vitals,
    primary_dx_by_member=primary_dx_by_member,
    secondary_dx_by_member=secondary_dx_by_member,
)

# Write each bundle as a separate JSON file (one per member)
fhir_output_path = f"{volume_base}/clinical/fhir_bundles"
dbutils.fs.mkdirs(fhir_output_path)
# Clear any previous bundles
for f in dbutils.fs.ls(fhir_output_path):
    dbutils.fs.rm(f.path)

for i, bundle_json in enumerate(fhir_bundle_strings):
    file_path = f"{fhir_output_path}/bundle_{i:05d}.json"
    dbutils.fs.put(file_path, bundle_json, overwrite=True)

print(f"FHIR R4 Bundles: {len(fhir_bundle_strings)} bundles written to {fhir_output_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Documents (case notes, call transcripts, claims summaries — PDF + metadata)

# COMMAND ----------

documents_data = dom_documents.generate_documents(
    members_data=members_data,
    encounters=encounters,
    claims_data=medical_claims_data,
    primary_dx_by_member=primary_dx_by_member,
    n_per_member=3,
)

# Create directories for PDFs (visual demo) and metadata JSON (pipeline ingestion)
for doc_type in ["case_notes", "call_transcripts", "claims_summarys"]:
    dbutils.fs.mkdirs(f"{volume_base}/documents/{doc_type}")
dbutils.fs.mkdirs(f"{volume_base}/documents/metadata")

# Build metadata records (everything except pdf_bytes) and write PDFs
# PDFs are written to the local FUSE mount path for binary compatibility
metadata_records = []
fuse_base = volume_base.replace("/Volumes/", "/Volumes/")  # Already FUSE-compatible

for doc in documents_data:
    # Metadata record for pipeline (no pdf_bytes)
    metadata_records.append({k: v for k, v in doc.items() if k != "pdf_bytes"})

    # Write PDF via FUSE mount (binary write)
    doc_type = doc["document_type"]
    pdf_fuse_path = f"/Volumes/{catalog}/{schema}/raw_sources/documents/{doc_type}s/{doc['file_name']}"
    os.makedirs(os.path.dirname(pdf_fuse_path), exist_ok=True)
    with open(pdf_fuse_path, "wb") as f:
        f.write(doc["pdf_bytes"])

# Write metadata as Spark JSON (for Auto Loader ingestion by documents pipeline)
meta_schema = T.StructType([
    T.StructField("document_id", T.StringType()),
    T.StructField("member_id", T.StringType()),
    T.StructField("document_type", T.StringType()),
    T.StructField("title", T.StringType()),
    T.StructField("created_date", T.StringType()),
    T.StructField("author", T.StringType()),
    T.StructField("full_text", T.StringType()),
    T.StructField("file_name", T.StringType()),
])
df_meta = spark.createDataFrame(metadata_records, schema=meta_schema)
df_meta.write.mode("overwrite").json(f"{volume_base}/documents/metadata/")

print(f"Documents: {len(documents_data)} total ({df_meta.count()} metadata records)")
print(f"  PDFs written to: {volume_base}/documents/")
print(f"  Metadata JSON: {volume_base}/documents/metadata/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Underwriting

# COMMAND ----------

risk_score_by_member = {e["member_id"]: e["risk_score"] for e in enrollment_data if e.get("risk_score") is not None}
underwriting_data = dom_underwriting.generate_underwriting(member_ids, risk_score_by_member)
uw_schema = T.StructType([
    T.StructField("member_id", T.StringType()),
    T.StructField("risk_tier", T.StringType()),
    T.StructField("smoker_indicator", T.StringType()),
    T.StructField("bmi_band", T.StringType()),
    T.StructField("occupation_class", T.StringType()),
    T.StructField("medical_history_indicator", T.BooleanType()),
    T.StructField("underwriting_effective_date", T.StringType()),
])
df_uw = spark.createDataFrame(underwriting_data, schema=uw_schema)
df_uw.write.mode("overwrite").parquet(f"{volume_base}/underwriting/")
print("Underwriting:", df_uw.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Risk Adjustment (member + provider level)

# COMMAND ----------

risk_member_data = dom_risk.generate_risk_adjustment_member(member_ids, model_year=2024)
risk_member_schema = T.StructType([
    T.StructField("member_id", T.StringType()),
    T.StructField("model_year", T.IntegerType()),
    T.StructField("raf_score", T.DoubleType()),
    T.StructField("hcc_codes", T.StringType()),
    T.StructField("measurement_period_start", T.StringType()),
    T.StructField("measurement_period_end", T.StringType()),
    T.StructField("measurement_date", T.StringType()),
])
df_risk_member = spark.createDataFrame(risk_member_data, schema=risk_member_schema)
df_risk_member.write.mode("overwrite").parquet(f"{volume_base}/risk_adjustment_member/")

risk_provider_data = dom_risk.generate_risk_adjustment_provider(
    provider_npis, risk_member_data, n_assignments=2500
)
if risk_provider_data:
    risk_provider_schema = T.StructType([
        T.StructField("provider_npi", T.StringType()),
        T.StructField("member_id", T.StringType()),
        T.StructField("raf_score", T.DoubleType()),
        T.StructField("attribution_date", T.StringType()),
    ])
    df_risk_provider = spark.createDataFrame(risk_provider_data, schema=risk_provider_schema)
    df_risk_provider.write.mode("overwrite").parquet(f"{volume_base}/risk_adjustment_provider/")
    print("Risk adjustment (member):", df_risk_member.count(), "(provider):", df_risk_provider.count())
else:
    print("Risk adjustment (member):", df_risk_member.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("RED BRICKS INSURANCE — DATA GENERATION COMPLETE")
print("=" * 60)
print(f"Volume: {volume_base}")
print("  providers/               -> Parquet")
print("  members/                 -> Parquet")
print("  enrollment/              -> Parquet")
print("  claims_medical/          -> Parquet")
print("  claims_pharmacy/         -> Parquet")
print("  clinical/encounters/     -> JSON")
print("  clinical/lab_results/    -> JSON")
print("  clinical/vitals/         -> JSON")
print("  clinical/fhir_bundles/   -> FHIR R4 JSON Bundles (dbignite)")
print("  documents/case_notes/    -> PDF")
print("  documents/call_transcripts/ -> PDF")
print("  documents/claims_summaries/ -> PDF")
print("  documents/metadata/      -> JSON (for pipeline ingestion)")
print("  underwriting/            -> Parquet")
print("  risk_adjustment_member/  -> Parquet")
print("  risk_adjustment_provider/-> Parquet")
print("Data quality: ~2% defects in key fields for SDP expectations.")
