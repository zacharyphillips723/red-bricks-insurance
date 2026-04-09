# Databricks notebook source
# MAGIC %md
# MAGIC # Red Bricks Insurance — Synthetic Data Generation
# MAGIC
# MAGIC Generates all domains for the healthcare insurance snapshot. Run this job to refresh
# MAGIC raw data before pipelines. Outputs Parquet (and JSON for clinical) under the bundle volume.
# MAGIC
# MAGIC **Domains:** Providers, Members, Enrollment, Claims (medical + pharmacy), Documents, Benefits, Underwriting, Risk Adjustment.
# MAGIC **Clinical data** (encounters, labs, vitals, FHIR bundles) now comes from Synthea — see `run_synthea_generation.py`.
# MAGIC **Data quality:** ~2% bad data (nulls, invalid codes, out-of-range dates) for SDP expectations.
# MAGIC **Correlation:** Diagnosis-driven labs, risk-tier vs risk_score, member–provider linkage.

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance", "Catalog")

catalog = dbutils.widgets.get("catalog")
volume_base = f"/Volumes/{catalog}/raw/raw_sources"

print(f"Catalog: {catalog}")
print(f"Volume base: {volume_base}")

# COMMAND ----------

# MAGIC %pip install faker fpdf2 --quiet --retries 10 --timeout 120

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Re-read widgets after Python restart
catalog = dbutils.widgets.get("catalog")
catalog_sql = f"`{catalog}`"  # SQL-safe quoting (handles hyphens in catalog names)
volume_base = f"/Volumes/{catalog}/raw/raw_sources"

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
from src.data_generation.domains import underwriting as dom_underwriting
from src.data_generation.domains import risk_adjustment as dom_risk
from src.data_generation.domains import documents as dom_documents
from src.data_generation.domains import benefits as dom_benefits
from src.data_generation.domains import groups as dom_groups
from src.data_generation.domains import fwa as dom_fwa

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create catalog/schema/volume and generate master data

# COMMAND ----------

# --- Catalog creation with graceful fallback ---
# Try SQL first; if permissions block us, attempt REST API with managed location;
# finally verify the catalog is usable or fail with a clear message.
_catalog_ready = False

# Attempt 1: SQL CREATE CATALOG
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_sql}")
    print(f"Catalog '{catalog}' created (or already exists) via SQL.")
    _catalog_ready = True
except Exception as e:
    print(f"SQL CREATE CATALOG failed: {e}")

# Attempt 2: REST API with workspace default storage (some metastores require this)
if not _catalog_ready:
    try:
        import requests as _cat_req
        _cat_host = spark.conf.get("spark.databricks.workspaceUrl")
        _cat_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
        _cat_resp = _cat_req.post(
            f"https://{_cat_host}/api/2.1/unity-catalog/catalogs",
            headers={"Authorization": f"Bearer {_cat_token}", "Content-Type": "application/json"},
            json={"name": catalog, "comment": "Red Bricks Insurance demo catalog"},
        )
        if _cat_resp.status_code == 200:
            print(f"Catalog '{catalog}' created via REST API.")
            _catalog_ready = True
        elif "already exists" in _cat_resp.text.lower():
            print(f"Catalog '{catalog}' already exists.")
            _catalog_ready = True
        else:
            print(f"REST API catalog creation failed: {_cat_resp.status_code} — {_cat_resp.text[:300]}")
    except Exception as e2:
        print(f"REST API catalog creation failed: {e2}")

# Attempt 3: Check if the catalog already exists and is usable
if not _catalog_ready:
    try:
        spark.sql(f"USE CATALOG {catalog_sql}")
        print(f"Catalog '{catalog}' exists and is usable (created externally).")
        _catalog_ready = True
    except Exception as e3:
        print(f"Catalog '{catalog}' does not exist and cannot be created.")
        print(f"Please create the catalog manually or pass an existing catalog name via the 'catalog' widget.")
        raise RuntimeError(
            f"Cannot proceed without catalog '{catalog}'. "
            f"Create it in the workspace UI or pass a different catalog name."
        ) from e3

# Create all domain schemas
DOMAIN_SCHEMAS = [
    "raw", "members", "claims", "providers", "documents",
    "risk_adjustment", "underwriting", "clinical", "benefits", "analytics", "fwa",
]
for s in DOMAIN_SCHEMAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_sql}.{s}")
    print(f"  Schema: {catalog}.{s}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_sql}.raw.raw_sources")

# Clean old data so Auto Loader doesn't re-ingest stale files on pipeline refresh
for subdir in ["members", "enrollment", "providers", "claims_medical", "claims_pharmacy",
               "underwriting", "risk_adjustment_member",
               "risk_adjustment_provider", "documents", "benefits", "groups",
               "fwa_signals", "fwa_provider_profiles", "fwa_investigation_cases"]:
    path = f"{volume_base}/{subdir}"
    try:
        dbutils.fs.rm(path, recurse=True)
        print(f"  Cleaned {path}")
    except Exception:
        pass  # directory may not exist yet
print("Volume cleaned — ready for fresh data generation.")

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
provider_npis = [p["npi"] for p in providers_data if p.get("npi")]
print("Providers:", df_providers.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Synthea Demographics (if available)

# COMMAND ----------

synthea_crosswalk_path = f"{volume_base}/synthea_demographics/crosswalk.parquet"
try:
    df_synthea = spark.read.parquet(synthea_crosswalk_path)
    synthea_demographics = [row.asDict() for row in df_synthea.collect()]
    member_lob_map = {r["member_id"]: r["line_of_business"] for r in synthea_demographics}
    print(f"Loaded {len(synthea_demographics)} Synthea demographics from {synthea_crosswalk_path}")
except Exception as e:
    print(f"No Synthea demographics found ({e}), falling back to Faker-only generation")
    synthea_demographics = None
    member_lob_map = None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Employer Groups

# COMMAND ----------

groups_data = dom_groups.generate_groups(200)
groups_schema = T.StructType([
    T.StructField("group_id", T.StringType()),
    T.StructField("group_name", T.StringType()),
    T.StructField("sic_code", T.StringType()),
    T.StructField("industry", T.StringType()),
    T.StructField("state", T.StringType()),
    T.StructField("group_size", T.IntegerType()),
    T.StructField("group_size_tier", T.StringType()),
    T.StructField("funding_type", T.StringType()),
    T.StructField("specific_stop_loss_attachment", T.DoubleType()),
    T.StructField("aggregate_stop_loss_attachment_pct", T.DoubleType()),
    T.StructField("expected_annual_claims", T.DoubleType()),
    T.StructField("admin_fee_pmpm", T.DoubleType()),
    T.StructField("stop_loss_premium_pmpm", T.DoubleType()),
    T.StructField("effective_date", T.StringType()),
    T.StructField("renewal_date", T.StringType()),
])
df_groups = spark.createDataFrame(groups_data, schema=groups_schema)
df_groups.write.mode("overwrite").parquet(f"{volume_base}/groups/")
print("Groups:", df_groups.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Members (demographics)

# COMMAND ----------

members_data = dom_members.generate_members(5000, synthea_demographics=synthea_demographics)
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

enrollment_data = dom_enrollment.generate_enrollment(member_ids, member_lob_map=member_lob_map, group_data=groups_data)
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
# MAGIC ### Benefits (plan benefit schedules and cost-sharing)

# COMMAND ----------

benefits_data = dom_benefits.generate_benefits(enrollment_data)
benefits_schema = T.StructType([
    T.StructField("benefit_id", T.StringType()),
    T.StructField("plan_id", T.StringType()),
    T.StructField("member_id", T.StringType()),
    T.StructField("line_of_business", T.StringType()),
    T.StructField("plan_type", T.StringType()),
    T.StructField("benefit_category", T.StringType()),
    T.StructField("benefit_name", T.StringType()),
    T.StructField("benefit_code", T.StringType()),
    T.StructField("in_network_copay", T.DoubleType()),
    T.StructField("in_network_coinsurance_pct", T.IntegerType()),
    T.StructField("out_of_network_copay", T.DoubleType()),
    T.StructField("out_of_network_coinsurance_pct", T.IntegerType()),
    T.StructField("deductible_applies", T.BooleanType()),
    T.StructField("prior_auth_required", T.BooleanType()),
    T.StructField("visit_limit", T.IntegerType()),
    T.StructField("annual_limit", T.DoubleType()),
    T.StructField("coverage_level", T.StringType()),
    T.StructField("individual_deductible", T.DoubleType()),
    T.StructField("family_deductible", T.DoubleType()),
    T.StructField("individual_oop_max", T.DoubleType()),
    T.StructField("family_oop_max", T.DoubleType()),
    # Tier 1: Actuarial / pricing levers
    T.StructField("actuarial_value_pct", T.IntegerType()),
    T.StructField("allowed_amount_schedule", T.StringType()),
    T.StructField("network_tier", T.StringType()),
    T.StructField("cost_trend_factor", T.DoubleType()),
    T.StructField("pharmacy_trend_factor", T.DoubleType()),
    T.StructField("age_sex_factor", T.DoubleType()),
    # Tier 1: Utilization modeling
    T.StructField("expected_utilization_per_1000", T.DoubleType()),
    T.StructField("unit_cost_assumption", T.DoubleType()),
    T.StructField("elasticity_factor", T.DoubleType()),
    # Tier 1: Benefit versioning
    T.StructField("benefit_effective_date", T.StringType()),
    T.StructField("benefit_termination_date", T.StringType()),
    T.StructField("benefit_version", T.IntegerType()),
    T.StructField("scenario_id", T.StringType()),
    T.StructField("is_baseline", T.BooleanType()),
    # Tier 1: Agent-friendly metadata
    T.StructField("benefit_description", T.StringType()),
    T.StructField("clinical_guideline_ref", T.StringType()),
    T.StructField("regulatory_mandate", T.StringType()),
])
df_benefits = spark.createDataFrame(benefits_data, schema=benefits_schema)
df_benefits.write.mode("overwrite").parquet(f"{volume_base}/benefits/")
print("Benefits:", df_benefits.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Claims (medical + pharmacy)

# COMMAND ----------

import time as _time

print("Generating 150K medical claims...")
_t0 = _time.time()
medical_claims_data = dom_claims.generate_medical_claims(enrollment_data, providers_data, 150000)
print(f"  Medical claims generated in {_time.time() - _t0:.0f}s")
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
spark.createDataFrame(medical_claims_data, schema=medical_schema).write.mode("overwrite").parquet(f"{volume_base}/claims_medical/")
print(f"  Medical claims total: {len(medical_claims_data):,}")

print("Generating 50K pharmacy claims...")
_t1 = _time.time()
pharmacy_claims_data = dom_claims.generate_pharmacy_claims(enrollment_data, providers_data, 50000)
print(f"  Pharmacy claims generated in {_time.time() - _t1:.0f}s")
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
spark.createDataFrame(pharmacy_claims_data, schema=pharmacy_schema).write.mode("overwrite").parquet(f"{volume_base}/claims_pharmacy/")
print(f"  Pharmacy claims total: {len(pharmacy_claims_data):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### FWA Signals, Provider Profiles, and Investigation Cases
# MAGIC
# MAGIC Derived domain — analyzes existing claims/providers/members to generate
# MAGIC FWA signal records, provider risk profiles, and pre-seeded investigation cases.

# COMMAND ----------

print("Generating FWA signals from medical + pharmacy claims...")
_t_fwa = _time.time()
fwa_signals_data = dom_fwa.generate_fwa_signals(
    medical_claims=medical_claims_data,
    pharmacy_claims=pharmacy_claims_data,
    providers=providers_data,
    members=members_data,
    fraud_rate=0.07,
)
print(f"  FWA signals generated in {_time.time() - _t_fwa:.0f}s")

fwa_signals_schema = T.StructType([
    T.StructField("signal_id", T.StringType()),
    T.StructField("claim_id", T.StringType()),
    T.StructField("member_id", T.StringType()),
    T.StructField("provider_npi", T.StringType()),
    T.StructField("fraud_type", T.StringType()),
    T.StructField("fraud_type_desc", T.StringType()),
    T.StructField("fraud_score", T.DoubleType()),
    T.StructField("severity", T.StringType()),
    T.StructField("detection_method", T.StringType()),
    T.StructField("evidence_summary", T.StringType()),
    T.StructField("evidence_detail_json", T.StringType()),
    T.StructField("service_date", T.StringType()),
    T.StructField("paid_amount", T.DoubleType()),
    T.StructField("estimated_overpayment", T.DoubleType()),
    T.StructField("detection_date", T.StringType()),
])
spark.createDataFrame(fwa_signals_data, schema=fwa_signals_schema).write.mode("overwrite").parquet(f"{volume_base}/fwa_signals/")
print(f"  FWA signals total: {len(fwa_signals_data):,}")

# COMMAND ----------

print("Generating FWA provider profiles...")
fwa_provider_profiles_data = dom_fwa.generate_fwa_provider_profiles(
    medical_claims=medical_claims_data,
    providers=providers_data,
    fwa_signals=fwa_signals_data,
)

fwa_profiles_schema = T.StructType([
    T.StructField("provider_npi", T.StringType()),
    T.StructField("provider_name", T.StringType()),
    T.StructField("specialty", T.StringType()),
    T.StructField("total_claims", T.IntegerType()),
    T.StructField("total_billed", T.DoubleType()),
    T.StructField("total_paid", T.DoubleType()),
    T.StructField("avg_billed_per_claim", T.DoubleType()),
    T.StructField("billed_to_allowed_ratio", T.DoubleType()),
    T.StructField("e5_visit_pct", T.DoubleType()),
    T.StructField("unique_members", T.IntegerType()),
    T.StructField("denial_rate", T.DoubleType()),
    T.StructField("fwa_signal_count", T.IntegerType()),
    T.StructField("fwa_score_avg", T.DoubleType()),
    T.StructField("risk_tier", T.StringType()),
    T.StructField("behavioral_flags", T.StringType()),
])
spark.createDataFrame(fwa_provider_profiles_data, schema=fwa_profiles_schema).write.mode("overwrite").parquet(f"{volume_base}/fwa_provider_profiles/")
print(f"  FWA provider profiles total: {len(fwa_provider_profiles_data):,}")

# COMMAND ----------

print("Generating FWA investigation cases...")
fwa_investigation_cases_data = dom_fwa.generate_fwa_investigation_cases(
    fwa_signals=fwa_signals_data,
    provider_profiles=fwa_provider_profiles_data,
    n_cases=75,
)

fwa_cases_schema = T.StructType([
    T.StructField("investigation_id", T.StringType()),
    T.StructField("investigation_type", T.StringType()),
    T.StructField("target_type", T.StringType()),
    T.StructField("target_id", T.StringType()),
    T.StructField("target_name", T.StringType()),
    T.StructField("fraud_types", T.StringType()),
    T.StructField("severity", T.StringType()),
    T.StructField("status", T.StringType()),
    T.StructField("estimated_overpayment", T.DoubleType()),
    T.StructField("claims_involved_count", T.IntegerType()),
    T.StructField("investigation_summary", T.StringType()),
    T.StructField("evidence_summary", T.StringType()),
    T.StructField("rules_risk_score", T.DoubleType()),
    T.StructField("ml_risk_score", T.DoubleType()),
    T.StructField("created_date", T.StringType()),
])
spark.createDataFrame(fwa_investigation_cases_data, schema=fwa_cases_schema).write.mode("overwrite").parquet(f"{volume_base}/fwa_investigation_cases/")
print(f"  FWA investigation cases total: {len(fwa_investigation_cases_data):,}")

# COMMAND ----------

# Create empty fwa_ml_predictions table so gold_fwa_model_scores MV can reference it
# before the ML model training notebook populates it with real scores.
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_sql}.analytics.fwa_ml_predictions (
        claim_id STRING,
        ml_fraud_probability DOUBLE,
        ml_risk_tier STRING,
        model_version STRING,
        scored_at STRING
    ) USING DELTA
""")
print(f"  Created (if not exists): {catalog}.analytics.fwa_ml_predictions")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Documents (case notes, call transcripts, claims summaries — PDF + metadata)
# MAGIC
# MAGIC Clinical data (encounters, labs, vitals, FHIR bundles) is now generated by Synthea
# MAGIC and parsed by dbignite — no longer produced by this notebook.

# COMMAND ----------

# Build primary diagnosis mapping for documents (still needed for document content)
primary_dx_by_member = {}
for c in medical_claims_data:
    mid = c.get("member_id")
    dx = c.get("primary_diagnosis_code")
    if mid and dx and dx != "INVALID":
        primary_dx_by_member[mid] = dx

documents_data = dom_documents.generate_documents(
    members_data=members_data,
    encounters=[],
    claims_data=medical_claims_data,
    primary_dx_by_member=primary_dx_by_member,
    n_per_member=3,
)

# Create directories for PDFs (visual demo) and metadata JSON (pipeline ingestion)
for doc_type in ["case_notes", "call_transcripts", "claims_summarys"]:
    dbutils.fs.mkdirs(f"{volume_base}/documents/{doc_type}")
dbutils.fs.mkdirs(f"{volume_base}/documents/metadata")

# Build metadata records (everything except pdf_bytes) and write PDFs in parallel
# PDFs are written to the local FUSE mount path for binary compatibility
metadata_records = []
for doc in documents_data:
    metadata_records.append({k: v for k, v in doc.items() if k != "pdf_bytes"})

# Ensure target directories exist
for doc_type in ["case_notes", "call_transcripts", "claims_summarys"]:
    os.makedirs(f"/Volumes/{catalog}/raw/raw_sources/documents/{doc_type}", exist_ok=True)

# Write PDFs in parallel using ThreadPoolExecutor
from concurrent.futures import ThreadPoolExecutor, as_completed

def _write_pdf(doc):
    doc_type = doc["document_type"]
    pdf_path = f"/Volumes/{catalog}/raw/raw_sources/documents/{doc_type}s/{doc['file_name']}"
    with open(pdf_path, "wb") as f:
        f.write(doc["pdf_bytes"])

with ThreadPoolExecutor(max_workers=16) as executor:
    futures = [executor.submit(_write_pdf, doc) for doc in documents_data]
    for i, future in enumerate(as_completed(futures)):
        future.result()  # Raise any exceptions
        if (i + 1) % 3000 == 0:
            print(f"  PDFs written: {i + 1}/{len(documents_data)}")

print(f"  PDFs written: {len(documents_data)}/{len(documents_data)} (complete)")

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
    provider_npis, risk_member_data, n_assignments=10000
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
print(f"Synthea demographics: {'YES' if synthea_demographics else 'NO (Faker fallback)'}")
print("  providers/               -> Parquet")
print("  members/                 -> Parquet")
print("  groups/                  -> Parquet")
print("  enrollment/              -> Parquet")
print("  claims_medical/          -> Parquet (750K)")
print("  claims_pharmacy/         -> Parquet (250K)")
print("  documents/case_notes/    -> PDF")
print("  documents/call_transcripts/ -> PDF")
print("  documents/claims_summaries/ -> PDF")
print("  documents/metadata/      -> JSON (for pipeline ingestion)")
print("  underwriting/            -> Parquet")
print("  risk_adjustment_member/  -> Parquet")
print("  risk_adjustment_provider/-> Parquet")
print("  benefits/                -> Parquet")
print("  fwa_signals/             -> Parquet")
print("  fwa_provider_profiles/   -> Parquet")
print("  fwa_investigation_cases/ -> Parquet")
print("NOTE: Clinical data (encounters, labs, vitals, FHIR) comes from Synthea via parse_fhir_with_dbignite.")
print("Data quality: ~2% defects in key fields for SDP expectations.")
