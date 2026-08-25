# Databricks notebook source
# MAGIC %md
# MAGIC # Red Bricks Insurance — Denial Reason Reference & Remediation Playbook
# MAGIC
# MAGIC Seeds two governed Delta tables in the `claims` schema that back the
# MAGIC provider-facing **Denial Risk Predictor / Claim Scrubber**:
# MAGIC
# MAGIC 1. `claims.carc_reference` — canonical CARC (Claim Adjustment Reason Code)
# MAGIC    dictionary. Maps each code to a `reason_category` used as the ML reason
# MAGIC    classifier's label space, its `group_code` (CO/PR/PI/OA), a human
# MAGIC    description, and who bears the balance (`patient_vs_payer`).
# MAGIC 2. `claims.denial_remediation_playbook` — concrete, provider-facing
# MAGIC    pre-submission guidance for each denial reason (what to fix before the
# MAGIC    claim/auth is submitted so it clears cleanly).
# MAGIC
# MAGIC **Why govern these in UC?** The reason taxonomy and remediation guidance are
# MAGIC shared reference data consumed by the scrubber app, the ML training notebook,
# MAGIC and analytics. A governed, versioned Delta table lets the payer's UM/coding
# MAGIC teams audit and update them without a code deploy.

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance_catalog", "Catalog")
catalog = dbutils.widgets.get("catalog")
catalog_sql = f"`{catalog}`"

CLAIMS_SCHEMA = "claims"
CARC_TABLE = f"{catalog_sql}.{CLAIMS_SCHEMA}.carc_reference"
PLAYBOOK_TABLE = f"{catalog_sql}.{CLAIMS_SCHEMA}.denial_remediation_playbook"

print(f"CARC reference:       {CARC_TABLE}")
print(f"Remediation playbook: {PLAYBOOK_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## CARC reference data
# MAGIC
# MAGIC One row per denial reason code. `reason_category` is the label space the
# MAGIC `denial_reason_model` predicts; anything not listed here maps to `other` at
# MAGIC training time. `patient_vs_payer` follows the CARC group-code convention:
# MAGIC CO (Contractual Obligation) → payer/provider write-off, PR (Patient
# MAGIC Responsibility) → patient, PI/OA → other.

# COMMAND ----------

# (carc_code, group_code, reason_category, description, patient_vs_payer)
CARC_ROWS = [
    ("CO-197", "CO", "no_auth",
     "Precertification/authorization/notification/pre-treatment absent. The service required prior authorization that was not obtained before it was rendered.",
     "payer"),
    ("CO-50", "CO", "not_medically_necessary",
     "These are non-covered services because this is not deemed a medical necessity by the payer per the applicable medical policy.",
     "payer"),
    ("CO-55", "CO", "experimental",
     "Procedure/treatment/drug is deemed experimental/investigational by the payer and is therefore non-covered.",
     "payer"),
    ("CO-96", "CO", "experimental",
     "Non-covered charge(s). Often used when a service is excluded as experimental/investigational or otherwise not a covered benefit under the plan.",
     "payer"),
    ("CO-16", "CO", "missing_info",
     "Claim/service lacks information or has submission/billing error(s) needed for adjudication (missing documentation, modifiers, or required attachments).",
     "payer"),
    ("CO-11", "CO", "coding_mismatch",
     "The diagnosis is inconsistent with the procedure. The submitted ICD-10 diagnosis does not support the CPT/HCPCS procedure billed.",
     "payer"),
    ("CO-27", "CO", "eligibility",
     "Expenses incurred after coverage terminated. The member was not eligible/enrolled on the date of service.",
     "payer"),
    ("CO-151", "CO", "frequency_limit",
     "Payment adjusted because the payer deems the information submitted does not support this many/frequency of services (benefit visit/quantity limit exceeded).",
     "payer"),
]

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType

carc_schema = StructType([
    StructField("carc_code", StringType(), False),
    StructField("group_code", StringType(), False),
    StructField("reason_category", StringType(), False),
    StructField("description", StringType(), True),
    StructField("patient_vs_payer", StringType(), True),
])
carc_df = spark.createDataFrame([Row(*r) for r in CARC_ROWS], schema=carc_schema)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_sql}.{CLAIMS_SCHEMA}")
(carc_df.write.mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable(f"{catalog}.{CLAIMS_SCHEMA}.carc_reference"))

spark.sql(f"""
    COMMENT ON TABLE {CARC_TABLE} IS
    'Governed CARC (Claim Adjustment Reason Code) dictionary for the denial-risk scrubber. Maps each code to a reason_category (ML label space), group_code (CO/PR/PI/OA), description, and patient_vs_payer responsibility. Codes not present map to reason_category = other during model training.'
""")

print(f"Wrote {carc_df.count()} CARC reference rows to {CARC_TABLE}")
display(spark.sql(f"SELECT reason_category, carc_code, group_code, patient_vs_payer FROM {CARC_TABLE} ORDER BY reason_category"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remediation playbook
# MAGIC
# MAGIC Provider-facing, pre-submission fixes surfaced on the scrubber's reason
# MAGIC cards. `required_action` is a machine key the UI can badge; `doc_needed`
# MAGIC names the artifact a revenue-cycle team must attach/correct.

# COMMAND ----------

# (carc_code, reason_category, remediation_text, required_action, doc_needed)
PLAYBOOK_ROWS = [
    ("CO-197", "no_auth",
     "This service requires prior authorization. Submit a PA request and obtain approval BEFORE rendering the service, then include the authorization/reference number on the claim. If the service was already performed, file a retro-authorization with clinical justification.",
     "obtain_prior_auth",
     "Approved authorization number"),
    ("CO-50", "not_medically_necessary",
     "Attach clinical documentation that maps directly to the applicable medical-policy criteria (relevant diagnoses, conservative therapies tried and failed, duration, objective findings/labs, and functional status). Confirm the member meets every listed indication before submitting.",
     "attach_medical_necessity",
     "Clinical notes evidencing medical-necessity criteria"),
    ("CO-55", "experimental",
     "The submitted procedure is flagged experimental/investigational under current policy. Verify the CPT/HCPCS code is correct; if the service is standard-of-care, cite peer-reviewed evidence and any FDA clearance, or route through the medical-exception/appeal pathway before billing.",
     "verify_coverage_or_appeal",
     "Coverage-exception request with supporting evidence"),
    ("CO-96", "experimental",
     "Charge is non-covered under the plan (often experimental/investigational or a plan exclusion). Confirm the benefit covers this service for this member's plan; if excluded, obtain an ABN/member acknowledgment or pursue a coverage exception before submitting.",
     "verify_benefit_coverage",
     "Benefit/coverage confirmation or ABN"),
    ("CO-16", "missing_info",
     "The claim is missing information required to adjudicate. Complete all required fields (valid rendering NPI, at least one diagnosis, a valid 5-digit procedure code, place-of-service, and units) and attach any operative/clinical documentation the policy requires before resubmitting.",
     "complete_claim_fields",
     "Corrected claim fields + required attachments"),
    ("CO-11", "coding_mismatch",
     "The diagnosis does not support the procedure billed. Re-code to an ICD-10 diagnosis that is a covered indication for the CPT/HCPCS procedure (or correct the procedure code). Verify the dx↔px pairing against the medical policy before submitting.",
     "correct_diagnosis_coding",
     "Corrected ICD-10/CPT code pairing"),
    ("CO-27", "eligibility",
     "The member was not eligible on the date of service. Verify enrollment and the active coverage window for the DOS; if coverage lapsed, confirm the correct payer/plan (COB) or bill the member's active coverage. Do not submit until eligibility on the DOS is confirmed.",
     "verify_eligibility",
     "Eligibility verification for the date of service"),
    ("CO-151", "frequency_limit",
     "The service exceeds the plan's frequency/visit or dollar limit. Check the member's benefit accumulators (visit_limit / annual_limit) and prior utilization; if the limit is reached, obtain an authorization for additional units with medical justification or defer to the next benefit period.",
     "check_benefit_limits",
     "Authorization for additional units / utilization history"),
]

# COMMAND ----------

playbook_schema = StructType([
    StructField("carc_code", StringType(), False),
    StructField("reason_category", StringType(), False),
    StructField("remediation_text", StringType(), True),
    StructField("required_action", StringType(), True),
    StructField("doc_needed", StringType(), True),
])
playbook_df = spark.createDataFrame([Row(*r) for r in PLAYBOOK_ROWS], schema=playbook_schema)

(playbook_df.write.mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable(f"{catalog}.{CLAIMS_SCHEMA}.denial_remediation_playbook"))

spark.sql(f"""
    COMMENT ON TABLE {PLAYBOOK_TABLE} IS
    'Provider-facing pre-submission remediation guidance keyed by CARC code / reason_category. Surfaced on the denial-risk scrubber reason cards so revenue-cycle teams can fix a claim or prior-auth request before it is submitted to the payer.'
""")

print(f"Wrote {playbook_df.count()} remediation rows to {PLAYBOOK_TABLE}")
display(spark.sql(f"SELECT carc_code, reason_category, required_action, doc_needed FROM {PLAYBOOK_TABLE} ORDER BY carc_code"))

# COMMAND ----------

print("Denial reference + remediation playbook build complete.")
