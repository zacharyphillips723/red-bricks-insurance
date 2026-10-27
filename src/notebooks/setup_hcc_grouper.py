# Databricks notebook source
# MAGIC %md
# MAGIC # Setup — CMS-HCC V28 Grouper (DEMO SUBSET)
# MAGIC
# MAGIC Phase 0 of the Risk Adjustment / HCC app (`app-risk-adjustment`). Creates the
# MAGIC **deterministic** ICD-10 → HCC → RAF reference the agent's grouper tool uses and
# MAGIC that Phase 1 uses to derive member HCCs from their actual conditions.
# MAGIC
# MAGIC **⚠ This is a curated DEMO SUBSET, not the full CMS-HCC grouper.** It covers exactly
# MAGIC the diagnoses the Red Bricks note generator emits (see
# MAGIC `src/data_generation/domains/documents.py :: PRESENTING_COMPLAINTS`), plus a few
# MAGIC adjacent codes. RAF values are the repo's simplified illustrative factors, **not**
# MAGIC official CMS V28 coefficients. See DESIGN.md §16.5 (grouper scope honesty) and §10.1.
# MAGIC
# MAGIC Note `I10` (essential hypertension) is intentionally mapped to **no HCC**
# MAGIC (`is_hcc = false`) — it is the "do-not-suspect" negative that keeps the suspecting
# MAGIC agent honest and drives the over-coding evaluation (DESIGN.md §15.1).

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance_catalog", "Unity Catalog")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = "reference"
TABLE = f"{CATALOG}.{SCHEMA}.cms_hcc_v28_grouper"
MODEL_VERSION = "CMS-HCC V28 (demo subset)"

# COMMAND ----------

# (icd10_code, icd10_description, hcc_code, hcc_label, raf_value, is_hcc)
# Aligned to documents.py PRESENTING_COMPLAINTS: E11.9, E11.65, I10, I50.9, J44.1, N18.3.
GROUPER_ROWS = [
    # Diabetes — without complication
    ("E11.9",  "Type 2 diabetes mellitus without complications",            "HCC19",  "Diabetes without complication",        0.2, True),
    # Diabetes — with chronic complications
    ("E11.65", "Type 2 diabetes mellitus with hyperglycemia",               "HCC18",  "Diabetes with chronic complications",  0.4, True),
    ("E11.21", "Type 2 diabetes mellitus with diabetic nephropathy",        "HCC18",  "Diabetes with chronic complications",  0.4, True),
    ("E11.22", "Type 2 diabetes mellitus with diabetic CKD",                "HCC18",  "Diabetes with chronic complications",  0.4, True),
    ("E11.40", "Type 2 diabetes mellitus with diabetic neuropathy",         "HCC18",  "Diabetes with chronic complications",  0.4, True),
    ("E11.42", "Type 2 diabetes mellitus with diabetic polyneuropathy",     "HCC18",  "Diabetes with chronic complications",  0.4, True),
    # Heart failure
    ("I50.9",  "Heart failure, unspecified",                                "HCC85",  "Congestive heart failure",             0.5, True),
    ("I50.22", "Chronic systolic (congestive) heart failure",              "HCC85",  "Congestive heart failure",             0.5, True),
    ("I50.32", "Chronic diastolic (congestive) heart failure",            "HCC85",  "Congestive heart failure",             0.5, True),
    # COPD
    ("J44.1",  "COPD with (acute) exacerbation",                            "HCC111", "Chronic obstructive pulmonary disease", 0.4, True),
    ("J44.0",  "COPD with acute lower respiratory infection",              "HCC111", "Chronic obstructive pulmonary disease", 0.4, True),
    ("J44.9",  "COPD, unspecified",                                         "HCC111", "Chronic obstructive pulmonary disease", 0.4, True),
    # CKD
    ("N18.3",  "Chronic kidney disease, stage 3",                           "HCC135", "Chronic kidney disease, stage 3",      0.2, True),
    ("N18.4",  "Chronic kidney disease, stage 4",                           "HCC134", "Chronic kidney disease, stage 4-5",    0.6, True),
    ("N18.5",  "Chronic kidney disease, stage 5",                           "HCC134", "Chronic kidney disease, stage 4-5",    0.6, True),
    ("N18.6",  "End stage renal disease",                                   "HCC134", "Chronic kidney disease, stage 4-5",    0.6, True),
    # Arrhythmia
    ("I48.0",  "Paroxysmal atrial fibrillation",                           "HCC96",  "Specified heart arrhythmias",          0.2, True),
    ("I48.91", "Unspecified atrial fibrillation",                          "HCC96",  "Specified heart arrhythmias",          0.2, True),
    # Acute coronary
    ("I21.4",  "Non-ST elevation (NSTEMI) myocardial infarction",          "HCC138", "Unstable angina and acute MI",         0.5, True),
    ("I20.0",  "Unstable angina",                                           "HCC138", "Unstable angina and acute MI",         0.5, True),
    # NEGATIVE — hypertension is NOT risk-adjusted; the "do-not-suspect" control
    ("I10",    "Essential (primary) hypertension",                          None,     "Not risk-adjusted (no HCC)",           0.0, False),
]

# COMMAND ----------

from pyspark.sql import Row

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

df = spark.createDataFrame([
    Row(
        icd10_code=r[0], icd10_description=r[1],
        hcc_code=r[2], hcc_label=r[3],
        raf_value=float(r[4]), is_hcc=bool(r[5]),
        model_version=MODEL_VERSION,
    )
    for r in GROUPER_ROWS
])

(df.write.format("delta").mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable(TABLE))

spark.sql(f"""
    COMMENT ON TABLE {TABLE} IS
    'CMS-HCC V28 DEMO SUBSET — illustrative ICD-10 to HCC/RAF map aligned to Red Bricks note templates. NOT the full CMS grouper; RAF values are simplified. See app-risk-adjustment/docs/DESIGN.md sections 10.1 and 16.5.'
""")

# COMMAND ----------

# MAGIC %md ### Verify
display(spark.sql(f"""
    SELECT is_hcc, COUNT(*) AS codes, COUNT(DISTINCT hcc_code) AS hccs
    FROM {TABLE} GROUP BY is_hcc ORDER BY is_hcc DESC
"""))
display(spark.sql(f"SELECT * FROM {TABLE} ORDER BY is_hcc DESC, hcc_code, icd10_code"))

print(f"✅ Created {TABLE} with {len(GROUPER_ROWS)} ICD-10 rows "
      f"({sum(1 for r in GROUPER_ROWS if r[5])} HCC-bearing, "
      f"{sum(1 for r in GROUPER_ROWS if not r[5])} non-HCC negative).")
