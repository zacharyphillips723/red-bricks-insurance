# Databricks notebook source
# MAGIC %md
# MAGIC # Setup — Risk Adjustment coherent HCC evidence layer (Phase 1)
# MAGIC
# MAGIC Additive/reversible. Does **not** modify any existing table. Builds an evidence layer
# MAGIC from the member's case-note **chunks** and classifies each (member, HCC) by the gap
# MAGIC between **note evidence** and the **coded** HCC set:
# MAGIC
# MAGIC | state | meaning | demo role |
# MAGIC |-------|---------|-----------|
# MAGIC | `documented` | evidenced in a note **and** coded | baseline |
# MAGIC | `suspected`  | evidenced in a note, **not** coded | **capture opportunity** (RAF gain) |
# MAGIC | `weak`       | coded, **no** supporting note | **RADV defect risk** |
# MAGIC | `non_hcc_noise` | documented but not risk-adjusted (`I10` hypertension) | do-not-suspect control |
# MAGIC
# MAGIC Evidence links at **chunk grain** (`evidence_chunk_id`) so the suspecting agent can cite
# MAGIC the exact passage. Depends on Phase 0 (`reference.cms_hcc_v28_grouper`). See DESIGN.md §19 / §10.1.

# COMMAND ----------
dbutils.widgets.text("catalog", "red_bricks_insurance_catalog", "Unity Catalog")
catalog = dbutils.widgets.get("catalog")
spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------
# MAGIC %md ### 1. Deterministic note-chunk text → ICD-10 (the clinical truth the agent reads)
spark.sql("""
CREATE OR REPLACE TABLE risk_adjustment.member_note_dx AS
SELECT member_id, document_id, chunk_id,
  CASE
    WHEN lower(chunk_text) LIKE '%neuropathy%' OR lower(chunk_text) LIKE '%nephropathy%'
         OR lower(chunk_text) LIKE '%hba1c > 9%'
         OR (lower(chunk_text) LIKE '%diabet%' AND lower(chunk_text) LIKE '%complication%')
         OR lower(chunk_text) LIKE '%worsening renal%' THEN 'E11.65'
    WHEN lower(chunk_text) LIKE '%diabet%' OR lower(chunk_text) LIKE '%blood glucose%'
         OR lower(chunk_text) LIKE '%hyperglycemia%' THEN 'E11.9'
    WHEN lower(chunk_text) LIKE '%heart failure%' THEN 'I50.9'
    WHEN lower(chunk_text) LIKE '%obstructive%' OR lower(chunk_text) LIKE '%copd%' THEN 'J44.1'
    WHEN lower(chunk_text) LIKE '%kidney%' OR lower(chunk_text) LIKE '%ckd%'
         OR lower(chunk_text) LIKE '%egfr%' OR lower(chunk_text) LIKE '%creatinine%' THEN 'N18.3'
    WHEN lower(chunk_text) LIKE '%hypertension%' THEN 'I10'
    ELSE NULL
  END AS icd10_code
FROM documents.silver_case_notes_chunks
WHERE document_type = 'case_note'
""")

# COMMAND ----------
# MAGIC %md ### 2. HCC evidence layer (documented / suspected / weak / non_hcc_noise) with chunk citation
spark.sql("""
CREATE OR REPLACE TABLE risk_adjustment.hcc_evidence AS
WITH note_dx AS (
  SELECT DISTINCT member_id, icd10_code, document_id, chunk_id
  FROM risk_adjustment.member_note_dx WHERE icd10_code IS NOT NULL
),
note_hcc AS (
  SELECT nd.member_id, g.hcc_code, g.hcc_label, g.raf_value, g.is_hcc,
         min(nd.icd10_code) AS icd10_code, min(nd.document_id) AS document_id,
         min(nd.chunk_id) AS evidence_chunk_id, count(*) AS note_hits
  FROM note_dx nd
  JOIN reference.cms_hcc_v28_grouper g ON nd.icd10_code = g.icd10_code
  GROUP BY nd.member_id, g.hcc_code, g.hcc_label, g.raf_value, g.is_hcc
),
coded AS (
  SELECT member_id, trim(hcc) AS hcc_code
  FROM risk_adjustment.silver_risk_adjustment_member
  LATERAL VIEW explode(split(hcc_codes, ',')) t AS hcc
  WHERE hcc_codes IS NOT NULL
),
evidenced AS (
  SELECT n.member_id, n.hcc_code, n.hcc_label, n.raf_value, n.document_id, n.evidence_chunk_id, n.note_hits,
         CASE WHEN n.is_hcc = false THEN 'non_hcc_noise'
              WHEN c.hcc_code IS NOT NULL THEN 'documented'
              ELSE 'suspected' END AS evidence_state
  FROM note_hcc n
  LEFT JOIN coded c ON n.member_id = c.member_id AND n.hcc_code = c.hcc_code
),
weak AS (
  SELECT c.member_id, c.hcc_code, g.hcc_label, g.raf_value,
         CAST(NULL AS STRING) AS document_id, CAST(NULL AS STRING) AS evidence_chunk_id,
         0 AS note_hits, 'weak' AS evidence_state
  FROM coded c
  LEFT JOIN note_hcc n ON c.member_id = n.member_id AND c.hcc_code = n.hcc_code
  LEFT JOIN (SELECT DISTINCT hcc_code, hcc_label, raf_value
             FROM reference.cms_hcc_v28_grouper WHERE is_hcc) g ON c.hcc_code = g.hcc_code
  WHERE n.hcc_code IS NULL
)
SELECT member_id, hcc_code, hcc_label, raf_value, document_id, evidence_chunk_id, note_hits, evidence_state FROM evidenced
UNION ALL
SELECT member_id, hcc_code, hcc_label, raf_value, document_id, evidence_chunk_id, note_hits, evidence_state FROM weak
""")

# COMMAND ----------
# MAGIC %md ### 3. Per-member summary (capture opportunity + RADV risk)
spark.sql("""
CREATE OR REPLACE VIEW risk_adjustment.v_member_hcc_coherent AS
SELECT member_id,
  concat_ws(',', collect_list(CASE WHEN evidence_state='documented' THEN hcc_code END)) AS documented_hccs,
  concat_ws(',', collect_list(CASE WHEN evidence_state='suspected'  THEN hcc_code END)) AS suspected_hccs,
  concat_ws(',', collect_list(CASE WHEN evidence_state='weak'       THEN hcc_code END)) AS weak_hccs,
  round(sum(CASE WHEN evidence_state='suspected' THEN raf_value ELSE 0 END),3) AS potential_raf_gain,
  round(sum(CASE WHEN evidence_state='weak'      THEN raf_value ELSE 0 END),3) AS raf_at_radv_risk
FROM risk_adjustment.hcc_evidence
GROUP BY member_id
""")

# COMMAND ----------
# MAGIC %md ### Verify
display(spark.sql("SELECT evidence_state, COUNT(*) rows, COUNT(DISTINCT member_id) members FROM risk_adjustment.hcc_evidence GROUP BY evidence_state ORDER BY rows DESC"))
