-- =============================================================================
-- Red Bricks Insurance — Gold Analytics: Risk Adjustment Metrics
-- =============================================================================
-- Research-level risk adjustment analysis: RAF score distributions, revenue
-- estimation, and HCC coding gap detection.
-- Pipeline: gold_analytics (runs separately from domain pipelines)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- gold_risk_adjustment_analysis — Risk Adjustment Metrics by LOB and Model Year
-- -----------------------------------------------------------------------------
-- Estimated annual revenue uses a simplified $12,000 per RAF unit for
-- Medicare Advantage. This is a demo approximation — actual CMS rates vary
-- by county, age/sex, and payment model.
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_risk_adjustment_analysis
COMMENT 'Risk adjustment summary by line of business and model year. Includes RAF score distributions, HCC counts, high-risk prevalence, and estimated MA revenue.'
AS
SELECT
  e.line_of_business,
  r.model_year,
  COUNT(DISTINCT r.member_id)                                      AS member_count,
  AVG(r.raf_score)                                                 AS avg_raf_score,
  PERCENTILE_APPROX(r.raf_score, 0.5)                              AS median_raf_score,
  MIN(r.raf_score)                                                 AS min_raf_score,
  MAX(r.raf_score)                                                 AS max_raf_score,
  SUM(r.raf_score)                                                 AS total_raf,
  AVG(r.hcc_count)                                                 AS avg_hcc_count,
  SUM(CASE WHEN r.is_high_risk = TRUE THEN 1 ELSE 0 END)
    / NULLIF(COUNT(DISTINCT r.member_id), 0)                       AS pct_high_risk,
  -- Estimated annual revenue: only meaningful for Medicare Advantage
  CASE
    WHEN e.line_of_business = 'Medicare Advantage'
    THEN SUM(r.raf_score) * 12000
    ELSE NULL
  END                                                              AS estimated_annual_revenue
FROM ${catalog}.${schema}.silver_risk_adjustment_member r
INNER JOIN ${catalog}.${schema}.silver_enrollment e
  ON r.member_id = e.member_id
GROUP BY
  e.line_of_business,
  r.model_year;

-- -----------------------------------------------------------------------------
-- gold_coding_completeness — HCC Coding Gap Analysis
-- -----------------------------------------------------------------------------
-- Identifies members with chronic condition diagnoses in claims that may be
-- missing corresponding HCC codes in risk adjustment data. Coding gaps
-- represent potential revenue leakage for MA plans and care coordination gaps.
--
-- Diagnosis-to-HCC mapping (simplified):
--   E11% (Type 2 Diabetes)    → HCC18, HCC19
--   I50% (Heart Failure)      → HCC85
--   J44% (COPD)               → HCC111
--   N18% (CKD)                → HCC134, HCC135
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_coding_completeness
COMMENT 'HCC coding gap analysis. Identifies members with chronic diagnoses in claims but missing corresponding HCC codes in risk adjustment, indicating potential coding or documentation gaps.'
AS
WITH chronic_diagnoses AS (
  -- Find members with chronic condition claims
  SELECT DISTINCT
    c.member_id,
    c.primary_diagnosis_code AS diagnosis_code,
    CASE
      WHEN c.primary_diagnosis_code LIKE 'E11%' THEN 'Diabetes'
      WHEN c.primary_diagnosis_code LIKE 'I50%' THEN 'Heart Failure'
      WHEN c.primary_diagnosis_code LIKE 'J44%' THEN 'COPD'
      WHEN c.primary_diagnosis_code LIKE 'N18%' THEN 'CKD'
    END AS condition_name,
    CASE
      WHEN c.primary_diagnosis_code LIKE 'E11%' THEN 'HCC18,HCC19'
      WHEN c.primary_diagnosis_code LIKE 'I50%' THEN 'HCC85'
      WHEN c.primary_diagnosis_code LIKE 'J44%' THEN 'HCC111'
      WHEN c.primary_diagnosis_code LIKE 'N18%' THEN 'HCC134,HCC135'
    END AS expected_hcc
  FROM ${catalog}.${schema}.silver_claims_medical c
  WHERE c.primary_diagnosis_code LIKE 'E11%'
     OR c.primary_diagnosis_code LIKE 'I50%'
     OR c.primary_diagnosis_code LIKE 'J44%'
     OR c.primary_diagnosis_code LIKE 'N18%'
),

hcc_lookup AS (
  -- Explode expected HCCs so we can check each individually
  SELECT
    cd.member_id,
    cd.diagnosis_code,
    cd.condition_name,
    TRIM(hcc.col) AS expected_hcc
  FROM chronic_diagnoses cd
  LATERAL VIEW EXPLODE(SPLIT(cd.expected_hcc, ',')) hcc AS col
)

SELECT
  hl.member_id,
  hl.diagnosis_code,
  hl.condition_name,
  hl.expected_hcc,
  CASE
    WHEN ram.hcc_codes IS NOT NULL
     AND ram.hcc_codes LIKE CONCAT('%', hl.expected_hcc, '%')
    THEN 1
    ELSE 0
  END AS has_hcc_coded,
  CASE
    WHEN ram.hcc_codes IS NULL
      OR ram.hcc_codes NOT LIKE CONCAT('%', hl.expected_hcc, '%')
    THEN 1
    ELSE 0
  END AS coding_gap
FROM hcc_lookup hl
LEFT JOIN ${catalog}.${schema}.silver_risk_adjustment_member ram
  ON hl.member_id = ram.member_id;
