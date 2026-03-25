-- =============================================================================
-- Red Bricks Insurance — Gold Analytics: Quality Metrics
-- =============================================================================
-- Simplified HEDIS-like quality measures and CMS Stars composite ratings.
-- NOTE: These are proxy measures for demo purposes — production HEDIS requires
-- certified measure engines with full specifications and exclusion logic.
-- Pipeline: gold_analytics (runs separately from domain pipelines)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- gold_hedis_member — Simplified HEDIS-like Measures per Member (Long Format)
-- -----------------------------------------------------------------------------
-- Measures (simplified proxies):
--   1. Diabetes Care (HbA1c Testing) — E11% dx + HbA1c lab
--   2. Breast Cancer Screening — Female 50-74 + mammography CPT
--   3. Colorectal Cancer Screening — Age 45-75 + colonoscopy CPT
--   4. Preventive Visit — Preventive care CPT codes
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_hedis_member
COMMENT 'Simplified HEDIS-like quality measures per member in long format. Includes diabetes care, cancer screenings, and preventive visits. These are proxy measures for demo purposes.'
AS

-- Diabetes Care: HbA1c Testing
-- Eligible: members with diabetes diagnosis (E11%). Compliant: has HbA1c lab result.
WITH diabetes_care AS (
  SELECT
    c.member_id,
    e.line_of_business,
    'Diabetes Care - HbA1c Testing' AS measure_name,
    CASE
      WHEN EXISTS (
        SELECT 1
        FROM ${catalog}.${schema}.silver_lab_results lr
        WHERE lr.member_id = c.member_id
          AND lr.lab_name = 'HbA1c'
          AND YEAR(lr.collection_date) = YEAR(c.service_from_date)
      ) THEN 1
      ELSE 0
    END AS is_compliant,
    YEAR(c.service_from_date) AS measurement_year
  FROM ${catalog}.${schema}.silver_claims_medical c
  INNER JOIN ${catalog}.${schema}.silver_enrollment e
    ON c.member_id = e.member_id
  WHERE c.primary_diagnosis_code LIKE 'E11%'
  GROUP BY
    c.member_id,
    e.line_of_business,
    YEAR(c.service_from_date),
    CASE
      WHEN EXISTS (
        SELECT 1
        FROM ${catalog}.${schema}.silver_lab_results lr
        WHERE lr.member_id = c.member_id
          AND lr.lab_name = 'HbA1c'
          AND YEAR(lr.collection_date) = YEAR(c.service_from_date)
      ) THEN 1
      ELSE 0
    END
),

-- Breast Cancer Screening: Mammography
-- Eligible: female members age 50-74. Compliant: mammography CPT code.
breast_cancer_screening AS (
  SELECT
    m.member_id,
    e.line_of_business,
    'Breast Cancer Screening' AS measure_name,
    CASE
      WHEN EXISTS (
        SELECT 1
        FROM ${catalog}.${schema}.silver_claims_medical c2
        WHERE c2.member_id = m.member_id
          AND c2.procedure_code IN ('77067', '77066', '77065')
      ) THEN 1
      ELSE 0
    END AS is_compliant,
    YEAR(CURRENT_DATE()) AS measurement_year
  FROM ${catalog}.${schema}.silver_members m
  INNER JOIN ${catalog}.${schema}.silver_enrollment e
    ON m.member_id = e.member_id
  WHERE m.gender = 'Female'
    AND FLOOR(DATEDIFF(CURRENT_DATE(), m.date_of_birth) / 365.25) BETWEEN 50 AND 74
  GROUP BY
    m.member_id,
    e.line_of_business,
    CASE
      WHEN EXISTS (
        SELECT 1
        FROM ${catalog}.${schema}.silver_claims_medical c2
        WHERE c2.member_id = m.member_id
          AND c2.procedure_code IN ('77067', '77066', '77065')
      ) THEN 1
      ELSE 0
    END
),

-- Colorectal Cancer Screening: Colonoscopy
-- Eligible: members age 45-75. Compliant: colonoscopy CPT code.
colorectal_screening AS (
  SELECT
    m.member_id,
    e.line_of_business,
    'Colorectal Cancer Screening' AS measure_name,
    CASE
      WHEN EXISTS (
        SELECT 1
        FROM ${catalog}.${schema}.silver_claims_medical c3
        WHERE c3.member_id = m.member_id
          AND c3.procedure_code = '45380'
      ) THEN 1
      ELSE 0
    END AS is_compliant,
    YEAR(CURRENT_DATE()) AS measurement_year
  FROM ${catalog}.${schema}.silver_members m
  INNER JOIN ${catalog}.${schema}.silver_enrollment e
    ON m.member_id = e.member_id
  WHERE FLOOR(DATEDIFF(CURRENT_DATE(), m.date_of_birth) / 365.25) BETWEEN 45 AND 75
  GROUP BY
    m.member_id,
    e.line_of_business,
    CASE
      WHEN EXISTS (
        SELECT 1
        FROM ${catalog}.${schema}.silver_claims_medical c3
        WHERE c3.member_id = m.member_id
          AND c3.procedure_code = '45380'
      ) THEN 1
      ELSE 0
    END
),

-- Preventive Visit
-- Eligible: all enrolled members. Compliant: has preventive visit CPT code.
preventive_visit AS (
  SELECT
    e.member_id,
    e.line_of_business,
    'Preventive Visit' AS measure_name,
    CASE
      WHEN EXISTS (
        SELECT 1
        FROM ${catalog}.${schema}.silver_claims_medical c4
        WHERE c4.member_id = e.member_id
          AND c4.procedure_code IN ('99395', '99396')
      ) THEN 1
      ELSE 0
    END AS is_compliant,
    YEAR(CURRENT_DATE()) AS measurement_year
  FROM ${catalog}.${schema}.silver_enrollment e
  GROUP BY
    e.member_id,
    e.line_of_business,
    CASE
      WHEN EXISTS (
        SELECT 1
        FROM ${catalog}.${schema}.silver_claims_medical c4
        WHERE c4.member_id = e.member_id
          AND c4.procedure_code IN ('99395', '99396')
      ) THEN 1
      ELSE 0
    END
)

SELECT member_id, line_of_business, measure_name, is_compliant, measurement_year FROM diabetes_care
UNION ALL
SELECT member_id, line_of_business, measure_name, is_compliant, measurement_year FROM breast_cancer_screening
UNION ALL
SELECT member_id, line_of_business, measure_name, is_compliant, measurement_year FROM colorectal_screening
UNION ALL
SELECT member_id, line_of_business, measure_name, is_compliant, measurement_year FROM preventive_visit;

-- -----------------------------------------------------------------------------
-- gold_hedis_provider — HEDIS Compliance Rates by Provider
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_hedis_provider
COMMENT 'HEDIS compliance rates aggregated by provider and measure. Joins member-level measures with claims to attribute to rendering provider.'
AS
WITH member_provider AS (
  SELECT DISTINCT
    c.member_id,
    c.rendering_provider_npi AS provider_npi
  FROM ${catalog}.${schema}.silver_claims_medical c
  WHERE c.rendering_provider_npi IS NOT NULL
)
SELECT
  mp.provider_npi,
  p.specialty,
  h.measure_name,
  COUNT(DISTINCT h.member_id)                                           AS eligible_members,
  SUM(h.is_compliant)                                                   AS compliant_members,
  CAST(SUM(h.is_compliant) AS DOUBLE) / NULLIF(COUNT(DISTINCT h.member_id), 0) AS compliance_rate
FROM gold_hedis_member h
INNER JOIN member_provider mp
  ON h.member_id = mp.member_id
LEFT JOIN ${catalog}.${schema}.silver_providers p
  ON mp.provider_npi = p.npi
GROUP BY
  mp.provider_npi,
  p.specialty,
  h.measure_name;

-- -----------------------------------------------------------------------------
-- gold_stars_provider — CMS Stars-like Composite Rating per Provider
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_stars_provider
COMMENT 'CMS Stars-like composite star rating per provider. Averages compliance across all HEDIS measures and assigns 1-5 star rating.'
AS
SELECT
  hp.provider_npi,
  p.provider_name,
  p.specialty,
  COUNT(DISTINCT hp.measure_name)                  AS measure_count,
  AVG(hp.compliance_rate)                          AS overall_compliance_rate,
  -- Thresholds calibrated for synthetic data to produce a realistic distribution
  -- across all 5 star levels. Production systems use CMS cut points.
  CASE
    WHEN AVG(hp.compliance_rate) >= 0.42 THEN 5
    WHEN AVG(hp.compliance_rate) >= 0.37 THEN 4
    WHEN AVG(hp.compliance_rate) >= 0.33 THEN 3
    WHEN AVG(hp.compliance_rate) >= 0.29 THEN 2
    ELSE 1
  END                                              AS star_rating
FROM gold_hedis_provider hp
LEFT JOIN ${catalog}.${schema}.silver_providers p
  ON hp.provider_npi = p.npi
GROUP BY
  hp.provider_npi,
  p.provider_name,
  p.specialty;
