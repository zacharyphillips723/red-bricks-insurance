-- =============================================================================
-- Red Bricks Insurance — Gold Analytics: Actuarial Metrics
-- =============================================================================
-- Advanced financial/actuarial views: member-months exposure, utilization per
-- 1,000, IBNR development triangle, completion factors, and AI-powered MLR
-- action recommendations. These complement the existing financial_metrics.sql.
-- Pipeline: gold_analytics (runs separately from domain pipelines)
-- =============================================================================


-- NOTE: silver_member_months is created by src/notebooks/build_member_months.py
-- (standalone notebook outside SDP — EXPLODE+SEQUENCE is too slow in SDP pipelines).
-- All views below reference it from the catalog.


-- -----------------------------------------------------------------------------
-- gold_utilization_per_1000 — Claims and admits per 1,000 member months
-- -----------------------------------------------------------------------------
-- Industry-standard utilization benchmarking metric. Enables comparison against
-- national norms and peer plan benchmarks by LOB and service category.
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_utilization_per_1000
COMMENT 'Utilization rates per 1,000 member months by LOB and service category. Standard actuarial benchmarking metric for comparing against industry norms.'
AS
WITH claims_agg AS (
  SELECT
    e.line_of_business,
    YEAR(c.service_from_date)       AS service_year,
    CASE
      WHEN c.claim_type LIKE '%IP%'  THEN 'Inpatient'
      WHEN c.claim_type LIKE '%OP%'  THEN 'Outpatient'
      WHEN c.claim_type LIKE '%ER%'  THEN 'Emergency'
      ELSE 'Professional'
    END                             AS service_category,
    COUNT(*)                        AS total_claims,
    COUNT(DISTINCT c.member_id)     AS unique_patients,
    SUM(c.paid_amount)              AS total_paid,
    SUM(c.allowed_amount)           AS total_allowed,
    -- IP admits: count distinct claim_ids for inpatient only
    COUNT(DISTINCT CASE WHEN c.claim_type LIKE '%IP%' THEN c.claim_id END) AS ip_admits
  FROM ${catalog}.claims.silver_claims_medical c
  INNER JOIN ${catalog}.members.silver_enrollment e
    ON c.member_id = e.member_id
  WHERE c.claim_status != 'Denied'
  GROUP BY e.line_of_business, YEAR(c.service_from_date),
    CASE
      WHEN c.claim_type LIKE '%IP%'  THEN 'Inpatient'
      WHEN c.claim_type LIKE '%OP%'  THEN 'Outpatient'
      WHEN c.claim_type LIKE '%ER%'  THEN 'Emergency'
      ELSE 'Professional'
    END
),

member_months_agg AS (
  SELECT
    line_of_business,
    eligibility_year AS service_year,
    COUNT(*)         AS member_months
  FROM ${catalog}.members.silver_member_months
  GROUP BY line_of_business, eligibility_year
)

SELECT
  ca.line_of_business,
  ca.service_year,
  ca.service_category,
  mm.member_months,
  ca.total_claims,
  ca.unique_patients,
  ca.total_paid,
  ca.total_allowed,
  ROUND(ca.total_paid / NULLIF(ca.total_claims, 0), 2)     AS avg_cost_per_claim,
  ca.ip_admits,
  ROUND((ca.total_claims * 1000.0) / NULLIF(mm.member_months, 0), 2)    AS claims_per_1000,
  ROUND((ca.unique_patients * 1000.0) / NULLIF(mm.member_months, 0), 2) AS patients_per_1000,
  ROUND((ca.total_paid * 1000.0) / NULLIF(mm.member_months, 0), 2)      AS cost_per_1000,
  CASE
    WHEN ca.service_category = 'Inpatient'
    THEN ROUND((ca.ip_admits * 1000.0) / NULLIF(mm.member_months, 0), 2)
    ELSE NULL
  END                                                                     AS admits_per_1000
FROM claims_agg ca
INNER JOIN member_months_agg mm
  ON ca.line_of_business = mm.line_of_business
  AND ca.service_year = mm.service_year;


-- -----------------------------------------------------------------------------
-- gold_ibnr_triangle — Payment development triangle (chain-ladder method)
-- -----------------------------------------------------------------------------
-- Shows how claims paid amounts develop over time from the service month.
-- Each row = one service_month × development_month intersection.
-- Used for chain-ladder IBNR reserve estimation — the actuarial gold standard.
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_ibnr_triangle
COMMENT 'IBNR payment development triangle by service month, LOB, and development month (0-24). Used for chain-ladder reserve estimation.'
AS
SELECT
  DATE_TRUNC('month', c.service_from_date) AS service_month,
  e.line_of_business,
  CAST(MONTHS_BETWEEN(
    DATE_TRUNC('month', c.paid_date),
    DATE_TRUNC('month', c.service_from_date)
  ) AS INT)                                AS development_month,
  SUM(c.paid_amount)                       AS incremental_paid,
  COUNT(*)                                 AS claim_count
FROM ${catalog}.claims.silver_claims_medical c
INNER JOIN ${catalog}.members.silver_enrollment e
  ON c.member_id = e.member_id
WHERE c.paid_date IS NOT NULL
  AND c.claim_status != 'Denied'
  AND MONTHS_BETWEEN(
    DATE_TRUNC('month', c.paid_date),
    DATE_TRUNC('month', c.service_from_date)
  ) BETWEEN 0 AND 24
GROUP BY
  DATE_TRUNC('month', c.service_from_date),
  e.line_of_business,
  CAST(MONTHS_BETWEEN(
    DATE_TRUNC('month', c.paid_date),
    DATE_TRUNC('month', c.service_from_date)
  ) AS INT);


-- -----------------------------------------------------------------------------
-- gold_ibnr_completion_factors — Chain-ladder completion factors by LOB
-- -----------------------------------------------------------------------------
-- Derives the average proportion of ultimate liability paid at each
-- development month. Only uses mature service periods (12+ months of history)
-- for statistical stability.
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_ibnr_completion_factors
COMMENT 'IBNR chain-ladder completion factors by LOB and development month. Derived from mature service periods (12+ months of payment history).'
AS
WITH cumulative AS (
  SELECT
    service_month,
    line_of_business,
    development_month,
    incremental_paid,
    SUM(incremental_paid) OVER (
      PARTITION BY service_month, line_of_business
      ORDER BY development_month
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_paid,
    SUM(incremental_paid) OVER (
      PARTITION BY service_month, line_of_business
    ) AS total_paid_to_date
  FROM gold_ibnr_triangle
),

mature_periods AS (
  SELECT *
  FROM cumulative
  WHERE MONTHS_BETWEEN(CURRENT_DATE(), service_month) >= 12
    AND total_paid_to_date > 0
)

SELECT
  line_of_business,
  development_month,
  ROUND(AVG(cumulative_paid / total_paid_to_date), 4) AS avg_completion_factor,
  COUNT(DISTINCT service_month)                        AS data_points
FROM mature_periods
GROUP BY line_of_business, development_month
ORDER BY line_of_business, development_month;


-- -----------------------------------------------------------------------------
-- gold_mlr_ai_insights — MLR summary with AI-generated action recommendations
-- -----------------------------------------------------------------------------
-- Enriches the existing gold_mlr table with LLM-generated actuarial action
-- items. Uses the foundation model API to produce 2-3 sentence recommendations
-- based on MLR performance relative to ACA thresholds.
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_mlr_ai_insights
COMMENT 'Medical Loss Ratio summary enriched with AI-generated actuarial action recommendations. Uses Llama 3.3 70B via ai_query() to produce targeted guidance.'
AS
SELECT
  line_of_business,
  service_year,
  medical_claims_paid,
  pharmacy_claims_paid,
  total_claims_paid,
  total_premiums,
  ROUND(mlr * 100, 2)        AS mlr_pct,
  ROUND(target_mlr * 100, 2) AS target_mlr_pct,
  CASE
    WHEN mlr >= target_mlr THEN 'Compliant'
    ELSE 'Below Threshold — Rebate Risk'
  END                         AS mlr_status,
  ROUND((total_premiums - total_claims_paid) / NULLIF(total_premiums, 0) * 100, 2) AS admin_ratio_pct,
  ai_query(
    'databricks-meta-llama-3-3-70b-instruct',
    CONCAT(
      'You are a health plan actuary. Given the following MLR data for a Blues insurance plan, ',
      'provide 2-3 sentences of specific, actionable recommendations. ',
      'Line of Business: ', line_of_business,
      '. Service Year: ', CAST(service_year AS STRING),
      '. MLR: ', CAST(ROUND(mlr * 100, 2) AS STRING), '%',
      '. Target MLR: ', CAST(ROUND(target_mlr * 100, 2) AS STRING), '%',
      '. Total Claims Paid: $', CAST(ROUND(total_claims_paid, 0) AS STRING),
      '. Total Premiums: $', CAST(ROUND(total_premiums, 0) AS STRING),
      '. Admin Ratio: ', CAST(ROUND((total_premiums - total_claims_paid) / NULLIF(total_premiums, 0) * 100, 2) AS STRING), '%',
      '. Medical Claims: $', CAST(ROUND(medical_claims_paid, 0) AS STRING),
      '. Pharmacy Claims: $', CAST(ROUND(pharmacy_claims_paid, 0) AS STRING),
      '. Focus on: MLR compliance, cost containment levers, and premium adequacy.'
    )
  ) AS ai_recommendation
FROM gold_mlr
WHERE total_premiums > 0;
