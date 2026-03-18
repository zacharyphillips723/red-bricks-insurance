-- =============================================================================
-- Red Bricks Insurance — Gold Analytics: Financial Metrics
-- =============================================================================
-- Cross-domain financial KPIs: PMPM, Medical Loss Ratio, IBNR estimates.
-- These views read from published silver/gold tables in the catalog, not LIVE.
-- Pipeline: gold_analytics (runs separately from domain pipelines)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- gold_pmpm — Per Member Per Month by Line of Business and Service Month
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_pmpm
COMMENT 'Per Member Per Month (PMPM) paid and allowed amounts by line of business and service month. Key metric for actuarial trend analysis and budgeting.'
AS
SELECT
  e.line_of_business,
  c.service_year_month,
  SUM(c.paid_amount)                                        AS total_paid,
  SUM(c.allowed_amount)                                     AS total_allowed,
  COUNT(DISTINCT c.member_id)                               AS member_months,
  SUM(c.paid_amount) / NULLIF(COUNT(DISTINCT c.member_id), 0)   AS pmpm_paid,
  SUM(c.allowed_amount) / NULLIF(COUNT(DISTINCT c.member_id), 0) AS pmpm_allowed
FROM ${catalog}.${schema}.silver_claims_medical c
INNER JOIN ${catalog}.${schema}.silver_enrollment e
  ON c.member_id = e.member_id
GROUP BY
  e.line_of_business,
  c.service_year_month;

-- -----------------------------------------------------------------------------
-- gold_mlr — Medical Loss Ratio by Line of Business and Service Year
-- -----------------------------------------------------------------------------
-- MLR = Total Claims Paid / Total Premiums Collected
-- ACA requires: >= 0.85 for large group/Medicare/Medicaid, >= 0.80 for individual/small group
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_mlr
COMMENT 'Medical Loss Ratio (MLR) by line of business and service year. Combines medical and pharmacy claims against premium revenue. Includes ACA target thresholds.'
AS
WITH medical_claims AS (
  SELECT
    e.line_of_business,
    YEAR(c.service_from_date) AS service_year,
    SUM(c.paid_amount)        AS medical_paid
  FROM ${catalog}.${schema}.silver_claims_medical c
  INNER JOIN ${catalog}.${schema}.silver_enrollment e
    ON c.member_id = e.member_id
  GROUP BY e.line_of_business, YEAR(c.service_from_date)
),

pharmacy_claims AS (
  SELECT
    e.line_of_business,
    YEAR(p.fill_date) AS service_year,
    SUM(p.plan_paid)  AS pharmacy_paid
  FROM ${catalog}.${schema}.silver_claims_pharmacy p
  INNER JOIN ${catalog}.${schema}.silver_enrollment e
    ON p.member_id = e.member_id
  GROUP BY e.line_of_business, YEAR(p.fill_date)
),

premiums AS (
  SELECT
    line_of_business,
    YEAR(eligibility_start_date) AS service_year,
    SUM(monthly_premium * coverage_months) AS total_premiums
  FROM ${catalog}.${schema}.silver_enrollment
  GROUP BY line_of_business, YEAR(eligibility_start_date)
)

SELECT
  COALESCE(mc.line_of_business, pc.line_of_business) AS line_of_business,
  COALESCE(mc.service_year, pc.service_year)          AS service_year,
  COALESCE(mc.medical_paid, 0)                        AS medical_claims_paid,
  COALESCE(pc.pharmacy_paid, 0)                       AS pharmacy_claims_paid,
  COALESCE(mc.medical_paid, 0) + COALESCE(pc.pharmacy_paid, 0) AS total_claims_paid,
  pr.total_premiums,
  (COALESCE(mc.medical_paid, 0) + COALESCE(pc.pharmacy_paid, 0))
    / NULLIF(pr.total_premiums, 0)                    AS mlr,
  CASE
    WHEN COALESCE(mc.line_of_business, pc.line_of_business) IN ('Medicare Advantage', 'Medicaid') THEN 0.85
    ELSE 0.80
  END                                                 AS target_mlr
FROM medical_claims mc
FULL OUTER JOIN pharmacy_claims pc
  ON mc.line_of_business = pc.line_of_business
  AND mc.service_year = pc.service_year
LEFT JOIN premiums pr
  ON COALESCE(mc.line_of_business, pc.line_of_business) = pr.line_of_business
  AND COALESCE(mc.service_year, pc.service_year) = pr.service_year;

-- -----------------------------------------------------------------------------
-- gold_ibnr_estimate — Incurred But Not Reported Exposure Analysis
-- -----------------------------------------------------------------------------
-- Payment lag distribution serves as a proxy for IBNR reserve estimation.
-- Completion factor = proportion of claims settled within 90 days.
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_ibnr_estimate
COMMENT 'Incurred But Not Reported (IBNR) exposure analysis by service month. Shows payment lag distribution and completion factors for reserve estimation.'
AS
SELECT
  service_year_month,
  COUNT(*)                                                        AS total_claims,
  AVG(DATEDIFF(paid_date, service_from_date))                     AS avg_lag_days,
  SUM(CASE WHEN DATEDIFF(paid_date, service_from_date) < 30 THEN 1 ELSE 0 END)
                                                                  AS claims_under_30_days,
  SUM(CASE WHEN DATEDIFF(paid_date, service_from_date) BETWEEN 30 AND 89 THEN 1 ELSE 0 END)
                                                                  AS claims_30_to_90,
  SUM(CASE WHEN DATEDIFF(paid_date, service_from_date) BETWEEN 90 AND 179 THEN 1 ELSE 0 END)
                                                                  AS claims_90_to_180,
  SUM(CASE WHEN DATEDIFF(paid_date, service_from_date) >= 180 THEN 1 ELSE 0 END)
                                                                  AS claims_over_180,
  SUM(CASE WHEN DATEDIFF(paid_date, service_from_date) >= 90 THEN 1 ELSE 0 END)
    / NULLIF(COUNT(*), 0)                                         AS pct_over_90,
  SUM(CASE WHEN DATEDIFF(paid_date, service_from_date) < 90 THEN 1 ELSE 0 END)
    / NULLIF(COUNT(*), 0)                                         AS completion_factor
FROM ${catalog}.${schema}.silver_claims_medical
WHERE paid_date IS NOT NULL
  AND service_from_date IS NOT NULL
GROUP BY service_year_month;
