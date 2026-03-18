-- =============================================================================
-- Red Bricks Insurance — Clinical Domain: Gold Layer
-- =============================================================================
-- Business-level aggregations for population health monitoring, utilization
-- analysis, and clinical benchmarking. Materialized views auto-refresh when
-- upstream silver tables update.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- gold_encounter_summary
-- Monthly encounter utilization by type — supports capacity planning and
-- utilization review dashboards.
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_encounter_summary
COMMENT 'Monthly encounter summary by encounter and visit type. Supports utilization analysis and capacity planning.'
AS
SELECT
  DATE_TRUNC('MONTH', date_of_service)    AS service_month,
  encounter_type,
  visit_type,
  COUNT(*)                                AS encounter_count,
  COUNT(DISTINCT member_id)               AS unique_members,
  ROUND(
    COUNT(*) / NULLIF(COUNT(DISTINCT member_id), 0),
    2
  )                                       AS encounters_per_member
FROM LIVE.silver_encounters
WHERE date_of_service IS NOT NULL
GROUP BY
  DATE_TRUNC('MONTH', date_of_service),
  encounter_type,
  visit_type;

-- ---------------------------------------------------------------------------
-- gold_lab_results_summary
-- Aggregate lab statistics by test name — supports population health
-- monitoring, abnormal-rate trending, and quality-of-care metrics.
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_lab_results_summary
COMMENT 'Lab result statistics by test name including abnormal rates. Supports population health monitoring and quality-of-care analysis.'
AS
SELECT
  lab_name,
  COUNT(*)                                              AS total_results,
  SUM(CASE WHEN is_abnormal THEN 1 ELSE 0 END)         AS abnormal_count,
  ROUND(
    SUM(CASE WHEN is_abnormal THEN 1 ELSE 0 END)
    / NULLIF(COUNT(*), 0),
    4
  )                                                     AS abnormal_rate,
  ROUND(AVG(value), 2)                                  AS avg_value,
  MIN(value)                                            AS min_value,
  MAX(value)                                            AS max_value
FROM LIVE.silver_lab_results
GROUP BY lab_name;

-- ---------------------------------------------------------------------------
-- gold_vitals_summary
-- Population-level vital sign baselines — supports wellness program design,
-- risk stratification, and clinical benchmarking.
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_vitals_summary
COMMENT 'Population-level vital sign statistics including median approximation. Supports wellness programs and risk stratification.'
AS
SELECT
  vital_name,
  COUNT(*)                                AS measurement_count,
  ROUND(AVG(value), 2)                    AS avg_value,
  ROUND(PERCENTILE_APPROX(value, 0.5), 2) AS median_value,
  ROUND(STDDEV(value), 2)                 AS std_dev
FROM LIVE.silver_vitals
GROUP BY vital_name;
