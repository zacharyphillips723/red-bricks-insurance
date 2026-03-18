-- =============================================================================
-- Red Bricks Insurance — Members Domain: Gold Layer
-- =============================================================================
-- Business-level aggregations for analytics and reporting. Materialized views
-- join silver members and enrollment to produce demographic and enrollment
-- summaries consumed by dashboards and downstream applications.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Gold Member Demographics — population breakdown by geography & plan
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_member_demographics
COMMENT 'Member population counts segmented by county, gender, age band, and line of business. Refreshes automatically as silver tables update.'
AS SELECT
  m.county,
  m.gender,
  CASE
    WHEN FLOOR(DATEDIFF(current_date(), m.date_of_birth) / 365.25) < 18  THEN '0-17'
    WHEN FLOOR(DATEDIFF(current_date(), m.date_of_birth) / 365.25) < 35  THEN '18-34'
    WHEN FLOOR(DATEDIFF(current_date(), m.date_of_birth) / 365.25) < 50  THEN '35-49'
    WHEN FLOOR(DATEDIFF(current_date(), m.date_of_birth) / 365.25) < 65  THEN '50-64'
    ELSE '65+'
  END AS age_band,
  e.line_of_business,
  COUNT(DISTINCT m.member_id) AS member_count
FROM LIVE.silver_members m
INNER JOIN LIVE.silver_enrollment e
  ON m.member_id = e.member_id
GROUP BY
  m.county,
  m.gender,
  CASE
    WHEN FLOOR(DATEDIFF(current_date(), m.date_of_birth) / 365.25) < 18  THEN '0-17'
    WHEN FLOOR(DATEDIFF(current_date(), m.date_of_birth) / 365.25) < 35  THEN '18-34'
    WHEN FLOOR(DATEDIFF(current_date(), m.date_of_birth) / 365.25) < 50  THEN '35-49'
    WHEN FLOOR(DATEDIFF(current_date(), m.date_of_birth) / 365.25) < 65  THEN '50-64'
    ELSE '65+'
  END,
  e.line_of_business;

-- ---------------------------------------------------------------------------
-- Gold Enrollment Summary — plan performance & churn metrics
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_enrollment_summary
COMMENT 'Enrollment KPIs by line of business and plan type: active counts, average premium, average risk score, and churn rate. Refreshes automatically.'
AS SELECT
  line_of_business,
  plan_type,
  COUNT(DISTINCT CASE WHEN is_active THEN member_id END) AS active_member_count,
  COUNT(DISTINCT member_id) AS total_member_count,
  ROUND(AVG(monthly_premium), 2) AS avg_premium,
  ROUND(AVG(risk_score), 3) AS avg_risk_score,
  ROUND(
    COUNT(DISTINCT CASE WHEN NOT is_active THEN member_id END) * 100.0
    / NULLIF(COUNT(DISTINCT member_id), 0),
    2
  ) AS churn_rate_pct
FROM LIVE.silver_enrollment
GROUP BY
  line_of_business,
  plan_type;
