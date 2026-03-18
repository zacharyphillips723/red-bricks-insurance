-- =============================================================================
-- Red Bricks Insurance — Underwriting Domain: Gold Layer
-- =============================================================================
-- Aggregated underwriting summary materialized view for analytics consumption.
-- Groups by risk_tier, smoker_indicator, and bmi_band to provide population-
-- level risk metrics.
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW gold_underwriting_summary
COMMENT 'Underwriting population summary by risk tier, smoker status, and BMI band. Includes member counts, average risk factors, and medical history prevalence.'
AS
SELECT
  risk_tier,
  smoker_indicator,
  bmi_band,
  COUNT(*)                                                          AS member_count,
  ROUND(AVG(risk_factor_count), 2)                                  AS avg_risk_factor_count,
  ROUND(
    SUM(CASE WHEN medical_history_indicator = true THEN 1 ELSE 0 END)
    * 100.0 / COUNT(*), 2
  )                                                                 AS pct_with_medical_history
FROM LIVE.silver_underwriting
GROUP BY risk_tier, smoker_indicator, bmi_band;
