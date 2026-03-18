-- =============================================================================
-- Red Bricks Insurance — Risk Adjustment Domain: Gold Layer
-- =============================================================================
-- Provider-level risk profile for standalone pipeline use.
-- Population-level RAF analysis lives in gold_analytics (gold_risk_adjustment_analysis)
-- which adds LOB breakdown and estimated MA revenue.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Provider-level risk profile with attributed member metrics
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_provider_risk_profile
COMMENT 'Provider risk profile showing attributed member counts, average RAF scores, and high-risk member prevalence per provider NPI.'
AS
SELECT
  p.provider_npi,
  COUNT(DISTINCT p.member_id)                                          AS attributed_member_count,
  ROUND(AVG(p.raf_score), 4)                                          AS avg_raf_score,
  SUM(CASE WHEN m.is_high_risk = true THEN 1 ELSE 0 END)             AS high_risk_member_count,
  ROUND(
    SUM(CASE WHEN m.is_high_risk = true THEN 1 ELSE 0 END)
    * 100.0 / COUNT(DISTINCT p.member_id), 2
  )                                                                    AS high_risk_pct
FROM LIVE.silver_risk_adjustment_provider p
LEFT JOIN LIVE.silver_risk_adjustment_member m
  ON p.member_id = m.member_id
GROUP BY p.provider_npi;
