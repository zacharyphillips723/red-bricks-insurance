-- =============================================================================
-- Red Bricks Insurance — Underwriting Domain: Silver Layer
-- =============================================================================
-- Cleanses and enriches underwriting data. Applies data quality expectations:
--   - DROP ROW on null member_id
--   - Track-only on risk_tier, smoker_indicator, bmi_band, effective_date
-- Computes risk_factor_count as sum of risk indicators.
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE silver_underwriting (
  CONSTRAINT valid_member_id       EXPECT (member_id IS NOT NULL)                                        ON VIOLATION DROP ROW,
  CONSTRAINT valid_risk_tier       EXPECT (risk_tier IN ('Standard', 'Preferred', 'Substandard')),
  CONSTRAINT valid_smoker          EXPECT (smoker_indicator IN ('Y', 'N')),
  CONSTRAINT valid_bmi_band        EXPECT (bmi_band IN ('underweight', 'normal', 'overweight', 'obese')),
  CONSTRAINT valid_effective_date  EXPECT (TRY_CAST(underwriting_effective_date AS DATE) IS NOT NULL)
)
COMMENT 'Cleansed underwriting data with validated fields, cast dates, and computed risk_factor_count.'
AS
SELECT
  member_id,
  risk_tier,
  smoker_indicator,
  bmi_band,
  occupation_class,
  medical_history_indicator,
  CAST(underwriting_effective_date AS DATE) AS underwriting_effective_date,

  -- Count of risk indicators present for this member
  (
    CASE WHEN smoker_indicator = 'Y'                          THEN 1 ELSE 0 END +
    CASE WHEN bmi_band = 'obese'                              THEN 1 ELSE 0 END +
    CASE WHEN occupation_class IN ('Heavy', 'Hazardous')      THEN 1 ELSE 0 END +
    CASE WHEN medical_history_indicator = true                 THEN 1 ELSE 0 END
  ) AS risk_factor_count,

  source_file,
  ingestion_timestamp
FROM STREAM(LIVE.bronze_underwriting);
