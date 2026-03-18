-- =============================================================================
-- Red Bricks Insurance — Risk Adjustment Domain: Silver Layer
-- =============================================================================
-- Cleanses and enriches risk adjustment data at member and provider levels.
-- Member table: derives hcc_count and is_high_risk flag.
-- Provider table: validates NPI format and casts dates.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Silver: Member-Level Risk Adjustment
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver_risk_adjustment_member (
  CONSTRAINT valid_member_id        EXPECT (member_id IS NOT NULL)                         ON VIOLATION DROP ROW,
  CONSTRAINT valid_raf_score        EXPECT (raf_score >= 0 AND raf_score <= 10),
  CONSTRAINT valid_model_year       EXPECT (model_year >= 2020 AND model_year <= 2030),
  CONSTRAINT valid_measurement_date EXPECT (TRY_CAST(measurement_date AS DATE) IS NOT NULL)
)
COMMENT 'Cleansed member-level risk adjustment data with derived HCC count and high-risk flag.'
AS
SELECT
  member_id,
  model_year,
  raf_score,
  hcc_codes,
  CAST(measurement_period_start AS DATE) AS measurement_period_start,
  CAST(measurement_period_end   AS DATE) AS measurement_period_end,
  CAST(measurement_date         AS DATE) AS measurement_date,

  -- Derive HCC condition count from comma-separated code list
  CASE
    WHEN hcc_codes IS NOT NULL AND TRIM(hcc_codes) != ''
      THEN SIZE(SPLIT(hcc_codes, ','))
    ELSE 0
  END AS hcc_count,

  -- Flag members with RAF score above 2.0 as high risk
  CASE WHEN raf_score > 2.0 THEN true ELSE false END AS is_high_risk,

  source_file,
  ingestion_timestamp
FROM STREAM(LIVE.bronze_risk_adjustment_member);

-- ---------------------------------------------------------------------------
-- Silver: Provider-Level Risk Adjustment Attribution
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver_risk_adjustment_provider (
  CONSTRAINT valid_npi              EXPECT (provider_npi IS NOT NULL AND provider_npi RLIKE '^[0-9]{10}$')  ON VIOLATION DROP ROW,
  CONSTRAINT valid_member_id        EXPECT (member_id IS NOT NULL)                                          ON VIOLATION DROP ROW,
  CONSTRAINT valid_raf_score        EXPECT (raf_score >= 0 AND raf_score <= 10),
  CONSTRAINT valid_attribution_date EXPECT (TRY_CAST(attribution_date AS DATE) IS NOT NULL)
)
COMMENT 'Cleansed provider-level risk adjustment attribution with validated NPI and cast dates.'
AS
SELECT
  provider_npi,
  member_id,
  raf_score,
  CAST(attribution_date AS DATE) AS attribution_date,
  source_file,
  ingestion_timestamp
FROM STREAM(LIVE.bronze_risk_adjustment_provider);
