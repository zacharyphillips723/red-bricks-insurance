-- =============================================================================
-- Red Bricks Insurance — Risk Adjustment Domain: Bronze Layer
-- =============================================================================
-- Ingests raw risk adjustment parquet files (member-level and provider-level)
-- into streaming bronze tables. Adds source lineage metadata.
-- Expectation: ~2% of source records contain intentional quality defects.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Member-level risk adjustment scores and HCC codes
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE bronze_risk_adjustment_member
COMMENT 'Raw member-level risk adjustment data including RAF scores and HCC codes, ingested from parquet.'
AS
SELECT
  member_id,
  model_year,
  raf_score,
  hcc_codes,
  measurement_period_start,
  measurement_period_end,
  measurement_date,
  _metadata.file_path        AS source_file,
  current_timestamp()        AS ingestion_timestamp
FROM STREAM read_files(
  '${source_volume}/risk_adjustment_member/',
  format => 'parquet'
);

-- ---------------------------------------------------------------------------
-- Provider-level risk adjustment attribution
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE bronze_risk_adjustment_provider
COMMENT 'Raw provider-level risk adjustment attribution data ingested from parquet.'
AS
SELECT
  provider_npi,
  member_id,
  raf_score,
  attribution_date,
  _metadata.file_path        AS source_file,
  current_timestamp()        AS ingestion_timestamp
FROM STREAM read_files(
  '${source_volume}/risk_adjustment_provider/',
  format => 'parquet'
);
