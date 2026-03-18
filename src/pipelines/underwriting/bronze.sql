-- =============================================================================
-- Red Bricks Insurance — Underwriting Domain: Bronze Layer
-- =============================================================================
-- Ingests raw underwriting parquet files into a streaming bronze table.
-- Adds source_file and ingestion_timestamp metadata columns for lineage.
-- Expectation: ~2% of source records contain intentional quality defects.
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE bronze_underwriting
COMMENT 'Raw underwriting data ingested from parquet source files. Contains source lineage metadata columns.'
AS
SELECT
  member_id,
  risk_tier,
  smoker_indicator,
  bmi_band,
  occupation_class,
  medical_history_indicator,
  underwriting_effective_date,
  _metadata.file_path        AS source_file,
  current_timestamp()        AS ingestion_timestamp
FROM STREAM read_files(
  '${source_volume}/underwriting/',
  format => 'parquet'
);
