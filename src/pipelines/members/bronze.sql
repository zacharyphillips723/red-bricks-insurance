-- =============================================================================
-- Red Bricks Insurance — Members Domain: Bronze Layer
-- =============================================================================
-- Raw ingestion of member demographics and enrollment data from parquet files.
-- No transformations applied — faithful copy of source with audit metadata.
-- ~2% of source records contain intentional data quality defects for demo.
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE bronze_members
COMMENT 'Raw member demographics ingested from parquet source files. No transformations applied.'
AS SELECT
  *,
  _metadata.file_path AS source_file,
  current_timestamp() AS ingestion_timestamp
FROM STREAM read_files(
  '${source_volume}/members/',
  format => 'parquet'
);

CREATE OR REFRESH STREAMING TABLE bronze_enrollment
COMMENT 'Raw enrollment/eligibility records ingested from parquet source files. No transformations applied.'
AS SELECT
  *,
  _metadata.file_path AS source_file,
  current_timestamp() AS ingestion_timestamp
FROM STREAM read_files(
  '${source_volume}/enrollment/',
  format => 'parquet'
);
