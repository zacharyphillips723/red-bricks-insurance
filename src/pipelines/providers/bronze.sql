-- =============================================================================
-- Red Bricks Insurance — Providers Domain: Bronze Layer
-- =============================================================================
-- Raw ingestion of provider directory data from parquet files.
-- No transformations applied — faithful copy of source with audit metadata.
-- ~2% of source records contain intentional data quality defects for demo.
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE bronze_providers
COMMENT 'Raw provider directory records ingested from parquet source files. No transformations applied.'
AS SELECT
  *,
  _metadata.file_path AS source_file,
  current_timestamp() AS ingestion_timestamp
FROM STREAM read_files(
  '${source_volume}/providers/',
  format => 'parquet'
);
