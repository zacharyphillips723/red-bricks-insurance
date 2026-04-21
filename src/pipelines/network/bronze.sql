-- =============================================================================
-- Red Bricks Insurance — Network Adequacy Domain: Bronze Layer
-- =============================================================================
-- Raw ingestion of geocoded providers, members, CMS reference tables,
-- county classification, and claims network enrichment from parquet files.
-- No transformations applied — faithful copy of source with audit metadata.
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE bronze_provider_locations
COMMENT 'Geocoded provider records with network adequacy enhancement fields (panel size, telehealth, credentialing, wait times).'
AS SELECT
  *,
  _metadata.file_path AS source_file,
  current_timestamp() AS ingestion_timestamp
FROM STREAM read_files(
  '${source_volume}/provider_locations/',
  format => 'parquet'
);

CREATE OR REFRESH STREAMING TABLE bronze_member_locations
COMMENT 'Geocoded member records with latitude/longitude derived from ZIP code centroids.'
AS SELECT
  *,
  _metadata.file_path AS source_file,
  current_timestamp() AS ingestion_timestamp
FROM STREAM read_files(
  '${source_volume}/member_locations/',
  format => 'parquet'
);

CREATE OR REFRESH STREAMING TABLE bronze_cms_standards
COMMENT 'CMS HSD time/distance standards by specialty type and county type (42 CFR 422.116).'
AS SELECT
  *,
  _metadata.file_path AS source_file,
  current_timestamp() AS ingestion_timestamp
FROM STREAM read_files(
  '${source_volume}/cms_standards/',
  format => 'parquet'
);

CREATE OR REFRESH STREAMING TABLE bronze_county_classification
COMMENT 'NC county type classification (Large Metro, Metro, Micro, Rural, CEAC) with population and density.'
AS SELECT
  *,
  _metadata.file_path AS source_file,
  current_timestamp() AS ingestion_timestamp
FROM STREAM read_files(
  '${source_volume}/county_classification/',
  format => 'parquet'
);

CREATE OR REFRESH STREAMING TABLE bronze_claims_network
COMMENT 'Medical claims enriched with in-network/out-of-network indicators and leakage analysis fields.'
AS SELECT
  *,
  _metadata.file_path AS source_file,
  current_timestamp() AS ingestion_timestamp
FROM STREAM read_files(
  '${source_volume}/claims_network/',
  format => 'parquet'
);
