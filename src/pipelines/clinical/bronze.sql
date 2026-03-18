-- =============================================================================
-- Red Bricks Insurance — Clinical Domain: Bronze Layer
-- =============================================================================
-- Ingests raw clinical JSON files (dbignite-ready format) into bronze streaming
-- tables. Unlike claims/pharmacy domains that use Parquet, clinical data lands
-- as JSON from EHR extracts and FHIR-adjacent pipelines.
--
-- Sources:
--   ${source_volume}/clinical/encounters/
--   ${source_volume}/clinical/lab_results/
--   ${source_volume}/clinical/vitals/
--
-- ~2% of source records contain intentional data quality defects for
-- downstream expectation testing.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- bronze_encounters
-- Raw encounter records from clinical systems (JSON)
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE bronze_encounters
COMMENT 'Raw clinical encounter records ingested from JSON. Contains ~2% intentional quality defects for pipeline testing.'
AS
SELECT
  encounter_id,
  member_id,
  provider_npi,
  date_of_service,
  encounter_type,
  visit_type,
  _metadata.file_path       AS source_file,
  current_timestamp()       AS ingestion_timestamp
FROM STREAM read_files(
  '${source_volume}/clinical/encounters/',
  format => 'json'
);

-- ---------------------------------------------------------------------------
-- bronze_lab_results
-- Raw lab result records from clinical systems (JSON)
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE bronze_lab_results
COMMENT 'Raw clinical lab result records ingested from JSON. Contains ~2% intentional quality defects for pipeline testing.'
AS
SELECT
  lab_result_id,
  member_id,
  lab_name,
  value,
  unit,
  reference_range_low,
  reference_range_high,
  collection_date,
  _metadata.file_path       AS source_file,
  current_timestamp()       AS ingestion_timestamp
FROM STREAM read_files(
  '${source_volume}/clinical/lab_results/',
  format => 'json'
);

-- ---------------------------------------------------------------------------
-- bronze_vitals
-- Raw vitals measurement records from clinical systems (JSON)
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE bronze_vitals
COMMENT 'Raw clinical vitals records ingested from JSON. Contains ~2% intentional quality defects for pipeline testing.'
AS
SELECT
  vital_id,
  member_id,
  vital_name,
  value,
  measurement_date,
  _metadata.file_path       AS source_file,
  current_timestamp()       AS ingestion_timestamp
FROM STREAM read_files(
  '${source_volume}/clinical/vitals/',
  format => 'json'
);
