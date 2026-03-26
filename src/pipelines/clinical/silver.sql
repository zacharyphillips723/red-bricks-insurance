-- =============================================================================
-- Red Bricks Insurance — Clinical Domain: Silver Layer
-- =============================================================================
-- Applies data quality expectations, type casting, and enrichment to bronze
-- clinical tables. Critical fields (IDs) use DROP ROW enforcement; format and
-- range validations are tracked only so the quality dashboard captures defect
-- rates without losing records.
--
-- Uses materialized views (not streaming tables) because upstream bronze
-- layer reads from dbignite Delta tables via full-table overwrite.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- silver_encounters
-- Cleansed and typed encounter records
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW silver_encounters (
  CONSTRAINT valid_encounter_id EXPECT (encounter_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_member_id    EXPECT (member_id IS NOT NULL)    ON VIOLATION DROP ROW,
  CONSTRAINT valid_npi          EXPECT (provider_npi RLIKE '^[0-9]{10}$'),
  CONSTRAINT valid_encounter_type EXPECT (encounter_type IN ('office', 'outpatient', 'inpatient', 'emergency', 'telehealth')),
  CONSTRAINT valid_date         EXPECT (TRY_CAST(date_of_service AS DATE) IS NOT NULL)
)
COMMENT 'Cleansed clinical encounters with validated IDs, typed dates, and quality tracking on NPI/encounter_type/date formats.'
AS
SELECT
  encounter_id,
  member_id,
  provider_npi,
  TRY_CAST(date_of_service AS DATE)  AS date_of_service,
  encounter_type,
  visit_type,
  source_file,
  ingestion_timestamp
FROM LIVE.bronze_encounters;

-- ---------------------------------------------------------------------------
-- silver_lab_results
-- Cleansed lab results with abnormal flag
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW silver_lab_results (
  CONSTRAINT valid_lab_id          EXPECT (lab_result_id IS NOT NULL)  ON VIOLATION DROP ROW,
  CONSTRAINT valid_member_id       EXPECT (member_id IS NOT NULL)      ON VIOLATION DROP ROW,
  CONSTRAINT valid_value           EXPECT (value IS NOT NULL AND value >= 0),
  CONSTRAINT valid_collection_date EXPECT (TRY_CAST(collection_date AS DATE) IS NOT NULL),
  CONSTRAINT valid_reference_range EXPECT (reference_range_low <= reference_range_high)
)
COMMENT 'Cleansed clinical lab results with typed dates, abnormal flag, and quality tracking on value/date/range constraints.'
AS
SELECT
  lab_result_id,
  member_id,
  lab_name,
  value,
  unit,
  reference_range_low,
  reference_range_high,
  TRY_CAST(collection_date AS DATE)  AS collection_date,
  CASE
    WHEN value < reference_range_low OR value > reference_range_high THEN TRUE
    ELSE FALSE
  END AS is_abnormal,
  source_file,
  ingestion_timestamp
FROM LIVE.bronze_lab_results;

-- ---------------------------------------------------------------------------
-- silver_vitals
-- Cleansed vitals measurements
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW silver_vitals (
  CONSTRAINT valid_vital_id         EXPECT (vital_id IS NOT NULL)   ON VIOLATION DROP ROW,
  CONSTRAINT valid_member_id        EXPECT (member_id IS NOT NULL)  ON VIOLATION DROP ROW,
  CONSTRAINT valid_value            EXPECT (value > 0),
  CONSTRAINT valid_measurement_date EXPECT (TRY_CAST(measurement_date AS DATE) IS NOT NULL)
)
COMMENT 'Cleansed clinical vitals with typed dates and quality tracking on value positivity and date validity.'
AS
SELECT
  vital_id,
  member_id,
  vital_name,
  value,
  TRY_CAST(measurement_date AS DATE)  AS measurement_date,
  source_file,
  ingestion_timestamp
FROM LIVE.bronze_vitals;
