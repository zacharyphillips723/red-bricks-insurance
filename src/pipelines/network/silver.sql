-- =============================================================================
-- Red Bricks Insurance — Network Adequacy Domain: Silver Layer
-- =============================================================================
-- Cleansed and conformed network adequacy data. Validates coordinates and
-- applies data quality expectations.
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE silver_provider_geo (
  CONSTRAINT valid_npi_not_null
    EXPECT (npi IS NOT NULL)
    ON VIOLATION DROP ROW,
  CONSTRAINT valid_latitude
    EXPECT (provider_latitude BETWEEN 33.5 AND 37.0)
    ON VIOLATION DROP ROW,
  CONSTRAINT valid_longitude
    EXPECT (provider_longitude BETWEEN -84.5 AND -75.5)
    ON VIOLATION DROP ROW,
  CONSTRAINT valid_specialty
    EXPECT (specialty IS NOT NULL),
  CONSTRAINT valid_credentialing
    EXPECT (credentialing_status IN ('Active', 'Provisional', 'Expired', 'Suspended'))
)
COMMENT 'Geocoded providers with validated coordinates. Invalid coordinates are dropped.'
AS SELECT
  npi,
  provider_name,
  specialty,
  cms_specialty_type,
  network_status,
  county,
  county_fips,
  zip_code,
  provider_latitude,
  provider_longitude,
  accepts_new_patients,
  telehealth_capable,
  panel_size,
  panel_capacity,
  appointment_wait_days,
  credentialing_status,
  languages_spoken,
  CAST(last_claims_date AS DATE) AS last_claims_date,
  CAST(effective_date AS DATE) AS effective_date,
  CAST(termination_date AS DATE) AS termination_date,
  CASE
    WHEN termination_date IS NULL
      OR CAST(termination_date AS DATE) >= current_date()
    THEN TRUE
    ELSE FALSE
  END AS is_active,
  source_file,
  ingestion_timestamp
FROM STREAM(LIVE.bronze_provider_locations);

CREATE OR REFRESH STREAMING TABLE silver_member_geo (
  CONSTRAINT valid_member_id
    EXPECT (member_id IS NOT NULL)
    ON VIOLATION DROP ROW,
  CONSTRAINT valid_latitude
    EXPECT (member_latitude BETWEEN 33.5 AND 37.0)
    ON VIOLATION DROP ROW,
  CONSTRAINT valid_longitude
    EXPECT (member_longitude BETWEEN -84.5 AND -75.5)
    ON VIOLATION DROP ROW
)
COMMENT 'Geocoded members with validated coordinates. Invalid coordinates are dropped.'
AS SELECT
  member_id,
  member_latitude,
  member_longitude,
  county,
  county_fips,
  zip_code,
  source_file,
  ingestion_timestamp
FROM STREAM(LIVE.bronze_member_locations);

CREATE OR REFRESH STREAMING TABLE silver_claims_network (
  CONSTRAINT valid_claim_id
    EXPECT (claim_id IS NOT NULL)
    ON VIOLATION DROP ROW,
  CONSTRAINT valid_network_indicator
    EXPECT (network_indicator IN ('INN', 'OON'))
)
COMMENT 'Claims enriched with in/out-of-network indicators, distance, cost differential, and leakage reason.'
AS SELECT
  claim_id,
  member_id,
  rendering_provider_npi,
  network_indicator,
  CAST(member_to_provider_distance_mi AS DOUBLE) AS member_to_provider_distance_mi,
  CAST(oon_cost_differential AS DOUBLE) AS oon_cost_differential,
  nearest_inn_npi,
  CAST(nearest_inn_distance_mi AS DOUBLE) AS nearest_inn_distance_mi,
  leakage_reason,
  CAST(paid_amount AS DOUBLE) AS paid_amount,
  CAST(service_date AS DATE) AS service_date,
  source_file,
  ingestion_timestamp
FROM STREAM(LIVE.bronze_claims_network);

-- Reference tables pass through with minimal transformation
CREATE OR REFRESH STREAMING TABLE silver_cms_standards
COMMENT 'CMS HSD time/distance standards — cleansed reference table.'
AS SELECT
  specialty_type,
  specialty_category,
  county_type,
  CAST(max_distance_miles AS INT) AS max_distance_miles,
  CAST(max_time_minutes AS INT) AS max_time_minutes,
  source_file,
  ingestion_timestamp
FROM STREAM(LIVE.bronze_cms_standards);

CREATE OR REFRESH STREAMING TABLE silver_county_classification
COMMENT 'NC county classification — cleansed reference table.'
AS SELECT
  county_fips,
  county_name,
  county_type,
  CAST(population AS INT) AS population,
  CAST(density_per_sq_mi AS DOUBLE) AS density_per_sq_mi,
  cbsa_code,
  cbsa_name,
  source_file,
  ingestion_timestamp
FROM STREAM(LIVE.bronze_county_classification);
