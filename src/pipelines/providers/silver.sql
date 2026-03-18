-- =============================================================================
-- Red Bricks Insurance — Providers Domain: Silver Layer
-- =============================================================================
-- Cleansed and conformed provider data. NPI format and network status are
-- critical constraints (drop on violation); specialty nulls are tracked only.
-- ~2% of records will fail one or more expectations.
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE silver_providers (
  CONSTRAINT valid_npi_not_null
    EXPECT (npi IS NOT NULL)
    ON VIOLATION DROP ROW,
  CONSTRAINT valid_npi_format
    EXPECT (npi RLIKE '^[0-9]{10}$')
    ON VIOLATION DROP ROW,
  CONSTRAINT valid_specialty
    EXPECT (specialty IS NOT NULL),
  CONSTRAINT valid_network_status
    EXPECT (network_status IN ('In-Network', 'Out-of-Network'))
)
COMMENT 'Cleansed provider directory with validated NPI, specialty, and network status. Invalid NPI rows are dropped; other violations are tracked.'
AS SELECT
  npi,
  provider_first_name,
  provider_last_name,
  provider_name,
  credential,
  specialty,
  taxonomy_code,
  tax_id,
  group_name,
  network_status,
  CAST(effective_date AS DATE) AS effective_date,
  CAST(termination_date AS DATE) AS termination_date,
  address_line_1,
  city,
  state,
  zip_code,
  county,
  phone,
  CASE
    WHEN termination_date IS NULL
      OR CAST(termination_date AS DATE) >= current_date()
    THEN TRUE
    ELSE FALSE
  END AS is_active,
  source_file,
  ingestion_timestamp
FROM STREAM(LIVE.bronze_providers);
