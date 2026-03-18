-- =============================================================================
-- Red Bricks Insurance — Providers Domain: Gold Layer
-- =============================================================================
-- Business-level provider directory aggregations for network adequacy
-- reporting and analytics dashboards.
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW gold_provider_directory
COMMENT 'Provider network summary by specialty, network status, county, and active flag. Includes average providers per group for network adequacy analysis.'
AS SELECT
  specialty,
  network_status,
  county,
  is_active,
  COUNT(DISTINCT npi) AS provider_count,
  COUNT(DISTINCT group_name) AS group_count,
  ROUND(
    COUNT(DISTINCT npi) * 1.0 / NULLIF(COUNT(DISTINCT group_name), 0),
    2
  ) AS avg_providers_per_group
FROM LIVE.silver_providers
GROUP BY
  specialty,
  network_status,
  county,
  is_active;
