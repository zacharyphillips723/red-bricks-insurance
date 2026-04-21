-- =============================================================================
-- Red Bricks Insurance — Network Adequacy Domain: Gold Layer
-- =============================================================================
-- Business-level analytics for CMS compliance, ghost network detection,
-- and patient outmigration/leakage intelligence.
--
-- Uses haversine distance formula for member-to-provider distance calculations.
-- Pre-filters by county_fips to keep the cross-join manageable.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. Network Adequacy Compliance (county x specialty vs CMS thresholds)
-- ---------------------------------------------------------------------------
-- For each county and CMS specialty, calculate the percentage of members
-- within the CMS maximum distance threshold. Pre-filters by county_fips
-- matching to keep the join size manageable (~500 providers x 5K members).
-- ---------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW gold_network_adequacy_compliance
COMMENT 'CMS network adequacy compliance by county and specialty. Measures % of members within time/distance standards. 90% threshold required for compliance.'
AS
WITH provider_by_county_specialty AS (
  -- Active, in-network providers with their county and CMS specialty
  SELECT
    p.npi,
    p.cms_specialty_type,
    p.provider_latitude,
    p.provider_longitude,
    p.telehealth_capable,
    cc.county_fips,
    cc.county_name,
    cc.county_type
  FROM LIVE.silver_provider_geo p
  JOIN LIVE.silver_county_classification cc
    ON p.county_fips = cc.county_fips
  WHERE p.is_active = TRUE
    AND p.network_status = 'In-Network'
    AND p.credentialing_status IN ('Active', 'Provisional')
),

member_county AS (
  -- Members with their county classification
  SELECT
    m.member_id,
    m.member_latitude,
    m.member_longitude,
    cc.county_fips,
    cc.county_name,
    cc.county_type
  FROM LIVE.silver_member_geo m
  JOIN LIVE.silver_county_classification cc
    ON m.county_fips = cc.county_fips
),

standards AS (
  SELECT * FROM LIVE.silver_cms_standards
  WHERE specialty_category = 'Provider'
),

-- For each member x specialty, find the minimum distance to an in-network provider
-- Haversine formula: 3958.8 miles = Earth radius
member_nearest_provider AS (
  SELECT
    mc.member_id,
    mc.county_fips,
    mc.county_name,
    mc.county_type,
    pcs.cms_specialty_type,
    MIN(
      ROUND(
        3958.8 * 2 * ASIN(SQRT(
          POWER(SIN(RADIANS(pcs.provider_latitude - mc.member_latitude) / 2), 2) +
          COS(RADIANS(mc.member_latitude)) * COS(RADIANS(pcs.provider_latitude)) *
          POWER(SIN(RADIANS(pcs.provider_longitude - mc.member_longitude) / 2), 2)
        )),
        1
      )
    ) AS nearest_provider_distance_mi,
    MAX(CASE WHEN pcs.telehealth_capable THEN 1 ELSE 0 END) AS has_telehealth_provider
  FROM member_county mc
  CROSS JOIN (SELECT DISTINCT cms_specialty_type FROM provider_by_county_specialty) spec
  LEFT JOIN provider_by_county_specialty pcs
    ON pcs.cms_specialty_type = spec.cms_specialty_type
    -- Pre-filter: only consider providers in same or adjacent counties
    -- (haversine < 60 mi covers max CMS distance for any county type)
    AND 3958.8 * 2 * ASIN(SQRT(
          POWER(SIN(RADIANS(pcs.provider_latitude - mc.member_latitude) / 2), 2) +
          COS(RADIANS(mc.member_latitude)) * COS(RADIANS(pcs.provider_latitude)) *
          POWER(SIN(RADIANS(pcs.provider_longitude - mc.member_longitude) / 2), 2)
        )) <= 60.0
  WHERE pcs.npi IS NOT NULL
  GROUP BY
    mc.member_id,
    mc.county_fips,
    mc.county_name,
    mc.county_type,
    pcs.cms_specialty_type
)

SELECT
  mnp.county_fips,
  mnp.county_name,
  mnp.county_type,
  mnp.cms_specialty_type,
  s.max_distance_miles,
  s.max_time_minutes,
  COUNT(DISTINCT mnp.member_id) AS total_members,
  COUNT(DISTINCT CASE
    WHEN mnp.nearest_provider_distance_mi <= s.max_distance_miles
    THEN mnp.member_id
  END) AS compliant_members,
  ROUND(
    COUNT(DISTINCT CASE
      WHEN mnp.nearest_provider_distance_mi <= s.max_distance_miles
      THEN mnp.member_id
    END) * 100.0 / NULLIF(COUNT(DISTINCT mnp.member_id), 0),
    1
  ) AS pct_compliant,
  CASE
    WHEN ROUND(
      COUNT(DISTINCT CASE
        WHEN mnp.nearest_provider_distance_mi <= s.max_distance_miles
        THEN mnp.member_id
      END) * 100.0 / NULLIF(COUNT(DISTINCT mnp.member_id), 0), 1
    ) >= 90.0
    THEN TRUE
    ELSE FALSE
  END AS is_compliant,
  COUNT(DISTINCT mnp.member_id) - COUNT(DISTINCT CASE
    WHEN mnp.nearest_provider_distance_mi <= s.max_distance_miles
    THEN mnp.member_id
  END) AS gap_members,
  ROUND(AVG(mnp.nearest_provider_distance_mi), 1) AS avg_nearest_distance_mi,
  ROUND(MIN(mnp.nearest_provider_distance_mi), 1) AS min_nearest_distance_mi,
  ROUND(MAX(mnp.nearest_provider_distance_mi), 1) AS max_nearest_distance_mi,
  -- Telehealth credit: CMS allows 10 ppt credit for specialties with telehealth
  MAX(mnp.has_telehealth_provider) AS telehealth_available,
  CASE
    WHEN MAX(mnp.has_telehealth_provider) = 1
    AND ROUND(
      COUNT(DISTINCT CASE
        WHEN mnp.nearest_provider_distance_mi <= s.max_distance_miles
        THEN mnp.member_id
      END) * 100.0 / NULLIF(COUNT(DISTINCT mnp.member_id), 0), 1
    ) BETWEEN 80.0 AND 89.9
    THEN TRUE
    ELSE FALSE
  END AS telehealth_credit_applied
FROM member_nearest_provider mnp
JOIN standards s
  ON mnp.cms_specialty_type = s.specialty_type
  AND mnp.county_type = s.county_type
GROUP BY
  mnp.county_fips,
  mnp.county_name,
  mnp.county_type,
  mnp.cms_specialty_type,
  s.max_distance_miles,
  s.max_time_minutes;


-- ---------------------------------------------------------------------------
-- 2. Ghost Network Detection
-- ---------------------------------------------------------------------------
-- Multi-signal detection of providers listed in the directory but potentially
-- not available to members (no claims activity, not accepting patients,
-- expired credentials, extreme wait times, panel at capacity).
-- ---------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW gold_ghost_network_flags
COMMENT 'Provider directory integrity alerts — multi-signal ghost network detection with severity and member impact.'
AS
WITH provider_signals AS (
  SELECT
    p.npi,
    p.provider_name,
    p.specialty,
    p.cms_specialty_type,
    p.network_status,
    p.county,
    p.county_fips,
    p.provider_latitude,
    p.provider_longitude,
    p.accepts_new_patients,
    p.telehealth_capable,
    p.panel_size,
    p.panel_capacity,
    p.appointment_wait_days,
    p.credentialing_status,
    p.last_claims_date,
    p.is_active,
    -- Signal: No claims in 12 months
    CASE WHEN p.last_claims_date IS NULL
         OR p.last_claims_date < DATE_ADD(current_date(), -365)
    THEN TRUE ELSE FALSE END AS no_claims_12m,
    -- Signal: No claims in 6 months
    CASE WHEN p.last_claims_date IS NULL
         OR p.last_claims_date < DATE_ADD(current_date(), -180)
    THEN TRUE ELSE FALSE END AS no_claims_6m,
    -- Signal: Not accepting new patients
    CASE WHEN p.accepts_new_patients = FALSE
    THEN TRUE ELSE FALSE END AS not_accepting,
    -- Signal: Extreme wait times (> 45 days)
    CASE WHEN p.appointment_wait_days > 45
    THEN TRUE ELSE FALSE END AS extreme_wait,
    -- Signal: Credential expired
    CASE WHEN p.credentialing_status = 'Expired'
    THEN TRUE ELSE FALSE END AS credential_expired,
    -- Signal: Panel at capacity (>= 95%)
    CASE WHEN p.panel_capacity > 0
         AND p.panel_size >= p.panel_capacity * 0.95
    THEN TRUE ELSE FALSE END AS panel_full
  FROM LIVE.silver_provider_geo p
  WHERE p.is_active = TRUE
    AND p.network_status = 'In-Network'
),

-- Count members who depend on each provider (within 25 miles — approximate service area)
member_dependency AS (
  SELECT
    ps.npi,
    COUNT(DISTINCT m.member_id) AS dependent_members
  FROM provider_signals ps
  JOIN LIVE.silver_member_geo m
    ON 3958.8 * 2 * ASIN(SQRT(
         POWER(SIN(RADIANS(ps.provider_latitude - m.member_latitude) / 2), 2) +
         COS(RADIANS(m.member_latitude)) * COS(RADIANS(ps.provider_latitude)) *
         POWER(SIN(RADIANS(ps.provider_longitude - m.member_longitude) / 2), 2)
       )) <= 25.0
  GROUP BY ps.npi
)

SELECT
  ps.npi,
  ps.provider_name,
  ps.specialty,
  ps.cms_specialty_type,
  ps.county,
  ps.county_fips,
  ps.accepts_new_patients,
  ps.telehealth_capable,
  ps.panel_size,
  ps.panel_capacity,
  ps.appointment_wait_days,
  ps.credentialing_status,
  ps.last_claims_date,
  -- Flags
  ps.no_claims_12m,
  ps.no_claims_6m,
  ps.not_accepting,
  ps.extreme_wait,
  ps.credential_expired,
  ps.panel_full,
  -- Signal count
  (CAST(ps.no_claims_12m AS INT) + CAST(ps.not_accepting AS INT) +
   CAST(ps.extreme_wait AS INT) + CAST(ps.credential_expired AS INT) +
   CAST(ps.panel_full AS INT)) AS ghost_signal_count,
  -- Severity
  CASE
    WHEN ps.credential_expired OR ps.no_claims_12m THEN 'High'
    WHEN ps.no_claims_6m OR ps.not_accepting OR ps.extreme_wait THEN 'Medium'
    WHEN ps.panel_full THEN 'Low'
    ELSE 'None'
  END AS ghost_severity,
  -- Is flagged (any signal fires)
  CASE
    WHEN ps.no_claims_12m OR ps.not_accepting OR ps.extreme_wait
         OR ps.credential_expired OR ps.panel_full
    THEN TRUE
    ELSE FALSE
  END AS is_ghost_flagged,
  -- Member impact
  COALESCE(md.dependent_members, 0) AS impact_members
FROM provider_signals ps
LEFT JOIN member_dependency md ON ps.npi = md.npi;


-- ---------------------------------------------------------------------------
-- 3. Network Leakage Summary (aggregated OON utilization)
-- ---------------------------------------------------------------------------
-- Aggregate out-of-network claims by specialty, county, and leakage reason.
-- Quantifies the cost of leakage and identifies recruitment targets.
-- ---------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW gold_network_leakage
COMMENT 'Aggregate OON leakage metrics by county and specialty with cost impact and recruitment targets.'
AS
SELECT
  cn.rendering_provider_npi,
  pg.specialty,
  pg.cms_specialty_type,
  cc.county_name,
  cc.county_fips,
  cc.county_type,
  cn.network_indicator,
  cn.leakage_reason,
  COUNT(*) AS claim_count,
  ROUND(SUM(cn.paid_amount), 2) AS total_paid,
  ROUND(SUM(cn.oon_cost_differential), 2) AS total_leakage_cost,
  ROUND(AVG(cn.member_to_provider_distance_mi), 1) AS avg_distance_mi,
  ROUND(AVG(cn.nearest_inn_distance_mi), 1) AS avg_nearest_inn_distance_mi,
  COUNT(DISTINCT cn.member_id) AS unique_members
FROM LIVE.silver_claims_network cn
JOIN LIVE.silver_provider_geo pg
  ON cn.rendering_provider_npi = pg.npi
JOIN LIVE.silver_county_classification cc
  ON pg.county_fips = cc.county_fips
WHERE cn.network_indicator = 'OON'
GROUP BY
  cn.rendering_provider_npi,
  pg.specialty,
  pg.cms_specialty_type,
  cc.county_name,
  cc.county_fips,
  cc.county_type,
  cn.network_indicator,
  cn.leakage_reason;


CREATE OR REFRESH MATERIALIZED VIEW gold_leakage_summary
COMMENT 'High-level OON leakage summary by specialty and county with recruitable provider counts.'
AS
SELECT
  cms_specialty_type,
  county_name,
  county_fips,
  county_type,
  SUM(claim_count) AS total_oon_claims,
  ROUND(SUM(total_paid), 2) AS total_oon_paid,
  ROUND(SUM(total_leakage_cost), 2) AS total_leakage_cost,
  SUM(unique_members) AS total_oon_members,
  COUNT(DISTINCT rendering_provider_npi) AS oon_provider_count,
  ROUND(AVG(avg_distance_mi), 1) AS avg_oon_distance_mi,
  ROUND(AVG(avg_nearest_inn_distance_mi), 1) AS avg_nearest_inn_distance_mi
FROM LIVE.gold_network_leakage
GROUP BY
  cms_specialty_type,
  county_name,
  county_fips,
  county_type;


-- ---------------------------------------------------------------------------
-- 4. Provider Recruitment Targets
-- ---------------------------------------------------------------------------
-- OON providers worth contracting: high volume, low nearest INN distance
-- (meaning members have no close in-network alternative).
-- ---------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW gold_provider_recruitment_targets
COMMENT 'OON providers recommended for in-network recruitment based on claim volume, leakage cost, and member impact.'
AS
SELECT
  rendering_provider_npi,
  specialty,
  cms_specialty_type,
  county_name,
  county_fips,
  SUM(claim_count) AS total_claims,
  ROUND(SUM(total_paid), 2) AS total_paid,
  ROUND(SUM(total_leakage_cost), 2) AS potential_savings,
  SUM(unique_members) AS members_served,
  ROUND(AVG(avg_distance_mi), 1) AS avg_member_distance_mi,
  ROUND(AVG(avg_nearest_inn_distance_mi), 1) AS avg_nearest_inn_mi,
  -- Recruitment priority score: higher = more impactful to recruit
  ROUND(
    (SUM(total_leakage_cost) / 1000.0) *
    (SUM(unique_members) / 10.0) *
    (1.0 / GREATEST(AVG(avg_nearest_inn_distance_mi), 1.0)),
    1
  ) AS recruitment_priority_score
FROM LIVE.gold_network_leakage
GROUP BY
  rendering_provider_npi,
  specialty,
  cms_specialty_type,
  county_name,
  county_fips;


-- ---------------------------------------------------------------------------
-- 5. Network Gaps (where to add providers)
-- ---------------------------------------------------------------------------
-- Counties x specialties that are non-compliant or at risk of non-compliance.
-- Includes member count, gap size, and suggested action.
-- ---------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW gold_network_gaps
COMMENT 'Network gaps where provider recruitment is needed to achieve CMS compliance.'
AS
SELECT
  county_fips,
  county_name,
  county_type,
  cms_specialty_type,
  total_members,
  compliant_members,
  gap_members,
  pct_compliant,
  is_compliant,
  max_distance_miles AS cms_threshold_miles,
  avg_nearest_distance_mi,
  telehealth_available,
  telehealth_credit_applied,
  CASE
    WHEN pct_compliant < 80.0 THEN 'Critical — Immediate recruitment needed'
    WHEN pct_compliant < 90.0 AND telehealth_credit_applied THEN 'At Risk — Telehealth credit applied, monitor closely'
    WHEN pct_compliant < 90.0 THEN 'Non-Compliant — Recruitment or telehealth expansion needed'
    WHEN pct_compliant < 95.0 THEN 'Marginal — Monitor for compliance risk'
    ELSE 'Compliant'
  END AS gap_status,
  CASE
    WHEN pct_compliant < 80.0 THEN 1
    WHEN pct_compliant < 90.0 THEN 2
    WHEN pct_compliant < 95.0 THEN 3
    ELSE 4
  END AS priority_rank
FROM LIVE.gold_network_adequacy_compliance
ORDER BY priority_rank, gap_members DESC;
