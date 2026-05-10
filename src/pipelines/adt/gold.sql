-- =============================================================================
-- Red Bricks Insurance — ADT Domain: Gold Layer
-- =============================================================================
-- Aggregated views for analytics, dashboards, and Genie queries.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Gold: ADT Event Summary — daily event counts by type and facility
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_adt_daily_summary
COMMENT 'Daily ADT event counts by type, facility, and priority. Powers ADT dashboard and Genie queries.'
TBLPROPERTIES (
  'quality' = 'gold',
  'domain'  = 'adt'
)
AS
SELECT
  DATE(event_timestamp) AS event_date,
  event_type,
  event_description,
  event_category,
  facility_name,
  facility_county,
  priority_level,
  COUNT(*) AS event_count,
  SUM(CASE WHEN triggers_alert THEN 1 ELSE 0 END) AS alert_trigger_count,
  SUM(CASE WHEN is_readmission THEN 1 ELSE 0 END) AS readmission_count
FROM LIVE.silver_adt_events
GROUP BY ALL;

-- ---------------------------------------------------------------------------
-- Gold: Recent ADT Alerts — events that should trigger care management action
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_adt_alerts
COMMENT 'ADT events flagged for care management action. Includes readmissions, admissions, discharges, and high-acuity ED visits.'
TBLPROPERTIES (
  'quality' = 'gold',
  'domain'  = 'adt'
)
AS
SELECT
  adt_event_id,
  member_id,
  event_type,
  event_description,
  event_category,
  event_timestamp,
  priority_level,
  facility_name,
  facility_county,
  admit_reason,
  primary_diagnosis_code,
  service_line,
  patient_class,
  attending_physician_name,
  discharge_disposition,
  is_readmission,
  acuity_level,
  expected_los_days,
  source_system,
  batch_id
FROM LIVE.silver_adt_events
WHERE triggers_alert = TRUE;

-- ---------------------------------------------------------------------------
-- Gold: Readmission Analysis — members with readmissions for quality metrics
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_readmission_analysis
COMMENT 'Readmission events for quality reporting and care management targeting.'
TBLPROPERTIES (
  'quality' = 'gold',
  'domain'  = 'adt'
)
AS
SELECT
  member_id,
  COUNT(*) AS total_admissions,
  SUM(CASE WHEN is_readmission THEN 1 ELSE 0 END) AS readmission_count,
  ROUND(
    SUM(CASE WHEN is_readmission THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
    1
  ) AS readmission_rate_pct,
  MAX(event_timestamp) AS last_admission_date,
  COLLECT_SET(facility_name) AS facilities_visited,
  COLLECT_SET(admit_reason) AS admit_reasons
FROM LIVE.silver_adt_events
WHERE event_type = 'A01'
GROUP BY member_id;

-- ---------------------------------------------------------------------------
-- Gold: Facility ADT Volume — facility-level summary for network analysis
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_facility_adt_volume
COMMENT 'Facility-level ADT event volumes for network analysis and utilization management.'
TBLPROPERTIES (
  'quality' = 'gold',
  'domain'  = 'adt'
)
AS
SELECT
  facility_name,
  facility_county,
  event_category,
  COUNT(*) AS total_events,
  COUNT(DISTINCT member_id) AS unique_members,
  SUM(CASE WHEN priority_level IN ('Critical', 'High') THEN 1 ELSE 0 END) AS high_priority_events,
  SUM(CASE WHEN is_readmission THEN 1 ELSE 0 END) AS readmissions,
  MIN(event_timestamp) AS earliest_event,
  MAX(event_timestamp) AS latest_event
FROM LIVE.silver_adt_events
GROUP BY facility_name, facility_county, event_category;
