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

-- ---------------------------------------------------------------------------
-- Gold: Readmission Feature Table — one row per INDEX inpatient stay
-- ---------------------------------------------------------------------------
-- Training-grade feature table for the 30-day inpatient readmission model.
-- Grain: one row per index admission (A01 that is NOT itself a readmission),
-- paired with its discharge (A03) and labeled by looking forward 30 days for a
-- readmission admit. ADT-derived features only; member risk (RAF / HCC / SDOH)
-- is joined in the training notebook to keep this view within the ADT pipeline.
--
-- The label window is fully observed: episodes are seeded ending >=45 days
-- before today (see generate_readmission_episodes), so every index stay has a
-- complete 30-day look-forward and no right-censoring.
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_readmission_features
COMMENT 'One row per index inpatient stay with ADT-derived features and a 30-day readmission label. Training corpus for the readmission risk model.'
TBLPROPERTIES (
  'quality' = 'gold',
  'domain'  = 'adt'
)
AS
WITH inpatient_events AS (
  SELECT
    adt_event_id,
    member_id,
    event_type,
    event_timestamp,
    patient_class,
    admit_reason,
    primary_diagnosis_code,
    service_line,
    expected_los_days,
    discharge_disposition,
    is_readmission,
    facility_name,
    facility_county
  FROM LIVE.silver_adt_events
  WHERE event_type IN ('A01', 'A03')
),

-- Index admissions: A01 events that are not themselves readmissions.
index_admits AS (
  SELECT *
  FROM inpatient_events
  WHERE event_type = 'A01' AND is_readmission = FALSE
),

-- Pair each index admit with its next discharge (>= admit time) for the member.
index_stays AS (
  SELECT
    a.adt_event_id            AS index_admit_id,
    a.member_id,
    a.event_timestamp         AS admit_timestamp,
    a.admit_reason,
    a.primary_diagnosis_code,
    a.service_line,
    a.patient_class,
    a.facility_name,
    a.facility_county,
    a.expected_los_days,
    d.event_timestamp         AS discharge_timestamp,
    d.discharge_disposition,
    DATEDIFF(d.event_timestamp, a.event_timestamp) AS length_of_stay_days
  FROM index_admits a
  LEFT JOIN inpatient_events d
    ON d.member_id = a.member_id
   AND d.event_type = 'A03'
   AND d.event_timestamp >= a.event_timestamp
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY a.adt_event_id ORDER BY d.event_timestamp
  ) = 1
),

-- Prior inpatient admissions in the 180 days before each index admit.
prior_util AS (
  SELECT
    s.index_admit_id,
    COUNT(p.adt_event_id) AS prior_admits_180d
  FROM index_stays s
  LEFT JOIN inpatient_events p
    ON p.member_id = s.member_id
   AND p.event_type = 'A01'
   AND p.event_timestamp <  s.admit_timestamp
   AND p.event_timestamp >= s.admit_timestamp - INTERVAL 180 DAYS
  GROUP BY s.index_admit_id
),

-- Label: any readmission admit within 30 days AFTER the index discharge.
readmit_label AS (
  SELECT
    s.index_admit_id,
    MAX(CASE WHEN r.adt_event_id IS NOT NULL THEN 1 ELSE 0 END) AS readmitted_30d,
    MIN(DATEDIFF(r.event_timestamp, s.discharge_timestamp))     AS days_to_readmit
  FROM index_stays s
  LEFT JOIN inpatient_events r
    ON r.member_id = s.member_id
   AND r.event_type = 'A01'
   AND r.event_timestamp >  s.discharge_timestamp
   AND r.event_timestamp <= s.discharge_timestamp + INTERVAL 30 DAYS
  GROUP BY s.index_admit_id
)

SELECT
  s.index_admit_id,
  s.member_id,
  s.admit_timestamp,
  s.discharge_timestamp,
  s.admit_reason,
  s.primary_diagnosis_code,
  s.service_line,
  s.patient_class,
  s.facility_name,
  s.facility_county,
  s.discharge_disposition,
  -- LOS falls back to expected_los_days when the discharge pairing is missing.
  COALESCE(s.length_of_stay_days, s.expected_los_days, 0) AS length_of_stay_days,
  CAST(s.patient_class = 'Inpatient' AS INT)              AS is_inpatient,
  CAST(s.discharge_disposition IN ('Skilled Nursing Facility', 'Rehabilitation Facility') AS INT) AS discharged_to_post_acute,
  CAST(s.discharge_disposition = 'Against Medical Advice' AS INT) AS discharged_ama,
  COALESCE(pu.prior_admits_180d, 0)                       AS prior_admits_180d,
  COALESCE(rl.readmitted_30d, 0)                          AS readmitted_30d,
  rl.days_to_readmit
FROM index_stays s
LEFT JOIN prior_util pu   ON s.index_admit_id = pu.index_admit_id
LEFT JOIN readmit_label rl ON s.index_admit_id = rl.index_admit_id
-- Only fully-observed stays: index discharge must be > 30 days in the past.
WHERE s.discharge_timestamp IS NOT NULL
  AND s.discharge_timestamp <= CURRENT_TIMESTAMP() - INTERVAL 30 DAYS;
