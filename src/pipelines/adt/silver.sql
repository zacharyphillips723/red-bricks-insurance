-- =============================================================================
-- Red Bricks Insurance — ADT Domain: Silver Layer
-- =============================================================================
-- Cleansed and enriched ADT events with data quality expectations.
-- Joins with member data to add demographics and risk context.
-- Flags high-priority events (readmissions, high-acuity ED visits).
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Silver: ADT Events — validated and enriched
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver_adt_events (
  CONSTRAINT valid_event_id     EXPECT (adt_event_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_member_id    EXPECT (member_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_event_type   EXPECT (event_type IN ('A01', 'A02', 'A03', 'A04')) ON VIOLATION DROP ROW,
  CONSTRAINT valid_timestamp    EXPECT (event_timestamp IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_facility     EXPECT (facility_name IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT 'Cleansed ADT events with quality expectations. Enriched with priority flags.'
TBLPROPERTIES (
  'quality' = 'silver',
  'domain'  = 'adt'
)
AS
SELECT
  b.adt_event_id,
  b.message_control_id,
  b.event_type,
  b.event_description,
  b.event_timestamp,
  b.member_id,
  b.patient_class,
  b.facility_id,
  b.facility_name,
  b.facility_type,
  b.facility_county,
  b.attending_physician_name,
  b.attending_physician_npi,
  b.admit_reason,
  b.primary_diagnosis_code,
  b.service_line,
  b.expected_los_days,
  b.discharge_disposition,
  b.is_readmission,
  b.acuity_level,
  b.source_system,
  b.sending_facility,
  b.batch_id,
  b.batch_timestamp,

  -- Priority classification for care management
  CASE
    WHEN b.is_readmission = TRUE THEN 'Critical'
    WHEN b.event_type = 'A01' AND b.patient_class = 'Inpatient' THEN 'High'
    WHEN b.event_type = 'A04' AND b.acuity_level IN ('1-Resuscitation', '2-Emergent') THEN 'High'
    WHEN b.event_type = 'A03' AND b.discharge_disposition IN ('Against Medical Advice', 'Skilled Nursing Facility') THEN 'High'
    WHEN b.event_type = 'A04' THEN 'Medium'
    WHEN b.event_type = 'A03' THEN 'Medium'
    ELSE 'Low'
  END AS priority_level,

  -- Alert trigger flag — should this event generate a care management alert?
  CASE
    WHEN b.is_readmission = TRUE THEN TRUE
    WHEN b.event_type = 'A01' AND b.patient_class = 'Inpatient' THEN TRUE
    WHEN b.event_type = 'A03' THEN TRUE  -- All discharges trigger TOC protocol
    WHEN b.event_type = 'A04' AND b.acuity_level IN ('1-Resuscitation', '2-Emergent', '3-Urgent') THEN TRUE
    ELSE FALSE
  END AS triggers_alert,

  -- Event category for dashboard grouping
  CASE
    WHEN b.is_readmission = TRUE THEN 'Readmission'
    WHEN b.event_type = 'A01' THEN 'Admission'
    WHEN b.event_type = 'A02' THEN 'Transfer'
    WHEN b.event_type = 'A03' THEN 'Discharge'
    WHEN b.event_type = 'A04' THEN 'ED Visit'
  END AS event_category,

  b._source_file,
  b._file_mod_time
FROM STREAM(LIVE.bronze_adt_events) b;
