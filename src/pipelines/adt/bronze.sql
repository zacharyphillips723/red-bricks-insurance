-- =============================================================================
-- Red Bricks Insurance — ADT (Admit, Discharge, Transfer) Domain: Bronze Layer
-- =============================================================================
-- Ingests raw ADT event feeds from the source volume. Uses Autoloader
-- (read_files STREAM) for incremental pickup — new batches of ADT JSON
-- files dropped every 3 hours are automatically ingested.
--
-- ADT events arrive as JSON files simulating HL7 ADT^A01/A02/A03/A04
-- messages from partner hospitals (Epic, Cerner, MEDITECH, Allscripts).
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE bronze_adt_events
COMMENT 'Raw ADT (Admit, Discharge, Transfer) events from partner hospital feeds. Incrementally ingested via Autoloader.'
TBLPROPERTIES (
  'quality' = 'bronze',
  'domain'  = 'adt',
  'pipelines.autoOptimize.zOrderCols' = 'member_id,event_type'
)
AS
SELECT
  adt_event_id,
  message_control_id,
  event_type,
  event_description,
  CAST(event_timestamp AS TIMESTAMP) AS event_timestamp,
  member_id,
  patient_class,
  facility_id,
  facility_name,
  facility_type,
  facility_county,
  attending_physician_name,
  attending_physician_npi,
  admit_reason,
  primary_diagnosis_code,
  service_line,
  CAST(expected_los_days AS INT) AS expected_los_days,
  discharge_disposition,
  CAST(is_readmission AS BOOLEAN) AS is_readmission,
  acuity_level,
  source_system,
  sending_facility,
  receiving_facility,
  batch_id,
  CAST(batch_timestamp AS TIMESTAMP) AS batch_timestamp,
  _metadata.file_path AS _source_file,
  _metadata.file_modification_time AS _file_mod_time
FROM STREAM read_files('${source_volume}/adt_events/', format => 'json');
