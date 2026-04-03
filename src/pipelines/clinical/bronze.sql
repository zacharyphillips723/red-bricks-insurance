-- =============================================================================
-- Red Bricks Insurance — Clinical Domain: Bronze Layer
-- =============================================================================
-- Flattens nested FHIR structs from dbignite-parsed Delta tables into
-- analytics-ready columns. dbignite writes full-table overwrites, so these
-- are materialized views (not streaming tables).
--
-- dbignite schema notes:
--   - Resource columns (Encounter, Observation) are ARRAY<STRUCT<...>>
--   - Reference fields (subject, individual) are JSON strings, not structs
--   - Access pattern: Resource[0].field, GET_JSON_OBJECT(Resource[0].subject, '$.reference')
--
-- Sources:
--   ${schema}.Encounter   (dbignite Delta)
--   ${schema}.Observation  (dbignite Delta)
--
-- Column contracts are preserved for downstream silver/gold consumers.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- bronze_encounters
-- Flatten Encounter FHIR structs into analytics columns
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW bronze_encounters
COMMENT 'Flattened FHIR Encounter records from dbignite Delta tables. Maps FHIR class codes to standard encounter types.'
AS
SELECT
  e.Encounter[0].id                                         AS encounter_id,
  COALESCE(
    cx.member_id,
    REGEXP_EXTRACT(
      GET_JSON_OBJECT(e.Encounter[0].subject, '$.reference'),
      'Patient/(.+)', 1
    )
  )                                                         AS member_id,
  COALESCE(
    pcx.provider_npi,
    REGEXP_EXTRACT(
      GET_JSON_OBJECT(e.Encounter[0].participant[0].individual, '$.reference'),
      '([0-9]{10})', 1
    )
  )                                                         AS provider_npi,
  e.Encounter[0].period.start                               AS date_of_service,
  CASE e.Encounter[0].class.code
    WHEN 'AMB'  THEN 'office'
    WHEN 'IMP'  THEN 'inpatient'
    WHEN 'EMER' THEN 'emergency'
    WHEN 'VR'   THEN 'telehealth'
    WHEN 'HH'   THEN 'outpatient'
    WHEN 'SS'   THEN 'outpatient'
    ELSE 'outpatient'
  END                                                       AS encounter_type,
  COALESCE(
    e.Encounter[0].type[0].coding[0].display,
    'general'
  )                                                         AS visit_type,
  'dbignite'                                                AS source_file,
  current_timestamp()                                       AS ingestion_timestamp
FROM ${schema}.Encounter e
LEFT JOIN ${schema}.synthea_crosswalk cx
  ON REGEXP_EXTRACT(
    GET_JSON_OBJECT(e.Encounter[0].subject, '$.reference'),
    'Patient/(.+)', 1
  ) = cx.synthea_uuid
LEFT JOIN ${schema}.synthea_practitioner_crosswalk pcx
  ON REGEXP_EXTRACT(
    GET_JSON_OBJECT(e.Encounter[0].participant[0].individual, '$.reference'),
    'Practitioner/(.+)', 1
  ) = pcx.synthea_practitioner_uuid;

-- ---------------------------------------------------------------------------
-- bronze_lab_results
-- Flatten Observation FHIR structs (category = laboratory)
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW bronze_lab_results
COMMENT 'Flattened FHIR Observation records (laboratory category) from dbignite Delta tables. Maps LOINC codes to standard lab names.'
AS
SELECT
  o.Observation[0].id                                       AS lab_result_id,
  COALESCE(
    cx.member_id,
    REGEXP_EXTRACT(
      GET_JSON_OBJECT(o.Observation[0].subject, '$.reference'),
      'Patient/(.+)', 1
    )
  )                                                         AS member_id,
  CASE o.Observation[0].code.coding[0].code
    WHEN '2345-7'  THEN 'glucose'
    WHEN '4548-4'  THEN 'HbA1c'
    WHEN '2160-0'  THEN 'creatinine'
    WHEN '2093-3'  THEN 'total_cholesterol'
    WHEN '2571-8'  THEN 'triglycerides'
    WHEN '2085-9'  THEN 'hdl_cholesterol'
    WHEN '2089-1'  THEN 'ldl_cholesterol'
    WHEN '6690-2'  THEN 'wbc_count'
    WHEN '789-8'   THEN 'rbc_count'
    WHEN '718-7'   THEN 'hemoglobin'
    WHEN '4544-3'  THEN 'hematocrit'
    WHEN '777-3'   THEN 'platelet_count'
    WHEN '33914-3' THEN 'egfr'
    WHEN '2823-3'  THEN 'potassium'
    WHEN '2951-2'  THEN 'sodium'
    WHEN '1742-6'  THEN 'alt'
    WHEN '1920-8'  THEN 'ast'
    WHEN '1975-2'  THEN 'bilirubin'
    WHEN '2885-2'  THEN 'protein_urine'
    WHEN '14959-1' THEN 'microalbumin_urine'
    ELSE o.Observation[0].code.coding[0].display
  END                                                       AS lab_name,
  o.Observation[0].valueQuantity.value                      AS value,
  o.Observation[0].valueQuantity.unit                       AS unit,
  o.Observation[0].referenceRange[0].low.value              AS reference_range_low,
  o.Observation[0].referenceRange[0].high.value             AS reference_range_high,
  o.Observation[0].effectiveDateTime                        AS collection_date,
  'dbignite'                                                AS source_file,
  current_timestamp()                                       AS ingestion_timestamp
FROM ${schema}.Observation o
LEFT JOIN ${schema}.synthea_crosswalk cx
  ON REGEXP_EXTRACT(
    GET_JSON_OBJECT(o.Observation[0].subject, '$.reference'),
    'Patient/(.+)', 1
  ) = cx.synthea_uuid
WHERE o.Observation[0].category[0].coding[0].code = 'laboratory';

-- ---------------------------------------------------------------------------
-- bronze_vitals
-- Flatten Observation FHIR structs (category = vital-signs)
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW bronze_vitals
COMMENT 'Flattened FHIR Observation records (vital-signs category) from dbignite Delta tables. Maps LOINC codes to standard vital names.'
AS
SELECT
  o.Observation[0].id                                       AS vital_id,
  COALESCE(
    cx.member_id,
    REGEXP_EXTRACT(
      GET_JSON_OBJECT(o.Observation[0].subject, '$.reference'),
      'Patient/(.+)', 1
    )
  )                                                         AS member_id,
  CASE o.Observation[0].code.coding[0].code
    WHEN '8480-6'  THEN 'systolic_bp'
    WHEN '8462-4'  THEN 'diastolic_bp'
    WHEN '8867-4'  THEN 'heart_rate'
    WHEN '9279-1'  THEN 'respiratory_rate'
    WHEN '8310-5'  THEN 'body_temperature'
    WHEN '29463-7' THEN 'body_weight'
    WHEN '8302-2'  THEN 'body_height'
    WHEN '39156-5' THEN 'bmi'
    WHEN '59408-5' THEN 'oxygen_saturation'
    ELSE o.Observation[0].code.coding[0].display
  END                                                       AS vital_name,
  o.Observation[0].valueQuantity.value                      AS value,
  o.Observation[0].effectiveDateTime                        AS measurement_date,
  'dbignite'                                                AS source_file,
  current_timestamp()                                       AS ingestion_timestamp
FROM ${schema}.Observation o
LEFT JOIN ${schema}.synthea_crosswalk cx
  ON REGEXP_EXTRACT(
    GET_JSON_OBJECT(o.Observation[0].subject, '$.reference'),
    'Patient/(.+)', 1
  ) = cx.synthea_uuid
WHERE o.Observation[0].category[0].coding[0].code = 'vital-signs';
