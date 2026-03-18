-- =============================================================================
-- Red Bricks Insurance — Claims Domain: Bronze Layer
-- =============================================================================
-- Raw ingestion of medical and pharmacy claims from source volumes.
-- No transformations applied; data lands as-is with ingestion metadata.
-- Source format: Parquet files delivered to Unity Catalog volumes.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Bronze: Medical Claims
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE bronze_claims_medical
COMMENT 'Raw medical claims ingested from source parquet files. No cleansing applied.'
TBLPROPERTIES (
  'quality' = 'bronze',
  'domain'  = 'claims',
  'pipelines.autoOptimize.zOrderCols' = 'claim_id,member_id'
)
AS
SELECT
  claim_id,
  claim_line_number,
  claim_type,
  member_id,
  rendering_provider_npi,
  billing_provider_npi,
  service_from_date,
  service_to_date,
  paid_date,
  admission_date,
  discharge_date,
  admission_type,
  discharge_status,
  bill_type,
  place_of_service_code,
  place_of_service_desc,
  procedure_code,
  procedure_desc,
  revenue_code,
  revenue_desc,
  drg_code,
  drg_desc,
  primary_diagnosis_code,
  primary_diagnosis_desc,
  secondary_diagnosis_code_1,
  secondary_diagnosis_code_2,
  secondary_diagnosis_code_3,
  billed_amount,
  allowed_amount,
  paid_amount,
  copay,
  coinsurance,
  deductible,
  member_responsibility,
  claim_status,
  denial_reason_code,
  adjustment_reason,
  source_system,
  _metadata.file_path   AS source_file,
  current_timestamp()   AS ingestion_timestamp
FROM STREAM read_files(
  '${source_volume}/claims_medical/',
  format => 'parquet'
);

-- ---------------------------------------------------------------------------
-- Bronze: Pharmacy Claims
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE bronze_claims_pharmacy
COMMENT 'Raw pharmacy claims ingested from source parquet files. No cleansing applied.'
TBLPROPERTIES (
  'quality' = 'bronze',
  'domain'  = 'claims',
  'pipelines.autoOptimize.zOrderCols' = 'claim_id,member_id'
)
AS
SELECT
  claim_id,
  member_id,
  prescriber_npi,
  pharmacy_npi,
  pharmacy_name,
  fill_date,
  paid_date,
  ndc,
  drug_name,
  therapeutic_class,
  is_specialty,
  days_supply,
  quantity,
  ingredient_cost,
  dispensing_fee,
  total_cost,
  member_copay,
  plan_paid,
  claim_status,
  formulary_tier,
  mail_order_flag,
  _metadata.file_path   AS source_file,
  current_timestamp()   AS ingestion_timestamp
FROM STREAM read_files(
  '${source_volume}/claims_pharmacy/',
  format => 'parquet'
);
