-- =============================================================================
-- Red Bricks Insurance — Prior Authorization Domain: Bronze Layer
-- =============================================================================
-- Raw ingestion of prior authorization requests, medical policy metadata,
-- and structured policy rules from source volumes.
-- Source format: Parquet files delivered to Unity Catalog volumes.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Bronze: Prior Authorization Requests
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE bronze_pa_requests
COMMENT 'Raw prior authorization requests ingested from synthetic data generation. Contains member, provider, service, determination, and appeal data.'
TBLPROPERTIES (
  'quality' = 'bronze',
  'domain'  = 'prior_auth',
  'pipelines.autoOptimize.zOrderCols' = 'auth_request_id,member_id'
)
AS
SELECT
  auth_request_id,
  member_id,
  requesting_provider_npi,
  service_type,
  procedure_code,
  procedure_description,
  policy_id,
  diagnosis_codes,
  urgency,
  line_of_business,
  request_date,
  determination,
  determination_tier,
  determination_date,
  determination_reason,
  denial_reason_code,
  reviewer_type,
  clinical_summary,
  estimated_cost,
  turnaround_hours,
  appeal_filed,
  appeal_outcome,
  _metadata.file_path   AS source_file,
  current_timestamp()   AS ingestion_timestamp
FROM STREAM read_files(
  '${source_volume}/prior_auth_requests/',
  format => 'parquet'
);

-- ---------------------------------------------------------------------------
-- Bronze: Medical Policy Metadata
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE bronze_medical_policies
COMMENT 'Medical policy metadata: policy IDs, service categories, effective dates, and PDF file references.'
TBLPROPERTIES (
  'quality' = 'bronze',
  'domain'  = 'prior_auth'
)
AS
SELECT
  policy_id,
  policy_name,
  service_category,
  effective_date,
  last_reviewed,
  file_name,
  file_path,
  num_covered_services,
  num_criteria,
  num_step_therapy_steps,
  _metadata.file_path   AS source_file,
  current_timestamp()   AS ingestion_timestamp
FROM STREAM read_files(
  '${source_volume}/medical_policies/',
  format => 'parquet'
);

-- ---------------------------------------------------------------------------
-- Bronze: Medical Policy Rules (flattened)
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE bronze_medical_policy_rules
COMMENT 'Structured rules extracted from medical policies. Each row is one clinical criterion or step therapy requirement linked to a policy.'
TBLPROPERTIES (
  'quality' = 'bronze',
  'domain'  = 'prior_auth'
)
AS
SELECT
  policy_id,
  policy_name,
  service_category,
  rule_id,
  rule_type,
  rule_text,
  procedure_codes,
  diagnosis_codes,
  effective_date,
  _metadata.file_path   AS source_file,
  current_timestamp()   AS ingestion_timestamp
FROM STREAM read_files(
  '${source_volume}/medical_policy_rules/',
  format => 'parquet'
);
