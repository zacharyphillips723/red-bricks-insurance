-- =============================================================================
-- Red Bricks Insurance — FWA Domain: Bronze Layer
-- =============================================================================
-- Raw ingestion of FWA signals, provider profiles, and investigation cases
-- from source volumes. No transformations applied; data lands as-is with
-- ingestion metadata.
-- Source format: Parquet files delivered to Unity Catalog volumes.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Bronze: FWA Signals
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE bronze_fwa_signals
COMMENT 'Raw FWA signals ingested from source parquet files. Each record represents a suspected fraudulent claim flagged by rules engine, statistical models, or other detection methods.'
TBLPROPERTIES (
  'quality' = 'bronze',
  'domain'  = 'fwa',
  'pipelines.autoOptimize.zOrderCols' = 'signal_id,claim_id,provider_npi'
)
AS
SELECT
  signal_id,
  claim_id,
  member_id,
  provider_npi,
  fraud_type,
  fraud_type_desc,
  fraud_score,
  severity,
  detection_method,
  evidence_summary,
  evidence_detail_json,
  service_date,
  paid_amount,
  estimated_overpayment,
  detection_date,
  _metadata.file_path   AS source_file,
  current_timestamp()   AS ingestion_timestamp
FROM STREAM read_files(
  '${source_volume}/fwa_signals/',
  format => 'parquet'
);

-- ---------------------------------------------------------------------------
-- Bronze: FWA Provider Profiles
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE bronze_fwa_provider_profiles
COMMENT 'Raw FWA provider risk profiles. Per-provider aggregates of billing patterns, peer comparisons, and fraud signal counts.'
TBLPROPERTIES (
  'quality' = 'bronze',
  'domain'  = 'fwa',
  'pipelines.autoOptimize.zOrderCols' = 'provider_npi'
)
AS
SELECT
  provider_npi,
  provider_name,
  specialty,
  total_claims,
  total_billed,
  total_paid,
  avg_billed_per_claim,
  billed_to_allowed_ratio,
  e5_visit_pct,
  unique_members,
  denial_rate,
  fwa_signal_count,
  fwa_score_avg,
  risk_tier,
  behavioral_flags,
  _metadata.file_path   AS source_file,
  current_timestamp()   AS ingestion_timestamp
FROM STREAM read_files(
  '${source_volume}/fwa_provider_profiles/',
  format => 'parquet'
);

-- ---------------------------------------------------------------------------
-- Bronze: FWA Investigation Cases
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE bronze_fwa_investigation_cases
COMMENT 'Raw pre-seeded investigation cases for FWA Investigation Portal. Each case targets a provider, member, or network for suspected fraud.'
TBLPROPERTIES (
  'quality' = 'bronze',
  'domain'  = 'fwa',
  'pipelines.autoOptimize.zOrderCols' = 'investigation_id'
)
AS
SELECT
  investigation_id,
  investigation_type,
  target_type,
  target_id,
  target_name,
  fraud_types,
  severity,
  status,
  estimated_overpayment,
  claims_involved_count,
  investigation_summary,
  evidence_summary,
  rules_risk_score,
  ml_risk_score,
  created_date,
  _metadata.file_path   AS source_file,
  current_timestamp()   AS ingestion_timestamp
FROM STREAM read_files(
  '${source_volume}/fwa_investigation_cases/',
  format => 'parquet'
);
