-- =============================================================================
-- Red Bricks Insurance — FWA Domain: Silver Layer
-- =============================================================================
-- Cleansed and validated FWA data. IDs validated, scores range-checked,
-- computed columns added. ~2% intentional DQ defects from source data are
-- caught and dropped by critical expectations.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Silver: FWA Signals
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver_fwa_signals (
  -- Critical expectations — drop invalid rows
  CONSTRAINT valid_signal_id
    EXPECT (signal_id IS NOT NULL AND signal_id != '')
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_claim_id
    EXPECT (claim_id IS NOT NULL AND claim_id != '')
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_provider_npi
    EXPECT (provider_npi RLIKE '^[0-9]{10}$')
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_fraud_score_range
    EXPECT (fraud_score >= 0 AND fraud_score <= 1)
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_service_date
    EXPECT (service_date IS NOT NULL AND CAST(service_date AS DATE) >= '2022-01-01')
    ON VIOLATION DROP ROW,

  -- Soft expectations — track but keep the row
  CONSTRAINT positive_paid_amount
    EXPECT (paid_amount >= 0),

  CONSTRAINT positive_overpayment
    EXPECT (estimated_overpayment >= 0),

  CONSTRAINT valid_member_id
    EXPECT (member_id IS NOT NULL)
)
COMMENT 'Validated FWA signals with fraud score range checks, NPI validation, and computed risk buckets. ~2% defective rows dropped by expectations.'
TBLPROPERTIES (
  'quality' = 'silver',
  'domain'  = 'fwa'
)
AS
SELECT
  signal_id,
  claim_id,
  member_id,
  provider_npi,
  fraud_type,
  fraud_type_desc,
  CAST(fraud_score AS DOUBLE) AS fraud_score,
  severity,
  detection_method,
  evidence_summary,
  evidence_detail_json,

  CAST(service_date AS DATE) AS service_date,
  DATE_TRUNC('month', CAST(service_date AS DATE)) AS service_year_month,

  CAST(paid_amount AS DOUBLE) AS paid_amount,
  CAST(estimated_overpayment AS DOUBLE) AS estimated_overpayment,
  CAST(detection_date AS DATE) AS detection_date,

  -- Computed: risk bucket for aggregation
  CASE
    WHEN fraud_score >= 0.8 THEN 'Critical'
    WHEN fraud_score >= 0.6 THEN 'High'
    WHEN fraud_score >= 0.4 THEN 'Medium'
    ELSE 'Low'
  END AS risk_bucket,

  source_file,
  ingestion_timestamp
FROM STREAM(LIVE.bronze_fwa_signals);

-- ---------------------------------------------------------------------------
-- Silver: FWA Provider Profiles
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver_fwa_provider_profiles (
  CONSTRAINT valid_provider_npi
    EXPECT (provider_npi RLIKE '^[0-9]{10}$')
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_total_claims
    EXPECT (total_claims > 0)
    ON VIOLATION DROP ROW,

  -- Soft expectations
  CONSTRAINT valid_billed_ratio
    EXPECT (billed_to_allowed_ratio >= 0),

  CONSTRAINT valid_e5_pct
    EXPECT (e5_visit_pct >= 0 AND e5_visit_pct <= 1)
)
COMMENT 'Validated FWA provider risk profiles with NPI validation and computed composite risk scores.'
TBLPROPERTIES (
  'quality' = 'silver',
  'domain'  = 'fwa'
)
AS
SELECT
  provider_npi,
  provider_name,
  specialty,
  CAST(total_claims AS INT) AS total_claims,
  CAST(total_billed AS DOUBLE) AS total_billed,
  CAST(total_paid AS DOUBLE) AS total_paid,
  CAST(avg_billed_per_claim AS DOUBLE) AS avg_billed_per_claim,
  CAST(billed_to_allowed_ratio AS DOUBLE) AS billed_to_allowed_ratio,
  CAST(e5_visit_pct AS DOUBLE) AS e5_visit_pct,
  CAST(unique_members AS INT) AS unique_members,
  CAST(denial_rate AS DOUBLE) AS denial_rate,
  CAST(fwa_signal_count AS INT) AS fwa_signal_count,
  CAST(fwa_score_avg AS DOUBLE) AS fwa_score_avg,
  risk_tier,
  behavioral_flags,

  -- Computed: composite risk score (weighted blend of billing and fraud indicators)
  ROUND(
    0.30 * LEAST(CAST(billed_to_allowed_ratio AS DOUBLE) / 3.0, 1.0) +
    0.25 * LEAST(CAST(e5_visit_pct AS DOUBLE) * 2.0, 1.0) +
    0.25 * LEAST(CAST(fwa_score_avg AS DOUBLE), 1.0) +
    0.10 * LEAST(CAST(denial_rate AS DOUBLE) * 5.0, 1.0) +
    0.10 * LEAST(CAST(fwa_signal_count AS DOUBLE) / 20.0, 1.0),
    4
  ) AS composite_risk_score,

  source_file,
  ingestion_timestamp
FROM STREAM(LIVE.bronze_fwa_provider_profiles);

-- ---------------------------------------------------------------------------
-- Silver: FWA Investigation Cases
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver_fwa_investigation_cases (
  CONSTRAINT valid_investigation_id
    EXPECT (investigation_id IS NOT NULL AND investigation_id RLIKE '^INV-[0-9]{4}$')
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_target_id
    EXPECT (target_id IS NOT NULL AND target_id != '')
    ON VIOLATION DROP ROW,

  -- Soft expectations
  CONSTRAINT positive_overpayment
    EXPECT (estimated_overpayment >= 0)
)
COMMENT 'Validated FWA investigation cases with ID format validation. Used to seed the Lakebase investigation database.'
TBLPROPERTIES (
  'quality' = 'silver',
  'domain'  = 'fwa'
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
  CAST(estimated_overpayment AS DOUBLE) AS estimated_overpayment,
  CAST(claims_involved_count AS INT) AS claims_involved_count,
  investigation_summary,
  evidence_summary,
  CAST(rules_risk_score AS DOUBLE) AS rules_risk_score,
  CAST(ml_risk_score AS DOUBLE) AS ml_risk_score,
  CAST(created_date AS DATE) AS created_date,

  source_file,
  ingestion_timestamp
FROM STREAM(LIVE.bronze_fwa_investigation_cases);
