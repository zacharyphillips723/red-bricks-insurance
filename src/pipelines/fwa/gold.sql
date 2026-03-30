-- =============================================================================
-- Red Bricks Insurance — FWA Domain: Gold Layer
-- =============================================================================
-- Business-ready aggregated views for FWA analytics and the Investigation
-- Portal. Materialized views refresh automatically when upstream silver
-- tables update.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Gold: Provider Risk Scorecard
-- ---------------------------------------------------------------------------
-- Per-provider risk scorecard with signal counts, average fraud scores,
-- top fraud types, peer rank, and billing pattern indicators. Primary
-- table for the Provider Analysis page in the FWA Investigation Portal.
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_fwa_provider_risk
COMMENT 'Provider-level FWA risk scorecard. Aggregates fraud signals, billing anomalies, and peer comparisons into a single risk profile per provider.'
TBLPROPERTIES (
  'quality' = 'gold',
  'domain'  = 'fwa'
)
AS
WITH signal_agg AS (
  SELECT
    provider_npi,
    COUNT(*) AS signal_count,
    AVG(fraud_score) AS avg_fraud_score,
    MAX(fraud_score) AS max_fraud_score,
    SUM(estimated_overpayment) AS total_estimated_overpayment,
    SUM(paid_amount) AS total_flagged_paid,
    COUNT(DISTINCT fraud_type) AS distinct_fraud_types,
    COUNT(DISTINCT claim_id) AS distinct_flagged_claims,
    COUNT(DISTINCT member_id) AS distinct_flagged_members,
    -- Top fraud type by count
    MODE() WITHIN GROUP (ORDER BY fraud_type) AS primary_fraud_type,
    -- Severity distribution
    COUNT(*) FILTER (WHERE risk_bucket = 'Critical') AS critical_signals,
    COUNT(*) FILTER (WHERE risk_bucket = 'High') AS high_signals,
    COUNT(*) FILTER (WHERE risk_bucket = 'Medium') AS medium_signals,
    COUNT(*) FILTER (WHERE risk_bucket = 'Low') AS low_signals
  FROM LIVE.silver_fwa_signals
  GROUP BY provider_npi
),

provider_context AS (
  SELECT
    p.provider_npi,
    p.provider_name,
    p.specialty,
    p.total_claims,
    p.total_billed,
    p.total_paid,
    p.avg_billed_per_claim,
    p.billed_to_allowed_ratio,
    p.e5_visit_pct,
    p.unique_members,
    p.denial_rate,
    p.composite_risk_score,
    p.risk_tier,
    p.behavioral_flags
  FROM LIVE.silver_fwa_provider_profiles p
)

SELECT
  pc.provider_npi,
  pc.provider_name,
  pc.specialty,
  pc.total_claims,
  pc.total_billed,
  pc.total_paid,
  pc.avg_billed_per_claim,
  pc.billed_to_allowed_ratio,
  pc.e5_visit_pct,
  pc.unique_members,
  pc.denial_rate,
  pc.risk_tier,
  pc.behavioral_flags,
  pc.composite_risk_score,

  -- Fraud signal metrics
  COALESCE(sa.signal_count, 0) AS fwa_signal_count,
  COALESCE(sa.avg_fraud_score, 0) AS fwa_avg_score,
  COALESCE(sa.max_fraud_score, 0) AS fwa_max_score,
  COALESCE(sa.total_estimated_overpayment, 0) AS fwa_estimated_overpayment,
  COALESCE(sa.total_flagged_paid, 0) AS fwa_flagged_paid,
  COALESCE(sa.distinct_fraud_types, 0) AS fwa_distinct_fraud_types,
  COALESCE(sa.distinct_flagged_claims, 0) AS fwa_distinct_flagged_claims,
  COALESCE(sa.distinct_flagged_members, 0) AS fwa_distinct_flagged_members,
  sa.primary_fraud_type AS fwa_primary_fraud_type,
  COALESCE(sa.critical_signals, 0) AS fwa_critical_signals,
  COALESCE(sa.high_signals, 0) AS fwa_high_signals,

  -- Computed: flagged claim percentage
  ROUND(
    COALESCE(sa.distinct_flagged_claims, 0) * 100.0 /
    NULLIF(pc.total_claims, 0),
    2
  ) AS fwa_flagged_claim_pct,

  -- Peer rank by composite risk score (within specialty)
  RANK() OVER (
    PARTITION BY pc.specialty
    ORDER BY pc.composite_risk_score DESC
  ) AS specialty_risk_rank,

  RANK() OVER (
    ORDER BY pc.composite_risk_score DESC
  ) AS overall_risk_rank

FROM provider_context pc
LEFT JOIN signal_agg sa ON pc.provider_npi = sa.provider_npi;

-- ---------------------------------------------------------------------------
-- Gold: Flagged Claims with Full Context
-- ---------------------------------------------------------------------------
-- Joins FWA signals back to silver medical claims for full claim context.
-- Used by the Investigation Detail page and agent for evidence analysis.
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_fwa_claim_flags
COMMENT 'Flagged claims enriched with full claim context from silver medical claims. Each row is a claim flagged by the FWA detection pipeline with its original billing details.'
TBLPROPERTIES (
  'quality' = 'gold',
  'domain'  = 'fwa'
)
AS
SELECT
  s.signal_id,
  s.claim_id,
  s.member_id,
  s.provider_npi,
  s.fraud_type,
  s.fraud_type_desc,
  s.fraud_score,
  s.risk_bucket,
  s.severity,
  s.detection_method,
  s.evidence_summary,
  s.estimated_overpayment,
  s.service_date,
  s.service_year_month,
  s.detection_date,

  -- Claim context from silver
  c.claim_type,
  c.billing_provider_npi,
  c.procedure_code,
  c.procedure_desc,
  c.primary_diagnosis_code,
  c.primary_diagnosis_desc,
  c.place_of_service_code,
  c.place_of_service_desc,
  c.billed_amount,
  c.allowed_amount,
  c.paid_amount AS claim_paid_amount,
  c.copay,
  c.coinsurance,
  c.deductible,
  c.member_responsibility,
  c.claim_status,
  c.denial_reason_code,

  -- Enrollment context
  e.line_of_business,
  e.plan_type

FROM LIVE.silver_fwa_signals s
LEFT JOIN ${catalog}.claims.silver_claims_medical c
  ON s.claim_id = c.claim_id
LEFT JOIN ${catalog}.members.silver_enrollment e
  ON s.member_id = e.member_id;

-- ---------------------------------------------------------------------------
-- Gold: FWA Summary Metrics
-- ---------------------------------------------------------------------------
-- Aggregate metrics by fraud type, severity, and LOB for executive
-- dashboards and Genie queries.
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_fwa_summary
COMMENT 'Aggregate FWA metrics by fraud type, severity, and line of business. Powers the FWA dashboard KPIs and trend analysis.'
TBLPROPERTIES (
  'quality' = 'gold',
  'domain'  = 'fwa'
)
AS
SELECT
  s.fraud_type,
  s.fraud_type_desc,
  s.severity,
  s.risk_bucket,
  s.detection_method,
  e.line_of_business,
  s.service_year_month,

  COUNT(*) AS signal_count,
  COUNT(DISTINCT s.claim_id) AS distinct_claims,
  COUNT(DISTINCT s.provider_npi) AS distinct_providers,
  COUNT(DISTINCT s.member_id) AS distinct_members,

  SUM(s.paid_amount) AS total_paid_amount,
  SUM(s.estimated_overpayment) AS total_estimated_overpayment,
  AVG(s.fraud_score) AS avg_fraud_score,
  MAX(s.fraud_score) AS max_fraud_score,

  ROUND(SUM(s.estimated_overpayment) / NULLIF(SUM(s.paid_amount), 0), 4) AS overpayment_ratio

FROM LIVE.silver_fwa_signals s
LEFT JOIN ${catalog}.members.silver_enrollment e
  ON s.member_id = e.member_id
GROUP BY
  s.fraud_type,
  s.fraud_type_desc,
  s.severity,
  s.risk_bucket,
  s.detection_method,
  e.line_of_business,
  s.service_year_month;
