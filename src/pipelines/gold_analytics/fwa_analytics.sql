-- =============================================================================
-- Red Bricks Insurance — Gold Analytics: FWA Cross-Domain Analytics
-- =============================================================================
-- Cross-domain FWA analytics that span claims, providers, members, and FWA
-- domain tables. Includes network analysis, member risk scoring, AI-generated
-- investigation narratives, and ML model batch scores.
-- Pipeline: gold_analytics (runs after fwa_pipeline + domain pipelines)
-- =============================================================================

-- ---------------------------------------------------------------------------
-- gold_fwa_network_analysis — Provider Referral Ring Detection
-- ---------------------------------------------------------------------------
-- Identifies clusters of providers that share members at unusually high rates,
-- suggesting potential referral rings or collusive billing arrangements.
-- Uses rendering_npi/billing_npi patterns across shared members.
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_fwa_network_analysis
COMMENT 'Provider referral network analysis. Identifies provider pairs sharing unusually high member overlap, suggesting potential referral rings or collusive billing.'
AS
WITH provider_member_sets AS (
  SELECT
    rendering_provider_npi AS provider_npi,
    COLLECT_SET(member_id) AS member_set,
    COUNT(DISTINCT member_id) AS unique_members,
    COUNT(DISTINCT claim_id) AS total_claims,
    SUM(paid_amount) AS total_paid
  FROM claims.silver_claims_medical
  GROUP BY rendering_provider_npi
  HAVING COUNT(DISTINCT member_id) >= 10
),

provider_pairs AS (
  SELECT
    a.provider_npi AS provider_a_npi,
    b.provider_npi AS provider_b_npi,
    SIZE(ARRAY_INTERSECT(a.member_set, b.member_set)) AS shared_members,
    a.unique_members AS provider_a_members,
    b.unique_members AS provider_b_members,
    a.total_claims AS provider_a_claims,
    b.total_claims AS provider_b_claims,
    a.total_paid AS provider_a_paid,
    b.total_paid AS provider_b_paid
  FROM provider_member_sets a
  CROSS JOIN provider_member_sets b
  WHERE a.provider_npi < b.provider_npi
    AND SIZE(ARRAY_INTERSECT(a.member_set, b.member_set)) >= 5
)

SELECT
  pp.provider_a_npi,
  pa.provider_name AS provider_a_name,
  pa.specialty AS provider_a_specialty,
  pp.provider_b_npi,
  pb.provider_name AS provider_b_name,
  pb.specialty AS provider_b_specialty,
  pp.shared_members,
  pp.provider_a_members,
  pp.provider_b_members,
  ROUND(pp.shared_members * 100.0 / LEAST(pp.provider_a_members, pp.provider_b_members), 2) AS overlap_pct,
  pp.provider_a_claims + pp.provider_b_claims AS combined_claims,
  pp.provider_a_paid + pp.provider_b_paid AS combined_paid,
  -- Flag pairs where >30% of the smaller panel overlaps
  CASE
    WHEN pp.shared_members * 100.0 / LEAST(pp.provider_a_members, pp.provider_b_members) > 50 THEN 'High'
    WHEN pp.shared_members * 100.0 / LEAST(pp.provider_a_members, pp.provider_b_members) > 30 THEN 'Medium'
    ELSE 'Low'
  END AS network_risk_level
FROM provider_pairs pp
LEFT JOIN providers.silver_providers pa ON pp.provider_a_npi = pa.npi
LEFT JOIN providers.silver_providers pb ON pp.provider_b_npi = pb.npi
WHERE pp.shared_members * 100.0 / LEAST(pp.provider_a_members, pp.provider_b_members) > 20
ORDER BY overlap_pct DESC;

-- ---------------------------------------------------------------------------
-- gold_fwa_member_risk — Member-Level Fraud Indicators
-- ---------------------------------------------------------------------------
-- Aggregates member-level fraud risk indicators including doctor shopping
-- scores, pharmacy abuse indicators, and geographic anomalies.
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_fwa_member_risk
COMMENT 'Member-level FWA risk indicators. Aggregates doctor shopping, pharmacy abuse, and geographic anomaly scores per member for investigation prioritization.'
AS
WITH member_provider_counts AS (
  SELECT
    member_id,
    COUNT(DISTINCT rendering_provider_npi) AS unique_providers_90d,
    COUNT(DISTINCT primary_diagnosis_code) AS unique_diagnoses_90d,
    COUNT(DISTINCT claim_id) AS claims_90d
  FROM claims.silver_claims_medical
  WHERE service_from_date >= DATE_ADD(CURRENT_DATE(), -90)
  GROUP BY member_id
),

member_pharmacy AS (
  SELECT
    member_id,
    COUNT(DISTINCT prescriber_npi) AS unique_prescribers,
    COUNT(DISTINCT pharmacy_npi) AS unique_pharmacies,
    COUNT(*) AS total_fills,
    SUM(CASE WHEN is_specialty THEN 1 ELSE 0 END) AS specialty_fills,
    SUM(total_cost) AS total_rx_cost
  FROM claims.silver_claims_pharmacy
  WHERE fill_date >= DATE_ADD(CURRENT_DATE(), -180)
  GROUP BY member_id
),

member_fwa_signals AS (
  SELECT
    member_id,
    COUNT(*) AS fwa_signal_count,
    AVG(fraud_score) AS avg_fraud_score,
    SUM(estimated_overpayment) AS total_estimated_overpayment,
    COUNT(DISTINCT fraud_type) AS distinct_fraud_types,
    COLLECT_SET(fraud_type) AS fraud_types_flagged
  FROM fwa.silver_fwa_signals
  GROUP BY member_id
)

SELECT
  m.member_id,
  e.line_of_business,
  e.plan_type,
  e.risk_score AS enrollment_risk_score,

  -- Doctor shopping indicators
  COALESCE(mpc.unique_providers_90d, 0) AS unique_providers_90d,
  COALESCE(mpc.unique_diagnoses_90d, 0) AS unique_diagnoses_90d,
  COALESCE(mpc.claims_90d, 0) AS claims_90d,
  -- Score: normalize to 0-1 (5+ providers in 90 days is suspicious)
  LEAST(COALESCE(mpc.unique_providers_90d, 0) / 10.0, 1.0) AS doctor_shopping_score,

  -- Pharmacy abuse indicators
  COALESCE(mp.unique_prescribers, 0) AS unique_prescribers_180d,
  COALESCE(mp.unique_pharmacies, 0) AS unique_pharmacies_180d,
  COALESCE(mp.total_fills, 0) AS total_fills_180d,
  COALESCE(mp.specialty_fills, 0) AS specialty_fills_180d,
  COALESCE(mp.total_rx_cost, 0) AS total_rx_cost_180d,
  -- Score: normalize (3+ prescribers AND 3+ pharmacies is suspicious)
  LEAST(
    (COALESCE(mp.unique_prescribers, 0) / 6.0 + COALESCE(mp.unique_pharmacies, 0) / 6.0) / 2.0,
    1.0
  ) AS pharmacy_abuse_score,

  -- FWA signal summary
  COALESCE(fs.fwa_signal_count, 0) AS fwa_signal_count,
  COALESCE(fs.avg_fraud_score, 0) AS avg_fwa_score,
  COALESCE(fs.total_estimated_overpayment, 0) AS total_estimated_overpayment,
  COALESCE(fs.distinct_fraud_types, 0) AS distinct_fraud_types,
  fs.fraud_types_flagged,

  -- Composite member risk score
  ROUND(
    0.35 * LEAST(COALESCE(mpc.unique_providers_90d, 0) / 10.0, 1.0) +
    0.25 * LEAST(
      (COALESCE(mp.unique_prescribers, 0) / 6.0 + COALESCE(mp.unique_pharmacies, 0) / 6.0) / 2.0,
      1.0
    ) +
    0.40 * COALESCE(fs.avg_fraud_score, 0),
    4
  ) AS composite_member_fwa_score

FROM members.silver_members m
LEFT JOIN members.silver_enrollment e ON m.member_id = e.member_id
LEFT JOIN member_provider_counts mpc ON m.member_id = mpc.member_id
LEFT JOIN member_pharmacy mp ON m.member_id = mp.member_id
LEFT JOIN member_fwa_signals fs ON m.member_id = fs.member_id;

-- ---------------------------------------------------------------------------
-- gold_fwa_ai_classification — AI-Generated Investigation Narratives
-- ---------------------------------------------------------------------------
-- For the top ~100 highest-score FWA signals, generates investigation-ready
-- narratives using a foundation model. Demonstrates GenAI applied to fraud
-- investigation use cases.
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_fwa_ai_classification
COMMENT 'AI-generated investigation narratives for top 100 highest-score FWA signals. Provides SIU-ready summaries with evidence analysis and recommended actions.'
AS
WITH top_signals AS (
  SELECT
    s.signal_id,
    s.claim_id,
    s.member_id,
    s.provider_npi,
    s.fraud_type,
    s.fraud_type_desc,
    s.fraud_score,
    s.severity,
    s.evidence_summary,
    s.estimated_overpayment,
    s.service_date,
    c.procedure_code,
    c.procedure_desc,
    c.billed_amount,
    c.paid_amount AS claim_paid,
    c.primary_diagnosis_code,
    c.claim_type,
    e.line_of_business,
    ROW_NUMBER() OVER (ORDER BY s.fraud_score DESC) AS signal_rank
  FROM fwa.silver_fwa_signals s
  LEFT JOIN claims.silver_claims_medical c ON s.claim_id = c.claim_id
  LEFT JOIN members.silver_enrollment e ON s.member_id = e.member_id
)
SELECT
  signal_id,
  claim_id,
  member_id,
  provider_npi,
  fraud_type,
  fraud_score,
  severity,
  estimated_overpayment,
  signal_rank,
  ai_query(
    'databricks-meta-llama-3-3-70b-instruct',
    CONCAT(
      'You are a fraud, waste, and abuse (FWA) investigation analyst for a health insurance company. ',
      'Classify this suspected fraud signal into an investigation priority: Immediate Action, High Priority, Standard Review, or Monitor Only. ',
      'Fraud Type: ', fraud_type_desc,
      ', Score: ', CAST(fraud_score AS STRING),
      ', Severity: ', severity,
      ', Estimated Overpayment: $', CAST(ROUND(estimated_overpayment, 2) AS STRING),
      ', Procedure: ', COALESCE(procedure_code, 'N/A'), ' (', COALESCE(procedure_desc, 'N/A'), ')',
      ', Billed: $', COALESCE(CAST(ROUND(billed_amount, 2) AS STRING), 'N/A'),
      ', LOB: ', COALESCE(line_of_business, 'N/A'),
      '. Respond with only the priority level, nothing else.'
    )
  ) AS investigation_priority,
  ai_query(
    'databricks-meta-llama-3-3-70b-instruct',
    CONCAT(
      'You are a senior FWA investigator. Write a 3-sentence investigation brief for this suspected fraud signal. ',
      'Include: (1) what was flagged and why, (2) key evidence points, (3) recommended next steps. ',
      'Fraud Type: ', fraud_type_desc,
      ', Evidence: ', COALESCE(evidence_summary, 'No evidence summary available'),
      ', Score: ', CAST(fraud_score AS STRING),
      ', Estimated Overpayment: $', CAST(ROUND(estimated_overpayment, 2) AS STRING),
      ', Procedure: ', COALESCE(procedure_code, 'N/A'), ' (', COALESCE(procedure_desc, 'N/A'), ')',
      ', Claim Type: ', COALESCE(claim_type, 'N/A'),
      ', LOB: ', COALESCE(line_of_business, 'N/A'),
      '. Be concise and actionable.'
    )
  ) AS investigation_narrative
FROM top_signals
WHERE signal_rank <= 100;

-- ---------------------------------------------------------------------------
-- gold_fwa_model_scores — ML Model Batch Scoring + Rules Fallback
-- ---------------------------------------------------------------------------
-- Prefers real ML model predictions from analytics.fwa_ml_predictions
-- (written by train_fwa_model notebook). Falls back to rules-based fraud
-- scores from silver_fwa_signals when ML predictions are not available.
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_fwa_model_scores
COMMENT 'Fraud probability scores for claims. Prefers ML model predictions; falls back to rules-based FWA signal scores. Refreshed after model training.'
AS
SELECT
  c.claim_id,
  c.member_id,
  c.rendering_provider_npi AS provider_npi,
  c.claim_type,
  c.procedure_code,
  c.billed_amount,
  c.allowed_amount,
  c.paid_amount,
  c.service_from_date,
  c.service_year_month,
  e.line_of_business,

  -- Prefer ML model predictions; fall back to rules-based signal scores
  COALESCE(ml.ml_fraud_probability, s.fraud_score, 0) AS ml_fraud_probability,

  COALESCE(
    ml.ml_risk_tier,
    CASE
      WHEN s.fraud_score >= 0.7 THEN 'High'
      WHEN s.fraud_score >= 0.4 THEN 'Medium'
      WHEN s.fraud_score > 0 THEN 'Low'
      ELSE 'None'
    END
  ) AS ml_risk_tier

FROM claims.silver_claims_medical c
LEFT JOIN members.silver_enrollment e
  ON c.member_id = e.member_id
LEFT JOIN analytics.fwa_ml_predictions ml
  ON c.claim_id = ml.claim_id
LEFT JOIN fwa.silver_fwa_signals s
  ON c.claim_id = s.claim_id;
