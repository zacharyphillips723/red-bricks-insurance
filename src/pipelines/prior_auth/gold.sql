-- =============================================================================
-- Red Bricks Insurance — Prior Authorization Domain: Gold Layer
-- =============================================================================
-- Business-ready views for PA analytics, CMS-0057-F compliance reporting,
-- auto-adjudication performance, and provider pattern analysis.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Gold: Enriched PA Requests (Genie-friendly, detail-level)
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_pa_requests
COMMENT 'Enriched prior authorization requests with CMS compliance flags and appeal outcomes. Primary table for Genie PA queries and the PA Review Portal.'
TBLPROPERTIES (
  'quality' = 'gold',
  'domain'  = 'prior_auth'
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
  cms_compliant,
  request_year_month,

  -- Derived: days to decision
  DATEDIFF(determination_date, request_date) AS days_to_decision,

  -- Derived: final outcome (accounts for appeal overturn)
  CASE
    WHEN determination = 'approved' THEN 'approved'
    WHEN determination = 'denied' AND appeal_outcome = 'overturned' THEN 'approved_on_appeal'
    WHEN determination = 'denied' AND appeal_outcome = 'partially_overturned' THEN 'partially_approved_on_appeal'
    WHEN determination = 'denied' THEN 'denied_final'
    ELSE determination
  END AS final_outcome

FROM LIVE.silver_pa_requests;

-- ---------------------------------------------------------------------------
-- Gold: PA Metrics — CMS-0057-F Compliance Dashboard
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_pa_metrics
COMMENT 'Monthly PA metrics for CMS-0057-F public reporting: approval/denial rates, turnaround times, appeal volumes, and electronic processing rates.'
TBLPROPERTIES (
  'quality' = 'gold',
  'domain'  = 'prior_auth'
)
AS
SELECT
  request_year_month,
  line_of_business,
  service_type,
  urgency,

  COUNT(*)                                                            AS total_requests,
  SUM(CASE WHEN determination = 'approved' THEN 1 ELSE 0 END)        AS approved_count,
  SUM(CASE WHEN determination = 'denied' THEN 1 ELSE 0 END)          AS denied_count,
  SUM(CASE WHEN determination = 'pended' THEN 1 ELSE 0 END)          AS pended_count,

  ROUND(SUM(CASE WHEN determination = 'approved' THEN 1 ELSE 0 END)
    * 100.0 / NULLIF(COUNT(*), 0), 2)                                AS approval_rate_pct,
  ROUND(SUM(CASE WHEN determination = 'denied' THEN 1 ELSE 0 END)
    * 100.0 / NULLIF(COUNT(*), 0), 2)                                AS denial_rate_pct,

  -- Turnaround time metrics
  ROUND(AVG(turnaround_hours), 1)                                     AS avg_turnaround_hours,
  ROUND(PERCENTILE(turnaround_hours, 0.5), 1)                         AS median_turnaround_hours,
  ROUND(PERCENTILE(turnaround_hours, 0.95), 1)                        AS p95_turnaround_hours,

  -- CMS compliance rate
  ROUND(SUM(CASE WHEN cms_compliant THEN 1 ELSE 0 END)
    * 100.0 / NULLIF(COUNT(*), 0), 2)                                AS cms_compliance_rate_pct,

  -- Auto-adjudication rate (Tier 1 auto)
  ROUND(SUM(CASE WHEN determination_tier = 'tier_1_auto' THEN 1 ELSE 0 END)
    * 100.0 / NULLIF(COUNT(*), 0), 2)                                AS auto_adjudication_rate_pct,

  -- Appeal metrics
  SUM(CASE WHEN appeal_filed THEN 1 ELSE 0 END)                      AS appeals_filed,
  SUM(CASE WHEN appeal_outcome = 'overturned' THEN 1 ELSE 0 END)     AS appeals_overturned,
  SUM(CASE WHEN appeal_outcome = 'upheld' THEN 1 ELSE 0 END)         AS appeals_upheld,

  -- Cost
  ROUND(SUM(estimated_cost), 2)                                       AS total_estimated_cost,
  ROUND(AVG(estimated_cost), 2)                                       AS avg_estimated_cost

FROM LIVE.silver_pa_requests
GROUP BY request_year_month, line_of_business, service_type, urgency;

-- ---------------------------------------------------------------------------
-- Gold: PA Provider Patterns
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_pa_provider_patterns
COMMENT 'Provider-level PA patterns: request volume, approval rates, denial reasons, and auto-adjudication rates. Identifies high-volume and high-denial providers.'
TBLPROPERTIES (
  'quality' = 'gold',
  'domain'  = 'prior_auth'
)
AS
SELECT
  requesting_provider_npi,
  service_type,

  COUNT(*)                                                            AS total_requests,
  SUM(CASE WHEN determination = 'approved' THEN 1 ELSE 0 END)        AS approved_count,
  SUM(CASE WHEN determination = 'denied' THEN 1 ELSE 0 END)          AS denied_count,

  ROUND(SUM(CASE WHEN determination = 'approved' THEN 1 ELSE 0 END)
    * 100.0 / NULLIF(COUNT(*), 0), 2)                                AS approval_rate_pct,
  ROUND(SUM(CASE WHEN determination = 'denied' THEN 1 ELSE 0 END)
    * 100.0 / NULLIF(COUNT(*), 0), 2)                                AS denial_rate_pct,

  ROUND(AVG(turnaround_hours), 1)                                     AS avg_turnaround_hours,

  -- Auto-adjudication rate for this provider
  ROUND(SUM(CASE WHEN determination_tier = 'tier_1_auto' THEN 1 ELSE 0 END)
    * 100.0 / NULLIF(COUNT(*), 0), 2)                                AS auto_adjudication_rate_pct,

  -- Appeal rate
  ROUND(SUM(CASE WHEN appeal_filed THEN 1 ELSE 0 END)
    * 100.0 / NULLIF(SUM(CASE WHEN determination = 'denied' THEN 1 ELSE 0 END), 0), 2)
                                                                      AS denial_appeal_rate_pct,

  ROUND(SUM(estimated_cost), 2)                                       AS total_estimated_cost

FROM LIVE.silver_pa_requests
GROUP BY requesting_provider_npi, service_type;

-- ---------------------------------------------------------------------------
-- Gold: PA Policy Utilization
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_pa_policy_utilization
COMMENT 'Medical policy utilization: request volume, approval/denial rates, and top denial reasons per policy. Shows which policies generate the most PA activity.'
TBLPROPERTIES (
  'quality' = 'gold',
  'domain'  = 'prior_auth'
)
AS
SELECT
  p.policy_id,
  p.policy_name,
  p.service_category,

  COUNT(r.auth_request_id)                                            AS total_requests,
  SUM(CASE WHEN r.determination = 'approved' THEN 1 ELSE 0 END)      AS approved_count,
  SUM(CASE WHEN r.determination = 'denied' THEN 1 ELSE 0 END)        AS denied_count,

  ROUND(SUM(CASE WHEN r.determination = 'approved' THEN 1 ELSE 0 END)
    * 100.0 / NULLIF(COUNT(r.auth_request_id), 0), 2)                AS approval_rate_pct,

  ROUND(AVG(r.turnaround_hours), 1)                                   AS avg_turnaround_hours,
  ROUND(SUM(r.estimated_cost), 2)                                     AS total_estimated_cost,

  -- Most common denial reason for this policy
  MODE(r.denial_reason_code)                                          AS top_denial_reason

FROM LIVE.silver_medical_policies p
LEFT JOIN LIVE.silver_pa_requests r ON p.policy_id = r.policy_id
GROUP BY p.policy_id, p.policy_name, p.service_category;

-- ---------------------------------------------------------------------------
-- Gold: Auto-Adjudication Performance
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_pa_auto_adjudication_performance
COMMENT 'Auto-adjudication funnel: Tier 1 (deterministic rules), Tier 2 (ML classification), Tier 3 (LLM clinical review). Tracks volume and accuracy per tier.'
TBLPROPERTIES (
  'quality' = 'gold',
  'domain'  = 'prior_auth'
)
AS
SELECT
  determination_tier,
  service_type,
  request_year_month,

  COUNT(*)                                                            AS total_requests,
  SUM(CASE WHEN determination = 'approved' THEN 1 ELSE 0 END)        AS approved_count,
  SUM(CASE WHEN determination = 'denied' THEN 1 ELSE 0 END)          AS denied_count,
  SUM(CASE WHEN determination = 'pended' THEN 1 ELSE 0 END)          AS pended_count,

  ROUND(AVG(turnaround_hours), 1)                                     AS avg_turnaround_hours,
  ROUND(SUM(estimated_cost), 2)                                       AS total_estimated_cost,

  -- Appeal overturn rate per tier (quality signal)
  ROUND(SUM(CASE WHEN appeal_outcome IN ('overturned', 'partially_overturned') THEN 1 ELSE 0 END)
    * 100.0 / NULLIF(SUM(CASE WHEN appeal_filed THEN 1 ELSE 0 END), 0), 2)
                                                                      AS appeal_overturn_rate_pct

FROM LIVE.silver_pa_requests
GROUP BY determination_tier, service_type, request_year_month;

-- ---------------------------------------------------------------------------
-- Gold: Denial Reason Analysis
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_pa_denial_analysis
COMMENT 'Denial reason breakdown with appeal outcomes. Supports CMS-0057-F public reporting of top denial reasons by service category.'
TBLPROPERTIES (
  'quality' = 'gold',
  'domain'  = 'prior_auth'
)
AS
SELECT
  denial_reason_code,
  determination_reason,
  service_type,
  line_of_business,

  COUNT(*)                                                            AS denial_count,
  ROUND(SUM(estimated_cost), 2)                                       AS total_denied_cost,

  SUM(CASE WHEN appeal_filed THEN 1 ELSE 0 END)                      AS appeals_filed,
  SUM(CASE WHEN appeal_outcome = 'overturned' THEN 1 ELSE 0 END)     AS appeals_overturned,
  ROUND(SUM(CASE WHEN appeal_outcome = 'overturned' THEN 1 ELSE 0 END)
    * 100.0 / NULLIF(SUM(CASE WHEN appeal_filed THEN 1 ELSE 0 END), 0), 2)
                                                                      AS overturn_rate_pct

FROM LIVE.silver_pa_requests
WHERE determination = 'denied'
GROUP BY denial_reason_code, determination_reason, service_type, line_of_business;

-- ---------------------------------------------------------------------------
-- Gold: Tier 1 Deterministic Rules Evaluation
-- ---------------------------------------------------------------------------
-- Evaluates each PA request against deterministic policy rules:
--   1. Procedure code match (is the CPT in the policy's covered codes?)
--   2. Diagnosis code match (does the ICD-10 align with policy requirements?)
--   3. Urgency compliance (turnaround within CMS limits?)
-- Flags requests that could have been auto-adjudicated.
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_pa_tier1_evaluation
COMMENT 'Deterministic rules evaluation for each PA request. Shows which requests could be auto-adjudicated via Tier 1 rules without ML or LLM review.'
TBLPROPERTIES (
  'quality' = 'gold',
  'domain'  = 'prior_auth'
)
AS
SELECT
  r.auth_request_id,
  r.member_id,
  r.service_type,
  r.procedure_code,
  r.policy_id,
  r.urgency,
  r.determination,
  r.determination_tier,

  -- Rule 1: Procedure code exists in the policy's covered procedures
  CASE
    WHEN EXISTS (
      SELECT 1 FROM LIVE.silver_medical_policy_rules pr
      WHERE pr.policy_id = r.policy_id
        AND pr.rule_type = 'coverage_criteria'
        AND pr.procedure_codes LIKE CONCAT('%', r.procedure_code, '%')
    ) THEN TRUE
    ELSE FALSE
  END AS procedure_code_match,

  -- Rule 2: Diagnosis code aligns with policy requirements
  CASE
    WHEN EXISTS (
      SELECT 1 FROM LIVE.silver_medical_policy_rules pr
      WHERE pr.policy_id = r.policy_id
        AND pr.diagnosis_codes IS NOT NULL
        AND pr.diagnosis_codes LIKE CONCAT('%', SPLIT(r.diagnosis_codes, '\\|')[0], '%')
    ) THEN TRUE
    ELSE FALSE
  END AS diagnosis_code_match,

  -- Rule 3: CMS turnaround compliance
  r.cms_compliant,

  -- Rule 4: Has clinical summary (documentation present)
  CASE WHEN LENGTH(COALESCE(r.clinical_summary, '')) > 50 THEN TRUE ELSE FALSE END AS has_documentation,

  -- Tier 1 auto-eligible: all deterministic rules pass
  CASE
    WHEN EXISTS (
      SELECT 1 FROM LIVE.silver_medical_policy_rules pr
      WHERE pr.policy_id = r.policy_id
        AND pr.rule_type = 'coverage_criteria'
        AND pr.procedure_codes LIKE CONCAT('%', r.procedure_code, '%')
    )
    AND EXISTS (
      SELECT 1 FROM LIVE.silver_medical_policy_rules pr
      WHERE pr.policy_id = r.policy_id
        AND pr.diagnosis_codes IS NOT NULL
        AND pr.diagnosis_codes LIKE CONCAT('%', SPLIT(r.diagnosis_codes, '\\|')[0], '%')
    )
    AND LENGTH(COALESCE(r.clinical_summary, '')) > 50
    THEN TRUE
    ELSE FALSE
  END AS tier1_auto_eligible,

  -- Agreement: would Tier 1 auto-approve match the actual determination?
  CASE
    WHEN r.determination = 'approved'
    AND EXISTS (
      SELECT 1 FROM LIVE.silver_medical_policy_rules pr
      WHERE pr.policy_id = r.policy_id
        AND pr.rule_type = 'coverage_criteria'
        AND pr.procedure_codes LIKE CONCAT('%', r.procedure_code, '%')
    )
    AND EXISTS (
      SELECT 1 FROM LIVE.silver_medical_policy_rules pr
      WHERE pr.policy_id = r.policy_id
        AND pr.diagnosis_codes IS NOT NULL
        AND pr.diagnosis_codes LIKE CONCAT('%', SPLIT(r.diagnosis_codes, '\\|')[0], '%')
    )
    THEN 'correct_approve'
    WHEN r.determination = 'denied'
    AND NOT EXISTS (
      SELECT 1 FROM LIVE.silver_medical_policy_rules pr
      WHERE pr.policy_id = r.policy_id
        AND pr.rule_type = 'coverage_criteria'
        AND pr.procedure_codes LIKE CONCAT('%', r.procedure_code, '%')
    )
    THEN 'correct_deny'
    ELSE 'needs_review'
  END AS tier1_accuracy

FROM LIVE.silver_pa_requests r;

-- ---------------------------------------------------------------------------
-- Gold: Clinical Summary Analysis (AI-enriched)
-- ---------------------------------------------------------------------------
-- Uses ai_query() to extract structured clinical facts from the free-text
-- clinical summaries attached to PA requests. Enables criteria matching
-- without manual chart review.
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_pa_clinical_analysis
COMMENT 'AI-extracted clinical facts from PA request summaries. Maps clinical evidence to policy criteria for automated criteria matching.'
TBLPROPERTIES (
  'quality' = 'gold',
  'domain'  = 'prior_auth'
)
AS
SELECT
  auth_request_id,
  member_id,
  service_type,
  procedure_code,
  policy_id,
  determination,
  clinical_summary,

  -- LLM extracts key clinical facts from the summary
  ai_query(
    'databricks-meta-llama-3-3-70b-instruct',
    CONCAT(
      'Extract clinical facts from this prior authorization clinical summary. ',
      'Respond in exactly this format with no other text:\n',
      'DIAGNOSES: [comma-separated ICD-10 codes or condition names mentioned]\n',
      'LAB_VALUES: [any lab results mentioned with values, e.g., HbA1c=8.2]\n',
      'TREATMENTS_TRIED: [prior treatments or medications mentioned]\n',
      'FUNCTIONAL_STATUS: [any functional limitations or severity indicators]\n',
      'CRITERIA_MET: [YES/NO/PARTIAL - does the summary support medical necessity?]\n\n',
      'Clinical Summary: ', COALESCE(clinical_summary, 'No summary provided')
    )
  ) AS clinical_extraction

FROM LIVE.silver_pa_requests
WHERE clinical_summary IS NOT NULL
  AND LENGTH(clinical_summary) > 20
  AND determination IN ('denied', 'pended');
