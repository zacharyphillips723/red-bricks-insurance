-- =============================================================================
-- Red Bricks Insurance — Gold Analytics: AI-Powered Classification
-- =============================================================================
-- Uses ai_query() with Databricks foundation model endpoints for intelligent
-- classification and narrative generation. Demonstrates GenAI integration
-- directly within SDP/DLT pipelines.
-- Pipeline: gold_analytics (runs separately from domain pipelines)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- gold_denial_classification — AI-Classified Denial Reason Categories
-- -----------------------------------------------------------------------------
-- Uses a foundation model to classify raw denial reason codes into actionable
-- categories (Administrative, Clinical, Eligibility, Financial). This enables
-- downstream analytics and denial management workflows.
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_denial_classification
COMMENT 'AI-classified denial reason categories using Databricks foundation model. Maps raw denial codes to actionable categories for denial management.'
AS
WITH distinct_denials AS (
  SELECT DISTINCT denial_reason_code
  FROM ${catalog}.${schema}.silver_claims_medical
  WHERE denial_reason_code IS NOT NULL
)
SELECT
  denial_reason_code,
  ai_query(
    'databricks-meta-llama-3-3-70b-instruct',
    CONCAT(
      'You are a healthcare claims expert. Classify this claim denial reason code into exactly one category: Administrative, Clinical, Eligibility, or Financial. ',
      'Code: ', denial_reason_code,
      '. Respond with only the category name, nothing else.'
    )
  ) AS denial_category
FROM distinct_denials;

-- -----------------------------------------------------------------------------
-- gold_denial_analysis — Denial Trends by AI-Classified Category
-- -----------------------------------------------------------------------------
-- Joins AI classification back to claims for volumetric and financial analysis
-- of denials by category, claim type, and line of business.
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_denial_analysis
COMMENT 'Denial analysis by AI-classified category, claim type, and line of business. Shows denial volumes, financial impact, and distribution for denial management.'
AS
WITH denial_claims AS (
  SELECT
    c.claim_id,
    c.claim_type,
    c.billed_amount,
    c.denial_reason_code,
    dc.denial_category,
    e.line_of_business
  FROM ${catalog}.${schema}.silver_claims_medical c
  INNER JOIN gold_denial_classification dc
    ON c.denial_reason_code = dc.denial_reason_code
  LEFT JOIN ${catalog}.${schema}.silver_enrollment e
    ON c.member_id = e.member_id
  WHERE c.denial_reason_code IS NOT NULL
),

totals AS (
  SELECT COUNT(*) AS total_denials
  FROM denial_claims
)

SELECT
  d.denial_category,
  d.claim_type,
  d.line_of_business,
  COUNT(*)                                             AS denial_count,
  SUM(d.billed_amount)                                 AS total_denied_amount,
  AVG(d.billed_amount)                                 AS avg_denied_amount,
  CAST(COUNT(*) AS DOUBLE) / NULLIF(t.total_denials, 0) AS pct_of_total_denials
FROM denial_claims d
CROSS JOIN totals t
GROUP BY
  d.denial_category,
  d.claim_type,
  d.line_of_business,
  t.total_denials;

-- -----------------------------------------------------------------------------
-- gold_member_risk_narrative — AI-Generated Clinical Risk Summaries
-- -----------------------------------------------------------------------------
-- For the top 500 highest-risk members (by RAF score), generates a brief
-- clinical summary suitable for care management triage and outreach planning.
-- Demonstrates GenAI applied to population health use cases.
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_member_risk_narrative
COMMENT 'AI-generated clinical risk narratives for top 500 high-risk members. Provides care management-ready summaries based on RAF scores, HCC codes, and line of business.'
AS
WITH high_risk_members AS (
  SELECT
    ram.member_id,
    ram.raf_score,
    ram.hcc_codes,
    ram.hcc_count,
    e.line_of_business,
    ROW_NUMBER() OVER (ORDER BY ram.raf_score DESC) AS risk_rank
  FROM ${catalog}.${schema}.silver_risk_adjustment_member ram
  INNER JOIN ${catalog}.${schema}.silver_enrollment e
    ON ram.member_id = e.member_id
)
SELECT
  member_id,
  raf_score,
  hcc_codes,
  hcc_count,
  line_of_business,
  risk_rank,
  ai_query(
    'databricks-meta-llama-3-3-70b-instruct',
    CONCAT(
      'You are a care management analyst. Given this health plan member profile, write a 2-sentence clinical summary for care coordination. ',
      'RAF Score: ', CAST(raf_score AS STRING),
      ', HCC Codes: ', COALESCE(hcc_codes, 'None'),
      ', HCC Count: ', CAST(hcc_count AS STRING),
      ', Line of Business: ', line_of_business,
      '. Focus on key risk factors and recommended interventions.'
    )
  ) AS clinical_summary
FROM high_risk_members
WHERE risk_rank <= 500;
