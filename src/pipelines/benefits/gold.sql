-- =============================================================================
-- Red Bricks Insurance — Benefits Domain: Gold Layer
-- =============================================================================
-- Business-ready benefit summaries:
--   1. gold_member_benefits_summary — one row per member plan summary
--   2. gold_member_benefit_utilization — Tier 2 digital twin: joins benefits
--      with actual claims to produce baseline vs. projected cost by category
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Gold: Member Benefits Summary (plan structure overview)
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_member_benefits_summary
COMMENT 'One-row-per-member summary of plan benefits: deductibles, OOP max, actuarial value, benefit category counts, and prior auth requirements.'
TBLPROPERTIES (
  'quality' = 'gold',
  'domain'  = 'benefits'
)
AS
SELECT
  member_id,
  plan_id,
  line_of_business,
  plan_type,
  coverage_level,
  individual_deductible,
  family_deductible,
  individual_oop_max,
  family_oop_max,
  actuarial_value_pct,
  network_tier,
  allowed_amount_schedule,
  cost_trend_factor,
  pharmacy_trend_factor,
  COUNT(DISTINCT benefit_category) AS benefit_categories_covered,
  COUNT(*) AS total_benefits,
  SUM(CASE WHEN prior_auth_required THEN 1 ELSE 0 END) AS prior_auth_benefit_count,
  SUM(CASE WHEN visit_limit IS NOT NULL THEN 1 ELSE 0 END) AS visit_limited_benefit_count,
  SUM(CASE WHEN regulatory_mandate IS NOT NULL THEN 1 ELSE 0 END) AS mandated_benefit_count,
  -- Coverage flags
  MAX(CASE WHEN benefit_category = 'Behavioral Health' THEN 1 ELSE 0 END) AS has_behavioral_health,
  MAX(CASE WHEN benefit_category = 'Vision' THEN 1 ELSE 0 END) AS has_vision,
  MAX(CASE WHEN benefit_category = 'Dental' THEN 1 ELSE 0 END) AS has_dental,
  MAX(CASE WHEN benefit_category = 'Pharmacy' THEN 1 ELSE 0 END) AS has_pharmacy,
  -- Average in-network cost-sharing
  ROUND(AVG(in_network_copay), 2) AS avg_in_network_copay,
  ROUND(AVG(in_network_coinsurance_pct), 1) AS avg_in_network_coinsurance_pct,
  -- Projected annual plan cost (sum of expected_util/1000 * unit_cost across all benefits)
  ROUND(SUM(expected_utilization_per_1000 / 1000.0 * unit_cost_assumption), 2) AS projected_annual_cost_per_member
FROM LIVE.silver_benefits
WHERE is_baseline = TRUE
GROUP BY
  member_id, plan_id, line_of_business, plan_type, coverage_level,
  individual_deductible, family_deductible, individual_oop_max, family_oop_max,
  actuarial_value_pct, network_tier, allowed_amount_schedule,
  cost_trend_factor, pharmacy_trend_factor;

-- ---------------------------------------------------------------------------
-- Gold: Member Benefit Utilization — Tier 2 Digital Twin
-- Joins benefit design with actual claims to produce per-category baseline
-- utilization, actual spend, and projected next-year cost.
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_member_benefit_utilization
COMMENT 'Per-member, per-benefit-category utilization baseline joining benefit design parameters with actual claims data. Powers the digital twin scenario modeling.'
TBLPROPERTIES (
  'quality' = 'gold',
  'domain'  = 'benefits'
)
AS
WITH

-- Aggregate medical claims by member and map to benefit categories
medical_by_category AS (
  SELECT
    member_id,
    CASE
      WHEN claim_type = 'Institutional_IP' THEN 'Medical - Inpatient'
      WHEN claim_type = 'ER'               THEN 'Medical - Outpatient'
      WHEN claim_type IN ('Professional', 'Institutional_OP') THEN 'Medical - Outpatient'
      ELSE 'Medical - Outpatient'
    END AS benefit_category,
    COUNT(DISTINCT claim_id)   AS actual_claim_count,
    SUM(paid_amount)           AS actual_paid_amount,
    SUM(billed_amount)         AS actual_billed_amount,
    SUM(member_responsibility) AS actual_member_responsibility
  FROM ${catalog}.claims.silver_claims_medical
  GROUP BY member_id,
    CASE
      WHEN claim_type = 'Institutional_IP' THEN 'Medical - Inpatient'
      WHEN claim_type = 'ER'               THEN 'Medical - Outpatient'
      WHEN claim_type IN ('Professional', 'Institutional_OP') THEN 'Medical - Outpatient'
      ELSE 'Medical - Outpatient'
    END
),

-- Aggregate pharmacy claims
pharmacy_agg AS (
  SELECT
    member_id,
    'Pharmacy' AS benefit_category,
    COUNT(DISTINCT claim_id)  AS actual_claim_count,
    SUM(plan_paid)            AS actual_paid_amount,
    SUM(total_cost)           AS actual_billed_amount,
    SUM(member_copay)         AS actual_member_responsibility
  FROM ${catalog}.claims.silver_claims_pharmacy
  GROUP BY member_id
),

-- Union medical + pharmacy claims
all_claims AS (
  SELECT * FROM medical_by_category
  UNION ALL
  SELECT * FROM pharmacy_agg
),

-- Aggregate benefit design params by member + category
benefit_design AS (
  SELECT
    member_id,
    plan_id,
    benefit_category,
    line_of_business,
    plan_type,
    -- Plan-level accumulators (same across all benefits for a member)
    MAX(individual_deductible) AS individual_deductible,
    MAX(individual_oop_max)    AS individual_oop_max,
    MAX(actuarial_value_pct)   AS actuarial_value_pct,
    MAX(cost_trend_factor)     AS cost_trend_factor,
    MAX(pharmacy_trend_factor) AS pharmacy_trend_factor,
    MAX(age_sex_factor)        AS age_sex_factor,
    MAX(network_tier)          AS network_tier,
    -- Category-level design params
    ROUND(AVG(in_network_copay), 2)              AS avg_copay,
    ROUND(AVG(in_network_coinsurance_pct), 1)    AS avg_coinsurance_pct,
    ROUND(AVG(elasticity_factor), 3)             AS avg_elasticity,
    ROUND(SUM(expected_utilization_per_1000 / 1000.0 * unit_cost_assumption), 2) AS expected_cost_per_member,
    COUNT(*) AS benefit_count
  FROM LIVE.silver_benefits
  WHERE is_baseline = TRUE
  GROUP BY member_id, plan_id, benefit_category, line_of_business, plan_type
),

-- Risk adjustment context
risk AS (
  SELECT *
  FROM (
    SELECT
      member_id,
      raf_score,
      hcc_codes,
      is_high_risk,
      ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY raf_score DESC) AS rn
    FROM ${catalog}.risk_adjustment.silver_risk_adjustment_member
  )
  WHERE rn = 1
)

SELECT
  bd.member_id,
  bd.plan_id,
  bd.benefit_category,
  bd.line_of_business,
  bd.plan_type,
  bd.network_tier,
  bd.actuarial_value_pct,
  bd.age_sex_factor,
  -- Plan accumulators
  bd.individual_deductible,
  bd.individual_oop_max,
  -- Benefit design
  bd.avg_copay,
  bd.avg_coinsurance_pct,
  bd.avg_elasticity,
  bd.expected_cost_per_member,
  bd.benefit_count,
  -- Actual claims (Tier 2: historical baseline)
  COALESCE(ac.actual_claim_count, 0)              AS actual_claim_count,
  COALESCE(ac.actual_paid_amount, 0)              AS actual_paid_amount,
  COALESCE(ac.actual_billed_amount, 0)            AS actual_billed_amount,
  COALESCE(ac.actual_member_responsibility, 0)    AS actual_member_responsibility,
  -- Utilization rate (actual vs expected)
  CASE
    WHEN bd.expected_cost_per_member > 0
    THEN ROUND(COALESCE(ac.actual_paid_amount, 0) / bd.expected_cost_per_member, 3)
    ELSE NULL
  END AS utilization_ratio,
  -- Risk context
  COALESCE(r.raf_score, 0)   AS raf_score,
  r.hcc_codes,
  COALESCE(r.is_high_risk, FALSE) AS is_high_risk,
  -- Risk pool segmentation (Tier 2)
  CASE
    WHEN COALESCE(ac.actual_paid_amount, 0) > 50000 THEN 'Catastrophic'
    WHEN COALESCE(r.raf_score, 0) > 2.5             THEN 'Complex'
    WHEN COALESCE(ac.actual_paid_amount, 0) > 10000  THEN 'Moderate'
    ELSE 'Healthy'
  END AS risk_pool_segment,
  -- Projected next-year cost (actual × trend factor)
  ROUND(
    COALESCE(ac.actual_paid_amount, 0) *
    CASE
      WHEN bd.benefit_category = 'Pharmacy' THEN bd.pharmacy_trend_factor
      ELSE bd.cost_trend_factor
    END,
    2
  ) AS projected_next_year_cost,
  -- Accumulator YTD (Tier 2: derived from actual claims)
  COALESCE(ac.actual_member_responsibility, 0) AS accumulator_ytd_member_cost,
  LEAST(
    COALESCE(ac.actual_member_responsibility, 0),
    bd.individual_oop_max
  ) AS accumulator_ytd_toward_oop_max,
  CASE
    WHEN COALESCE(ac.actual_member_responsibility, 0) >= bd.individual_oop_max
    THEN TRUE ELSE FALSE
  END AS oop_max_reached

FROM benefit_design bd
LEFT JOIN all_claims ac ON bd.member_id = ac.member_id AND bd.benefit_category = ac.benefit_category
LEFT JOIN risk r ON bd.member_id = r.member_id;
