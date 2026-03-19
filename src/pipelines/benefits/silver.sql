-- =============================================================================
-- Red Bricks Insurance — Benefits Domain: Silver Layer
-- =============================================================================
-- Cleansed benefit schedules with validated cost-sharing, actuarial parameters,
-- and utilization assumptions. Quality expectations enforce data contracts.
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE silver_benefits (
  CONSTRAINT valid_benefit_id_not_null
    EXPECT (benefit_id IS NOT NULL)
    ON VIOLATION DROP ROW,
  CONSTRAINT valid_plan_id_not_null
    EXPECT (plan_id IS NOT NULL)
    ON VIOLATION DROP ROW,
  CONSTRAINT valid_member_id_not_null
    EXPECT (member_id IS NOT NULL)
    ON VIOLATION DROP ROW,
  CONSTRAINT valid_copay_non_negative
    EXPECT (in_network_copay >= 0),
  CONSTRAINT valid_coinsurance_range
    EXPECT (in_network_coinsurance_pct >= 0 AND in_network_coinsurance_pct <= 100),
  CONSTRAINT valid_oon_coinsurance_range
    EXPECT (out_of_network_coinsurance_pct >= 0 AND out_of_network_coinsurance_pct <= 100),
  CONSTRAINT valid_deductible_non_negative
    EXPECT (individual_deductible >= 0),
  CONSTRAINT valid_oop_max_non_negative
    EXPECT (individual_oop_max >= 0),
  CONSTRAINT valid_actuarial_value
    EXPECT (actuarial_value_pct >= 50 AND actuarial_value_pct <= 100),
  CONSTRAINT valid_trend_factor
    EXPECT (cost_trend_factor >= 1.0 AND cost_trend_factor <= 1.25),
  CONSTRAINT valid_elasticity
    EXPECT (elasticity_factor <= 0),
  CONSTRAINT valid_utilization
    EXPECT (expected_utilization_per_1000 >= 0),
  CONSTRAINT valid_unit_cost
    EXPECT (unit_cost_assumption >= 0)
)
COMMENT 'Cleansed benefit schedule with validated cost-sharing, actuarial parameters, and utilization assumptions for digital twin modeling.'
TBLPROPERTIES (
  'quality' = 'silver',
  'domain'  = 'benefits'
)
AS
SELECT
  benefit_id,
  plan_id,
  member_id,
  line_of_business,
  plan_type,
  benefit_category,
  benefit_name,
  benefit_code,
  -- Cost-sharing
  CAST(in_network_copay AS DOUBLE)                AS in_network_copay,
  CAST(in_network_coinsurance_pct AS INT)          AS in_network_coinsurance_pct,
  CAST(out_of_network_copay AS DOUBLE)             AS out_of_network_copay,
  CAST(out_of_network_coinsurance_pct AS INT)      AS out_of_network_coinsurance_pct,
  CAST(deductible_applies AS BOOLEAN)              AS deductible_applies,
  CAST(prior_auth_required AS BOOLEAN)             AS prior_auth_required,
  CAST(visit_limit AS INT)                         AS visit_limit,
  CAST(annual_limit AS DOUBLE)                     AS annual_limit,
  coverage_level,
  -- Plan accumulators
  CAST(individual_deductible AS DOUBLE)            AS individual_deductible,
  CAST(family_deductible AS DOUBLE)                AS family_deductible,
  CAST(individual_oop_max AS DOUBLE)               AS individual_oop_max,
  CAST(family_oop_max AS DOUBLE)                   AS family_oop_max,
  -- Actuarial / pricing levers
  CAST(actuarial_value_pct AS INT)                 AS actuarial_value_pct,
  allowed_amount_schedule,
  network_tier,
  CAST(cost_trend_factor AS DOUBLE)                AS cost_trend_factor,
  CAST(pharmacy_trend_factor AS DOUBLE)            AS pharmacy_trend_factor,
  CAST(age_sex_factor AS DOUBLE)                   AS age_sex_factor,
  -- Utilization modeling
  CAST(expected_utilization_per_1000 AS DOUBLE)    AS expected_utilization_per_1000,
  CAST(unit_cost_assumption AS DOUBLE)             AS unit_cost_assumption,
  CAST(elasticity_factor AS DOUBLE)                AS elasticity_factor,
  -- Benefit versioning
  CAST(benefit_effective_date AS DATE)             AS benefit_effective_date,
  CAST(benefit_termination_date AS DATE)           AS benefit_termination_date,
  CAST(benefit_version AS INT)                     AS benefit_version,
  scenario_id,
  CAST(is_baseline AS BOOLEAN)                     AS is_baseline,
  -- Agent-friendly metadata
  benefit_description,
  clinical_guideline_ref,
  regulatory_mandate,
  -- Audit
  source_file,
  ingestion_timestamp
FROM STREAM(LIVE.bronze_benefits);
