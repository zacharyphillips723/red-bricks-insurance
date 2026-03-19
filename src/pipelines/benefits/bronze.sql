-- =============================================================================
-- Red Bricks Insurance — Benefits Domain: Bronze Layer
-- =============================================================================
-- Raw ingestion of plan benefit schedules, cost-sharing, actuarial parameters,
-- and utilization assumptions from source volumes. Supports digital twin
-- scenario modeling for underwriting.
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE bronze_benefits
COMMENT 'Raw benefit schedule records with actuarial and utilization assumptions. No cleansing applied.'
TBLPROPERTIES (
  'quality' = 'bronze',
  'domain'  = 'benefits',
  'pipelines.autoOptimize.zOrderCols' = 'benefit_id,plan_id,member_id'
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
  in_network_copay,
  in_network_coinsurance_pct,
  out_of_network_copay,
  out_of_network_coinsurance_pct,
  deductible_applies,
  prior_auth_required,
  visit_limit,
  annual_limit,
  coverage_level,
  -- Plan accumulators
  individual_deductible,
  family_deductible,
  individual_oop_max,
  family_oop_max,
  -- Actuarial / pricing levers
  actuarial_value_pct,
  allowed_amount_schedule,
  network_tier,
  cost_trend_factor,
  pharmacy_trend_factor,
  age_sex_factor,
  -- Utilization modeling
  expected_utilization_per_1000,
  unit_cost_assumption,
  elasticity_factor,
  -- Benefit versioning
  benefit_effective_date,
  benefit_termination_date,
  benefit_version,
  scenario_id,
  is_baseline,
  -- Agent-friendly metadata
  benefit_description,
  clinical_guideline_ref,
  regulatory_mandate,
  -- Ingestion metadata
  _metadata.file_path   AS source_file,
  current_timestamp()   AS ingestion_timestamp
FROM STREAM read_files(
  '${source_volume}/benefits/',
  format => 'parquet'
);
