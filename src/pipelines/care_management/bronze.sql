-- =============================================================================
-- Red Bricks Insurance — Care Management Domain: Bronze Layer
-- =============================================================================
-- Raw ingestion of care management data from source volumes.
-- Covers: programs, program enrollment, case episodes, case activities,
-- case assessments, member SDOH, SDOH referrals, SDOH Z-codes,
-- TOC episodes, TOC follow-up, TOC barriers, care gaps,
-- gap interventions, gap closure events.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Bronze: Care Programs (static reference)
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE bronze_care_programs
COMMENT 'Reference table of disease management programs.'
TBLPROPERTIES (
  'quality' = 'bronze',
  'domain'  = 'care_management'
)
AS
SELECT
  program_id,
  program_name,
  program_type,
  target_conditions,
  milestones_json
FROM STREAM read_files('${source_volume}/care_programs/', format => 'parquet');

-- ---------------------------------------------------------------------------
-- Bronze: Program Enrollment
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE bronze_program_enrollment
COMMENT 'Raw disease management program enrollment records.'
TBLPROPERTIES (
  'quality' = 'bronze',
  'domain'  = 'care_management',
  'pipelines.autoOptimize.zOrderCols' = 'member_id,program_id'
)
AS
SELECT
  enrollment_id,
  member_id,
  program_id,
  enrollment_date,
  disenrollment_date,
  status,
  referral_source,
  enrollment_reason,
  line_of_business
FROM STREAM read_files('${source_volume}/program_enrollment/', format => 'parquet');

-- ---------------------------------------------------------------------------
-- Bronze: Case Management Episodes
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE bronze_case_episodes
COMMENT 'Raw case management episode records.'
TBLPROPERTIES (
  'quality' = 'bronze',
  'domain'  = 'care_management',
  'pipelines.autoOptimize.zOrderCols' = 'case_id,member_id'
)
AS
SELECT
  case_id,
  member_id,
  case_manager_id,
  episode_type,
  acuity,
  open_date,
  close_date,
  close_reason
FROM STREAM read_files('${source_volume}/case_episodes/', format => 'parquet');

-- ---------------------------------------------------------------------------
-- Bronze: Case Activities
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE bronze_case_activities
COMMENT 'Raw timestamped case management activities.'
TBLPROPERTIES (
  'quality' = 'bronze',
  'domain'  = 'care_management'
)
AS
SELECT
  activity_id,
  case_id,
  activity_type,
  activity_date,
  duration_minutes,
  notes
FROM STREAM read_files('${source_volume}/case_activities/', format => 'parquet');

-- ---------------------------------------------------------------------------
-- Bronze: Case Assessments
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE bronze_case_assessments
COMMENT 'Raw structured assessments (PHQ-9, GAD-7, PRAPARE, Fall Risk).'
TBLPROPERTIES (
  'quality' = 'bronze',
  'domain'  = 'care_management'
)
AS
SELECT
  assessment_id,
  case_id,
  assessment_type,
  score,
  risk_level,
  assessment_date
FROM STREAM read_files('${source_volume}/case_assessments/', format => 'parquet');

-- ---------------------------------------------------------------------------
-- Bronze: Member SDOH Screenings
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE bronze_member_sdoh
COMMENT 'Raw SDOH screening results per member.'
TBLPROPERTIES (
  'quality' = 'bronze',
  'domain'  = 'care_management',
  'pipelines.autoOptimize.zOrderCols' = 'member_id'
)
AS
SELECT
  member_id,
  screening_date,
  county,
  food_insecurity_flag,
  housing_instability_flag,
  transportation_barrier_flag,
  social_isolation_flag,
  financial_strain_flag,
  composite_sdoh_risk_score
FROM STREAM read_files('${source_volume}/member_sdoh/', format => 'parquet');

-- ---------------------------------------------------------------------------
-- Bronze: SDOH Referrals
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE bronze_sdoh_referrals
COMMENT 'Raw community resource referrals for SDOH needs.'
TBLPROPERTIES (
  'quality' = 'bronze',
  'domain'  = 'care_management'
)
AS
SELECT
  referral_id,
  member_id,
  referral_type,
  community_resource,
  referral_date,
  status,
  outcome
FROM STREAM read_files('${source_volume}/sdoh_referrals/', format => 'parquet');

-- ---------------------------------------------------------------------------
-- Bronze: SDOH Z-Codes
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE bronze_sdoh_z_codes
COMMENT 'Z-code diagnoses from claims indicating SDOH factors.'
TBLPROPERTIES (
  'quality' = 'bronze',
  'domain'  = 'care_management'
)
AS
SELECT
  member_id,
  z_code,
  z_code_description,
  claim_date
FROM STREAM read_files('${source_volume}/sdoh_z_codes/', format => 'parquet');

-- ---------------------------------------------------------------------------
-- Bronze: Transitions of Care Episodes
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE bronze_toc_episodes
COMMENT 'Raw post-discharge transitions of care episodes.'
TBLPROPERTIES (
  'quality' = 'bronze',
  'domain'  = 'care_management',
  'pipelines.autoOptimize.zOrderCols' = 'toc_id,member_id'
)
AS
SELECT
  toc_id,
  member_id,
  discharge_date,
  discharge_facility,
  discharge_type,
  readmission_risk_score
FROM STREAM read_files('${source_volume}/toc_episodes/', format => 'parquet');

-- ---------------------------------------------------------------------------
-- Bronze: TOC Follow-Up
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE bronze_toc_followup
COMMENT 'Raw post-discharge follow-up tracking (48hr call, 7-day PCP, med rec).'
TBLPROPERTIES (
  'quality' = 'bronze',
  'domain'  = 'care_management'
)
AS
SELECT
  followup_id,
  toc_id,
  followup_type,
  due_date,
  completed_date,
  status
FROM STREAM read_files('${source_volume}/toc_followup/', format => 'parquet');

-- ---------------------------------------------------------------------------
-- Bronze: TOC Barriers
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE bronze_toc_barriers
COMMENT 'Barriers to successful care transitions.'
TBLPROPERTIES (
  'quality' = 'bronze',
  'domain'  = 'care_management'
)
AS
SELECT
  barrier_id,
  toc_id,
  barrier_type,
  description,
  resolved_flag
FROM STREAM read_files('${source_volume}/toc_barriers/', format => 'parquet');

-- ---------------------------------------------------------------------------
-- Bronze: Care Gaps
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE bronze_care_gaps
COMMENT 'Raw open care gaps per member per HEDIS measure.'
TBLPROPERTIES (
  'quality' = 'bronze',
  'domain'  = 'care_management',
  'pipelines.autoOptimize.zOrderCols' = 'gap_id,member_id'
)
AS
SELECT
  gap_id,
  member_id,
  measure_name,
  condition,
  gap_open_date,
  priority
FROM STREAM read_files('${source_volume}/care_gaps/', format => 'parquet');

-- ---------------------------------------------------------------------------
-- Bronze: Gap Interventions
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE bronze_gap_interventions
COMMENT 'Raw outreach attempts to close care gaps.'
TBLPROPERTIES (
  'quality' = 'bronze',
  'domain'  = 'care_management'
)
AS
SELECT
  intervention_id,
  gap_id,
  intervention_type,
  intervention_date,
  outcome
FROM STREAM read_files('${source_volume}/gap_interventions/', format => 'parquet');

-- ---------------------------------------------------------------------------
-- Bronze: Gap Closure Events
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE bronze_gap_closure_events
COMMENT 'Closure records for care gaps that were successfully resolved.'
TBLPROPERTIES (
  'quality' = 'bronze',
  'domain'  = 'care_management'
)
AS
SELECT
  closure_id,
  gap_id,
  closure_date,
  closure_method,
  days_to_close
FROM STREAM read_files('${source_volume}/gap_closure_events/', format => 'parquet');
