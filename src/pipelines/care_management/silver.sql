-- =============================================================================
-- Red Bricks Insurance — Care Management Domain: Silver Layer
-- =============================================================================
-- Cleansed and validated care management data.
-- Date strings cast to DATE; data quality expectations enforce valid ranges.
-- Critical violations drop rows; soft violations are tracked.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Silver: Program Enrollment
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver_program_enrollment (
  CONSTRAINT valid_enrollment_id
    EXPECT (enrollment_id IS NOT NULL AND enrollment_id != '')
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_member_id
    EXPECT (member_id IS NOT NULL AND member_id != '')
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_enrollment_date
    EXPECT (CAST(enrollment_date AS DATE) IS NOT NULL)
    ON VIOLATION DROP ROW,

  CONSTRAINT reasonable_enrollment_date
    EXPECT (CAST(enrollment_date AS DATE) >= '2022-01-01' AND CAST(enrollment_date AS DATE) <= '2026-12-31')
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_status
    EXPECT (status IN ('Active', 'Completed', 'Withdrawn', 'On Hold'))
)
COMMENT 'Cleansed program enrollment with validated dates and member IDs.'
TBLPROPERTIES (
  'quality' = 'silver',
  'domain'  = 'care_management'
)
AS
SELECT
  enrollment_id,
  member_id,
  b.program_id,
  CAST(b.enrollment_date AS DATE)      AS enrollment_date,
  CAST(b.disenrollment_date AS DATE)   AS disenrollment_date,
  b.status,
  b.referral_source,
  b.enrollment_reason,
  b.line_of_business,
  -- Enrichment from program reference
  p.program_name,
  p.program_type
FROM STREAM(LIVE.bronze_program_enrollment) b
LEFT JOIN LIVE.bronze_care_programs p
  ON b.program_id = p.program_id;

-- ---------------------------------------------------------------------------
-- Silver: Case Episodes
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver_case_episodes (
  CONSTRAINT valid_case_id
    EXPECT (case_id IS NOT NULL AND case_id != '')
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_member_id
    EXPECT (member_id IS NOT NULL AND member_id != '')
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_open_date
    EXPECT (CAST(open_date AS DATE) IS NOT NULL)
    ON VIOLATION DROP ROW,

  CONSTRAINT reasonable_open_date
    EXPECT (CAST(open_date AS DATE) >= '2022-01-01' AND CAST(open_date AS DATE) <= '2026-12-31')
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_acuity
    EXPECT (acuity IN ('Low', 'Moderate', 'High', 'Critical'))
)
COMMENT 'Cleansed case management episodes with validated dates and acuity levels.'
TBLPROPERTIES (
  'quality' = 'silver',
  'domain'  = 'care_management'
)
AS
SELECT
  case_id,
  member_id,
  case_manager_id,
  episode_type,
  acuity,
  CAST(open_date AS DATE)   AS open_date,
  CAST(close_date AS DATE)  AS close_date,
  close_reason,
  DATEDIFF(COALESCE(CAST(close_date AS DATE), CURRENT_DATE()), CAST(open_date AS DATE)) AS case_duration_days
FROM STREAM(LIVE.bronze_case_episodes);

-- ---------------------------------------------------------------------------
-- Silver: Case Activities
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver_case_activities (
  CONSTRAINT valid_activity_id
    EXPECT (activity_id IS NOT NULL)
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_case_id
    EXPECT (case_id IS NOT NULL)
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_activity_date
    EXPECT (CAST(activity_date AS DATE) IS NOT NULL)
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_duration
    EXPECT (duration_minutes > 0 AND duration_minutes <= 480)
)
COMMENT 'Cleansed case activities with validated dates and durations.'
TBLPROPERTIES (
  'quality' = 'silver',
  'domain'  = 'care_management'
)
AS
SELECT
  activity_id,
  case_id,
  activity_type,
  CAST(activity_date AS DATE) AS activity_date,
  duration_minutes,
  notes
FROM STREAM(LIVE.bronze_case_activities);

-- ---------------------------------------------------------------------------
-- Silver: Case Assessments
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver_case_assessments (
  CONSTRAINT valid_assessment_id
    EXPECT (assessment_id IS NOT NULL AND assessment_id != '')
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_case_id
    EXPECT (case_id IS NOT NULL)
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_assessment_date
    EXPECT (CAST(assessment_date AS DATE) IS NOT NULL)
    ON VIOLATION DROP ROW,

  -- Score range validation per assessment type
  CONSTRAINT valid_phq9_score
    EXPECT (assessment_type != 'PHQ-9' OR (score >= 0 AND score <= 27)),

  CONSTRAINT valid_gad7_score
    EXPECT (assessment_type != 'GAD-7' OR (score >= 0 AND score <= 21)),

  CONSTRAINT valid_prapare_score
    EXPECT (assessment_type != 'PRAPARE' OR (score >= 0 AND score <= 20)),

  CONSTRAINT valid_fall_risk_score
    EXPECT (assessment_type != 'Fall Risk' OR (score >= 0 AND score <= 10))
)
COMMENT 'Cleansed assessments with score range validation per assessment type.'
TBLPROPERTIES (
  'quality' = 'silver',
  'domain'  = 'care_management'
)
AS
SELECT
  assessment_id,
  case_id,
  assessment_type,
  CAST(score AS INT)                   AS score,
  risk_level,
  CAST(assessment_date AS DATE)        AS assessment_date
FROM STREAM(LIVE.bronze_case_assessments);

-- ---------------------------------------------------------------------------
-- Silver: Member SDOH
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver_member_sdoh (
  CONSTRAINT valid_member_id
    EXPECT (member_id IS NOT NULL AND member_id != '')
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_screening_date
    EXPECT (CAST(screening_date AS DATE) IS NOT NULL)
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_composite_score
    EXPECT (composite_sdoh_risk_score >= 0 AND composite_sdoh_risk_score <= 10)
)
COMMENT 'Cleansed SDOH screening results with validated scores and dates.'
TBLPROPERTIES (
  'quality' = 'silver',
  'domain'  = 'care_management'
)
AS
SELECT
  member_id,
  CAST(screening_date AS DATE)          AS screening_date,
  county,
  CAST(food_insecurity_flag AS BOOLEAN)        AS food_insecurity_flag,
  CAST(housing_instability_flag AS BOOLEAN)    AS housing_instability_flag,
  CAST(transportation_barrier_flag AS BOOLEAN) AS transportation_barrier_flag,
  CAST(social_isolation_flag AS BOOLEAN)       AS social_isolation_flag,
  CAST(financial_strain_flag AS BOOLEAN)       AS financial_strain_flag,
  ROUND(CAST(composite_sdoh_risk_score AS DOUBLE), 1) AS composite_sdoh_risk_score,
  -- Computed: total number of active SDOH flags
  (CASE WHEN food_insecurity_flag THEN 1 ELSE 0 END
   + CASE WHEN housing_instability_flag THEN 1 ELSE 0 END
   + CASE WHEN transportation_barrier_flag THEN 1 ELSE 0 END
   + CASE WHEN social_isolation_flag THEN 1 ELSE 0 END
   + CASE WHEN financial_strain_flag THEN 1 ELSE 0 END
  ) AS total_sdoh_flags
FROM STREAM(LIVE.bronze_member_sdoh);

-- ---------------------------------------------------------------------------
-- Silver: SDOH Referrals
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver_sdoh_referrals (
  CONSTRAINT valid_referral_id
    EXPECT (referral_id IS NOT NULL)
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_member_id
    EXPECT (member_id IS NOT NULL)
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_referral_date
    EXPECT (CAST(referral_date AS DATE) IS NOT NULL)
    ON VIOLATION DROP ROW
)
COMMENT 'Cleansed SDOH community resource referrals.'
TBLPROPERTIES (
  'quality' = 'silver',
  'domain'  = 'care_management'
)
AS
SELECT
  referral_id,
  member_id,
  referral_type,
  community_resource,
  CAST(referral_date AS DATE) AS referral_date,
  status,
  outcome
FROM STREAM(LIVE.bronze_sdoh_referrals);

-- ---------------------------------------------------------------------------
-- Silver: TOC Episodes
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver_toc_episodes (
  CONSTRAINT valid_toc_id
    EXPECT (toc_id IS NOT NULL AND toc_id != '')
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_member_id
    EXPECT (member_id IS NOT NULL AND member_id != '')
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_discharge_date
    EXPECT (CAST(discharge_date AS DATE) IS NOT NULL)
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_risk_score
    EXPECT (readmission_risk_score >= 0 AND readmission_risk_score <= 1)
)
COMMENT 'Cleansed transitions of care episodes with validated discharge dates and risk scores.'
TBLPROPERTIES (
  'quality' = 'silver',
  'domain'  = 'care_management'
)
AS
SELECT
  toc_id,
  member_id,
  CAST(discharge_date AS DATE)      AS discharge_date,
  discharge_facility,
  discharge_type,
  ROUND(CAST(readmission_risk_score AS DOUBLE), 2) AS readmission_risk_score,
  CASE
    WHEN readmission_risk_score >= 0.5 THEN 'High'
    WHEN readmission_risk_score >= 0.3 THEN 'Moderate'
    ELSE 'Low'
  END AS readmission_risk_tier
FROM STREAM(LIVE.bronze_toc_episodes);

-- ---------------------------------------------------------------------------
-- Silver: TOC Follow-Up
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver_toc_followup (
  CONSTRAINT valid_followup_id
    EXPECT (followup_id IS NOT NULL)
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_toc_id
    EXPECT (toc_id IS NOT NULL)
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_due_date
    EXPECT (CAST(due_date AS DATE) IS NOT NULL)
    ON VIOLATION DROP ROW
)
COMMENT 'Cleansed post-discharge follow-up tracking records.'
TBLPROPERTIES (
  'quality' = 'silver',
  'domain'  = 'care_management'
)
AS
SELECT
  followup_id,
  toc_id,
  followup_type,
  CAST(due_date AS DATE)        AS due_date,
  CAST(completed_date AS DATE)  AS completed_date,
  status,
  CASE WHEN completed_date IS NOT NULL
    THEN DATEDIFF(CAST(completed_date AS DATE), CAST(due_date AS DATE))
    ELSE NULL
  END AS days_from_due
FROM STREAM(LIVE.bronze_toc_followup);

-- ---------------------------------------------------------------------------
-- Silver: Care Gaps
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver_care_gaps (
  CONSTRAINT valid_gap_id
    EXPECT (gap_id IS NOT NULL AND gap_id != '')
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_member_id
    EXPECT (member_id IS NOT NULL AND member_id != '')
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_gap_open_date
    EXPECT (CAST(gap_open_date AS DATE) IS NOT NULL)
    ON VIOLATION DROP ROW,

  CONSTRAINT reasonable_gap_date
    EXPECT (CAST(gap_open_date AS DATE) >= '2022-01-01' AND CAST(gap_open_date AS DATE) <= '2026-12-31')
    ON VIOLATION DROP ROW
)
COMMENT 'Cleansed care gap records with validated dates.'
TBLPROPERTIES (
  'quality' = 'silver',
  'domain'  = 'care_management'
)
AS
SELECT
  gap_id,
  member_id,
  measure_name,
  condition,
  CAST(gap_open_date AS DATE) AS gap_open_date,
  priority,
  DATEDIFF(CURRENT_DATE(), CAST(gap_open_date AS DATE)) AS gap_age_days
FROM STREAM(LIVE.bronze_care_gaps);

-- ---------------------------------------------------------------------------
-- Silver: Gap Interventions
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver_gap_interventions (
  CONSTRAINT valid_intervention_id
    EXPECT (intervention_id IS NOT NULL)
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_gap_id
    EXPECT (gap_id IS NOT NULL)
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_intervention_date
    EXPECT (CAST(intervention_date AS DATE) IS NOT NULL)
    ON VIOLATION DROP ROW
)
COMMENT 'Cleansed gap closure intervention attempts.'
TBLPROPERTIES (
  'quality' = 'silver',
  'domain'  = 'care_management'
)
AS
SELECT
  intervention_id,
  gap_id,
  intervention_type,
  CAST(intervention_date AS DATE) AS intervention_date,
  outcome
FROM STREAM(LIVE.bronze_gap_interventions);

-- ---------------------------------------------------------------------------
-- Silver: Gap Closure Events
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver_gap_closure_events (
  CONSTRAINT valid_closure_id
    EXPECT (closure_id IS NOT NULL)
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_gap_id
    EXPECT (gap_id IS NOT NULL)
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_closure_date
    EXPECT (CAST(closure_date AS DATE) IS NOT NULL)
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_days_to_close
    EXPECT (days_to_close >= 0)
)
COMMENT 'Cleansed care gap closure events.'
TBLPROPERTIES (
  'quality' = 'silver',
  'domain'  = 'care_management'
)
AS
SELECT
  closure_id,
  gap_id,
  CAST(closure_date AS DATE) AS closure_date,
  closure_method,
  CAST(days_to_close AS INT) AS days_to_close
FROM STREAM(LIVE.bronze_gap_closure_events);
