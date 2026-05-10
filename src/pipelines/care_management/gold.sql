-- =============================================================================
-- Red Bricks Insurance — Care Management Domain: Gold Layer
-- =============================================================================
-- Business-ready aggregated views for care management analytics.
-- Materialized views refresh automatically when upstream silver tables update.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Gold: Program Performance
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_program_performance
COMMENT 'Program enrollment counts, completion rates, and average time to milestone by program and LOB.'
TBLPROPERTIES (
  'quality' = 'gold',
  'domain'  = 'care_management'
)
AS
SELECT
  e.program_name,
  e.program_type,
  e.line_of_business,

  COUNT(DISTINCT e.enrollment_id)                   AS total_enrollments,
  COUNT(DISTINCT e.member_id)                       AS unique_members,

  COUNT(DISTINCT CASE WHEN e.status = 'Active' THEN e.enrollment_id END)
                                                    AS active_enrollments,
  COUNT(DISTINCT CASE WHEN e.status = 'Completed' THEN e.enrollment_id END)
                                                    AS completed_enrollments,
  COUNT(DISTINCT CASE WHEN e.status = 'Withdrawn' THEN e.enrollment_id END)
                                                    AS withdrawn_enrollments,

  ROUND(
    COUNT(DISTINCT CASE WHEN e.status = 'Completed' THEN e.enrollment_id END)
    * 100.0 / NULLIF(COUNT(DISTINCT e.enrollment_id), 0), 1
  )                                                 AS completion_rate_pct,

  ROUND(AVG(
    CASE WHEN e.disenrollment_date IS NOT NULL
      THEN DATEDIFF(e.disenrollment_date, e.enrollment_date)
    END
  ), 0)                                             AS avg_enrollment_days

FROM LIVE.silver_program_enrollment e
GROUP BY
  e.program_name,
  e.program_type,
  e.line_of_business;

-- ---------------------------------------------------------------------------
-- Gold: Program Outcomes (pre/post metrics)
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_program_outcomes
COMMENT 'Pre/post clinical outcomes by program. Compares assessment scores before and after enrollment.'
TBLPROPERTIES (
  'quality' = 'gold',
  'domain'  = 'care_management'
)
AS
SELECT
  e.program_name,
  e.line_of_business,
  a.assessment_type,

  COUNT(DISTINCT e.member_id)       AS members_assessed,
  ROUND(AVG(a.score), 1)           AS avg_score,
  ROUND(MIN(a.score), 1)           AS min_score,
  ROUND(MAX(a.score), 1)           AS max_score,

  -- Distribution of risk levels
  COUNT(CASE WHEN a.risk_level = 'Low' THEN 1 END)              AS low_risk_count,
  COUNT(CASE WHEN a.risk_level = 'Moderate' THEN 1 END)         AS moderate_risk_count,
  COUNT(CASE WHEN a.risk_level IN ('High', 'Severe', 'Moderately Severe') THEN 1 END)
                                                                  AS high_risk_count

FROM LIVE.silver_program_enrollment e
INNER JOIN LIVE.silver_case_episodes c
  ON e.member_id = c.member_id
INNER JOIN LIVE.silver_case_assessments a
  ON c.case_id = a.case_id
GROUP BY
  e.program_name,
  e.line_of_business,
  a.assessment_type;

-- ---------------------------------------------------------------------------
-- Gold: Case Manager Productivity
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_case_manager_productivity
COMMENT 'Cases per case manager, avg case duration, activities per case, assessment completion rate.'
TBLPROPERTIES (
  'quality' = 'gold',
  'domain'  = 'care_management'
)
AS
SELECT
  c.case_manager_id,

  COUNT(DISTINCT c.case_id)                         AS total_cases,
  COUNT(DISTINCT CASE WHEN c.close_date IS NULL THEN c.case_id END)
                                                    AS open_cases,
  COUNT(DISTINCT CASE WHEN c.close_date IS NOT NULL THEN c.case_id END)
                                                    AS closed_cases,

  ROUND(AVG(c.case_duration_days), 0)              AS avg_case_duration_days,

  -- Activity metrics
  ROUND(COUNT(DISTINCT act.activity_id) * 1.0
    / NULLIF(COUNT(DISTINCT c.case_id), 0), 1)     AS avg_activities_per_case,
  ROUND(AVG(act.duration_minutes), 0)              AS avg_activity_minutes,

  -- Assessment completion
  COUNT(DISTINCT asmt.assessment_id)               AS total_assessments,
  ROUND(
    COUNT(DISTINCT CASE WHEN asmt.assessment_id IS NOT NULL THEN c.case_id END)
    * 100.0 / NULLIF(COUNT(DISTINCT c.case_id), 0), 1
  )                                                 AS assessment_completion_rate_pct

FROM LIVE.silver_case_episodes c
LEFT JOIN LIVE.silver_case_activities act
  ON c.case_id = act.case_id
LEFT JOIN LIVE.silver_case_assessments asmt
  ON c.case_id = asmt.case_id
GROUP BY
  c.case_manager_id;

-- ---------------------------------------------------------------------------
-- Gold: SDOH Prevalence
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_sdoh_prevalence
COMMENT 'SDOH factor prevalence rates by county, LOB, and age band.'
TBLPROPERTIES (
  'quality' = 'gold',
  'domain'  = 'care_management'
)
AS
SELECT
  s.county,

  COUNT(DISTINCT s.member_id)                       AS members_screened,

  ROUND(AVG(CAST(s.food_insecurity_flag AS INT)) * 100, 1)
                                                    AS food_insecurity_pct,
  ROUND(AVG(CAST(s.housing_instability_flag AS INT)) * 100, 1)
                                                    AS housing_instability_pct,
  ROUND(AVG(CAST(s.transportation_barrier_flag AS INT)) * 100, 1)
                                                    AS transportation_barrier_pct,
  ROUND(AVG(CAST(s.social_isolation_flag AS INT)) * 100, 1)
                                                    AS social_isolation_pct,
  ROUND(AVG(CAST(s.financial_strain_flag AS INT)) * 100, 1)
                                                    AS financial_strain_pct,

  ROUND(AVG(s.composite_sdoh_risk_score), 2)       AS avg_composite_score,
  ROUND(AVG(s.total_sdoh_flags), 1)                AS avg_flags_per_member

FROM LIVE.silver_member_sdoh s
GROUP BY
  s.county;

-- ---------------------------------------------------------------------------
-- Gold: SDOH Cost Impact
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_sdoh_cost_impact
COMMENT 'Cost differential for members with SDOH flags vs without. Requires join to claims.'
TBLPROPERTIES (
  'quality' = 'gold',
  'domain'  = 'care_management'
)
AS
SELECT
  CASE WHEN s.total_sdoh_flags > 0 THEN 'Has SDOH Flags' ELSE 'No SDOH Flags' END
                                                    AS sdoh_status,
  s.total_sdoh_flags,
  COUNT(DISTINCT s.member_id)                       AS member_count,
  ROUND(AVG(s.composite_sdoh_risk_score), 2)       AS avg_composite_score

FROM LIVE.silver_member_sdoh s
GROUP BY
  CASE WHEN s.total_sdoh_flags > 0 THEN 'Has SDOH Flags' ELSE 'No SDOH Flags' END,
  s.total_sdoh_flags;

-- ---------------------------------------------------------------------------
-- Gold: SDOH Referral Outcomes
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_sdoh_referral_outcomes
COMMENT 'Referral completion rates and outcomes by resource type.'
TBLPROPERTIES (
  'quality' = 'gold',
  'domain'  = 'care_management'
)
AS
SELECT
  referral_type,
  community_resource,

  COUNT(*)                                          AS total_referrals,
  COUNT(CASE WHEN status = 'Completed' THEN 1 END) AS completed,
  COUNT(CASE WHEN status = 'Pending' THEN 1 END)   AS pending,
  COUNT(CASE WHEN status = 'In Progress' THEN 1 END) AS in_progress,
  COUNT(CASE WHEN status = 'Declined' THEN 1 END)  AS declined,

  ROUND(
    COUNT(CASE WHEN status = 'Completed' THEN 1 END)
    * 100.0 / NULLIF(COUNT(*), 0), 1
  )                                                 AS completion_rate_pct

FROM LIVE.silver_sdoh_referrals
GROUP BY
  referral_type,
  community_resource;

-- ---------------------------------------------------------------------------
-- Gold: TOC Performance
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_toc_performance
COMMENT '48-hour call rate, 7-day PCP visit rate, med reconciliation rate, and readmission risk by discharge type.'
TBLPROPERTIES (
  'quality' = 'gold',
  'domain'  = 'care_management'
)
AS
SELECT
  t.discharge_type,
  t.readmission_risk_tier,

  COUNT(DISTINCT t.toc_id)                          AS total_episodes,

  -- Follow-up completion rates
  ROUND(
    COUNT(DISTINCT CASE WHEN f.followup_type = '48hr_call' AND f.status = 'Completed' THEN f.toc_id END)
    * 100.0 / NULLIF(COUNT(DISTINCT CASE WHEN f.followup_type = '48hr_call' THEN f.toc_id END), 0), 1
  )                                                 AS call_48hr_completion_pct,

  ROUND(
    COUNT(DISTINCT CASE WHEN f.followup_type = '7day_pcp' AND f.status = 'Completed' THEN f.toc_id END)
    * 100.0 / NULLIF(COUNT(DISTINCT CASE WHEN f.followup_type = '7day_pcp' THEN f.toc_id END), 0), 1
  )                                                 AS pcp_7day_completion_pct,

  ROUND(
    COUNT(DISTINCT CASE WHEN f.followup_type = 'med_reconciliation' AND f.status = 'Completed' THEN f.toc_id END)
    * 100.0 / NULLIF(COUNT(DISTINCT CASE WHEN f.followup_type = 'med_reconciliation' THEN f.toc_id END), 0), 1
  )                                                 AS med_rec_completion_pct,

  ROUND(AVG(t.readmission_risk_score), 3)          AS avg_readmission_risk

FROM LIVE.silver_toc_episodes t
LEFT JOIN LIVE.silver_toc_followup f
  ON t.toc_id = f.toc_id
GROUP BY
  t.discharge_type,
  t.readmission_risk_tier;

-- ---------------------------------------------------------------------------
-- Gold: TOC Barriers
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_toc_barriers
COMMENT 'Most common barriers to successful care transitions by type and resolution status.'
TBLPROPERTIES (
  'quality' = 'gold',
  'domain'  = 'care_management'
)
AS
SELECT
  b.barrier_type,

  COUNT(*)                                          AS total_occurrences,
  COUNT(CASE WHEN b.resolved_flag THEN 1 END)     AS resolved_count,

  ROUND(
    COUNT(CASE WHEN b.resolved_flag THEN 1 END)
    * 100.0 / NULLIF(COUNT(*), 0), 1
  )                                                 AS resolution_rate_pct

FROM LIVE.silver_toc_episodes t
INNER JOIN LIVE.bronze_toc_barriers b
  ON t.toc_id = b.toc_id
GROUP BY
  b.barrier_type;

-- ---------------------------------------------------------------------------
-- Gold: Gap Closure Rates
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_gap_closure_rates
COMMENT 'Care gap closure rates by HEDIS measure, priority, and intervention type.'
TBLPROPERTIES (
  'quality' = 'gold',
  'domain'  = 'care_management'
)
AS
SELECT
  g.measure_name,
  g.condition,
  g.priority,

  COUNT(DISTINCT g.gap_id)                          AS total_gaps,
  COUNT(DISTINCT cl.gap_id)                         AS closed_gaps,

  ROUND(
    COUNT(DISTINCT cl.gap_id)
    * 100.0 / NULLIF(COUNT(DISTINCT g.gap_id), 0), 1
  )                                                 AS closure_rate_pct,

  ROUND(AVG(cl.days_to_close), 0)                  AS avg_days_to_close,

  -- Intervention counts
  COUNT(DISTINCT i.intervention_id)                 AS total_interventions,
  ROUND(
    COUNT(DISTINCT i.intervention_id) * 1.0
    / NULLIF(COUNT(DISTINCT g.gap_id), 0), 1
  )                                                 AS avg_interventions_per_gap

FROM LIVE.silver_care_gaps g
LEFT JOIN LIVE.silver_gap_closure_events cl
  ON g.gap_id = cl.gap_id
LEFT JOIN LIVE.silver_gap_interventions i
  ON g.gap_id = i.gap_id
GROUP BY
  g.measure_name,
  g.condition,
  g.priority;

-- ---------------------------------------------------------------------------
-- Gold: Gap Closure Funnel
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_gap_closure_funnel
COMMENT 'Care gap closure funnel: Open -> Intervention -> Closed by HEDIS measure.'
TBLPROPERTIES (
  'quality' = 'gold',
  'domain'  = 'care_management'
)
AS
SELECT
  g.measure_name,

  COUNT(DISTINCT g.gap_id)                          AS total_open_gaps,

  COUNT(DISTINCT CASE WHEN i.gap_id IS NOT NULL THEN g.gap_id END)
                                                    AS gaps_with_intervention,

  COUNT(DISTINCT cl.gap_id)                         AS gaps_closed,

  -- Funnel conversion rates
  ROUND(
    COUNT(DISTINCT CASE WHEN i.gap_id IS NOT NULL THEN g.gap_id END)
    * 100.0 / NULLIF(COUNT(DISTINCT g.gap_id), 0), 1
  )                                                 AS intervention_rate_pct,

  ROUND(
    COUNT(DISTINCT cl.gap_id)
    * 100.0 / NULLIF(COUNT(DISTINCT CASE WHEN i.gap_id IS NOT NULL THEN g.gap_id END), 0), 1
  )                                                 AS intervention_to_closure_pct,

  ROUND(
    COUNT(DISTINCT cl.gap_id)
    * 100.0 / NULLIF(COUNT(DISTINCT g.gap_id), 0), 1
  )                                                 AS overall_closure_rate_pct

FROM LIVE.silver_care_gaps g
LEFT JOIN LIVE.silver_gap_interventions i
  ON g.gap_id = i.gap_id
LEFT JOIN LIVE.silver_gap_closure_events cl
  ON g.gap_id = cl.gap_id
GROUP BY
  g.measure_name;
