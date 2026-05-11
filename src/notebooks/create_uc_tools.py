# Databricks notebook source
# MAGIC %md
# MAGIC # Register AI Tool Functions in Unity Catalog
# MAGIC
# MAGIC Creates the `ai_tools` schema and registers governed SQL functions
# MAGIC that any agent, Genie space, or notebook can call. These are the **shared
# MAGIC tool layer** for all Red Bricks Insurance AI agents.
# MAGIC
# MAGIC **Consumers:**
# MAGIC - Care Intelligence Agent (Command Center app)
# MAGIC - FWA Investigation Agent
# MAGIC - Prior Auth Review Agent
# MAGIC - Group Sales Coach Agent
# MAGIC - Genie Spaces (natural language → function calls)
# MAGIC - Ad-hoc notebooks and dashboards

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance", "Catalog")
catalog = dbutils.widgets.get("catalog")

spark.sql(f"USE CATALOG `{catalog}`")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create `ai_tools` Schema

# COMMAND ----------

spark.sql("""
CREATE SCHEMA IF NOT EXISTS ai_tools
COMMENT 'Governed AI tool functions for agents, Genie, and analytics. Each function is a reusable, auditable data retrieval tool that returns JSON.'
""")

print(f"Schema {catalog}.ai_tools ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper: run DDL with error handling

# COMMAND ----------

def register_function(name, sql):
    """Register a UC function with error handling."""
    try:
        spark.sql(sql)
        print(f"  ✓ ai_tools.{name}")
    except Exception as e:
        print(f"  ✗ ai_tools.{name}: {e}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical Tools

# COMMAND ----------

register_function("get_member_profile", """
CREATE OR REPLACE FUNCTION ai_tools.get_member_profile(member_id STRING)
RETURNS STRING
COMMENT 'Get the full Member 360 profile for a health plan member. Returns demographics, enrollment details, risk scores (RAF, HCC), HEDIS care gap summary, claims totals (medical + pharmacy), top diagnoses, and PCP information as a JSON object. Use this as the starting point for any member inquiry.'
RETURN (
  SELECT to_json(struct(*))
  FROM analytics.gold_member_360
  WHERE member_id = get_member_profile.member_id
  LIMIT 1
)
""")

# COMMAND ----------

register_function("get_lab_results", """
CREATE OR REPLACE FUNCTION ai_tools.get_lab_results(
  member_id STRING,
  max_results INT DEFAULT 15
)
RETURNS STRING
COMMENT 'Get recent lab results for a member ordered by collection date (newest first). Returns lab name, numeric value, unit, reference range (low/high), and whether the result is abnormal. Useful for tracking trends in HbA1c, eGFR, lipid panels, glucose, and other clinical markers. Max 30 results.'
RETURN (
  SELECT to_json(collect_list(named_struct(
    'lab_name', lab_name,
    'value', value,
    'unit', unit,
    'reference_range_low', reference_range_low,
    'reference_range_high', reference_range_high,
    'collection_date', CAST(collection_date AS STRING),
    'is_abnormal', is_abnormal
  )))
  FROM (
    SELECT lab_name, value, unit, reference_range_low, reference_range_high,
           collection_date, is_abnormal
    FROM clinical.silver_lab_results
    WHERE member_id = get_lab_results.member_id
    ORDER BY collection_date DESC
    LIMIT 15
  )
)
""")

# COMMAND ----------

register_function("get_case_assessments", """
CREATE OR REPLACE FUNCTION ai_tools.get_case_assessments(member_id STRING)
RETURNS STRING
COMMENT 'Get clinical and behavioral health assessments for a member. Returns assessment type (PHQ-9 for depression 0-27, GAD-7 for anxiety 0-21, PRAPARE for SDOH screening 0-20, Fall Risk 0-10, Functional Status), numeric score, risk level, assessment date, and the associated case episode type and acuity level.'
RETURN (
  SELECT to_json(collect_list(named_struct(
    'assessment_type', a.assessment_type,
    'score', a.score,
    'risk_level', a.risk_level,
    'assessment_date', CAST(a.assessment_date AS STRING),
    'episode_type', c.episode_type,
    'acuity', c.acuity
  )))
  FROM care_management.silver_case_assessments a
  JOIN care_management.silver_case_episodes c ON a.case_id = c.case_id
  WHERE c.member_id = get_case_assessments.member_id
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Financial Tools

# COMMAND ----------

register_function("get_claims_summary", """
CREATE OR REPLACE FUNCTION ai_tools.get_claims_summary(member_id STRING)
RETURNS STRING
COMMENT 'Get the claims and cost summary for a member from the Member 360 view. Returns medical claim count, total paid YTD, total billed YTD, pharmacy claim count, pharmacy spend YTD, combined total paid YTD, and top diagnoses. Use this for financial analysis, cost trending, and identifying high-cost drivers.'
RETURN (
  SELECT to_json(named_struct(
    'medical_claim_count', medical_claim_count,
    'medical_total_paid_ytd', medical_total_paid_ytd,
    'medical_total_billed_ytd', medical_total_billed_ytd,
    'pharmacy_claim_count', pharmacy_claim_count,
    'pharmacy_spend_ytd', pharmacy_spend_ytd,
    'total_paid_ytd', total_paid_ytd,
    'top_diagnoses', top_diagnoses
  ))
  FROM analytics.gold_member_360
  WHERE member_id = get_claims_summary.member_id
  LIMIT 1
)
""")

# COMMAND ----------

register_function("get_denial_history", """
CREATE OR REPLACE FUNCTION ai_tools.get_denial_history(member_id STRING)
RETURNS STRING
COMMENT 'Get denied medical claims for a member. Returns claim ID, service date, procedure code and description, primary diagnosis code, billed amount, and denial reason code. Use this to identify denial patterns, appeal opportunities, and prior authorization gaps.'
RETURN (
  SELECT to_json(collect_list(named_struct(
    'claim_id', claim_id,
    'service_from_date', CAST(service_from_date AS STRING),
    'procedure_code', procedure_code,
    'procedure_desc', procedure_desc,
    'primary_diagnosis_code', primary_diagnosis_code,
    'billed_amount', billed_amount,
    'denial_reason_code', denial_reason_code
  )))
  FROM (
    SELECT claim_id, service_from_date, procedure_code, procedure_desc,
           primary_diagnosis_code, billed_amount, denial_reason_code
    FROM claims.silver_claims_medical
    WHERE member_id = get_denial_history.member_id
      AND claim_status = 'denied'
    ORDER BY service_from_date DESC
    LIMIT 10
  )
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Care Management Tools

# COMMAND ----------

register_function("get_care_programs", """
CREATE OR REPLACE FUNCTION ai_tools.get_care_programs(member_id STRING)
RETURNS STRING
COMMENT 'Get disease management program enrollments for a member. Returns program name (Diabetes Management, CHF Care, COPD Wellness, Behavioral Health, Maternal Health, Chronic Kidney Disease), program type, enrollment status (Active, Graduated, Disenrolled), enrollment and disenrollment dates, referral source, and enrollment reason. Use this to understand what structured care programs the member participates in.'
RETURN (
  SELECT to_json(collect_list(named_struct(
    'program_name', program_name,
    'program_type', program_type,
    'status', status,
    'enrollment_date', CAST(enrollment_date AS STRING),
    'disenrollment_date', CAST(disenrollment_date AS STRING),
    'referral_source', referral_source,
    'enrollment_reason', enrollment_reason
  )))
  FROM care_management.silver_program_enrollment
  WHERE member_id = get_care_programs.member_id
)
""")

# COMMAND ----------

register_function("get_sdoh_screening", """
CREATE OR REPLACE FUNCTION ai_tools.get_sdoh_screening(member_id STRING)
RETURNS STRING
COMMENT 'Get the most recent Social Determinants of Health (SDOH) screening results for a member. Returns screening date, county, and binary flags for: food insecurity, housing instability, transportation barriers, social isolation, and financial strain. Also includes a composite SDOH risk score (0-100) and total count of active SDOH flags. Members with 3+ flags or composite score > 60 are high SDOH risk and may need community resource referrals.'
RETURN (
  SELECT to_json(named_struct(
    'screening_date', CAST(screening_date AS STRING),
    'county', county,
    'food_insecurity_flag', food_insecurity_flag,
    'housing_instability_flag', housing_instability_flag,
    'transportation_barrier_flag', transportation_barrier_flag,
    'social_isolation_flag', social_isolation_flag,
    'financial_strain_flag', financial_strain_flag,
    'composite_sdoh_risk_score', composite_sdoh_risk_score,
    'total_sdoh_flags', total_sdoh_flags
  ))
  FROM care_management.silver_member_sdoh
  WHERE member_id = get_sdoh_screening.member_id
  ORDER BY screening_date DESC
  LIMIT 1
)
""")

# COMMAND ----------

register_function("get_care_gaps", """
CREATE OR REPLACE FUNCTION ai_tools.get_care_gaps(member_id STRING)
RETURNS STRING
COMMENT 'Get HEDIS care gaps for a member with intervention tracking. Returns measure name (e.g., HbA1c Testing, Breast Cancer Screening), associated condition, priority (Critical/High/Medium/Low), gap open date, gap age in days, number of outreach interventions attempted, date of last intervention, and closure date (null if still open). Open gaps with high priority and long age should be escalated. Use this to identify quality measure compliance gaps and plan targeted outreach.'
RETURN (
  SELECT to_json(collect_list(named_struct(
    'measure_name', measure_name,
    'condition', condition,
    'priority', priority,
    'gap_open_date', gap_open_date,
    'gap_age_days', gap_age_days,
    'intervention_count', intervention_count,
    'last_intervention_date', last_intervention_date,
    'closure_date', closure_date
  )))
  FROM (
    SELECT g.measure_name, g.condition, g.priority,
           CAST(g.gap_open_date AS STRING) AS gap_open_date,
           g.gap_age_days,
           CAST(COUNT(i.intervention_id) AS INT) AS intervention_count,
           CAST(MAX(i.intervention_date) AS STRING) AS last_intervention_date,
           CAST(cl.closure_date AS STRING) AS closure_date
    FROM care_management.silver_care_gaps g
    LEFT JOIN care_management.silver_gap_interventions i ON g.gap_id = i.gap_id
    LEFT JOIN care_management.silver_gap_closure_events cl ON g.gap_id = cl.gap_id
    WHERE g.member_id = get_care_gaps.member_id
    GROUP BY g.measure_name, g.condition, g.priority,
             g.gap_open_date, g.gap_age_days, cl.closure_date
  )
)
""")

# COMMAND ----------

register_function("get_toc_history", """
CREATE OR REPLACE FUNCTION ai_tools.get_toc_history(member_id STRING)
RETURNS STRING
COMMENT 'Get transitions of care (TOC) history for a member. Returns discharge date, discharge type, facility name, readmission risk score (0-100) and tier (Critical/High/Moderate/Low), follow-up type (48-Hour Call, 7-Day PCP Visit, Medication Reconciliation), follow-up status (Completed/Pending/Overdue/Missed), completion date, and days from due date. Use this to track post-discharge follow-up compliance and identify members at risk of readmission.'
RETURN (
  SELECT to_json(collect_list(named_struct(
    'discharge_date', CAST(t.discharge_date AS STRING),
    'discharge_type', t.discharge_type,
    'discharge_facility', t.discharge_facility,
    'readmission_risk_score', t.readmission_risk_score,
    'readmission_risk_tier', t.readmission_risk_tier,
    'followup_type', f.followup_type,
    'followup_status', f.status,
    'completed_date', CAST(f.completed_date AS STRING),
    'days_from_due', f.days_from_due
  )))
  FROM care_management.silver_toc_episodes t
  LEFT JOIN care_management.silver_toc_followup f ON t.toc_id = f.toc_id
  WHERE t.member_id = get_toc_history.member_id
)
""")

# COMMAND ----------

register_function("recommend_intervention", """
CREATE OR REPLACE FUNCTION ai_tools.recommend_intervention(member_id STRING)
RETURNS STRING
COMMENT 'Aggregate key data points for generating next-best-action recommendations for a member. Returns the member risk profile (risk tier, RAF score, HEDIS gap count, top diagnoses, line of business), latest SDOH screening results (all flags and composite score), and count of open care gaps. Use this as input for care management decision-making: which interventions to prioritize, whether to refer to community resources, and what outreach to schedule.'
RETURN (
  SELECT to_json(named_struct(
    'risk_tier', m.risk_tier,
    'raf_score', m.raf_score,
    'hedis_gap_count', m.hedis_gap_count,
    'hedis_gap_measures', m.hedis_gap_measures,
    'top_diagnoses', m.top_diagnoses,
    'line_of_business', m.line_of_business,
    'food_insecurity', s.food_insecurity_flag,
    'housing_instability', s.housing_instability_flag,
    'transportation_barrier', s.transportation_barrier_flag,
    'social_isolation', s.social_isolation_flag,
    'financial_strain', s.financial_strain_flag,
    'sdoh_risk_score', s.composite_sdoh_risk_score,
    'total_sdoh_flags', s.total_sdoh_flags
  ))
  FROM analytics.gold_member_360 m
  LEFT JOIN (
    SELECT * FROM (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY screening_date DESC) AS rn
      FROM care_management.silver_member_sdoh
    ) WHERE rn = 1
  ) s ON m.member_id = s.member_id
  WHERE m.member_id = recommend_intervention.member_id
  LIMIT 1
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cross-Domain Tools (FWA, PA, Group Sales)

# COMMAND ----------

# Only create FWA tools if the FWA schema/tables exist
try:
    spark.sql("SELECT 1 FROM fwa.gold_fwa_provider_risk LIMIT 1")
    fwa_exists = True
except:
    fwa_exists = False
    print("  ⚠ fwa.gold_fwa_provider_risk not found — skipping FWA tools")

if fwa_exists:
    register_function("get_fwa_risk_profile", """
    CREATE OR REPLACE FUNCTION ai_tools.get_fwa_risk_profile(provider_npi STRING)
    RETURNS STRING
    COMMENT 'Get the fraud, waste, and abuse (FWA) risk profile for a provider by NPI. Returns the provider risk score, risk tier, flagged claim count, investigation status, top fraud indicators, and billing pattern anomalies. Use this for FWA investigation triage and provider audit prioritization.'
    RETURN (
      SELECT to_json(struct(*))
      FROM fwa.gold_fwa_provider_risk
      WHERE provider_npi = get_fwa_risk_profile.provider_npi
      LIMIT 1
    )
    """)

    register_function("get_fwa_flagged_claims", """
    CREATE OR REPLACE FUNCTION ai_tools.get_fwa_flagged_claims(
      target_id STRING,
      target_type STRING DEFAULT 'provider'
    )
    RETURNS STRING
    COMMENT 'Get FWA-flagged claims for a provider (by NPI) or member (by member_id). Set target_type to "provider" or "member". Returns claim ID, fraud score, fraud type and description, severity, evidence summary, billed amount, procedure code, and service date. Ordered by fraud score descending. Use this to review suspicious claims during an FWA investigation.'
    RETURN (
      SELECT to_json(collect_list(named_struct(
        'claim_id', claim_id,
        'fraud_score', fraud_score,
        'fraud_type', fraud_type,
        'fraud_type_desc', fraud_type_desc,
        'severity', severity,
        'evidence_summary', evidence_summary,
        'billed_amount', billed_amount,
        'procedure_code', procedure_code,
        'service_date', CAST(service_date AS STRING),
        'member_id', member_id,
        'provider_npi', provider_npi
      )))
      FROM (
        SELECT claim_id, fraud_score, fraud_type, fraud_type_desc, severity,
               evidence_summary, billed_amount, procedure_code, service_date,
               member_id, provider_npi
        FROM fwa.gold_fwa_claim_flags
        WHERE (get_fwa_flagged_claims.target_type = 'provider' AND provider_npi = get_fwa_flagged_claims.target_id)
           OR (get_fwa_flagged_claims.target_type = 'member' AND member_id = get_fwa_flagged_claims.target_id)
        ORDER BY fraud_score DESC
        LIMIT 30
      )
    )
    """)

# COMMAND ----------

register_function("get_pa_clinical_summary", """
CREATE OR REPLACE FUNCTION ai_tools.get_pa_clinical_summary(member_id STRING)
RETURNS STRING
COMMENT 'Get a clinical summary for prior authorization review. Returns the member profile (name, DOB, gender, risk tier, RAF score, top diagnoses, HCC codes) from the Member 360 view. Use this to assess medical necessity context for prior authorization requests.'
RETURN (
  SELECT to_json(named_struct(
    'member_name', member_name,
    'date_of_birth', date_of_birth,
    'gender', gender,
    'risk_tier', risk_tier,
    'raf_score', raf_score,
    'top_diagnoses', top_diagnoses,
    'hcc_codes', hcc_codes,
    'hcc_count', hcc_count,
    'hedis_gap_measures', hedis_gap_measures
  ))
  FROM analytics.gold_member_360
  WHERE member_id = get_pa_clinical_summary.member_id
  LIMIT 1
)
""")

# COMMAND ----------

register_function("get_group_benefit_summary", """
CREATE OR REPLACE FUNCTION ai_tools.get_group_benefit_summary(group_name STRING)
RETURNS STRING
COMMENT 'Get benefit utilization and cost summary for an employer group. Returns member count, total paid YTD, average cost per member, average RAF score, and total open HEDIS gaps. Use this for group renewals, sales conversations, and employer reporting.'
RETURN (
  SELECT to_json(named_struct(
    'group_name', group_name,
    'member_count', CAST(COUNT(DISTINCT member_id) AS INT),
    'total_paid_ytd', SUM(CAST(total_paid_ytd AS DOUBLE)),
    'avg_paid_per_member', AVG(CAST(total_paid_ytd AS DOUBLE)),
    'avg_raf_score', AVG(CAST(raf_score AS DOUBLE)),
    'total_open_gaps', CAST(SUM(CAST(hedis_gap_count AS INT)) AS INT)
  ))
  FROM analytics.gold_member_360
  WHERE group_name = get_group_benefit_summary.group_name
  GROUP BY group_name
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Composite / Triage Tools

# COMMAND ----------

register_function("assess_risk", """
CREATE OR REPLACE FUNCTION ai_tools.assess_risk(member_id STRING)
RETURNS STRING
COMMENT 'Comprehensive risk assessment for a member. Aggregates clinical risk (RAF, HCC), social determinants (SDOH screening), care gap status, and recent transitions of care into a single risk profile. Returns risk_tier, raf_score, demographics, SDOH flags, open care gap count, recent discharge status, and a computed overall_risk_level. Use this for care management triage, population health stratification, and intervention prioritization.'
RETURN (
  SELECT to_json(named_struct(
    'risk_tier', m.risk_tier,
    'raf_score', m.raf_score,
    'age', m.age,
    'gender', m.gender,
    'top_diagnoses', m.top_diagnoses,
    'hcc_count', m.hcc_count,
    'hedis_gap_count', m.hedis_gap_count,
    'hedis_gap_measures', m.hedis_gap_measures,
    'total_paid_ytd', m.total_paid_ytd,
    'line_of_business', m.line_of_business,
    'composite_sdoh_risk_score', s.composite_sdoh_risk_score,
    'total_sdoh_flags', s.total_sdoh_flags,
    'food_insecurity', s.food_insecurity_flag,
    'housing_instability', s.housing_instability_flag,
    'transportation_barrier', s.transportation_barrier_flag,
    'social_isolation', s.social_isolation_flag,
    'financial_strain', s.financial_strain_flag,
    'open_care_gap_count', COALESCE(g.open_gap_count, 0),
    'recent_discharge', CASE WHEN t.toc_id IS NOT NULL THEN TRUE ELSE FALSE END,
    'readmission_risk_tier', t.readmission_risk_tier,
    'overall_risk_level', CASE
      WHEN m.raf_score > 3 OR COALESCE(s.composite_sdoh_risk_score, 0) > 70 THEN 'Critical'
      WHEN m.raf_score > 2 OR COALESCE(s.composite_sdoh_risk_score, 0) > 50 THEN 'High'
      WHEN m.raf_score > 1 THEN 'Moderate'
      ELSE 'Low'
    END
  ))
  FROM analytics.gold_member_360 m
  LEFT JOIN (
    SELECT * FROM (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY screening_date DESC) AS rn
      FROM care_management.silver_member_sdoh
    ) WHERE rn = 1
  ) s ON m.member_id = s.member_id
  LEFT JOIN (
    SELECT cg.member_id, CAST(COUNT(*) AS INT) AS open_gap_count
    FROM care_management.silver_care_gaps cg
    LEFT JOIN care_management.silver_gap_closure_events ce ON cg.gap_id = ce.gap_id
    WHERE ce.gap_id IS NULL
    GROUP BY cg.member_id
  ) g ON m.member_id = g.member_id
  LEFT JOIN (
    SELECT * FROM (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY discharge_date DESC) AS rn
      FROM care_management.silver_toc_episodes
      WHERE discharge_date >= DATE_ADD(CURRENT_DATE(), -30)
    ) WHERE rn = 1
  ) t ON m.member_id = t.member_id
  WHERE m.member_id = assess_risk.member_id
  LIMIT 1
)
""")

# COMMAND ----------

register_function("get_outreach_context", """
CREATE OR REPLACE FUNCTION ai_tools.get_outreach_context(member_id STRING)
RETURNS STRING
COMMENT 'Get all context needed to generate a personalized outreach script for a member. Returns member demographics, active conditions, SDOH concerns, open care gaps, and care program enrollments. Designed for care managers and AI agents to draft phone, email, or SMS outreach that is personalized and clinically relevant.'
RETURN (
  SELECT to_json(named_struct(
    'member_name', m.member_name,
    'age', m.age,
    'gender', m.gender,
    'risk_tier', m.risk_tier,
    'top_diagnoses', m.top_diagnoses,
    'hedis_gap_measures', m.hedis_gap_measures,
    'pcp_npi', m.pcp_npi,
    'line_of_business', m.line_of_business,
    'county', m.county,
    'sdoh_concerns', s.active_sdoh_concerns,
    'open_care_gaps', g.open_gaps,
    'active_programs', p.active_programs
  ))
  FROM analytics.gold_member_360 m
  LEFT JOIN (
    SELECT member_id,
      CONCAT_WS(', ',
        CASE WHEN food_insecurity_flag THEN 'Food Insecurity' END,
        CASE WHEN housing_instability_flag THEN 'Housing Instability' END,
        CASE WHEN transportation_barrier_flag THEN 'Transportation Barrier' END,
        CASE WHEN social_isolation_flag THEN 'Social Isolation' END,
        CASE WHEN financial_strain_flag THEN 'Financial Strain' END
      ) AS active_sdoh_concerns
    FROM (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY screening_date DESC) AS rn
      FROM care_management.silver_member_sdoh
    ) WHERE rn = 1
  ) s ON m.member_id = s.member_id
  LEFT JOIN (
    SELECT member_id, to_json(collect_list(named_struct(
      'measure_name', measure_name,
      'condition', condition,
      'priority', priority,
      'gap_age_days', gap_age_days
    ))) AS open_gaps
    FROM (
      SELECT cg.member_id, cg.measure_name, cg.condition, cg.priority, cg.gap_age_days
      FROM care_management.silver_care_gaps cg
      LEFT JOIN care_management.silver_gap_closure_events ce ON cg.gap_id = ce.gap_id
      WHERE ce.gap_id IS NULL
      ORDER BY cg.priority, cg.gap_age_days DESC
      LIMIT 5
    )
    GROUP BY member_id
  ) g ON m.member_id = g.member_id
  LEFT JOIN (
    SELECT member_id, to_json(collect_list(named_struct(
      'program_name', program_name,
      'enrollment_date', CAST(enrollment_date AS STRING)
    ))) AS active_programs
    FROM care_management.silver_program_enrollment
    WHERE status = 'Active'
    GROUP BY member_id
  ) p ON m.member_id = p.member_id
  WHERE m.member_id = get_outreach_context.member_id
  LIMIT 1
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant Permissions
# MAGIC Grant EXECUTE on the `ai_tools` schema so the Command Center app service
# MAGIC principal (and any other consumer) can invoke these functions.

# COMMAND ----------

spark.sql("GRANT USAGE ON SCHEMA ai_tools TO `account users`")
spark.sql("GRANT EXECUTE ON SCHEMA ai_tools TO `account users`")
print("Granted USAGE + EXECUTE on ai_tools to account users")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

tools = spark.sql("SHOW FUNCTIONS IN ai_tools").filter("function NOT LIKE 'builtin%'").collect()
print(f"\n{'='*60}")
print(f"  {len(tools)} UC functions registered in {catalog}.ai_tools")
print(f"{'='*60}")
for t in tools:
    fname = t["function"]
    print(f"  • ai_tools.{fname}")

# COMMAND ----------

# Quick smoke test
test_members = spark.sql("SELECT member_id FROM analytics.gold_member_360 LIMIT 1").collect()
if test_members:
    mid = test_members[0]["member_id"]
    result = spark.sql(f"SELECT ai_tools.get_member_profile('{mid}') AS profile").collect()
    profile_len = len(result[0]["profile"] or "")
    print(f"\nSmoke test — ai_tools.get_member_profile('{mid}'):")
    print(f"  Result: {profile_len} chars")
    assert profile_len > 10, "Profile result too short"
    print("  ✓ Function callable and returns data")
else:
    print("\n⚠ No members in gold_member_360 — skipping smoke test")
