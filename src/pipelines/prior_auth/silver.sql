-- =============================================================================
-- Red Bricks Insurance — Prior Authorization Domain: Silver Layer
-- =============================================================================
-- Cleansed and validated PA requests, policies, and rules.
-- Date strings cast to DATE/TIMESTAMP, quality expectations applied.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Silver: Prior Authorization Requests
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver_pa_requests (
  -- Hard constraints — drop row on violation
  CONSTRAINT valid_auth_id
    EXPECT (auth_request_id IS NOT NULL)
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_member_id
    EXPECT (member_id IS NOT NULL)
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_request_date
    EXPECT (TRY_CAST(request_date AS DATE) IS NOT NULL)
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_determination
    EXPECT (determination IN ('approved', 'denied', 'pended'))
    ON VIOLATION DROP ROW,

  -- Soft constraints — track but keep row
  CONSTRAINT valid_procedure_code
    EXPECT (procedure_code IS NOT NULL),

  CONSTRAINT valid_provider_npi
    EXPECT (requesting_provider_npi RLIKE '^[0-9]{10}$'),

  CONSTRAINT valid_urgency
    EXPECT (urgency IN ('standard', 'expedited')),

  CONSTRAINT valid_turnaround
    EXPECT (turnaround_hours >= 0),

  CONSTRAINT valid_estimated_cost
    EXPECT (estimated_cost >= 0)
)
COMMENT 'Cleansed prior authorization requests with validated dates, determination status, and quality tracking.'
TBLPROPERTIES (
  'quality' = 'silver',
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
  TRY_CAST(request_date AS DATE)             AS request_date,
  determination,
  determination_tier,
  TRY_CAST(determination_date AS TIMESTAMP)  AS determination_date,
  determination_reason,
  denial_reason_code,
  reviewer_type,
  clinical_summary,
  estimated_cost,
  turnaround_hours,
  appeal_filed,
  appeal_outcome,

  -- Derived: CMS-0057-F compliance flags
  CASE
    WHEN urgency = 'expedited' AND turnaround_hours <= 72 THEN TRUE
    WHEN urgency = 'standard' AND turnaround_hours <= 168 THEN TRUE
    ELSE FALSE
  END AS cms_compliant,

  -- Derived: request month for trending
  DATE_FORMAT(TRY_CAST(request_date AS DATE), 'yyyy-MM') AS request_year_month,

  source_file,
  ingestion_timestamp
FROM STREAM(LIVE.bronze_pa_requests);

-- ---------------------------------------------------------------------------
-- Silver: Medical Policies
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver_medical_policies (
  CONSTRAINT valid_policy_id
    EXPECT (policy_id IS NOT NULL)
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_policy_name
    EXPECT (policy_name IS NOT NULL)
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_effective_date
    EXPECT (TRY_CAST(effective_date AS DATE) IS NOT NULL)
)
COMMENT 'Cleansed medical policy metadata with validated dates.'
TBLPROPERTIES (
  'quality' = 'silver',
  'domain'  = 'prior_auth'
)
AS
SELECT
  policy_id,
  policy_name,
  service_category,
  TRY_CAST(effective_date AS DATE)   AS effective_date,
  TRY_CAST(last_reviewed AS DATE)    AS last_reviewed,
  file_name,
  file_path,
  num_covered_services,
  num_criteria,
  num_step_therapy_steps,
  source_file,
  ingestion_timestamp
FROM STREAM(LIVE.bronze_medical_policies);

-- ---------------------------------------------------------------------------
-- Silver: Medical Policy Rules
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver_medical_policy_rules (
  CONSTRAINT valid_rule_id
    EXPECT (rule_id IS NOT NULL)
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_policy_id
    EXPECT (policy_id IS NOT NULL)
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_rule_text
    EXPECT (rule_text IS NOT NULL)
)
COMMENT 'Validated structured policy rules with typed dates.'
TBLPROPERTIES (
  'quality' = 'silver',
  'domain'  = 'prior_auth'
)
AS
SELECT
  policy_id,
  policy_name,
  service_category,
  rule_id,
  rule_type,
  rule_text,
  procedure_codes,
  diagnosis_codes,
  TRY_CAST(effective_date AS DATE)   AS effective_date,
  source_file,
  ingestion_timestamp
FROM STREAM(LIVE.bronze_medical_policy_rules);
