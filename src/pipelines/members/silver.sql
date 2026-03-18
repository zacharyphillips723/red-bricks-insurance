-- =============================================================================
-- Red Bricks Insurance — Members Domain: Silver Layer
-- =============================================================================
-- Cleansed and conformed member and enrollment data. Data quality expectations
-- enforce schema contracts: critical violations drop the row, soft violations
-- are tracked for governance reporting. ~2% of records will fail expectations.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Silver Members — cleansed demographics
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver_members (
  CONSTRAINT valid_member_id_not_null
    EXPECT (member_id IS NOT NULL)
    ON VIOLATION DROP ROW,
  CONSTRAINT valid_member_id_format
    EXPECT (member_id RLIKE '^MBR[0-9]+$')
    ON VIOLATION DROP ROW,
  CONSTRAINT valid_dob_castable
    EXPECT (TRY_CAST(date_of_birth AS DATE) IS NOT NULL),
  CONSTRAINT valid_dob_not_future
    EXPECT (TRY_CAST(date_of_birth AS DATE) <= current_date()),
  CONSTRAINT valid_gender
    EXPECT (gender IN ('M', 'F'))
)
COMMENT 'Cleansed member demographics with validated IDs, dates, and gender codes. Critical constraint failures are dropped; soft failures are tracked.'
AS SELECT
  member_id,
  last_name,
  first_name,
  CONCAT_WS(', ', last_name, first_name) AS full_name,
  CAST(date_of_birth AS DATE) AS date_of_birth,
  gender,
  ssn_last_4,
  address_line_1,
  city,
  state,
  zip_code,
  county,
  phone,
  email,
  source_file,
  ingestion_timestamp
FROM STREAM(LIVE.bronze_members);

-- ---------------------------------------------------------------------------
-- Silver Enrollment — cleansed eligibility & plan data
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver_enrollment (
  CONSTRAINT valid_member_id
    EXPECT (member_id IS NOT NULL)
    ON VIOLATION DROP ROW,
  CONSTRAINT valid_line_of_business
    EXPECT (line_of_business IN ('Commercial', 'Medicare Advantage', 'Medicaid', 'ACA Marketplace')),
  CONSTRAINT valid_start_date
    EXPECT (TRY_CAST(eligibility_start_date AS DATE) IS NOT NULL),
  CONSTRAINT valid_premium
    EXPECT (monthly_premium > 0),
  CONSTRAINT valid_risk_score
    EXPECT (risk_score >= 0 AND risk_score <= 5.0)
)
COMMENT 'Cleansed enrollment records with validated LOB, dates, premiums, and risk scores. Null member_id rows are dropped; other violations are tracked.'
AS SELECT
  member_id,
  subscriber_id,
  relationship,
  line_of_business,
  plan_type,
  plan_id,
  group_number,
  group_name,
  CAST(eligibility_start_date AS DATE) AS eligibility_start_date,
  CAST(eligibility_end_date AS DATE) AS eligibility_end_date,
  monthly_premium,
  rating_area,
  risk_score,
  metal_level,
  COALESCE(
    CAST(MONTHS_BETWEEN(
      CAST(eligibility_end_date AS DATE),
      CAST(eligibility_start_date AS DATE)
    ) AS INT),
    12
  ) AS coverage_months,
  CASE
    WHEN eligibility_end_date IS NULL
      OR CAST(eligibility_end_date AS DATE) >= current_date()
    THEN TRUE
    ELSE FALSE
  END AS is_active,
  source_file,
  ingestion_timestamp
FROM STREAM(LIVE.bronze_enrollment);
