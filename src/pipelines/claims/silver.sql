-- =============================================================================
-- Red Bricks Insurance — Claims Domain: Silver Layer
-- =============================================================================
-- Cleansed and validated medical and pharmacy claims.
-- Date strings are cast to DATE types; computed month columns are added.
-- Data quality expectations catch the ~2% intentional defects seeded in
-- synthetic source data. Critical violations drop the row; soft violations
-- are tracked but rows are kept.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Silver: Medical Claims
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver_claims_medical (
  CONSTRAINT valid_claim_id
    EXPECT (claim_id IS NOT NULL)
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_rendering_npi
    EXPECT (rendering_provider_npi RLIKE '^[0-9]{10}$')
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_service_date
    EXPECT (service_from_date IS NOT NULL AND CAST(service_from_date AS DATE) IS NOT NULL)
    ON VIOLATION DROP ROW,

  CONSTRAINT reasonable_service_date
    EXPECT (CAST(service_from_date AS DATE) >= '2022-01-01' AND CAST(service_from_date AS DATE) <= '2026-12-31')
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_procedure_code
    EXPECT (procedure_code RLIKE '^[0-9]{5}$')
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_diagnosis
    EXPECT (primary_diagnosis_code IS NOT NULL)
    ON VIOLATION DROP ROW,

  -- Soft expectations — track but keep the row
  CONSTRAINT valid_billed_amount
    EXPECT (billed_amount >= 0),

  CONSTRAINT valid_allowed_amount
    EXPECT (allowed_amount >= 0),

  CONSTRAINT valid_paid_amount
    EXPECT (paid_amount >= 0),

  CONSTRAINT billed_gte_allowed
    EXPECT (billed_amount >= allowed_amount)
)
COMMENT 'Cleansed medical claims with validated dates, NPI, procedure, and diagnosis codes. ~2% defective rows are dropped by expectations.'
TBLPROPERTIES (
  'quality' = 'silver',
  'domain'  = 'claims'
)
AS
SELECT
  claim_id,
  claim_line_number,
  claim_type,
  member_id,
  rendering_provider_npi,
  billing_provider_npi,

  -- Cast date strings to DATE
  CAST(service_from_date AS DATE)   AS service_from_date,
  CAST(service_to_date   AS DATE)   AS service_to_date,
  CAST(paid_date         AS DATE)   AS paid_date,
  CAST(admission_date    AS DATE)   AS admission_date,
  CAST(discharge_date    AS DATE)   AS discharge_date,

  admission_type,
  discharge_status,
  bill_type,
  place_of_service_code,
  place_of_service_desc,
  procedure_code,
  procedure_desc,
  revenue_code,
  revenue_desc,
  drg_code,
  drg_desc,
  primary_diagnosis_code,
  primary_diagnosis_desc,
  secondary_diagnosis_code_1,
  secondary_diagnosis_code_2,
  secondary_diagnosis_code_3,

  billed_amount,
  allowed_amount,
  paid_amount,
  copay,
  coinsurance,
  deductible,
  member_responsibility,

  claim_status,
  denial_reason_code,
  adjustment_reason,
  source_system,

  -- Computed column: truncate to month for time-series aggregation
  DATE_TRUNC('month', CAST(service_from_date AS DATE)) AS service_year_month,

  source_file,
  ingestion_timestamp
FROM STREAM(LIVE.bronze_claims_medical);

-- ---------------------------------------------------------------------------
-- Silver: Pharmacy Claims
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver_claims_pharmacy (
  CONSTRAINT valid_claim_id
    EXPECT (claim_id IS NOT NULL)
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_prescriber_npi
    EXPECT (prescriber_npi RLIKE '^[0-9]{10}$')
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_fill_date
    EXPECT (fill_date IS NOT NULL AND CAST(fill_date AS DATE) IS NOT NULL)
    ON VIOLATION DROP ROW,

  CONSTRAINT reasonable_fill_date
    EXPECT (CAST(fill_date AS DATE) >= '2022-01-01' AND CAST(fill_date AS DATE) <= '2026-12-31')
    ON VIOLATION DROP ROW,

  CONSTRAINT valid_ndc
    EXPECT (ndc RLIKE '^[0-9]{11}$')
    ON VIOLATION DROP ROW,

  -- Soft expectation — track but keep the row
  CONSTRAINT valid_ingredient_cost
    EXPECT (ingredient_cost >= 0)
)
COMMENT 'Cleansed pharmacy claims with validated NPI, NDC, and fill dates. ~2% defective rows are dropped by expectations.'
TBLPROPERTIES (
  'quality' = 'silver',
  'domain'  = 'claims'
)
AS
SELECT
  claim_id,
  member_id,
  prescriber_npi,
  pharmacy_npi,
  pharmacy_name,

  -- Cast date strings to DATE
  CAST(fill_date AS DATE) AS fill_date,
  CAST(paid_date AS DATE) AS paid_date,

  ndc,
  drug_name,
  therapeutic_class,
  is_specialty,
  days_supply,
  quantity,

  ingredient_cost,
  dispensing_fee,
  total_cost,
  member_copay,
  plan_paid,

  claim_status,
  formulary_tier,
  mail_order_flag,

  -- Computed column: truncate to month for time-series aggregation
  DATE_TRUNC('month', CAST(fill_date AS DATE)) AS fill_year_month,

  source_file,
  ingestion_timestamp
FROM STREAM(LIVE.bronze_claims_pharmacy);
