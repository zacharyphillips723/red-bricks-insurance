-- =============================================================================
-- Red Bricks Insurance — Gold Analytics: Member 360 Denormalized View
-- =============================================================================
-- One-row-per-member denormalized view joining demographics, enrollment,
-- claims aggregates, risk adjustment, HEDIS gaps, and encounter data.
-- Designed for the Member 360 UI and RAG agent tool.
-- Pipeline: gold_analytics (runs separately from domain pipelines)
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW gold_member_360
COMMENT 'Denormalized member 360 view joining demographics, enrollment, claims, risk adjustment, HEDIS gaps, and encounters. One row per member for care management context.'
AS
WITH

-- Most recent enrollment per member
latest_enrollment AS (
  SELECT *
  FROM (
    SELECT
      e.*,
      ROW_NUMBER() OVER (PARTITION BY e.member_id ORDER BY e.eligibility_start_date DESC) AS rn
    FROM ${catalog}.members.silver_enrollment e
  )
  WHERE rn = 1
),

-- Medical claims aggregates
medical_agg AS (
  SELECT
    member_id,
    COUNT(DISTINCT claim_id)    AS medical_claim_count,
    SUM(paid_amount)            AS medical_total_paid_ytd,
    SUM(billed_amount)          AS medical_total_billed_ytd,
    SUM(member_responsibility)  AS medical_member_responsibility_ytd,
    -- Top 3 diagnoses by frequency
    CONCAT_WS(', ',
      SLICE(
        TRANSFORM(
          SLICE(
            ARRAY_SORT(
              COLLECT_SET(
                NAMED_STRUCT('dx', primary_diagnosis_code, 'desc', primary_diagnosis_desc)
              ),
              (l, r) -> CASE WHEN l.dx > r.dx THEN 1 WHEN l.dx < r.dx THEN -1 ELSE 0 END
            ),
            1, 3
          ),
          x -> CONCAT(x.dx, ' (', x.desc, ')')
        ),
        1, 3
      )
    ) AS top_diagnoses
  FROM ${catalog}.claims.silver_claims_medical
  GROUP BY member_id
),

-- Pharmacy spend
pharmacy_agg AS (
  SELECT
    member_id,
    COUNT(DISTINCT claim_id)  AS pharmacy_claim_count,
    SUM(plan_paid)            AS pharmacy_spend_ytd,
    SUM(member_copay)         AS pharmacy_member_copay_ytd
  FROM ${catalog}.claims.silver_claims_pharmacy
  GROUP BY member_id
),

-- Risk adjustment (dedup to one row per member)
risk AS (
  SELECT *
  FROM (
    SELECT
      member_id,
      raf_score,
      hcc_codes,
      hcc_count,
      is_high_risk,
      ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY raf_score DESC) AS rn
    FROM ${catalog}.risk_adjustment.silver_risk_adjustment_member
  )
  WHERE rn = 1
),

-- HEDIS gaps
hedis_gaps AS (
  SELECT
    member_id,
    SUM(CASE WHEN is_compliant = 0 THEN 1 ELSE 0 END) AS hedis_gap_count,
    CONCAT_WS(', ',
      COLLECT_SET(
        CASE WHEN is_compliant = 0 THEN measure_name END
      )
    ) AS hedis_gap_measures
  FROM gold_hedis_member
  GROUP BY member_id
),

-- Most recent encounter
latest_encounter AS (
  SELECT *
  FROM (
    SELECT
      enc.member_id,
      enc.date_of_service     AS last_encounter_date,
      enc.encounter_type      AS last_encounter_type,
      enc.provider_npi        AS pcp_npi,
      ROW_NUMBER() OVER (PARTITION BY enc.member_id ORDER BY enc.date_of_service DESC) AS rn
    FROM ${catalog}.clinical.silver_encounters enc
  )
  WHERE rn = 1
)

SELECT
  -- Demographics
  m.member_id,
  m.first_name,
  m.last_name,
  CONCAT(m.first_name, ' ', m.last_name) AS member_name,
  m.date_of_birth,
  FLOOR(DATEDIFF(CURRENT_DATE(), m.date_of_birth) / 365.25) AS age,
  m.gender,
  m.address_line_1,
  m.city,
  m.state,
  m.zip_code,
  m.county,
  m.phone,
  m.email,

  -- Enrollment
  e.line_of_business,
  e.plan_type,
  e.plan_id,
  e.group_name,
  e.eligibility_start_date,
  e.eligibility_end_date,
  e.monthly_premium,

  -- Claims: medical
  COALESCE(mc.medical_claim_count, 0)            AS medical_claim_count,
  COALESCE(mc.medical_total_paid_ytd, 0)         AS medical_total_paid_ytd,
  COALESCE(mc.medical_total_billed_ytd, 0)       AS medical_total_billed_ytd,
  COALESCE(mc.medical_member_responsibility_ytd, 0) AS medical_member_responsibility_ytd,
  mc.top_diagnoses,

  -- Claims: pharmacy
  COALESCE(rx.pharmacy_claim_count, 0)           AS pharmacy_claim_count,
  COALESCE(rx.pharmacy_spend_ytd, 0)             AS pharmacy_spend_ytd,
  COALESCE(rx.pharmacy_member_copay_ytd, 0)      AS pharmacy_member_copay_ytd,

  -- Total spend
  COALESCE(mc.medical_total_paid_ytd, 0) + COALESCE(rx.pharmacy_spend_ytd, 0) AS total_paid_ytd,

  -- Risk adjustment
  COALESCE(r.raf_score, 0)                       AS raf_score,
  r.hcc_codes,
  COALESCE(r.hcc_count, 0)                       AS hcc_count,
  COALESCE(r.is_high_risk, FALSE)                AS is_high_risk,

  -- Risk tier (derived from RAF score)
  CASE
    WHEN r.raf_score > 3.0  THEN 'Critical'
    WHEN r.raf_score > 2.5  THEN 'High'
    WHEN r.raf_score > 2.0  THEN 'Elevated'
    ELSE 'Moderate'
  END                                            AS risk_tier,

  -- HEDIS gaps
  COALESCE(hg.hedis_gap_count, 0)               AS hedis_gap_count,
  hg.hedis_gap_measures,

  -- Latest encounter
  le.last_encounter_date,
  le.last_encounter_type,
  le.pcp_npi

FROM (
  SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY member_id) AS m_rn
    FROM ${catalog}.members.silver_members
  ) WHERE m_rn = 1
) m
LEFT JOIN latest_enrollment e        ON m.member_id = e.member_id
LEFT JOIN medical_agg mc             ON m.member_id = mc.member_id
LEFT JOIN pharmacy_agg rx            ON m.member_id = rx.member_id
LEFT JOIN risk r                     ON m.member_id = r.member_id
LEFT JOIN hedis_gaps hg              ON m.member_id = hg.member_id
LEFT JOIN latest_encounter le        ON m.member_id = le.member_id;
