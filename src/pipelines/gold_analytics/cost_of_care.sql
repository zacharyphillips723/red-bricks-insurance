-- =============================================================================
-- Red Bricks Insurance — Gold Analytics: Total Cost of Care & Cost Index
-- =============================================================================
-- Population health cost metrics used by payers for value-based care programs,
-- network evaluation, and high-cost member identification.
--
-- TCOC = Risk-Adjusted Paid Amount / Member Months
--   "How much does this member cost per month, normalized for their acuity?"
--   A member with RAF 2.0 costing $2,000/month has the same TCOC as a member
--   with RAF 1.0 costing $1,000/month — both are "expected" for their risk.
--
-- TCI  = Member Actual PMPM / LOB Risk-Adjusted Average PMPM
--   "Is this member costing more or less than expected for their LOB?"
--   TCI > 1.0 = costs more than expected; TCI < 1.0 = costs less.
--   Actionable for care management, network steering, and COB review.
--
-- Pipeline: gold_analytics (runs after domain pipelines + build_member_months)
-- =============================================================================


-- -----------------------------------------------------------------------------
-- gold_member_tcoc — Member-level Total Cost of Care and Total Cost Index
-- -----------------------------------------------------------------------------
-- One row per member with TCOC, TCI, and cost breakdown.
-- Requires: silver_claims_medical, silver_claims_pharmacy, silver_member_months,
--           silver_risk_adjustment_member, silver_enrollment
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_member_tcoc
COMMENT 'Member-level Total Cost of Care (TCOC) and Total Cost Index (TCI). TCOC normalizes cost by risk acuity and exposure months. TCI benchmarks each member against their LOB average.'
AS
WITH member_costs AS (
  -- Total medical + pharmacy paid per member
  SELECT
    m.member_id,
    COALESCE(med.medical_paid, 0) + COALESCE(rx.pharmacy_paid, 0) AS total_paid,
    COALESCE(med.medical_paid, 0)   AS medical_paid,
    COALESCE(rx.pharmacy_paid, 0)   AS pharmacy_paid,
    COALESCE(med.medical_claims, 0) AS medical_claims,
    COALESCE(rx.pharmacy_claims, 0) AS pharmacy_claims
  FROM (SELECT DISTINCT member_id FROM members.silver_member_months) m
  LEFT JOIN (
    SELECT
      member_id,
      SUM(paid_amount) AS medical_paid,
      COUNT(*)         AS medical_claims
    FROM claims.silver_claims_medical
    WHERE claim_status != 'Denied'
    GROUP BY member_id
  ) med ON m.member_id = med.member_id
  LEFT JOIN (
    SELECT
      member_id,
      SUM(plan_paid)   AS pharmacy_paid,
      COUNT(*)         AS pharmacy_claims
    FROM claims.silver_claims_pharmacy
    GROUP BY member_id
  ) rx ON m.member_id = rx.member_id
),

member_exposure AS (
  -- Member months and LOB from the member_months table
  SELECT
    member_id,
    line_of_business,
    COUNT(*) AS member_months
  FROM members.silver_member_months
  GROUP BY member_id, line_of_business
),

member_risk AS (
  -- Latest RAF score per member
  SELECT
    member_id,
    raf_score,
    hcc_count,
    is_high_risk
  FROM risk_adjustment.silver_risk_adjustment_member
  QUALIFY ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY model_year DESC) = 1
),

member_tcoc AS (
  SELECT
    me.member_id,
    me.line_of_business,
    me.member_months,
    mc.total_paid,
    mc.medical_paid,
    mc.pharmacy_paid,
    mc.medical_claims,
    mc.pharmacy_claims,
    COALESCE(mr.raf_score, 1.0)  AS raf_score,
    COALESCE(mr.hcc_count, 0)    AS hcc_count,
    COALESCE(mr.is_high_risk, FALSE) AS is_high_risk,
    -- Actual PMPM (not risk-adjusted)
    mc.total_paid / NULLIF(me.member_months, 0) AS actual_pmpm,
    -- Risk-adjusted paid = total cost normalized by acuity
    mc.total_paid / NULLIF(COALESCE(mr.raf_score, 1.0), 0) AS risk_adjusted_paid,
    -- TCOC = risk-adjusted paid per member month
    (mc.total_paid / NULLIF(COALESCE(mr.raf_score, 1.0), 0))
      / NULLIF(me.member_months, 0) AS tcoc
  FROM member_exposure me
  INNER JOIN member_costs mc ON me.member_id = mc.member_id
  LEFT JOIN member_risk mr   ON me.member_id = mr.member_id
),

-- LOB-level risk-adjusted average PMPM (the TCI denominator)
lob_benchmarks AS (
  SELECT
    line_of_business,
    AVG(tcoc) AS lob_avg_tcoc
  FROM member_tcoc
  WHERE tcoc IS NOT NULL AND tcoc > 0
  GROUP BY line_of_business
)

SELECT
  mt.member_id,
  mt.line_of_business,
  mt.member_months,
  ROUND(mt.total_paid, 2)          AS total_paid,
  ROUND(mt.medical_paid, 2)        AS medical_paid,
  ROUND(mt.pharmacy_paid, 2)       AS pharmacy_paid,
  mt.medical_claims,
  mt.pharmacy_claims,
  ROUND(mt.raf_score, 3)           AS raf_score,
  mt.hcc_count,
  mt.is_high_risk,
  ROUND(mt.actual_pmpm, 2)         AS actual_pmpm,
  ROUND(mt.risk_adjusted_paid, 2)  AS risk_adjusted_paid,
  ROUND(mt.tcoc, 2)                AS tcoc,
  ROUND(lb.lob_avg_tcoc, 2)        AS lob_avg_tcoc,
  -- TCI = member actual PMPM / LOB risk-adjusted average PMPM
  ROUND(mt.actual_pmpm / NULLIF(lb.lob_avg_tcoc, 0), 3) AS tci,
  -- Categorize for care management triage
  CASE
    WHEN mt.actual_pmpm / NULLIF(lb.lob_avg_tcoc, 0) >= 3.0 THEN 'Extreme Outlier'
    WHEN mt.actual_pmpm / NULLIF(lb.lob_avg_tcoc, 0) >= 2.0 THEN 'High Cost'
    WHEN mt.actual_pmpm / NULLIF(lb.lob_avg_tcoc, 0) >= 1.5 THEN 'Rising Risk'
    WHEN mt.actual_pmpm / NULLIF(lb.lob_avg_tcoc, 0) >= 0.5 THEN 'Expected'
    ELSE 'Low Utilizer'
  END AS cost_tier
FROM member_tcoc mt
LEFT JOIN lob_benchmarks lb ON mt.line_of_business = lb.line_of_business;


-- -----------------------------------------------------------------------------
-- gold_tcoc_summary — LOB-level Total Cost of Care Summary
-- -----------------------------------------------------------------------------
-- Aggregated view for executive reporting and LOB benchmarking.
-- Shows TCOC distributions, TCI spread, and cost tier breakdowns per LOB.
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_tcoc_summary
COMMENT 'LOB-level Total Cost of Care summary with TCOC distributions, average TCI, and cost tier breakdowns for executive reporting.'
AS
WITH ranked AS (
  -- Rank members within each LOB by total_paid to find top 5% spenders
  SELECT
    *,
    PERCENT_RANK() OVER (PARTITION BY line_of_business ORDER BY total_paid) AS spend_pct_rank
  FROM gold_member_tcoc
)

SELECT
  line_of_business,
  COUNT(DISTINCT member_id)                     AS total_members,
  SUM(member_months)                            AS total_member_months,
  ROUND(SUM(total_paid), 2)                     AS total_paid,
  ROUND(AVG(actual_pmpm), 2)                    AS avg_actual_pmpm,
  ROUND(AVG(tcoc), 2)                           AS avg_tcoc,
  ROUND(PERCENTILE_APPROX(tcoc, 0.5), 2)        AS median_tcoc,
  ROUND(PERCENTILE_APPROX(tcoc, 0.25), 2)       AS p25_tcoc,
  ROUND(PERCENTILE_APPROX(tcoc, 0.75), 2)       AS p75_tcoc,
  ROUND(PERCENTILE_APPROX(tcoc, 0.90), 2)       AS p90_tcoc,
  ROUND(PERCENTILE_APPROX(tcoc, 0.95), 2)       AS p95_tcoc,
  ROUND(AVG(raf_score), 3)                      AS avg_raf_score,
  ROUND(AVG(tci), 3)                            AS avg_tci,
  -- Cost tier distribution
  SUM(CASE WHEN cost_tier = 'Extreme Outlier' THEN 1 ELSE 0 END) AS extreme_outlier_count,
  SUM(CASE WHEN cost_tier = 'High Cost' THEN 1 ELSE 0 END)       AS high_cost_count,
  SUM(CASE WHEN cost_tier = 'Rising Risk' THEN 1 ELSE 0 END)     AS rising_risk_count,
  SUM(CASE WHEN cost_tier = 'Expected' THEN 1 ELSE 0 END)        AS expected_count,
  SUM(CASE WHEN cost_tier = 'Low Utilizer' THEN 1 ELSE 0 END)    AS low_utilizer_count,
  -- % of total spend from top 5% of members (cost concentration metric)
  ROUND(
    SUM(CASE WHEN spend_pct_rank >= 0.95 THEN total_paid ELSE 0 END)
    / NULLIF(SUM(total_paid), 0) * 100, 2
  ) AS pct_spend_top_5pct
FROM ranked
GROUP BY line_of_business;
