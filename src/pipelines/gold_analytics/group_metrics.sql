-- =============================================================================
-- Red Bricks Insurance — Gold Analytics: Group Reporting
-- =============================================================================
-- Employer group experience, stop-loss analysis, and renewal analytics.
-- These views join silver enrollment, groups, and claims to produce
-- group-level financial metrics for underwriting and account management.
-- Pipeline: gold_analytics (runs separately from domain pipelines)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- gold_group_experience — Claims Experience by Employer Group
-- -----------------------------------------------------------------------------
-- PMPM, utilization, and loss ratio at the employer group level.
-- Key view for account managers and group underwriters.
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_group_experience
COMMENT 'Claims experience by employer group: PMPM, utilization, loss ratio, and member counts. Used for group underwriting reviews and account management.'
AS
WITH group_enrollment AS (
  SELECT
    e.group_number AS group_id,
    e.member_id,
    e.monthly_premium,
    e.coverage_months,
    e.is_active
  FROM ${catalog}.${schema}.silver_enrollment e
  WHERE e.group_number IS NOT NULL
),

group_medical AS (
  SELECT
    ge.group_id,
    COUNT(DISTINCT c.claim_id) AS medical_claim_count,
    SUM(c.paid_amount)         AS medical_paid,
    SUM(c.allowed_amount)      AS medical_allowed,
    SUM(c.billed_amount)       AS medical_billed,
    COUNT(DISTINCT CASE WHEN c.claim_type = 'Institutional IP' THEN c.claim_id END) AS inpatient_claims,
    COUNT(DISTINCT CASE WHEN c.claim_type = 'ER' THEN c.claim_id END)              AS er_claims
  FROM group_enrollment ge
  INNER JOIN ${catalog}.${schema}.silver_claims_medical c
    ON ge.member_id = c.member_id
  GROUP BY ge.group_id
),

group_pharmacy AS (
  SELECT
    ge.group_id,
    COUNT(DISTINCT p.claim_id) AS pharmacy_claim_count,
    SUM(p.plan_paid)           AS pharmacy_paid,
    SUM(p.total_cost)          AS pharmacy_total_cost,
    SUM(CASE WHEN p.is_specialty THEN p.plan_paid ELSE 0 END) AS specialty_rx_paid
  FROM group_enrollment ge
  INNER JOIN ${catalog}.${schema}.silver_claims_pharmacy p
    ON ge.member_id = p.member_id
  GROUP BY ge.group_id
),

group_summary AS (
  SELECT
    ge.group_id,
    COUNT(DISTINCT ge.member_id)                              AS total_members,
    COUNT(DISTINCT CASE WHEN ge.is_active THEN ge.member_id END) AS active_members,
    SUM(ge.monthly_premium * ge.coverage_months)              AS total_premium_revenue,
    SUM(ge.coverage_months)                                   AS total_member_months
  FROM group_enrollment ge
  GROUP BY ge.group_id
)

SELECT
  gs.group_id,
  g.group_name,
  g.industry,
  g.group_size_tier,
  g.funding_type,
  g.state,
  gs.total_members,
  gs.active_members,
  gs.total_member_months,
  gs.total_premium_revenue,
  COALESCE(gm.medical_paid, 0) + COALESCE(gp.pharmacy_paid, 0) AS total_claims_paid,
  COALESCE(gm.medical_paid, 0)                                  AS medical_claims_paid,
  COALESCE(gp.pharmacy_paid, 0)                                 AS pharmacy_claims_paid,
  COALESCE(gp.specialty_rx_paid, 0)                              AS specialty_rx_paid,
  COALESCE(gm.medical_claim_count, 0)                            AS medical_claim_count,
  COALESCE(gp.pharmacy_claim_count, 0)                           AS pharmacy_claim_count,
  COALESCE(gm.inpatient_claims, 0)                               AS inpatient_claims,
  COALESCE(gm.er_claims, 0)                                      AS er_claims,
  -- PMPM metrics
  ROUND((COALESCE(gm.medical_paid, 0) + COALESCE(gp.pharmacy_paid, 0))
    / NULLIF(gs.total_member_months, 0), 2)                      AS claims_pmpm,
  ROUND(COALESCE(gm.medical_paid, 0) / NULLIF(gs.total_member_months, 0), 2)  AS medical_pmpm,
  ROUND(COALESCE(gp.pharmacy_paid, 0) / NULLIF(gs.total_member_months, 0), 2) AS pharmacy_pmpm,
  -- Loss ratio
  ROUND((COALESCE(gm.medical_paid, 0) + COALESCE(gp.pharmacy_paid, 0))
    / NULLIF(gs.total_premium_revenue, 0), 4)                    AS loss_ratio,
  -- Utilization per 1000 member-months (normalizes for enrollment duration)
  ROUND(COALESCE(gm.inpatient_claims, 0) * 1000.0
    / NULLIF(gs.total_member_months, 0), 1)                      AS ip_admits_per_1000,
  ROUND(COALESCE(gm.er_claims, 0) * 1000.0
    / NULLIF(gs.total_member_months, 0), 1)                      AS er_visits_per_1000
FROM group_summary gs
INNER JOIN ${catalog}.${schema}.silver_groups g
  ON gs.group_id = g.group_id
LEFT JOIN group_medical gm
  ON gs.group_id = gm.group_id
LEFT JOIN group_pharmacy gp
  ON gs.group_id = gp.group_id;

-- -----------------------------------------------------------------------------
-- gold_group_stop_loss — Specific & Aggregate Stop-Loss Tracking
-- -----------------------------------------------------------------------------
-- Identifies claimants exceeding specific stop-loss attachment points and
-- groups approaching aggregate attachment thresholds.
-- Key view for stop-loss reinsurance and excess risk management.
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_group_stop_loss
COMMENT 'Stop-loss analysis by employer group: identifies high-cost claimants exceeding specific attachment points and groups approaching aggregate thresholds. Used for reinsurance and excess risk management.'
AS
WITH member_annual_claims AS (
  SELECT
    e.group_number AS group_id,
    c.member_id,
    YEAR(c.service_from_date) AS claim_year,
    SUM(c.paid_amount)        AS annual_paid
  FROM ${catalog}.${schema}.silver_claims_medical c
  INNER JOIN ${catalog}.${schema}.silver_enrollment e
    ON c.member_id = e.member_id
  WHERE e.group_number IS NOT NULL
  GROUP BY e.group_number, c.member_id, YEAR(c.service_from_date)
),

group_annual AS (
  SELECT
    group_id,
    claim_year,
    SUM(annual_paid)                                   AS total_group_claims,
    COUNT(DISTINCT member_id)                          AS member_count,
    MAX(annual_paid)                                   AS max_claimant_paid,
    COUNT(DISTINCT CASE WHEN annual_paid > 50000 THEN member_id END) AS high_cost_claimants
  FROM member_annual_claims
  GROUP BY group_id, claim_year
),

-- Pre-compute specific stop-loss metrics by joining member claims to group thresholds
specific_sl AS (
  SELECT
    mac.group_id,
    mac.claim_year,
    COUNT(DISTINCT mac.member_id) AS members_exceeding_specific_sl,
    COALESCE(SUM(mac.annual_paid - g.specific_stop_loss_attachment), 0) AS specific_sl_excess_amount
  FROM member_annual_claims mac
  INNER JOIN ${catalog}.${schema}.silver_groups g
    ON mac.group_id = g.group_id
  WHERE g.specific_stop_loss_attachment IS NOT NULL
    AND mac.annual_paid > g.specific_stop_loss_attachment
  GROUP BY mac.group_id, mac.claim_year
)

SELECT
  ga.group_id,
  g.group_name,
  g.funding_type,
  g.group_size_tier,
  ga.claim_year,
  ga.total_group_claims,
  ga.member_count,
  ga.max_claimant_paid,
  ga.high_cost_claimants,
  g.specific_stop_loss_attachment,
  g.aggregate_stop_loss_attachment_pct,
  g.expected_annual_claims,
  -- Specific stop-loss: count of members exceeding the per-claimant threshold
  COALESCE(ssl.members_exceeding_specific_sl, 0) AS members_exceeding_specific_sl,
  -- Specific stop-loss: total excess above attachment
  COALESCE(ssl.specific_sl_excess_amount, 0)     AS specific_sl_excess_amount,
  -- Aggregate stop-loss: actual vs threshold
  ROUND(g.expected_annual_claims * COALESCE(g.aggregate_stop_loss_attachment_pct, 1.25), 2)
    AS aggregate_sl_threshold,
  CASE
    WHEN ga.total_group_claims >
      g.expected_annual_claims * COALESCE(g.aggregate_stop_loss_attachment_pct, 1.25)
    THEN ga.total_group_claims -
      g.expected_annual_claims * COALESCE(g.aggregate_stop_loss_attachment_pct, 1.25)
    ELSE 0
  END AS aggregate_sl_excess_amount,
  -- Aggregate attachment ratio (actual / threshold)
  ROUND(ga.total_group_claims
    / NULLIF(g.expected_annual_claims * COALESCE(g.aggregate_stop_loss_attachment_pct, 1.25), 0), 4)
    AS aggregate_attachment_ratio
FROM group_annual ga
INNER JOIN ${catalog}.${schema}.silver_groups g
  ON ga.group_id = g.group_id
LEFT JOIN specific_sl ssl
  ON ga.group_id = ssl.group_id
  AND ga.claim_year = ssl.claim_year
WHERE g.funding_type IN ('Self-Funded', 'Level-Funded');

-- -----------------------------------------------------------------------------
-- gold_group_renewal — Renewal Analytics & Trend Factors
-- -----------------------------------------------------------------------------
-- Projects renewal premiums based on claims experience, trend factors, and
-- credibility weighting. Used by actuaries and account management for
-- group renewal pricing.
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_group_renewal
COMMENT 'Group renewal analytics: projected vs actual claims, trend factors, loss ratios, and credibility-weighted renewal premiums. Used for group renewal pricing and account reviews.'
AS
WITH group_premium AS (
  -- Premium aggregated separately to avoid LEFT JOIN fan-out with claims
  SELECT
    e.group_number AS group_id,
    COUNT(DISTINCT e.member_id) AS enrolled_members,
    SUM(e.monthly_premium * e.coverage_months) AS total_premium
  FROM ${catalog}.${schema}.silver_enrollment e
  WHERE e.group_number IS NOT NULL
  GROUP BY e.group_number
),

group_claims AS (
  -- Claims aggregated separately per group via enrollment join
  SELECT
    e.group_number AS group_id,
    SUM(c.paid_amount) AS total_claims_paid
  FROM ${catalog}.${schema}.silver_claims_medical c
  INNER JOIN ${catalog}.${schema}.silver_enrollment e
    ON c.member_id = e.member_id
  WHERE e.group_number IS NOT NULL
  GROUP BY e.group_number
),

group_experience AS (
  SELECT
    gp.group_id,
    COALESCE(gc.total_claims_paid, 0) AS total_claims_paid,
    gp.enrolled_members,
    gp.total_premium
  FROM group_premium gp
  LEFT JOIN group_claims gc ON gp.group_id = gc.group_id
)

SELECT
  ge.group_id,
  g.group_name,
  g.industry,
  g.group_size_tier,
  g.funding_type,
  g.state,
  g.group_size,
  ge.enrolled_members,
  ge.total_premium,
  COALESCE(ge.total_claims_paid, 0) AS total_claims_paid,
  g.expected_annual_claims,
  g.admin_fee_pmpm,
  g.stop_loss_premium_pmpm,
  g.effective_date,
  g.renewal_date,
  -- Loss ratio
  ROUND(COALESCE(ge.total_claims_paid, 0)
    / NULLIF(ge.total_premium, 0), 4) AS loss_ratio,
  -- Claims PMPM
  ROUND(COALESCE(ge.total_claims_paid, 0)
    / NULLIF(ge.enrolled_members * 12, 0), 2) AS actual_claims_pmpm,
  -- Expected claims PMPM (from group contract)
  ROUND(g.expected_annual_claims / NULLIF(g.group_size * 12, 0), 2) AS expected_claims_pmpm,
  -- Actual vs expected ratio
  ROUND(COALESCE(ge.total_claims_paid, 0)
    / NULLIF(g.expected_annual_claims, 0), 4) AS actual_to_expected_ratio,
  -- Medical trend factor (industry standard ~6-8% annually)
  ROUND(1.0 + (ABS(HASH(g.group_id)) % 40 + 40) / 1000.0, 3) AS medical_trend_factor,
  -- Credibility factor based on group size (larger groups = higher credibility)
  CASE
    WHEN g.group_size >= 1000 THEN 1.00
    WHEN g.group_size >= 500  THEN 0.90
    WHEN g.group_size >= 250  THEN 0.75
    WHEN g.group_size >= 100  THEN 0.60
    WHEN g.group_size >= 50   THEN 0.40
    ELSE 0.25
  END AS credibility_factor,
  -- Projected renewal premium (credibility-weighted blend of experience and manual rate)
  ROUND(
    CASE
      WHEN g.group_size >= 1000 THEN
        -- High credibility: mostly experience-rated
        (COALESCE(ge.total_claims_paid, 0) / NULLIF(ge.enrolled_members * 12, 0))
        * (1.0 + (ABS(HASH(g.group_id)) % 40 + 40) / 1000.0)
        + g.admin_fee_pmpm
        + COALESCE(g.stop_loss_premium_pmpm, 0)
      WHEN g.group_size >= 50 THEN
        -- Blended: credibility-weighted mix
        (
          (COALESCE(ge.total_claims_paid, 0) / NULLIF(ge.enrolled_members * 12, 0))
          * (CASE WHEN g.group_size >= 500 THEN 0.90 WHEN g.group_size >= 250 THEN 0.75 WHEN g.group_size >= 100 THEN 0.60 ELSE 0.40 END)
          + (g.expected_annual_claims / NULLIF(g.group_size * 12, 0))
          * (1.0 - CASE WHEN g.group_size >= 500 THEN 0.90 WHEN g.group_size >= 250 THEN 0.75 WHEN g.group_size >= 100 THEN 0.60 ELSE 0.40 END)
        )
        * (1.0 + (ABS(HASH(g.group_id)) % 40 + 40) / 1000.0)
        + g.admin_fee_pmpm
        + COALESCE(g.stop_loss_premium_pmpm, 0)
      ELSE
        -- Low credibility: mostly manual rate
        (g.expected_annual_claims / NULLIF(g.group_size * 12, 0))
        * (1.0 + (ABS(HASH(g.group_id)) % 40 + 40) / 1000.0)
        + g.admin_fee_pmpm
        + COALESCE(g.stop_loss_premium_pmpm, 0)
    END,
    2
  ) AS projected_renewal_pmpm,
  -- Renewal action recommendation
  CASE
    WHEN COALESCE(ge.total_claims_paid, 0) / NULLIF(ge.total_premium, 0) > 1.0
      THEN 'Rate Increase Required'
    WHEN COALESCE(ge.total_claims_paid, 0) / NULLIF(ge.total_premium, 0) > 0.85
      THEN 'Moderate Increase'
    WHEN COALESCE(ge.total_claims_paid, 0) / NULLIF(ge.total_premium, 0) > 0.70
      THEN 'Trend-Only Increase'
    ELSE 'Favorable - Hold or Decrease'
  END AS renewal_action
FROM group_experience ge
INNER JOIN ${catalog}.${schema}.silver_groups g
  ON ge.group_id = g.group_id;
