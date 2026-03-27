-- =============================================================================
-- Red Bricks Insurance — Gold Analytics: Group Report Card
-- =============================================================================
-- Single-row-per-group executive summary joining experience, stop-loss, renewal,
-- and TCOC data with peer percentile ranks and a composite health score.
-- Used by account executives and sales reps for renewal meeting preparation.
-- Pipeline: gold_analytics (runs after domain pipelines + build_member_months)
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW gold_group_report_card
COMMENT 'One-row-per-group executive report card: experience metrics, stop-loss exposure, renewal projections, TCOC aggregates, peer percentile ranks, and composite health score. Used by sales/account management for employer group renewals.'
AS
WITH group_tcoc AS (
  -- Aggregate member-level TCOC to the group level via enrollment
  SELECT
    e.group_number AS group_id,
    ROUND(AVG(t.tcoc), 2)        AS avg_member_tcoc,
    ROUND(AVG(t.tci), 3)         AS avg_tci,
    COUNT(DISTINCT CASE WHEN t.cost_tier IN ('High Cost', 'Extreme Outlier') THEN t.member_id END)
      AS high_cost_members,
    COUNT(DISTINCT t.member_id)   AS tcoc_member_count,
    ROUND(
      COUNT(DISTINCT CASE WHEN t.cost_tier IN ('High Cost', 'Extreme Outlier') THEN t.member_id END)
      * 100.0 / NULLIF(COUNT(DISTINCT t.member_id), 0), 1
    ) AS pct_high_cost,
    -- Cost tier distribution as JSON-like string
    CONCAT(
      'Extreme Outlier: ', SUM(CASE WHEN t.cost_tier = 'Extreme Outlier' THEN 1 ELSE 0 END),
      ' | High Cost: ', SUM(CASE WHEN t.cost_tier = 'High Cost' THEN 1 ELSE 0 END),
      ' | Rising Risk: ', SUM(CASE WHEN t.cost_tier = 'Rising Risk' THEN 1 ELSE 0 END),
      ' | Expected: ', SUM(CASE WHEN t.cost_tier = 'Expected' THEN 1 ELSE 0 END),
      ' | Low Utilizer: ', SUM(CASE WHEN t.cost_tier = 'Low Utilizer' THEN 1 ELSE 0 END)
    ) AS cost_tier_distribution
  FROM ${catalog}.${schema}.silver_enrollment e
  INNER JOIN ${catalog}.${schema}.gold_member_tcoc t
    ON e.member_id = t.member_id
  WHERE e.group_number IS NOT NULL
  GROUP BY e.group_number
),

base AS (
  SELECT
    -- Experience fields
    exp.group_id,
    exp.group_name,
    exp.industry,
    exp.group_size_tier,
    exp.funding_type,
    exp.state,
    exp.total_members,
    exp.active_members,
    exp.total_member_months,
    exp.total_premium_revenue,
    exp.total_claims_paid,
    exp.medical_claims_paid,
    exp.pharmacy_claims_paid,
    exp.claims_pmpm,
    exp.medical_pmpm,
    exp.pharmacy_pmpm,
    exp.loss_ratio,
    exp.ip_admits_per_1000,
    exp.er_visits_per_1000,
    -- Stop-loss fields (latest year)
    sl.high_cost_claimants,
    sl.specific_sl_excess_amount   AS specific_sl_excess,
    sl.aggregate_attachment_ratio,
    -- Renewal fields
    ren.actual_to_expected_ratio   AS actual_to_expected,
    ren.credibility_factor,
    ren.projected_renewal_pmpm,
    ren.renewal_action,
    ren.medical_trend_factor       AS trend_factor,
    ren.renewal_date,
    -- TCOC fields
    gt.avg_member_tcoc,
    gt.avg_tci,
    gt.pct_high_cost,
    gt.high_cost_members,
    gt.cost_tier_distribution
  FROM ${catalog}.${schema}.gold_group_experience exp
  LEFT JOIN (
    -- Latest year per group from stop-loss
    SELECT *
    FROM ${catalog}.${schema}.gold_group_stop_loss
    QUALIFY ROW_NUMBER() OVER (PARTITION BY group_id ORDER BY claim_year DESC) = 1
  ) sl ON exp.group_id = sl.group_id
  LEFT JOIN ${catalog}.${schema}.gold_group_renewal ren
    ON exp.group_id = ren.group_id
  LEFT JOIN group_tcoc gt
    ON exp.group_id = gt.group_id
),

-- Peer percentile ranks: how does this group compare to peers in the same
-- industry and group_size_tier?  When a cohort has fewer than 3 peers,
-- fall back to group_size_tier only so that singleton cohorts don't
-- get a meaningless 0th-percentile score.
peer_counts AS (
  SELECT
    industry,
    group_size_tier,
    COUNT(*) AS cohort_size
  FROM base
  GROUP BY industry, group_size_tier
),

ranked AS (
  SELECT
    b.*,
    -- Use narrow cohort (industry + size tier) when >= 3 peers; else fall back to size tier only
    ROUND(
      CASE WHEN pc.cohort_size >= 3
        THEN PERCENT_RANK() OVER (PARTITION BY b.industry, b.group_size_tier ORDER BY b.claims_pmpm)
        ELSE PERCENT_RANK() OVER (PARTITION BY b.group_size_tier ORDER BY b.claims_pmpm)
      END, 3) AS claims_pmpm_pctl,
    ROUND(
      CASE WHEN pc.cohort_size >= 3
        THEN PERCENT_RANK() OVER (PARTITION BY b.industry, b.group_size_tier ORDER BY b.loss_ratio)
        ELSE PERCENT_RANK() OVER (PARTITION BY b.group_size_tier ORDER BY b.loss_ratio)
      END, 3) AS loss_ratio_pctl,
    ROUND(
      CASE WHEN pc.cohort_size >= 3
        THEN PERCENT_RANK() OVER (PARTITION BY b.industry, b.group_size_tier ORDER BY b.er_visits_per_1000)
        ELSE PERCENT_RANK() OVER (PARTITION BY b.group_size_tier ORDER BY b.er_visits_per_1000)
      END, 3) AS er_visits_pctl,
    ROUND(
      CASE WHEN pc.cohort_size >= 3
        THEN PERCENT_RANK() OVER (PARTITION BY b.industry, b.group_size_tier ORDER BY b.avg_tci)
        ELSE PERCENT_RANK() OVER (PARTITION BY b.group_size_tier ORDER BY b.avg_tci)
      END, 3) AS tci_pctl
  FROM base b
  INNER JOIN peer_counts pc
    ON b.industry = pc.industry AND b.group_size_tier = pc.group_size_tier
)

SELECT
  *,
  -- Group Health Score (1-100): composite of loss ratio rank, utilization rank,
  -- and TCOC rank. Lower percentile = better = higher health score.
  ROUND(
    (
      (1.0 - COALESCE(loss_ratio_pctl, 0.5)) * 0.40   -- 40% weight on loss ratio
      + (1.0 - COALESCE(er_visits_pctl, 0.5)) * 0.30  -- 30% weight on ER utilization
      + (1.0 - COALESCE(tci_pctl, 0.5)) * 0.30         -- 30% weight on TCI
    ) * 100, 0
  ) AS group_health_score
FROM ranked;
