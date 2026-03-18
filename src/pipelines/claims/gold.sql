-- =============================================================================
-- Red Bricks Insurance — Claims Domain: Gold Layer
-- =============================================================================
-- Business-ready aggregated views for analytics and reporting.
-- Materialized views refresh automatically when upstream silver tables update.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Gold: Medical Claims Summary
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_claims_summary
COMMENT 'Monthly medical claims summary by claim type and status. Includes denial rate and average paid per claim for executive dashboards.'
TBLPROPERTIES (
  'quality' = 'gold',
  'domain'  = 'claims'
)
AS
SELECT
  claim_type,
  claim_status,
  service_year_month,

  COUNT(DISTINCT claim_id)          AS total_claims,
  COUNT(*)                          AS total_claim_lines,
  SUM(billed_amount)               AS total_billed,
  SUM(allowed_amount)              AS total_allowed,
  SUM(paid_amount)                 AS total_paid,
  SUM(member_responsibility)       AS total_member_responsibility,

  ROUND(SUM(paid_amount) / NULLIF(COUNT(DISTINCT claim_id), 0), 2)
                                    AS avg_paid_per_claim,

  ROUND(
    SUM(CASE WHEN claim_status = 'denied' THEN 1 ELSE 0 END)
    / NULLIF(COUNT(DISTINCT claim_id), 0), 4
  )                                 AS denial_rate

FROM LIVE.silver_claims_medical
GROUP BY
  claim_type,
  claim_status,
  service_year_month;

-- ---------------------------------------------------------------------------
-- Gold: Pharmacy Claims Summary
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_pharmacy_summary
COMMENT 'Monthly pharmacy claims summary by therapeutic class and formulary tier. Tracks specialty fill percentage and average cost per fill.'
TBLPROPERTIES (
  'quality' = 'gold',
  'domain'  = 'claims'
)
AS
SELECT
  therapeutic_class,
  formulary_tier,
  fill_year_month                   AS service_year_month,

  COUNT(*)                          AS total_fills,
  SUM(total_cost)                  AS total_cost,
  ROUND(SUM(total_cost) / NULLIF(COUNT(*), 0), 2)
                                    AS avg_cost_per_fill,

  ROUND(
    SUM(CASE WHEN is_specialty = TRUE THEN 1 ELSE 0 END)
    / NULLIF(COUNT(*), 0), 4
  )                                 AS specialty_fill_pct,

  ROUND(
    SUM(CASE WHEN formulary_tier = 'generic' THEN 1 ELSE 0 END)
    / NULLIF(COUNT(*), 0), 4
  )                                 AS generic_fill_rate

FROM LIVE.silver_claims_pharmacy
GROUP BY
  therapeutic_class,
  formulary_tier,
  fill_year_month;
