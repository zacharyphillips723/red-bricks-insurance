-- =============================================================================
-- Red Bricks Insurance — Gold Analytics: Governance-Aware Secure Views
-- =============================================================================
-- Dynamic views that demonstrate Row-Level Security and Column Masking directly
-- in pipeline SQL. These views apply governance policies inline using
-- current_user() and is_account_group_member() so different users see different
-- data — no external configuration needed.
--
-- During a live demo, show the same query returning different results for
-- different personas (full-access analyst vs commercial-only user vs restricted).
--
-- These views use the governance functions already created by
-- setup_uc_governance.py (governance.mask_ssn, governance.filter_by_lob, etc.)
-- but also demonstrate inline governance for teams that want to see the pattern
-- embedded directly in their analytics queries.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Gold: Secure Member Directory (Column Masking Demo)
-- ---------------------------------------------------------------------------
-- Shows how different users see different levels of PHI detail.
-- Full-access users see complete member records.
-- Restricted users see masked SSN, phone, email, DOB, and address.
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_member_directory_secure
COMMENT 'Governance-aware member directory. PHI columns are masked based on user group membership. Demonstrates column-level security for live demos.'
AS
SELECT
  m.member_id,
  m.first_name,
  m.last_name,
  CONCAT(m.first_name, ' ', m.last_name) AS member_name,

  -- Column masking: DOB shows full date for PHI users, year-only for others
  CASE
    WHEN is_account_group_member('phi_full_access') THEN m.date_of_birth
    ELSE MAKE_DATE(YEAR(COALESCE(m.date_of_birth, DATE '2000-01-01')), 1, 1)
  END AS date_of_birth,

  FLOOR(DATEDIFF(CURRENT_DATE(), m.date_of_birth) / 365.25) AS age,
  m.gender,

  -- Column masking: address redacted for non-PHI users
  CASE
    WHEN is_account_group_member('phi_full_access') THEN m.address_line_1
    ELSE '*** REDACTED ***'
  END AS address_line_1,

  m.city,
  m.state,
  m.zip_code,

  -- Column masking: phone and email masked
  CASE
    WHEN is_account_group_member('phi_full_access') THEN m.phone
    ELSE CONCAT('(***) ***-', RIGHT(COALESCE(m.phone, '0000'), 4))
  END AS phone,

  CASE
    WHEN is_account_group_member('phi_full_access') THEN m.email
    ELSE CONCAT(LEFT(COALESCE(m.email, 'x'), 2), '***@***.com')
  END AS email,

  -- Governance metadata: let the user see which access level applies
  CASE
    WHEN is_account_group_member('phi_full_access') THEN 'Full PHI Access'
    ELSE 'Restricted — PHI Masked'
  END AS access_level,

  current_user() AS queried_by

FROM members.silver_members m;

-- ---------------------------------------------------------------------------
-- Gold: Secure Claims Summary (Row-Level Security Demo)
-- ---------------------------------------------------------------------------
-- Row-level security: commercial_only users see only Commercial LOB claims.
-- Full-access users see all lines of business.
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_claims_by_lob_secure
COMMENT 'Line-of-business claims summary with row-level security. Commercial-only users see only their LOB. Demonstrates RLS for live demos.'
AS
SELECT
  e.line_of_business,
  COUNT(DISTINCT c.claim_id)                   AS total_claims,
  COUNT(DISTINCT c.member_id)                  AS unique_members,
  ROUND(SUM(c.paid_amount), 2)                 AS total_paid,
  ROUND(AVG(c.paid_amount), 2)                 AS avg_claim_amount,
  ROUND(SUM(c.allowed_amount), 2)              AS total_allowed,

  -- Show the user what RLS policy applies
  CASE
    WHEN is_account_group_member('phi_full_access') THEN 'Full Access — All LOBs'
    WHEN is_account_group_member('commercial_only') THEN 'Restricted — Commercial Only'
    ELSE 'Default Access'
  END AS access_level,

  current_user() AS queried_by

FROM claims.silver_claims_medical c
INNER JOIN members.silver_enrollment e
  ON c.member_id = e.member_id
WHERE
  -- Row-level security: filter by LOB based on group membership
  CASE
    WHEN is_account_group_member('phi_full_access') THEN TRUE
    WHEN is_account_group_member('commercial_only') THEN e.line_of_business = 'Commercial'
    ELSE TRUE
  END
GROUP BY e.line_of_business;

-- ---------------------------------------------------------------------------
-- Gold: Secure Risk Stratification (Combined RLS + Column Masking)
-- ---------------------------------------------------------------------------
-- Combines both patterns: row filtering by LOB and column masking on PHI.
-- This is the complete governance story for a care management demo.
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_risk_stratification_secure
COMMENT 'Risk stratification with combined RLS (LOB filtering) and column masking (PHI redaction). The complete governance demo view.'
AS
SELECT
  m.member_id,

  -- Column masking on PHI
  CASE
    WHEN is_account_group_member('phi_full_access')
      THEN CONCAT(m.first_name, ' ', m.last_name)
    ELSE CONCAT(LEFT(m.first_name, 1), '***', ' ', LEFT(m.last_name, 1), '***')
  END AS member_name,

  e.line_of_business,
  e.plan_type,
  r.raf_score,
  r.hcc_count,
  r.is_high_risk,

  CASE
    WHEN r.raf_score > 3.0  THEN 'Critical'
    WHEN r.raf_score > 2.5  THEN 'High'
    WHEN r.raf_score > 2.0  THEN 'Elevated'
    ELSE 'Moderate'
  END AS risk_tier,

  -- Governance metadata
  CASE
    WHEN is_account_group_member('phi_full_access') THEN 'Full PHI + All LOBs'
    WHEN is_account_group_member('commercial_only') THEN 'Masked PHI + Commercial Only'
    ELSE 'Default Access'
  END AS access_level,

  current_user() AS queried_by

FROM members.silver_members m
INNER JOIN (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY eligibility_start_date DESC) AS rn
  FROM members.silver_enrollment
) e ON m.member_id = e.member_id AND e.rn = 1
LEFT JOIN (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY raf_score DESC) AS rn
  FROM risk_adjustment.silver_risk_adjustment_member
) r ON m.member_id = r.member_id AND r.rn = 1
WHERE
  -- Row-level security
  CASE
    WHEN is_account_group_member('phi_full_access') THEN TRUE
    WHEN is_account_group_member('commercial_only') THEN e.line_of_business = 'Commercial'
    ELSE TRUE
  END;
