CREATE TABLE IF NOT EXISTS {catalog}.{schema}.simulations (
    simulation_id STRING,
    simulation_name STRING NOT NULL,
    simulation_type STRING NOT NULL,
    created_by STRING NOT NULL,
    parameters STRING NOT NULL DEFAULT '{}',
    results STRING,
    baseline_snapshot STRING,
    status STRING NOT NULL DEFAULT 'draft',
    scope_lob STRING,
    scope_group_id STRING,
    notes STRING,
    created_at TIMESTAMP DEFAULT current_timestamp(),
    updated_at TIMESTAMP DEFAULT current_timestamp()
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.comparison_sets (
    comparison_id STRING,
    comparison_name STRING NOT NULL,
    created_by STRING NOT NULL,
    simulation_ids STRING NOT NULL,
    notes STRING,
    created_at TIMESTAMP DEFAULT current_timestamp()
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.simulation_audit_log (
    audit_id STRING,
    simulation_id STRING NOT NULL,
    action STRING NOT NULL,
    actor STRING NOT NULL,
    details STRING,
    created_at TIMESTAMP DEFAULT current_timestamp()
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE OR REPLACE VIEW {catalog}.{schema}.v_simulation_list AS
SELECT
    s.simulation_id,
    s.simulation_name,
    s.simulation_type::string,
    s.status::string,
    s.scope_lob,
    s.scope_group_id,
    s.created_by,
    s.notes,
    
    (get_json_object(s.results, '$.narrative'))::string AS narrative,
    s.created_at,
    s.updated_at
FROM simulations s
ORDER BY s.created_at DESC;

CREATE OR REPLACE VIEW {catalog}.{schema}.v_comparison_detail AS
SELECT
    c.comparison_id,
    c.comparison_name,
    c.created_by,
    c.simulation_ids,
    c.notes,
    c.created_at
FROM comparison_sets c
ORDER BY c.created_at DESC;

-- ===================================================================
-- Phase 2 — funding quotes, approval routing, factor governance
-- ===================================================================

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.funding_quotes (
    quote_id STRING,
    group_name STRING,
    funding_arrangement STRING NOT NULL,
    lob STRING,
    scope_group_id STRING,
    inputs STRING,
    result STRING,
    total_annual_cost DOUBLE,
    dollar_impact DOUBLE,
    status STRING NOT NULL DEFAULT 'draft',
    created_by STRING NOT NULL,
    created_at TIMESTAMP DEFAULT current_timestamp(),
    updated_at TIMESTAMP DEFAULT current_timestamp()
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.approval_requests (
    approval_id STRING,
    subject STRING NOT NULL,
    decision_type STRING NOT NULL,
    group_id STRING,
    lob STRING,
    dollar_impact DOUBLE,
    rate_change_pct DOUBLE,
    required_tier INT,
    required_role STRING,
    status STRING NOT NULL DEFAULT 'pending',
    requested_by STRING NOT NULL,
    decided_by STRING,
    decision_notes STRING,
    context STRING,
    created_at TIMESTAMP DEFAULT current_timestamp(),
    decided_at TIMESTAMP
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.approval_audit_log (
    audit_id STRING,
    approval_id STRING NOT NULL,
    action STRING NOT NULL,
    actor STRING NOT NULL,
    details STRING,
    created_at TIMESTAMP DEFAULT current_timestamp()
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.factor_versions (
    version_id STRING,
    version INT NOT NULL,
    status STRING NOT NULL DEFAULT 'draft',
    factors STRING NOT NULL,
    source STRING,
    notes STRING,
    created_by STRING NOT NULL,
    approved_by STRING,
    published_by STRING,
    created_at TIMESTAMP DEFAULT current_timestamp(),
    approved_at TIMESTAMP,
    published_at TIMESTAMP
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

-- ===================================================================
-- Phase 3 — negotiation / re-rate lineage
-- ===================================================================

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.quote_revisions (
    revision_id STRING,
    quote_id STRING NOT NULL,
    instruction STRING,
    param_changes STRING,
    result STRING,
    total_annual_cost DOUBLE,
    created_by STRING NOT NULL,
    created_at TIMESTAMP DEFAULT current_timestamp()
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

-- ===================================================================
-- Phase 4 (optional) — Agentic memory / Underwriting Digital Twin
-- Delta-backed fallback for the Lakebase + pgvector memory layer; the
-- recall interface (twin_memory.py) is abstracted so a Lakebase/pgvector
-- backend can be swapped in without touching the preflight loop.
-- ===================================================================

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.decision_memory (
    decision_id STRING,
    analyst_id STRING NOT NULL,
    funding_arrangement STRING,
    lob STRING,
    group_id STRING,
    group_size INT,
    group_size_band STRING,
    industry STRING,
    scenario_chosen STRING,
    parameters STRING,
    projected STRING,
    projected_margin DOUBLE,
    projected_mlr DOUBLE,
    dollar_impact DOUBLE,
    rationale STRING,
    twin_verdict STRING,
    analyst_response STRING,
    status STRING NOT NULL DEFAULT 'candidate',
    created_at TIMESTAMP DEFAULT current_timestamp()
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.decision_embeddings (
    decision_id STRING,
    embedding STRING NOT NULL,
    embed_text STRING,
    created_at TIMESTAMP DEFAULT current_timestamp()
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.analyst_profile (
    analyst_id STRING,
    risk_appetite STRING,
    typical_margin DOUBLE,
    typical_overrides STRING,
    decision_count INT DEFAULT 0,
    updated_at TIMESTAMP DEFAULT current_timestamp()
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.org_guardrails (
    guardrail_id STRING,
    version INT NOT NULL,
    rule_type STRING NOT NULL,
    scope STRING,
    threshold DOUBLE,
    severity STRING,
    active BOOLEAN DEFAULT TRUE,
    approved_by STRING,
    approved_at TIMESTAMP DEFAULT current_timestamp()
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.outcome_feedback (
    feedback_id STRING,
    decision_id STRING NOT NULL,
    actual_mlr DOUBLE,
    actual_margin DOUBLE,
    retained BOOLEAN,
    note STRING,
    observed_at TIMESTAMP DEFAULT current_timestamp()
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

