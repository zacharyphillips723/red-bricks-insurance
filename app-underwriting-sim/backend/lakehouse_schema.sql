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

