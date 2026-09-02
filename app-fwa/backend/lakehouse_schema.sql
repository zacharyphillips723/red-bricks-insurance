CREATE TABLE IF NOT EXISTS {catalog}.{schema}.fraud_investigators (
    investigator_id STRING,
    email STRING NOT NULL,
    display_name STRING NOT NULL,
    role STRING NOT NULL,
    department STRING,
    max_caseload INT DEFAULT 30,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT current_timestamp(),
    updated_at TIMESTAMP DEFAULT current_timestamp()
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.fwa_investigations (
    investigation_id STRING,
    investigation_type STRING NOT NULL,
    target_type STRING NOT NULL,
    target_id STRING NOT NULL,
    target_name STRING NOT NULL,
    fraud_types STRING NOT NULL,
    severity STRING NOT NULL,
    source STRING NOT NULL DEFAULT 'Rules Engine',
    status STRING NOT NULL DEFAULT 'Open',
    assigned_investigator_id STRING,
    assigned_at TIMESTAMP,
    status_changed_at TIMESTAMP DEFAULT current_timestamp(),
    estimated_overpayment DECIMAL(12,2) DEFAULT 0,
    confirmed_overpayment DECIMAL(12,2),
    recovered_amount DECIMAL(12,2) DEFAULT 0,
    claims_involved_count INT DEFAULT 0,
    investigation_summary STRING,
    evidence_summary STRING,
    recommendation STRING,
    rules_risk_score DECIMAL(4,3),
    ml_risk_score DECIMAL(4,3),
    composite_risk_score DECIMAL(4,3),
    created_at TIMESTAMP DEFAULT current_timestamp(),
    updated_at TIMESTAMP DEFAULT current_timestamp(),
    closed_at TIMESTAMP
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.investigation_audit_log (
    audit_id STRING,
    investigation_id STRING NOT NULL,
    investigator_id STRING,
    action_type STRING NOT NULL,
    previous_status STRING,
    new_status STRING,
    note STRING,
    metadata_json STRING,
    created_at TIMESTAMP DEFAULT current_timestamp()
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.investigation_evidence (
    evidence_id STRING,
    investigation_id STRING NOT NULL,
    evidence_type STRING NOT NULL,
    reference_id STRING,
    description STRING NOT NULL,
    detail_json STRING,
    added_by STRING,
    created_at TIMESTAMP DEFAULT current_timestamp()
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE OR REPLACE VIEW {catalog}.{schema}.v_investigation_queue AS
SELECT
    i.investigation_id,
    i.investigation_type::string,
    i.target_type,
    i.target_id,
    i.target_name,
    i.fraud_types,
    i.severity::string,
    i.status::string,
    i.source::string,
    i.estimated_overpayment,
    i.claims_involved_count,
    i.composite_risk_score,
    i.rules_risk_score,
    i.ml_risk_score,
    inv.display_name AS investigator_name,
    inv.role AS investigator_role,
    i.assigned_at,
    i.created_at,
    current_timestamp() - i.created_at AS time_open
FROM fwa_investigations i
LEFT JOIN fraud_investigators inv ON i.assigned_investigator_id = inv.investigator_id
ORDER BY
    CASE i.severity
        WHEN 'Critical' THEN 1
        WHEN 'High'     THEN 2
        WHEN 'Medium'   THEN 3
        WHEN 'Low'      THEN 4
    END,
    i.composite_risk_score DESC NULLS LAST,
    i.created_at ASC;

CREATE OR REPLACE VIEW {catalog}.{schema}.v_investigator_caseload AS
SELECT
    inv.investigator_id,
    inv.display_name,
    inv.role,
    inv.max_caseload,
    COUNT(i.investigation_id) FILTER (WHERE i.status NOT IN (
        'Closed — Confirmed Fraud', 'Closed — No Fraud', 'Closed — Insufficient Evidence'
    )) AS active_cases,
    COUNT(i.investigation_id) FILTER (WHERE i.severity = 'Critical' AND i.status NOT IN (
        'Closed — Confirmed Fraud', 'Closed — No Fraud', 'Closed — Insufficient Evidence'
    )) AS critical_cases,
    COUNT(i.investigation_id) FILTER (WHERE i.status = 'Evidence Gathering') AS evidence_gathering,
    COUNT(i.investigation_id) FILTER (WHERE i.status = 'Recovery In Progress') AS recovery_in_progress,
    COALESCE(SUM(i.estimated_overpayment) FILTER (WHERE i.status NOT IN (
        'Closed — Confirmed Fraud', 'Closed — No Fraud', 'Closed — Insufficient Evidence'
    )), 0) AS total_active_overpayment,
    COALESCE(SUM(i.recovered_amount), 0) AS total_recovered,
    inv.max_caseload - COUNT(i.investigation_id) FILTER (WHERE i.status NOT IN (
        'Closed — Confirmed Fraud', 'Closed — No Fraud', 'Closed — Insufficient Evidence'
    )) AS available_capacity
FROM fraud_investigators inv
LEFT JOIN fwa_investigations i ON inv.investigator_id = i.assigned_investigator_id
WHERE inv.is_active = TRUE
GROUP BY inv.investigator_id, inv.display_name, inv.role, inv.max_caseload;

CREATE OR REPLACE VIEW {catalog}.{schema}.v_investigation_detail AS
SELECT
    i.investigation_id,
    i.investigation_type::string,
    i.target_type,
    i.target_id,
    i.target_name,
    i.fraud_types,
    i.severity::string,
    i.status::string,
    i.source::string,
    i.estimated_overpayment,
    i.confirmed_overpayment,
    i.recovered_amount,
    i.claims_involved_count,
    i.investigation_summary,
    i.evidence_summary,
    i.recommendation,
    i.rules_risk_score,
    i.ml_risk_score,
    i.composite_risk_score,
    inv.display_name AS investigator_name,
    inv.role AS investigator_role,
    i.assigned_at,
    i.created_at,
    i.updated_at,
    i.closed_at
FROM fwa_investigations i
LEFT JOIN fraud_investigators inv ON i.assigned_investigator_id = inv.investigator_id;

