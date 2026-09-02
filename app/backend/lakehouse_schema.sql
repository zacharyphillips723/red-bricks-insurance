CREATE TABLE IF NOT EXISTS {catalog}.{schema}.care_managers (
    care_manager_id STRING,
    email STRING NOT NULL,
    display_name STRING NOT NULL,
    role STRING NOT NULL,
    department STRING,
    max_caseload INT DEFAULT 50,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT current_timestamp(),
    updated_at TIMESTAMP DEFAULT current_timestamp()
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.risk_stratification_alerts (
    alert_id STRING,
    patient_id STRING NOT NULL,
    mrn STRING,
    member_id STRING,
    risk_tier STRING NOT NULL,
    risk_score DECIMAL(5,2),
    primary_driver STRING NOT NULL,
    secondary_drivers STRING,
    alert_source STRING NOT NULL,
    assigned_care_manager_id STRING,
    assigned_at TIMESTAMP,
    status STRING NOT NULL DEFAULT 'Unassigned',
    status_changed_at TIMESTAMP DEFAULT current_timestamp(),
    max_hba1c DECIMAL(4,1),
    max_blood_glucose DECIMAL(5,1),
    peak_ed_visits_12mo INT,
    last_encounter_date TIMESTAMP,
    last_facility STRING,
    payer STRING,
    active_medications STRING,
    notes STRING,
    created_at TIMESTAMP DEFAULT current_timestamp(),
    updated_at TIMESTAMP DEFAULT current_timestamp(),
    resolved_at TIMESTAMP
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.alert_activity_log (
    activity_id STRING,
    alert_id STRING NOT NULL,
    care_manager_id STRING,
    activity_type STRING NOT NULL,
    previous_status STRING,
    new_status STRING,
    note STRING,
    created_at TIMESTAMP DEFAULT current_timestamp()
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE OR REPLACE VIEW {catalog}.{schema}.v_unassigned_alerts AS
SELECT
    a.alert_id,
    a.patient_id,
    a.mrn,
    a.member_id,
    a.risk_tier,
    a.risk_score,
    a.primary_driver,
    a.alert_source,
    a.payer,
    a.max_hba1c,
    a.max_blood_glucose,
    a.peak_ed_visits_12mo,
    a.last_facility,
    a.created_at,
    current_timestamp() - a.created_at AS time_unassigned
FROM risk_stratification_alerts a
WHERE a.status = 'Unassigned'
ORDER BY
    CASE a.risk_tier
        WHEN 'Critical' THEN 1
        WHEN 'High'     THEN 2
        WHEN 'Elevated' THEN 3
        WHEN 'Moderate' THEN 4
        WHEN 'Low'      THEN 5
    END,
    a.created_at ASC;

CREATE OR REPLACE VIEW {catalog}.{schema}.v_care_manager_caseload AS
SELECT
    cm.care_manager_id,
    cm.display_name,
    cm.role,
    cm.max_caseload,
    COUNT(a.alert_id) FILTER (WHERE a.status NOT IN ('Resolved', 'Closed — Unable to Reach')) AS active_cases,
    COUNT(a.alert_id) FILTER (WHERE a.risk_tier = 'Critical' AND a.status NOT IN ('Resolved', 'Closed — Unable to Reach')) AS critical_cases,
    COUNT(a.alert_id) FILTER (WHERE a.status = 'Outreach Attempted') AS pending_outreach,
    COUNT(a.alert_id) FILTER (WHERE a.status = 'Follow-Up Scheduled') AS pending_followup,
    cm.max_caseload - COUNT(a.alert_id) FILTER (WHERE a.status NOT IN ('Resolved', 'Closed — Unable to Reach')) AS available_capacity
FROM care_managers cm
LEFT JOIN risk_stratification_alerts a ON cm.care_manager_id = a.assigned_care_manager_id
WHERE cm.is_active = TRUE
GROUP BY cm.care_manager_id, cm.display_name, cm.role, cm.max_caseload;

CREATE OR REPLACE VIEW {catalog}.{schema}.v_patient_alert_history AS
SELECT
    a.alert_id,
    a.patient_id,
    a.mrn,
    a.risk_tier,
    a.primary_driver,
    a.alert_source,
    a.status,
    a.status_changed_at,
    a.notes,
    cm.display_name AS care_manager_name,
    cm.role AS care_manager_role,
    a.assigned_at,
    a.resolved_at,
    a.created_at
FROM risk_stratification_alerts a
LEFT JOIN care_managers cm ON a.assigned_care_manager_id = cm.care_manager_id
ORDER BY a.patient_id, a.created_at DESC;

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.conversations (
    conversation_id STRING,
    member_id STRING NOT NULL,
    user_email STRING NOT NULL,
    title STRING,
    message_count INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT current_timestamp(),
    updated_at TIMESTAMP DEFAULT current_timestamp()
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.conversation_messages (
    message_id STRING,
    conversation_id STRING NOT NULL,
    role STRING NOT NULL,
    content STRING NOT NULL,
    metadata STRING DEFAULT '{}',
    created_at TIMESTAMP DEFAULT current_timestamp()
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.agent_feedback (
    feedback_id STRING,
    message_id STRING NOT NULL,
    conversation_id STRING NOT NULL,
    user_email STRING NOT NULL,
    rating STRING NOT NULL,
    comment STRING,
    created_at TIMESTAMP DEFAULT current_timestamp()
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.saved_cohorts (
    cohort_id STRING,
    cohort_name STRING NOT NULL,
    description STRING,
    criteria STRING NOT NULL,
    member_count INT NOT NULL DEFAULT 0,
    created_by STRING NOT NULL DEFAULT 'system',
    created_at TIMESTAMP NOT NULL DEFAULT current_timestamp(),
    updated_at TIMESTAMP NOT NULL DEFAULT current_timestamp()
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

