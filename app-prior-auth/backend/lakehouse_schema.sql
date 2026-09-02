CREATE TABLE IF NOT EXISTS {catalog}.{schema}.pa_reviewers (
    reviewer_id STRING,
    email STRING NOT NULL,
    display_name STRING NOT NULL,
    role STRING NOT NULL,
    department STRING,
    specialty STRING,
    max_caseload INT DEFAULT 50,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT current_timestamp(),
    updated_at TIMESTAMP DEFAULT current_timestamp()
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.pa_review_queue (
    auth_request_id STRING,
    member_id STRING NOT NULL,
    member_name STRING,
    requesting_provider_npi STRING NOT NULL,
    provider_name STRING,
    service_type STRING NOT NULL,
    procedure_code STRING NOT NULL,
    procedure_description STRING,
    diagnosis_codes STRING,
    policy_id STRING,
    policy_name STRING,
    line_of_business STRING,
    clinical_summary STRING,
    urgency STRING NOT NULL DEFAULT 'standard',
    estimated_cost DECIMAL(12,2) DEFAULT 0,
    status STRING NOT NULL DEFAULT 'Pending Review',
    determination_tier STRING,
    assigned_reviewer_id STRING,
    assigned_at TIMESTAMP,
    status_changed_at TIMESTAMP DEFAULT current_timestamp(),
    ai_recommendation STRING,
    ai_confidence DECIMAL(4,3),
    tier1_auto_eligible BOOLEAN DEFAULT FALSE,
    clinical_extraction STRING,
    determination_reason STRING,
    denial_reason_code STRING,
    reviewer_notes STRING,
    request_date TIMESTAMP NOT NULL DEFAULT current_timestamp(),
    determination_date TIMESTAMP,
    turnaround_hours DECIMAL(8,1),
    cms_compliant BOOLEAN DEFAULT TRUE,
    cms_deadline TIMESTAMP,
    appeal_filed BOOLEAN DEFAULT FALSE,
    appeal_date TIMESTAMP,
    appeal_outcome STRING,
    created_at TIMESTAMP DEFAULT current_timestamp(),
    updated_at TIMESTAMP DEFAULT current_timestamp()
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.pa_review_actions (
    action_id STRING,
    auth_request_id STRING NOT NULL,
    reviewer_id STRING,
    action_type STRING NOT NULL,
    previous_status STRING,
    new_status STRING,
    note STRING,
    metadata_json STRING,
    created_at TIMESTAMP DEFAULT current_timestamp()
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE OR REPLACE FUNCTION update_pa_review_timestamps()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = current_timestamp();

CREATE TRIGGER trg_pa_review_updated_at
    BEFORE UPDATE ON pa_review_queue
    FOR EACH ROW EXECUTE FUNCTION update_pa_review_timestamps();

CREATE TRIGGER trg_pa_cms_deadline
    BEFORE INSERT ON pa_review_queue
    FOR EACH ROW EXECUTE FUNCTION set_cms_deadline();

CREATE TRIGGER trg_reviewers_updated_at
    BEFORE UPDATE ON pa_reviewers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_pa();

CREATE OR REPLACE VIEW {catalog}.{schema}.v_review_queue AS
SELECT
    q.auth_request_id,
    q.member_id,
    q.member_name,
    q.requesting_provider_npi,
    q.provider_name,
    q.service_type,
    q.procedure_code,
    q.procedure_description,
    q.diagnosis_codes,
    q.policy_name,
    q.line_of_business,
    q.urgency::string,
    q.estimated_cost,
    q.status::string,
    q.determination_tier::string,
    q.ai_recommendation,
    q.ai_confidence,
    q.tier1_auto_eligible,
    r.display_name AS reviewer_name,
    r.role::string AS reviewer_role,
    q.assigned_at,
    q.request_date,
    q.cms_deadline,
    q.cms_compliant,
    current_timestamp() - q.request_date AS time_open,
    timestampdiff(SECOND, current_timestamp(), q.cms_deadline) / 3600.0 AS hours_until_deadline
FROM pa_review_queue q
LEFT JOIN pa_reviewers r ON q.assigned_reviewer_id = r.reviewer_id
ORDER BY
    CASE q.urgency
        WHEN 'expedited' THEN 1
        WHEN 'standard'  THEN 2
        WHEN 'retrospective' THEN 3
    END,
    q.cms_deadline ASC NULLS LAST,
    q.request_date ASC;

CREATE OR REPLACE VIEW {catalog}.{schema}.v_reviewer_caseload AS
SELECT
    r.reviewer_id,
    r.display_name,
    r.role::string,
    r.specialty,
    r.max_caseload,
    COUNT(q.auth_request_id) FILTER (WHERE q.status IN (
        'Pending Review', 'In Review', 'Additional Info Requested', 'Peer Review Requested'
    )) AS active_cases,
    COUNT(q.auth_request_id) FILTER (WHERE q.urgency = 'expedited' AND q.status IN (
        'Pending Review', 'In Review', 'Additional Info Requested'
    )) AS expedited_cases,
    COUNT(q.auth_request_id) FILTER (WHERE q.status = 'In Review') AS in_review,
    COUNT(q.auth_request_id) FILTER (WHERE q.status = 'Additional Info Requested') AS awaiting_info,
    r.max_caseload - COUNT(q.auth_request_id) FILTER (WHERE q.status IN (
        'Pending Review', 'In Review', 'Additional Info Requested', 'Peer Review Requested'
    )) AS available_capacity
FROM pa_reviewers r
LEFT JOIN pa_review_queue q ON r.reviewer_id = q.assigned_reviewer_id
WHERE r.is_active = TRUE
GROUP BY r.reviewer_id, r.display_name, r.role, r.specialty, r.max_caseload;

CREATE OR REPLACE VIEW {catalog}.{schema}.v_review_detail AS
SELECT
    q.auth_request_id,
    q.member_id,
    q.member_name,
    q.requesting_provider_npi,
    q.provider_name,
    q.service_type,
    q.procedure_code,
    q.procedure_description,
    q.diagnosis_codes,
    q.policy_id,
    q.policy_name,
    q.line_of_business,
    q.clinical_summary,
    q.urgency::string,
    q.estimated_cost,
    q.status::string,
    q.determination_tier::string,
    q.assigned_reviewer_id,
    r.display_name AS reviewer_name,
    r.role::string AS reviewer_role,
    q.assigned_at,
    q.ai_recommendation,
    q.ai_confidence,
    q.tier1_auto_eligible,
    q.clinical_extraction,
    q.determination_reason,
    q.denial_reason_code,
    q.reviewer_notes,
    q.request_date,
    q.determination_date,
    q.turnaround_hours,
    q.cms_compliant,
    q.cms_deadline,
    q.appeal_filed,
    q.appeal_date,
    q.appeal_outcome,
    q.created_at,
    q.updated_at,
    timestampdiff(SECOND, current_timestamp(), q.cms_deadline) / 3600.0 AS hours_until_deadline
FROM pa_review_queue q
LEFT JOIN pa_reviewers r ON q.assigned_reviewer_id = r.reviewer_id;

CREATE OR REPLACE VIEW {catalog}.{schema}.v_cms_compliance_summary AS
SELECT
    urgency::string,
    COUNT(*) AS total_requests,
    SUM(CASE WHEN status IN ('Approved', 'Denied', 'Partially Approved') THEN 1 ELSE 0 END) AS determined,
    SUM(CASE WHEN cms_compliant THEN 1 ELSE 0 END) AS compliant,
    ROUND(
        SUM(CASE WHEN cms_compliant THEN 1 ELSE 0 END) * 100.0
        / NULLIF(SUM(CASE WHEN status IN ('Approved', 'Denied', 'Partially Approved') THEN 1 ELSE 0 END), 0),
        2
    ) AS compliance_rate_pct,
    ROUND(AVG(turnaround_hours) FILTER (WHERE turnaround_hours IS NOT NULL), 1) AS avg_turnaround_hours,
    SUM(CASE WHEN status IN ('Pending Review', 'In Review', 'Additional Info Requested')
             AND cms_deadline < current_timestamp() THEN 1 ELSE 0 END) AS overdue_count
FROM pa_review_queue
GROUP BY urgency;

ALTER TABLE {catalog}.{schema}.pa_review_queue ADD COLUMN IF NOT EXISTS criteria_source STRING;

ALTER TABLE {catalog}.{schema}.pa_review_queue ADD COLUMN IF NOT EXISTS criteria_version STRING;

ALTER TABLE {catalog}.{schema}.pa_review_queue ADD COLUMN IF NOT EXISTS criteria_effective_date DATE;

ALTER TABLE {catalog}.{schema}.pa_review_queue ADD COLUMN IF NOT EXISTS determination_reason_external STRING;

ALTER TABLE {catalog}.{schema}.pa_review_queue ADD COLUMN IF NOT EXISTS reviewer_notes_internal STRING;

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.medical_criteria_versions (
    criteria_id STRING,
    criteria_source STRING NOT NULL,
    criteria_set STRING NOT NULL,
    version_label STRING NOT NULL,
    service_type STRING,
    procedure_codes STRING,
    effective_start_date DATE NOT NULL,
    effective_end_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT current_timestamp()
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.pa_appeals (
    appeal_id STRING,
    auth_request_id STRING NOT NULL,
    appeal_type STRING NOT NULL DEFAULT 'standard',
    urgency STRING NOT NULL DEFAULT 'standard',
    filed_by STRING,
    filed_role STRING,
    filing_reason STRING,
    supporting_docs STRING,
    original_reviewer_id STRING,
    assigned_reviewer_id STRING,
    assigned_at TIMESTAMP,
    status STRING NOT NULL DEFAULT 'Received',
    status_changed_at TIMESTAMP DEFAULT current_timestamp(),
    determination STRING,
    determination_reason STRING,
    determination_reason_external STRING,
    reviewer_notes_internal STRING,
    hearing_date TIMESTAMP,
    hearing_outcome STRING,
    iro_referred BOOLEAN DEFAULT FALSE,
    iro_referral_date TIMESTAMP,
    iro_outcome STRING,
    filed_date TIMESTAMP NOT NULL DEFAULT current_timestamp(),
    determination_date TIMESTAMP,
    turnaround_hours DECIMAL(8,1),
    cms_deadline TIMESTAMP,
    cms_compliant BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT current_timestamp(),
    updated_at TIMESTAMP DEFAULT current_timestamp()
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.pa_appeal_actions (
    action_id STRING,
    appeal_id STRING NOT NULL,
    reviewer_id STRING,
    action_type STRING NOT NULL,
    previous_status STRING,
    new_status STRING,
    note STRING,
    metadata_json STRING,
    created_at TIMESTAMP DEFAULT current_timestamp()
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE OR REPLACE VIEW {catalog}.{schema}.v_appeal_queue AS
SELECT
    a.appeal_id,
    a.auth_request_id,
    q.member_name,
    q.service_type,
    q.procedure_code,
    q.procedure_description,
    q.line_of_business,
    q.denial_reason_code       AS original_denial_reason_code,
    q.determination_reason     AS original_determination_reason,
    q.status::string             AS original_status,
    a.appeal_type::string,
    a.urgency::string,
    a.filed_by,
    a.filed_date,
    a.status::string,
    a.determination,
    orig.display_name          AS original_reviewer_name,
    rev.display_name           AS appeal_reviewer_name,
    rev.role::string             AS appeal_reviewer_role,
    a.assigned_at,
    a.cms_deadline,
    a.cms_compliant,
    a.determination_date,
    a.turnaround_hours,
    timestampdiff(SECOND, current_timestamp(), a.cms_deadline) / 3600.0 AS hours_until_deadline
FROM pa_appeals a
JOIN pa_review_queue q  ON a.auth_request_id = q.auth_request_id
LEFT JOIN pa_reviewers orig ON a.original_reviewer_id = orig.reviewer_id
LEFT JOIN pa_reviewers rev  ON a.assigned_reviewer_id = rev.reviewer_id
ORDER BY
    CASE a.urgency WHEN 'expedited' THEN 1 WHEN 'standard' THEN 2 ELSE 3 END,
    a.cms_deadline ASC NULLS LAST,
    a.filed_date ASC;

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.pa_peer_reviews (
    peer_review_id STRING,
    auth_request_id STRING NOT NULL,
    requested_by_id STRING,
    peer_reviewer_id STRING,
    requested_specialty STRING,
    reason STRING,
    status STRING NOT NULL DEFAULT 'Requested',
    p2p_requested BOOLEAN DEFAULT FALSE,
    p2p_scheduled_at TIMESTAMP,
    p2p_completed_at TIMESTAMP,
    p2p_summary STRING,
    determination STRING,
    determination_notes STRING,
    notified_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT current_timestamp(),
    updated_at TIMESTAMP DEFAULT current_timestamp()
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TRIGGER trg_peer_updated_at
    BEFORE UPDATE ON pa_peer_reviews
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_pa();

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.pa_correspondence (
    notice_id STRING,
    auth_request_id STRING,
    appeal_id STRING,
    notice_type STRING NOT NULL,
    recipient STRING,
    recipient_role STRING,
    language STRING DEFAULT 'en',
    template_version STRING,
    subject STRING,
    body_markdown STRING,
    body_redacted BOOLEAN DEFAULT FALSE,
    redaction_notes STRING,
    includes_appeal_rights BOOLEAN DEFAULT FALSE,
    criteria_citation STRING,
    pdf_path STRING,
    delivery_channel STRING DEFAULT 'portal',
    delivery_status STRING NOT NULL DEFAULT 'draft',
    generated_by STRING,
    generated_at TIMESTAMP DEFAULT current_timestamp(),
    released_at TIMESTAMP,
    delivered_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT current_timestamp(),
    updated_at TIMESTAMP DEFAULT current_timestamp()
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.pa_business_rules (
    rule_id STRING,
    name STRING NOT NULL,
    description STRING,
    category STRING,
    line_of_business STRING,
    service_type STRING,
    conditions_json STRING NOT NULL,
    action STRING NOT NULL,
    action_detail STRING,
    priority INT NOT NULL DEFAULT 100,
    effective_start_date DATE NOT NULL DEFAULT CURRENT_DATE,
    effective_end_date DATE,
    version INT NOT NULL DEFAULT 1,
    status STRING NOT NULL DEFAULT 'draft',
    created_by STRING,
    approved_by STRING,
    approved_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT current_timestamp(),
    updated_at TIMESTAMP DEFAULT current_timestamp()
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.pa_rule_versions (
    version_id STRING,
    rule_id STRING NOT NULL,
    version INT NOT NULL,
    change_type STRING NOT NULL,
    snapshot_json STRING NOT NULL,
    changed_by STRING,
    change_reason STRING,
    created_at TIMESTAMP DEFAULT current_timestamp()
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.pa_qa_questions (
    question_id STRING,
    question_text STRING NOT NULL,
    weight INT NOT NULL DEFAULT 10,
    is_critical BOOLEAN NOT NULL DEFAULT FALSE,
    sort_order INT NOT NULL DEFAULT 0,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT current_timestamp()
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.pa_qa_reviews (
    qa_id STRING,
    auth_request_id STRING NOT NULL,
    case_reviewer_id STRING,
    qa_reviewer_id STRING,
    sample_reason STRING,
    status STRING NOT NULL DEFAULT 'Pending Score',
    scores_json STRING,
    total_score DECIMAL(6,2),
    max_score DECIMAL(6,2),
    score_pct DECIMAL(5,2),
    passed BOOLEAN,
    critical_error BOOLEAN DEFAULT FALSE,
    findings STRING,
    coaching_notes STRING,
    reviewer_rebuttal STRING,
    sampled_at TIMESTAMP DEFAULT current_timestamp(),
    scored_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT current_timestamp(),
    updated_at TIMESTAMP DEFAULT current_timestamp()
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TRIGGER trg_qa_updated_at
    BEFORE UPDATE ON pa_qa_reviews
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_pa();

CREATE OR REPLACE VIEW {catalog}.{schema}.v_qa_reviewer_scorecard AS
SELECT
    r.reviewer_id,
    r.display_name,
    r.role::string,
    COUNT(q.qa_id)                                          AS reviews_scored,
    ROUND(AVG(q.score_pct) FILTER (WHERE q.status <> 'Pending Score'), 1) AS avg_score_pct,
    SUM(CASE WHEN q.passed THEN 1 ELSE 0 END)              AS passed,
    SUM(CASE WHEN q.passed = FALSE THEN 1 ELSE 0 END)      AS failed,
    SUM(CASE WHEN q.critical_error THEN 1 ELSE 0 END)      AS critical_errors,
    ROUND(SUM(CASE WHEN q.passed THEN 1 ELSE 0 END) * 100.0
        / NULLIF(COUNT(q.qa_id) FILTER (WHERE q.status <> 'Pending Score'), 0), 1) AS pass_rate_pct
FROM pa_reviewers r
LEFT JOIN pa_qa_reviews q ON r.reviewer_id = q.case_reviewer_id
GROUP BY r.reviewer_id, r.display_name, r.role;

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.pa_work_queues (
    queue_id STRING,
    name STRING NOT NULL,
    description STRING,
    queue_type STRING,
    service_types STRING,
    owner_team STRING,
    sla_hours INT NOT NULL DEFAULT 72,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT current_timestamp(),
    updated_at TIMESTAMP DEFAULT current_timestamp()
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.pa_routing_rules (
    routing_rule_id STRING,
    name STRING NOT NULL,
    description STRING,
    line_of_business STRING,
    service_type STRING,
    conditions_json STRING NOT NULL DEFAULT '{}',
    target_queue_id STRING,
    target_role STRING,
    assignment_strategy STRING NOT NULL DEFAULT 'least_loaded',
    priority INT NOT NULL DEFAULT 100,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_by STRING,
    created_at TIMESTAMP DEFAULT current_timestamp(),
    updated_at TIMESTAMP DEFAULT current_timestamp()
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

ALTER TABLE {catalog}.{schema}.pa_review_queue ADD COLUMN IF NOT EXISTS queue_id STRING;

ALTER TABLE {catalog}.{schema}.pa_review_queue ADD COLUMN IF NOT EXISTS region STRING;

ALTER TABLE {catalog}.{schema}.pa_review_queue ADD COLUMN IF NOT EXISTS priority_score INT DEFAULT 0;

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.pa_escalations (
    escalation_id STRING,
    auth_request_id STRING NOT NULL,
    reason STRING NOT NULL,
    detail STRING,
    escalated_by STRING,
    escalated_to_id STRING,
    status STRING NOT NULL DEFAULT 'open',
    resolution STRING,
    created_at TIMESTAMP DEFAULT current_timestamp(),
    resolved_at TIMESTAMP
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE OR REPLACE VIEW {catalog}.{schema}.v_work_queue_status AS
SELECT
    wq.queue_id,
    wq.name,
    wq.queue_type,
    wq.owner_team,
    wq.sla_hours,
    COUNT(q.auth_request_id) FILTER (WHERE q.status IN
        ('Pending Review','In Review','Additional Info Requested','Peer Review Requested')) AS open_cases,
    COUNT(q.auth_request_id) FILTER (WHERE q.status = 'Pending Review') AS unassigned_cases,
    COUNT(q.auth_request_id) FILTER (WHERE q.urgency = 'expedited' AND q.status IN
        ('Pending Review','In Review','Additional Info Requested','Peer Review Requested')) AS expedited_open,
    
    COUNT(q.auth_request_id) FILTER (WHERE q.status IN
        ('Pending Review','In Review','Additional Info Requested','Peer Review Requested')
        AND timestampdiff(HOUR, q.request_date, current_timestamp()) < 24) AS age_0_24h,
    COUNT(q.auth_request_id) FILTER (WHERE q.status IN
        ('Pending Review','In Review','Additional Info Requested','Peer Review Requested')
        AND timestampdiff(HOUR, q.request_date, current_timestamp()) >= 24
        AND timestampdiff(HOUR, q.request_date, current_timestamp()) < 72) AS age_24_72h,
    COUNT(q.auth_request_id) FILTER (WHERE q.status IN
        ('Pending Review','In Review','Additional Info Requested','Peer Review Requested')
        AND timestampdiff(HOUR, q.request_date, current_timestamp()) >= 72) AS age_72h_plus,
    
    COUNT(q.auth_request_id) FILTER (WHERE q.status IN
        ('Pending Review','In Review','Additional Info Requested','Peer Review Requested')
        AND q.cms_deadline < current_timestamp()) AS sla_breached,
    ROUND(AVG(timestampdiff(SECOND, q.request_date, current_timestamp()) / 3600.0) FILTER (WHERE q.status IN
        ('Pending Review','In Review','Additional Info Requested','Peer Review Requested')), 1) AS avg_age_hours
FROM pa_work_queues wq
LEFT JOIN pa_review_queue q ON q.queue_id = wq.queue_id
WHERE wq.is_active = TRUE
GROUP BY wq.queue_id, wq.name, wq.queue_type, wq.owner_team, wq.sla_hours
ORDER BY sla_breached DESC, open_cases DESC;

CREATE OR REPLACE VIEW {catalog}.{schema}.v_workload_balance AS
SELECT
    c.reviewer_id,
    c.display_name,
    c.role,
    c.specialty,
    c.max_caseload,
    c.active_cases,
    c.expedited_cases,
    c.available_capacity,
    ROUND(c.active_cases * 100.0 / NULLIF(c.max_caseload, 0), 1) AS utilization_pct,
    (c.active_cases > c.max_caseload) AS is_overloaded
FROM v_reviewer_caseload c;

CREATE OR REPLACE VIEW {catalog}.{schema}.v_stalled_cases AS
WITH last_action AS (
    SELECT auth_request_id, MAX(created_at) AS last_action_at
    FROM pa_review_actions GROUP BY auth_request_id
)
SELECT
    q.auth_request_id,
    q.member_name,
    q.service_type,
    q.urgency::string,
    q.status::string,
    wq.name AS queue_name,
    r.display_name AS reviewer_name,
    q.request_date,
    q.cms_deadline,
    la.last_action_at,
    ROUND(timestampdiff(SECOND, q.request_date, current_timestamp()) / 3600.0, 1) AS age_hours,
    ROUND(timestampdiff(SECOND, COALESCE(la.last_action_at, q.request_date), current_timestamp()) / 3600.0, 1) AS hours_since_action,
    CASE
        WHEN q.cms_deadline < current_timestamp() THEN 'sla_breached'
        WHEN q.assigned_reviewer_id IS NULL AND timestampdiff(HOUR, q.request_date, current_timestamp()) > 24 THEN 'orphaned'
        WHEN timestampdiff(HOUR, COALESCE(la.last_action_at, q.request_date), current_timestamp()) > 48 THEN 'stalled'
        ELSE 'at_risk'
    END AS flag_reason
FROM pa_review_queue q
LEFT JOIN pa_work_queues wq ON q.queue_id = wq.queue_id
LEFT JOIN pa_reviewers r ON q.assigned_reviewer_id = r.reviewer_id
LEFT JOIN last_action la ON q.auth_request_id = la.auth_request_id
WHERE q.status IN ('Pending Review','In Review','Additional Info Requested','Peer Review Requested')
  AND (
        q.cms_deadline < current_timestamp()
        OR (q.assigned_reviewer_id IS NULL AND timestampdiff(HOUR, q.request_date, current_timestamp()) > 24)
        OR timestampdiff(HOUR, COALESCE(la.last_action_at, q.request_date), current_timestamp()) > 48
      )
ORDER BY q.cms_deadline ASC NULLS LAST;

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.pa_inbound_correspondence (
    inbound_id STRING,
    auth_request_id STRING,
    source_channel STRING NOT NULL,
    sender STRING,
    received_at TIMESTAMP DEFAULT current_timestamp(),
    raw_text STRING,
    classified_type STRING,
    classification_confidence DECIMAL(4,3),
    extracted_summary STRING,
    indexed BOOLEAN DEFAULT FALSE,
    indexed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT current_timestamp()
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

ALTER TABLE {catalog}.{schema}.pa_correspondence ADD COLUMN IF NOT EXISTS validation_status STRING;

ALTER TABLE {catalog}.{schema}.pa_correspondence ADD COLUMN IF NOT EXISTS validation_notes STRING;

CREATE OR REPLACE VIEW {catalog}.{schema}.v_case_timeline AS
SELECT
    ra.auth_request_id,
    'review'        AS workflow,
    ra.action_type,
    ra.previous_status::string AS previous_status,
    ra.new_status::string      AS new_status,
    ra.note,
    rv.display_name AS actor,
    ra.created_at
FROM pa_review_actions ra
LEFT JOIN pa_reviewers rv ON ra.reviewer_id = rv.reviewer_id
UNION ALL
SELECT
    ap.auth_request_id,
    'appeal'        AS workflow,
    aa.action_type,
    aa.previous_status::string,
    aa.new_status::string,
    aa.note,
    rv.display_name,
    aa.created_at
FROM pa_appeal_actions aa
JOIN pa_appeals ap ON aa.appeal_id = ap.appeal_id
LEFT JOIN pa_reviewers rv ON aa.reviewer_id = rv.reviewer_id
UNION ALL
SELECT
    c.auth_request_id,
    'correspondence' AS workflow,
    c.notice_type::string AS action_type,
    NULL, c.delivery_status::string,
    c.subject,
    c.generated_by,
    c.created_at
FROM pa_correspondence c
WHERE c.auth_request_id IS NOT NULL
ORDER BY created_at DESC;

