-- ============================================================================
-- Lakebase (PostgreSQL) Schema — FWA Investigations
--
-- Run this against the fwa-investigations Lakebase instance to create the
-- operational tables that power the FWA Investigation Portal app.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- Enum types for controlled vocabularies
-- ---------------------------------------------------------------------------

CREATE TYPE investigation_status AS ENUM (
    'Open',                         -- Newly created, not yet reviewed
    'Under Review',                 -- Analyst has begun initial review
    'Evidence Gathering',           -- Actively collecting claims, records, documentation
    'Referred to SIU',              -- Escalated to Special Investigations Unit
    'Recovery In Progress',         -- Overpayment recovery initiated
    'Closed — Confirmed Fraud',     -- Investigation confirmed fraudulent activity
    'Closed — No Fraud',            -- Investigation cleared — no fraud found
    'Closed — Insufficient Evidence' -- Unable to confirm or deny
);

CREATE TYPE fraud_severity AS ENUM (
    'Critical',
    'High',
    'Medium',
    'Low'
);

CREATE TYPE investigation_source AS ENUM (
    'Rules Engine',           -- Automated rules-based detection
    'Statistical Outlier',    -- Statistical anomaly detection
    'Peer Comparison',        -- Provider peer benchmarking
    'AI Model',               -- ML model flagged
    'Tip Hotline',            -- Anonymous tip or whistleblower
    'Audit Sample',           -- Random or targeted audit
    'Referral',               -- Internal referral from claims team
    'Manual'                  -- Manually created by investigator
);

CREATE TYPE investigation_type AS ENUM (
    'Provider',               -- Provider-focused investigation
    'Member',                 -- Member-focused investigation
    'Network'                 -- Provider ring / network investigation
);

-- ---------------------------------------------------------------------------
-- Fraud investigators (lookup table)
-- ---------------------------------------------------------------------------

CREATE TABLE fraud_investigators (
    investigator_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email               TEXT NOT NULL UNIQUE,
    display_name        TEXT NOT NULL,
    role                TEXT NOT NULL CHECK (role IN (
                            'SIU Analyst', 'SIU Manager', 'Clinical Reviewer',
                            'Legal Counsel', 'Recovery Specialist', 'Data Analyst'
                        )),
    department          TEXT,
    max_caseload        INT DEFAULT 30,
    is_active           BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMPTZ DEFAULT now(),
    updated_at          TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_investigators_active ON fraud_investigators(is_active) WHERE is_active = TRUE;

-- ---------------------------------------------------------------------------
-- FWA investigations (core table)
-- ---------------------------------------------------------------------------

CREATE TABLE fwa_investigations (
    investigation_id        TEXT PRIMARY KEY,           -- INV-XXXX format
    investigation_type      investigation_type NOT NULL,
    target_type             TEXT NOT NULL CHECK (target_type IN ('provider', 'member', 'network')),
    target_id               TEXT NOT NULL,              -- NPI, member_id, or network group ID
    target_name             TEXT NOT NULL,              -- Provider name, member name, or ring name

    -- Fraud classification
    fraud_types             TEXT[] NOT NULL,            -- e.g., ['upcoding', 'unbundling']
    severity                fraud_severity NOT NULL,
    source                  investigation_source NOT NULL DEFAULT 'Rules Engine',

    -- Assignment & workflow
    status                  investigation_status NOT NULL DEFAULT 'Open',
    assigned_investigator_id UUID REFERENCES fraud_investigators(investigator_id),
    assigned_at             TIMESTAMPTZ,
    status_changed_at       TIMESTAMPTZ DEFAULT now(),

    -- Financial impact
    estimated_overpayment   NUMERIC(12,2) DEFAULT 0,   -- Estimated by rules/model
    confirmed_overpayment   NUMERIC(12,2),              -- Confirmed after investigation
    recovered_amount        NUMERIC(12,2) DEFAULT 0,    -- Actually recovered
    claims_involved_count   INT DEFAULT 0,

    -- Summaries
    investigation_summary   TEXT,                       -- Human-written or AI-generated summary
    evidence_summary        TEXT,                       -- Key evidence points
    recommendation          TEXT,                       -- Investigator's final recommendation

    -- Risk scores
    rules_risk_score        NUMERIC(4,3),               -- Rules-based score (0-1)
    ml_risk_score           NUMERIC(4,3),               -- ML model score (0-1)
    composite_risk_score    NUMERIC(4,3),               -- Blended score

    -- Audit
    created_at              TIMESTAMPTZ DEFAULT now(),
    updated_at              TIMESTAMPTZ DEFAULT now(),
    closed_at               TIMESTAMPTZ,

    -- Constraints
    CONSTRAINT chk_assigned_has_investigator CHECK (
        status IN ('Open') OR assigned_investigator_id IS NOT NULL
    )
);

-- Primary query patterns
CREATE INDEX idx_inv_status         ON fwa_investigations(status);
CREATE INDEX idx_inv_investigator   ON fwa_investigations(assigned_investigator_id)
    WHERE assigned_investigator_id IS NOT NULL;
CREATE INDEX idx_inv_severity       ON fwa_investigations(severity);
CREATE INDEX idx_inv_target         ON fwa_investigations(target_id);
CREATE INDEX idx_inv_type           ON fwa_investigations(investigation_type);
CREATE INDEX idx_inv_open           ON fwa_investigations(severity, created_at)
    WHERE status = 'Open';
CREATE INDEX idx_inv_composite_risk ON fwa_investigations(composite_risk_score DESC NULLS LAST);

-- ---------------------------------------------------------------------------
-- Investigation audit log (immutable — critical for compliance)
-- ---------------------------------------------------------------------------

CREATE TABLE investigation_audit_log (
    audit_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    investigation_id    TEXT NOT NULL REFERENCES fwa_investigations(investigation_id),
    investigator_id     UUID REFERENCES fraud_investigators(investigator_id),
    action_type         TEXT NOT NULL CHECK (action_type IN (
                            'status_change', 'note_added', 'assignment',
                            'reassignment', 'evidence_added', 'recovery_recorded',
                            'escalation', 'auto_generated', 'recommendation_added'
                        )),
    previous_status     investigation_status,
    new_status          investigation_status,
    note                TEXT,
    metadata_json       JSONB,              -- Flexible payload for action-specific data
    created_at          TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_audit_investigation ON investigation_audit_log(investigation_id, created_at DESC);

-- ---------------------------------------------------------------------------
-- Investigation evidence (linked claims, documents, data points)
-- ---------------------------------------------------------------------------

CREATE TABLE investigation_evidence (
    evidence_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    investigation_id    TEXT NOT NULL REFERENCES fwa_investigations(investigation_id),
    evidence_type       TEXT NOT NULL CHECK (evidence_type IN (
                            'claim', 'provider_profile', 'member_profile',
                            'billing_pattern', 'document', 'external_report',
                            'ml_model_output', 'peer_comparison'
                        )),
    reference_id        TEXT,               -- claim_id, NPI, member_id, etc.
    description         TEXT NOT NULL,
    detail_json         JSONB,              -- Full evidence payload
    added_by            UUID REFERENCES fraud_investigators(investigator_id),
    created_at          TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_evidence_investigation ON investigation_evidence(investigation_id, created_at DESC);
CREATE INDEX idx_evidence_type          ON investigation_evidence(evidence_type);
CREATE INDEX idx_evidence_reference     ON investigation_evidence(reference_id);

-- ---------------------------------------------------------------------------
-- Auto-update timestamps
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION update_investigation_timestamps()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    -- Set closed_at when status moves to a terminal status
    IF NEW.status IN ('Closed — Confirmed Fraud', 'Closed — No Fraud', 'Closed — Insufficient Evidence')
       AND (OLD.status IS NULL OR OLD.status NOT IN ('Closed — Confirmed Fraud', 'Closed — No Fraud', 'Closed — Insufficient Evidence'))
    THEN
        NEW.closed_at = now();
    END IF;
    -- Set assigned_at on first assignment
    IF NEW.assigned_investigator_id IS NOT NULL
       AND (OLD.assigned_investigator_id IS NULL OR OLD.assigned_investigator_id != NEW.assigned_investigator_id)
    THEN
        NEW.assigned_at = now();
    END IF;
    -- Track status change time
    IF NEW.status != OLD.status THEN
        NEW.status_changed_at = now();
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_investigations_updated_at
    BEFORE UPDATE ON fwa_investigations
    FOR EACH ROW EXECUTE FUNCTION update_investigation_timestamps();

CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_investigators_updated_at
    BEFORE UPDATE ON fraud_investigators
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- ---------------------------------------------------------------------------
-- Views for the app
-- ---------------------------------------------------------------------------

-- Investigation queue (what SIU analysts see when they open the app)
CREATE VIEW v_investigation_queue AS
SELECT
    i.investigation_id,
    i.investigation_type::text,
    i.target_type,
    i.target_id,
    i.target_name,
    i.fraud_types,
    i.severity::text,
    i.status::text,
    i.source::text,
    i.estimated_overpayment,
    i.claims_involved_count,
    i.composite_risk_score,
    i.rules_risk_score,
    i.ml_risk_score,
    inv.display_name AS investigator_name,
    inv.role AS investigator_role,
    i.assigned_at,
    i.created_at,
    now() - i.created_at AS time_open
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

-- Investigator caseload dashboard
CREATE VIEW v_investigator_caseload AS
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

-- Investigation detail with latest audit entry
CREATE VIEW v_investigation_detail AS
SELECT
    i.investigation_id,
    i.investigation_type::text,
    i.target_type,
    i.target_id,
    i.target_name,
    i.fraud_types,
    i.severity::text,
    i.status::text,
    i.source::text,
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
