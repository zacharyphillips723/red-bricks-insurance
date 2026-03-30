-- ============================================================================
-- Lakebase (PostgreSQL) Schema — Risk Stratification Alerts
--
-- Run this against your Lakebase Provisioned instance to create the
-- operational tables that power the Population Health Command Center app.
--
-- IDEMPOTENT: Safe to run multiple times (uses IF NOT EXISTS / DO blocks).
-- ============================================================================

-- ---------------------------------------------------------------------------
-- Enum types for controlled vocabularies
-- ---------------------------------------------------------------------------

DO $$ BEGIN
    CREATE TYPE risk_tier AS ENUM (
        'Critical',
        'High',
        'Elevated',
        'Moderate',
        'Low'
    );
EXCEPTION WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE care_cycle_status AS ENUM (
        'Unassigned',            -- Alert generated, no care manager yet
        'Assigned',              -- Care manager has claimed the patient
        'Outreach Attempted',    -- Initial contact attempted (call, letter, portal)
        'Outreach Successful',   -- Patient contacted and engaged
        'Assessment In Progress',-- Clinical assessment / care plan underway
        'Intervention Active',   -- Active care plan being executed
        'Follow-Up Scheduled',   -- Intervention complete, follow-up pending
        'Resolved',              -- Care gap closed or risk mitigated
        'Escalated',             -- Escalated to physician / specialist / supervisor
        'Closed — Unable to Reach' -- Exhausted outreach attempts
    );
EXCEPTION WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE alert_source AS ENUM (
        'High Glucose No Insulin',   -- From gold_high_glucose_no_insulin
        'ED High Utilizer',          -- From gold_ed_high_utilizers
        'SDOH Risk',                 -- Future: from sdoh_indicators
        'Readmission Risk',          -- Future: ML model prediction
        'Manual'                     -- Manually created by care manager
    );
EXCEPTION WHEN duplicate_object THEN null;
END $$;

-- ---------------------------------------------------------------------------
-- Care managers (lookup table)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS care_managers (
    care_manager_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email               TEXT NOT NULL UNIQUE,
    display_name        TEXT NOT NULL,
    role                TEXT NOT NULL CHECK (role IN (
                            'RN', 'LPN', 'NP', 'PA', 'SW', 'CHW', 'Pharmacist', 'MD'
                        )),
    department          TEXT,
    max_caseload        INT DEFAULT 50,
    is_active           BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMPTZ DEFAULT now(),
    updated_at          TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_care_managers_active ON care_managers(is_active) WHERE is_active = TRUE;

-- ---------------------------------------------------------------------------
-- Risk stratification alerts (core table)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS risk_stratification_alerts (
    alert_id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    patient_id              TEXT NOT NULL,           -- Links to Unity Catalog patient_id
    mrn                     TEXT,                    -- Medical Record Number
    member_id               TEXT,                    -- Insurance member ID

    -- Risk classification
    risk_tier               risk_tier NOT NULL,
    risk_score              NUMERIC(5,2),            -- Optional numeric score (0-100)
    primary_driver          TEXT NOT NULL,            -- e.g., "HbA1c 11.2% — No insulin refills"
    secondary_drivers       TEXT[],                   -- Additional contributing factors
    alert_source            alert_source NOT NULL,

    -- Assignment & workflow
    assigned_care_manager_id UUID REFERENCES care_managers(care_manager_id),
    assigned_at             TIMESTAMPTZ,
    status                  care_cycle_status NOT NULL DEFAULT 'Unassigned',
    status_changed_at       TIMESTAMPTZ DEFAULT now(),

    -- Clinical context (denormalized for app performance)
    max_hba1c               NUMERIC(4,1),
    max_blood_glucose       NUMERIC(5,1),
    peak_ed_visits_12mo     INT,
    last_encounter_date     TIMESTAMPTZ,
    last_facility           TEXT,
    payer                   TEXT,
    active_medications      TEXT[],

    -- Notes
    notes                   TEXT,                    -- Free-text notes from care manager

    -- Audit
    created_at              TIMESTAMPTZ DEFAULT now(),
    updated_at              TIMESTAMPTZ DEFAULT now(),
    resolved_at             TIMESTAMPTZ,

    -- Constraints
    CONSTRAINT chk_assigned_has_manager CHECK (
        (status = 'Unassigned') OR (assigned_care_manager_id IS NOT NULL)
    )
);

-- Primary query patterns: by status, by care manager, by risk tier, by patient
CREATE INDEX IF NOT EXISTS idx_alerts_status          ON risk_stratification_alerts(status);
CREATE INDEX IF NOT EXISTS idx_alerts_care_manager    ON risk_stratification_alerts(assigned_care_manager_id)
    WHERE assigned_care_manager_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_alerts_risk_tier       ON risk_stratification_alerts(risk_tier);
CREATE INDEX IF NOT EXISTS idx_alerts_patient         ON risk_stratification_alerts(patient_id);
CREATE INDEX IF NOT EXISTS idx_alerts_source          ON risk_stratification_alerts(alert_source);
CREATE INDEX IF NOT EXISTS idx_alerts_unassigned      ON risk_stratification_alerts(risk_tier, created_at)
    WHERE status = 'Unassigned';

-- ---------------------------------------------------------------------------
-- Alert activity log (audit trail for status changes and notes)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS alert_activity_log (
    activity_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    alert_id            UUID NOT NULL REFERENCES risk_stratification_alerts(alert_id),
    care_manager_id     UUID REFERENCES care_managers(care_manager_id),
    activity_type       TEXT NOT NULL CHECK (activity_type IN (
                            'status_change', 'note_added', 'assignment',
                            'reassignment', 'escalation', 'auto_generated'
                        )),
    previous_status     care_cycle_status,
    new_status          care_cycle_status,
    note                TEXT,
    created_at          TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_activity_alert ON alert_activity_log(alert_id, created_at DESC);

-- ---------------------------------------------------------------------------
-- Auto-update timestamps
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    -- Set resolved_at when status moves to Resolved
    IF NEW.status = 'Resolved' AND (OLD.status IS NULL OR OLD.status != 'Resolved') THEN
        NEW.resolved_at = now();
    END IF;
    -- Set assigned_at on first assignment
    IF NEW.assigned_care_manager_id IS NOT NULL
       AND (OLD.assigned_care_manager_id IS NULL OR OLD.assigned_care_manager_id != NEW.assigned_care_manager_id)
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

DROP TRIGGER IF EXISTS trg_alerts_updated_at ON risk_stratification_alerts;
CREATE TRIGGER trg_alerts_updated_at
    BEFORE UPDATE ON risk_stratification_alerts
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

DROP TRIGGER IF EXISTS trg_care_managers_updated_at ON care_managers;
CREATE TRIGGER trg_care_managers_updated_at
    BEFORE UPDATE ON care_managers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- ---------------------------------------------------------------------------
-- Views for the app
-- ---------------------------------------------------------------------------

-- Unassigned alerts queue (what clinicians see when they open the app)
CREATE OR REPLACE VIEW v_unassigned_alerts AS
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
    now() - a.created_at AS time_unassigned
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

-- Care manager caseload dashboard
CREATE OR REPLACE VIEW v_care_manager_caseload AS
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

-- Patient 360 view (all alerts for a single patient)
CREATE OR REPLACE VIEW v_patient_alert_history AS
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
