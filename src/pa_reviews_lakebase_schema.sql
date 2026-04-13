-- ============================================================================
-- Lakebase (PostgreSQL) Schema — PA Reviews
--
-- Run this against the pa_reviews Lakebase instance to create the
-- operational tables that power the PA Review Portal app.
--
-- IDEMPOTENT: Safe to run multiple times (uses IF NOT EXISTS / DO blocks).
-- ============================================================================

-- ---------------------------------------------------------------------------
-- Enum types for controlled vocabularies
-- ---------------------------------------------------------------------------

DO $$ BEGIN
    CREATE TYPE pa_review_status AS ENUM (
        'Pending Review',               -- New request, not yet assigned
        'In Review',                     -- Reviewer is actively evaluating
        'Additional Info Requested',     -- Waiting for clinical documentation
        'Approved',                      -- Prior authorization granted
        'Denied',                        -- Prior authorization denied
        'Partially Approved',            -- Approved with modifications
        'Peer Review Requested',         -- Escalated to physician reviewer
        'Appealed',                      -- Member/provider filed appeal
        'Appeal Overturned',             -- Denial reversed on appeal
        'Appeal Upheld'                  -- Denial upheld on appeal
    );
EXCEPTION WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE pa_urgency AS ENUM (
        'expedited',                     -- 72-hour CMS requirement
        'standard',                      -- 168-hour (7-day) CMS requirement
        'retrospective'                  -- Post-service review
    );
EXCEPTION WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE pa_determination_tier AS ENUM (
        'tier_1_auto',                   -- Deterministic rules auto-decision
        'tier_2_ml',                     -- ML model classification
        'tier_3_llm',                    -- LLM clinical review
        'manual'                         -- Human reviewer
    );
EXCEPTION WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE reviewer_role AS ENUM (
        'UM Nurse',
        'Medical Director',
        'Peer Reviewer',
        'Clinical Pharmacist',
        'Appeals Coordinator'
    );
EXCEPTION WHEN duplicate_object THEN null;
END $$;

-- ---------------------------------------------------------------------------
-- PA Reviewers (lookup table)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS pa_reviewers (
    reviewer_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email               TEXT NOT NULL UNIQUE,
    display_name        TEXT NOT NULL,
    role                reviewer_role NOT NULL,
    department          TEXT,
    specialty           TEXT,
    max_caseload        INT DEFAULT 50,
    is_active           BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMPTZ DEFAULT now(),
    updated_at          TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_reviewers_active ON pa_reviewers(is_active) WHERE is_active = TRUE;

-- ---------------------------------------------------------------------------
-- PA Review Queue (core table)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS pa_review_queue (
    auth_request_id     TEXT PRIMARY KEY,           -- Matches UC gold_pa_requests.auth_request_id
    member_id           TEXT NOT NULL,
    member_name         TEXT,
    requesting_provider_npi TEXT NOT NULL,
    provider_name       TEXT,

    -- Service details
    service_type        TEXT NOT NULL,
    procedure_code      TEXT NOT NULL,
    procedure_description TEXT,
    diagnosis_codes     TEXT,
    policy_id           TEXT,
    policy_name         TEXT,
    line_of_business    TEXT,

    -- Clinical
    clinical_summary    TEXT,
    urgency             pa_urgency NOT NULL DEFAULT 'standard',
    estimated_cost      NUMERIC(12,2) DEFAULT 0,

    -- Review workflow
    status              pa_review_status NOT NULL DEFAULT 'Pending Review',
    determination_tier  pa_determination_tier,
    assigned_reviewer_id UUID REFERENCES pa_reviewers(reviewer_id),
    assigned_at         TIMESTAMPTZ,
    status_changed_at   TIMESTAMPTZ DEFAULT now(),

    -- AI enrichment
    ai_recommendation   TEXT,                       -- ML/LLM recommendation
    ai_confidence       NUMERIC(4,3),               -- Model confidence score
    tier1_auto_eligible BOOLEAN DEFAULT FALSE,      -- Can be auto-adjudicated
    clinical_extraction TEXT,                        -- AI-extracted clinical facts

    -- Determination
    determination_reason TEXT,
    denial_reason_code  TEXT,
    reviewer_notes      TEXT,

    -- CMS compliance
    request_date        TIMESTAMPTZ NOT NULL DEFAULT now(),
    determination_date  TIMESTAMPTZ,
    turnaround_hours    NUMERIC(8,1),
    cms_compliant       BOOLEAN DEFAULT TRUE,
    cms_deadline        TIMESTAMPTZ,                -- Computed from urgency + request_date

    -- Appeal tracking
    appeal_filed        BOOLEAN DEFAULT FALSE,
    appeal_date         TIMESTAMPTZ,
    appeal_outcome      TEXT,

    -- Audit
    created_at          TIMESTAMPTZ DEFAULT now(),
    updated_at          TIMESTAMPTZ DEFAULT now(),

    -- Constraints
    CONSTRAINT chk_determination_has_date CHECK (
        status NOT IN ('Approved', 'Denied', 'Partially Approved')
        OR determination_date IS NOT NULL
    )
);

-- Primary query patterns
CREATE INDEX IF NOT EXISTS idx_pa_status           ON pa_review_queue(status);
CREATE INDEX IF NOT EXISTS idx_pa_reviewer         ON pa_review_queue(assigned_reviewer_id)
    WHERE assigned_reviewer_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_pa_urgency          ON pa_review_queue(urgency);
CREATE INDEX IF NOT EXISTS idx_pa_member           ON pa_review_queue(member_id);
CREATE INDEX IF NOT EXISTS idx_pa_provider         ON pa_review_queue(requesting_provider_npi);
CREATE INDEX IF NOT EXISTS idx_pa_service_type     ON pa_review_queue(service_type);
CREATE INDEX IF NOT EXISTS idx_pa_pending          ON pa_review_queue(urgency, request_date)
    WHERE status = 'Pending Review';
CREATE INDEX IF NOT EXISTS idx_pa_cms_deadline     ON pa_review_queue(cms_deadline)
    WHERE status IN ('Pending Review', 'In Review', 'Additional Info Requested');

-- ---------------------------------------------------------------------------
-- PA Review Actions (immutable audit log)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS pa_review_actions (
    action_id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    auth_request_id     TEXT NOT NULL REFERENCES pa_review_queue(auth_request_id),
    reviewer_id         UUID REFERENCES pa_reviewers(reviewer_id),
    action_type         TEXT NOT NULL CHECK (action_type IN (
                            'status_change', 'note_added', 'assignment',
                            'reassignment', 'ai_recommendation', 'determination',
                            'appeal_filed', 'appeal_decision', 'auto_generated',
                            'info_requested', 'peer_review_requested'
                        )),
    previous_status     pa_review_status,
    new_status          pa_review_status,
    note                TEXT,
    metadata_json       JSONB,              -- Flexible payload for action-specific data
    created_at          TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_pa_actions_request  ON pa_review_actions(auth_request_id, created_at DESC);

-- ---------------------------------------------------------------------------
-- Auto-update timestamps and CMS deadline
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION update_pa_review_timestamps()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();

    -- Set determination_date when status moves to a terminal status
    IF NEW.status IN ('Approved', 'Denied', 'Partially Approved')
       AND (OLD.status IS NULL OR OLD.status NOT IN ('Approved', 'Denied', 'Partially Approved'))
    THEN
        NEW.determination_date = now();
        NEW.turnaround_hours = EXTRACT(EPOCH FROM (now() - NEW.request_date)) / 3600.0;
        -- CMS compliance check
        IF NEW.urgency = 'expedited' THEN
            NEW.cms_compliant = (now() - NEW.request_date) <= INTERVAL '72 hours';
        ELSIF NEW.urgency = 'standard' THEN
            NEW.cms_compliant = (now() - NEW.request_date) <= INTERVAL '168 hours';
        END IF;
    END IF;

    -- Set assigned_at on first assignment
    IF NEW.assigned_reviewer_id IS NOT NULL
       AND (OLD.assigned_reviewer_id IS NULL OR OLD.assigned_reviewer_id != NEW.assigned_reviewer_id)
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

DROP TRIGGER IF EXISTS trg_pa_review_updated_at ON pa_review_queue;
CREATE TRIGGER trg_pa_review_updated_at
    BEFORE UPDATE ON pa_review_queue
    FOR EACH ROW EXECUTE FUNCTION update_pa_review_timestamps();

-- CMS deadline auto-compute on INSERT
CREATE OR REPLACE FUNCTION set_cms_deadline()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.urgency = 'expedited' THEN
        NEW.cms_deadline = NEW.request_date + INTERVAL '72 hours';
    ELSIF NEW.urgency = 'standard' THEN
        NEW.cms_deadline = NEW.request_date + INTERVAL '168 hours';
    ELSE
        NEW.cms_deadline = NEW.request_date + INTERVAL '30 days';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_pa_cms_deadline ON pa_review_queue;
CREATE TRIGGER trg_pa_cms_deadline
    BEFORE INSERT ON pa_review_queue
    FOR EACH ROW EXECUTE FUNCTION set_cms_deadline();

CREATE OR REPLACE FUNCTION update_updated_at_pa()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_reviewers_updated_at ON pa_reviewers;
CREATE TRIGGER trg_reviewers_updated_at
    BEFORE UPDATE ON pa_reviewers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_pa();

-- ---------------------------------------------------------------------------
-- Views for the app
-- ---------------------------------------------------------------------------

-- Review queue (what UM nurses see when they open the app)
CREATE OR REPLACE VIEW v_review_queue AS
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
    q.urgency::text,
    q.estimated_cost,
    q.status::text,
    q.determination_tier::text,
    q.ai_recommendation,
    q.ai_confidence,
    q.tier1_auto_eligible,
    r.display_name AS reviewer_name,
    r.role::text AS reviewer_role,
    q.assigned_at,
    q.request_date,
    q.cms_deadline,
    q.cms_compliant,
    now() - q.request_date AS time_open,
    EXTRACT(EPOCH FROM (q.cms_deadline - now())) / 3600.0 AS hours_until_deadline
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

-- Reviewer caseload dashboard
CREATE OR REPLACE VIEW v_reviewer_caseload AS
SELECT
    r.reviewer_id,
    r.display_name,
    r.role::text,
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

-- Review detail with latest action
CREATE OR REPLACE VIEW v_review_detail AS
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
    q.urgency::text,
    q.estimated_cost,
    q.status::text,
    q.determination_tier::text,
    q.assigned_reviewer_id,
    r.display_name AS reviewer_name,
    r.role::text AS reviewer_role,
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
    EXTRACT(EPOCH FROM (q.cms_deadline - now())) / 3600.0 AS hours_until_deadline
FROM pa_review_queue q
LEFT JOIN pa_reviewers r ON q.assigned_reviewer_id = r.reviewer_id;

-- CMS compliance summary
CREATE OR REPLACE VIEW v_cms_compliance_summary AS
SELECT
    urgency::text,
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
             AND cms_deadline < now() THEN 1 ELSE 0 END) AS overdue_count
FROM pa_review_queue
GROUP BY urgency;
