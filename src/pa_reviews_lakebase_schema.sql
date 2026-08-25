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

-- ===========================================================================
-- CLINICAL CRITERIA VERSIONING  (RFI: PA & Clinical Reviews — criteria version
-- control + effective dating; internal vs external rationale)
-- Idempotent column additions so both fresh and existing pa_review_queue tables
-- carry the point-in-time criteria citation and the split internal/external
-- rationale that determination notices and appeals reference.
-- ===========================================================================

ALTER TABLE pa_review_queue ADD COLUMN IF NOT EXISTS criteria_source TEXT;              -- InterQual / MCG / NCD / LCD / custom
ALTER TABLE pa_review_queue ADD COLUMN IF NOT EXISTS criteria_version TEXT;             -- e.g. 'InterQual 2026.1'
ALTER TABLE pa_review_queue ADD COLUMN IF NOT EXISTS criteria_effective_date DATE;      -- point-in-time criteria applied
ALTER TABLE pa_review_queue ADD COLUMN IF NOT EXISTS determination_reason_external TEXT; -- member/provider-facing rationale
ALTER TABLE pa_review_queue ADD COLUMN IF NOT EXISTS reviewer_notes_internal TEXT;       -- internal-only notes

-- Effective-dated criteria catalog (InterQual / MCG / NCD / LCD placeholders).
-- In production these version rows are synced from the criteria vendor API; here
-- they demonstrate version control + effective dating of review guidelines.
CREATE TABLE IF NOT EXISTS medical_criteria_versions (
    criteria_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    criteria_source      TEXT NOT NULL,          -- InterQual / MCG / NCD / LCD / custom
    criteria_set         TEXT NOT NULL,          -- e.g. 'Imaging — Advanced'
    version_label        TEXT NOT NULL,          -- e.g. '2026.1'
    service_type         TEXT,
    procedure_codes      TEXT,                    -- pipe-delimited applicability
    effective_start_date DATE NOT NULL,
    effective_end_date   DATE,                    -- NULL = currently active
    is_active            BOOLEAN DEFAULT TRUE,
    created_at           TIMESTAMPTZ DEFAULT now(),
    UNIQUE (criteria_source, criteria_set, version_label)
);

CREATE INDEX IF NOT EXISTS idx_criteria_active ON medical_criteria_versions(is_active)
    WHERE is_active = TRUE;

-- ===========================================================================
-- APPEALS & RECONSIDERATIONS  (RFI: Appeals & Reconsiderations tab)
--
-- A denied/partially-approved determination can be appealed. Appeals are a
-- distinct operational record linked back to the originating auth_request_id,
-- must be routed to a reviewer OTHER than the original determiner, and carry
-- their own CMS timeliness clock (expedited 72h / standard 30 calendar days
-- for pre-service Part C appeals; retrospective 60 days).
-- ===========================================================================

DO $$ BEGIN
    CREATE TYPE appeal_type AS ENUM (
        'standard',            -- Standard pre/post-service appeal
        'expedited',           -- Expedited (urgent) appeal — 72h
        'provider',            -- Provider-filed
        'member',              -- Member/beneficiary-filed
        'administrative',      -- Administrative (non-clinical) reconsideration
        'clinical'             -- Clinical reconsideration
    );
EXCEPTION WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE appeal_status AS ENUM (
        'Received',                    -- Appeal intake logged
        'In Review',                   -- Appeals reviewer evaluating
        'Additional Info Requested',   -- Awaiting records
        'Peer Review Requested',       -- Escalated to physician/peer
        'Hearing Scheduled',           -- State fair hearing / formal hearing
        'IRO Referred',                -- External independent review org
        'Overturned',                  -- Original denial reversed (fully favorable)
        'Partially Overturned',        -- Partially favorable
        'Upheld'                       -- Original denial upheld
    );
EXCEPTION WHEN duplicate_object THEN null;
END $$;

CREATE TABLE IF NOT EXISTS pa_appeals (
    appeal_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    auth_request_id      TEXT NOT NULL REFERENCES pa_review_queue(auth_request_id),

    -- Intake
    appeal_type          appeal_type NOT NULL DEFAULT 'standard',
    urgency              pa_urgency NOT NULL DEFAULT 'standard',
    filed_by             TEXT,                       -- 'provider' | 'member' | name
    filed_role           TEXT,                       -- requesting party descriptor
    filing_reason        TEXT,                       -- why the determination is disputed
    supporting_docs      TEXT,                        -- references to attached records

    -- Routing (must differ from the original determiner)
    original_reviewer_id UUID REFERENCES pa_reviewers(reviewer_id),
    assigned_reviewer_id UUID REFERENCES pa_reviewers(reviewer_id),
    assigned_at          TIMESTAMPTZ,

    -- Workflow
    status               appeal_status NOT NULL DEFAULT 'Received',
    status_changed_at    TIMESTAMPTZ DEFAULT now(),

    -- Determination
    determination        TEXT,                        -- Overturned / Partially Overturned / Upheld
    determination_reason TEXT,
    determination_reason_external TEXT,               -- member/provider-facing rationale
    reviewer_notes_internal TEXT,

    -- Hearing / IRO
    hearing_date         TIMESTAMPTZ,
    hearing_outcome      TEXT,
    iro_referred         BOOLEAN DEFAULT FALSE,
    iro_referral_date    TIMESTAMPTZ,
    iro_outcome          TEXT,

    -- CMS timeliness
    filed_date           TIMESTAMPTZ NOT NULL DEFAULT now(),
    determination_date   TIMESTAMPTZ,
    turnaround_hours     NUMERIC(8,1),
    cms_deadline         TIMESTAMPTZ,
    cms_compliant        BOOLEAN DEFAULT TRUE,

    created_at           TIMESTAMPTZ DEFAULT now(),
    updated_at           TIMESTAMPTZ DEFAULT now(),

    -- Appeals integrity: cannot be assigned to the original determiner
    CONSTRAINT chk_appeal_non_original CHECK (
        assigned_reviewer_id IS NULL
        OR original_reviewer_id IS NULL
        OR assigned_reviewer_id <> original_reviewer_id
    ),
    CONSTRAINT chk_appeal_determination_date CHECK (
        status NOT IN ('Overturned', 'Partially Overturned', 'Upheld')
        OR determination_date IS NOT NULL
    )
);

CREATE INDEX IF NOT EXISTS idx_appeals_status    ON pa_appeals(status);
CREATE INDEX IF NOT EXISTS idx_appeals_request   ON pa_appeals(auth_request_id);
CREATE INDEX IF NOT EXISTS idx_appeals_reviewer  ON pa_appeals(assigned_reviewer_id)
    WHERE assigned_reviewer_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_appeals_deadline  ON pa_appeals(cms_deadline)
    WHERE status IN ('Received', 'In Review', 'Additional Info Requested', 'Peer Review Requested');

CREATE TABLE IF NOT EXISTS pa_appeal_actions (
    action_id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    appeal_id           UUID NOT NULL REFERENCES pa_appeals(appeal_id),
    reviewer_id         UUID REFERENCES pa_reviewers(reviewer_id),
    action_type         TEXT NOT NULL CHECK (action_type IN (
                            'filed', 'assignment', 'reassignment', 'status_change',
                            'note_added', 'info_requested', 'peer_review_requested',
                            'hearing_scheduled', 'iro_referred', 'determination'
                        )),
    previous_status     appeal_status,
    new_status          appeal_status,
    note                TEXT,
    metadata_json       JSONB,
    created_at          TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_appeal_actions_appeal ON pa_appeal_actions(appeal_id, created_at DESC);

-- CMS appeal deadline on INSERT (Part C: expedited 72h, standard 30 days pre-service,
-- retrospective 60 days). Demo-simplified windows.
CREATE OR REPLACE FUNCTION set_appeal_cms_deadline()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.urgency = 'expedited' THEN
        NEW.cms_deadline = NEW.filed_date + INTERVAL '72 hours';
    ELSIF NEW.urgency = 'standard' THEN
        NEW.cms_deadline = NEW.filed_date + INTERVAL '30 days';
    ELSE
        NEW.cms_deadline = NEW.filed_date + INTERVAL '60 days';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_appeal_cms_deadline ON pa_appeals;
CREATE TRIGGER trg_appeal_cms_deadline
    BEFORE INSERT ON pa_appeals
    FOR EACH ROW EXECUTE FUNCTION set_appeal_cms_deadline();

CREATE OR REPLACE FUNCTION update_appeal_timestamps()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();

    IF NEW.status IN ('Overturned', 'Partially Overturned', 'Upheld')
       AND (OLD.status IS NULL OR OLD.status NOT IN ('Overturned', 'Partially Overturned', 'Upheld'))
    THEN
        NEW.determination_date = now();
        NEW.turnaround_hours = EXTRACT(EPOCH FROM (now() - NEW.filed_date)) / 3600.0;
        IF NEW.urgency = 'expedited' THEN
            NEW.cms_compliant = (now() - NEW.filed_date) <= INTERVAL '72 hours';
        ELSIF NEW.urgency = 'standard' THEN
            NEW.cms_compliant = (now() - NEW.filed_date) <= INTERVAL '30 days';
        ELSE
            NEW.cms_compliant = (now() - NEW.filed_date) <= INTERVAL '60 days';
        END IF;
    END IF;

    IF NEW.assigned_reviewer_id IS NOT NULL
       AND (OLD.assigned_reviewer_id IS NULL OR OLD.assigned_reviewer_id != NEW.assigned_reviewer_id)
    THEN
        NEW.assigned_at = now();
    END IF;

    IF NEW.status != OLD.status THEN
        NEW.status_changed_at = now();
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_appeal_updated_at ON pa_appeals;
CREATE TRIGGER trg_appeal_updated_at
    BEFORE UPDATE ON pa_appeals
    FOR EACH ROW EXECUTE FUNCTION update_appeal_timestamps();

-- Appeals work queue (linked to the original determination)
CREATE OR REPLACE VIEW v_appeal_queue AS
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
    q.status::text             AS original_status,
    a.appeal_type::text,
    a.urgency::text,
    a.filed_by,
    a.filed_date,
    a.status::text,
    a.determination,
    orig.display_name          AS original_reviewer_name,
    rev.display_name           AS appeal_reviewer_name,
    rev.role::text             AS appeal_reviewer_role,
    a.assigned_at,
    a.cms_deadline,
    a.cms_compliant,
    a.determination_date,
    a.turnaround_hours,
    EXTRACT(EPOCH FROM (a.cms_deadline - now())) / 3600.0 AS hours_until_deadline
FROM pa_appeals a
JOIN pa_review_queue q  ON a.auth_request_id = q.auth_request_id
LEFT JOIN pa_reviewers orig ON a.original_reviewer_id = orig.reviewer_id
LEFT JOIN pa_reviewers rev  ON a.assigned_reviewer_id = rev.reviewer_id
ORDER BY
    CASE a.urgency WHEN 'expedited' THEN 1 WHEN 'standard' THEN 2 ELSE 3 END,
    a.cms_deadline ASC NULLS LAST,
    a.filed_date ASC;

-- ===========================================================================
-- PEER / PHYSICIAN REVIEW  (RFI: Prior Auth & Clinical Reviews — Peer Review)
--
-- Escalation from a UM nurse to a Medical Director / Peer Reviewer, with
-- specialty matching and peer-to-peer (P2P) discussion tracking.
-- ===========================================================================

DO $$ BEGIN
    CREATE TYPE peer_review_status AS ENUM (
        'Requested',
        'Scheduled',
        'P2P Completed',
        'Determination Made',
        'Cancelled'
    );
EXCEPTION WHEN duplicate_object THEN null;
END $$;

CREATE TABLE IF NOT EXISTS pa_peer_reviews (
    peer_review_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    auth_request_id      TEXT NOT NULL REFERENCES pa_review_queue(auth_request_id),
    requested_by_id      UUID REFERENCES pa_reviewers(reviewer_id),      -- UM nurse
    peer_reviewer_id     UUID REFERENCES pa_reviewers(reviewer_id),      -- Medical Director / Peer
    requested_specialty  TEXT,                                           -- specialty match target
    reason               TEXT,
    status               peer_review_status NOT NULL DEFAULT 'Requested',
    p2p_requested        BOOLEAN DEFAULT FALSE,      -- provider requested peer-to-peer
    p2p_scheduled_at     TIMESTAMPTZ,
    p2p_completed_at     TIMESTAMPTZ,
    p2p_summary          TEXT,                        -- outcome of the discussion
    determination        TEXT,                        -- uphold / overturn recommendation
    determination_notes  TEXT,
    notified_at          TIMESTAMPTZ,                 -- outcome notification sent
    created_at           TIMESTAMPTZ DEFAULT now(),
    updated_at           TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_peer_request  ON pa_peer_reviews(auth_request_id);
CREATE INDEX IF NOT EXISTS idx_peer_status   ON pa_peer_reviews(status);
CREATE INDEX IF NOT EXISTS idx_peer_reviewer ON pa_peer_reviews(peer_reviewer_id)
    WHERE peer_reviewer_id IS NOT NULL;

DROP TRIGGER IF EXISTS trg_peer_updated_at ON pa_peer_reviews;
CREATE TRIGGER trg_peer_updated_at
    BEFORE UPDATE ON pa_peer_reviews
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_pa();

-- ===========================================================================
-- CORRESPONDENCE  (RFI: Decision Processing + Correspondence Management)
--
-- Determination notices (approval / denial / partial) generated for a case,
-- with template versioning, PHI-redaction gating, and delivery tracking.
-- ===========================================================================

DO $$ BEGIN
    CREATE TYPE notice_type AS ENUM (
        'approval',
        'denial',
        'partial_approval',
        'additional_info_request',
        'appeal_acknowledgement',
        'appeal_determination'
    );
EXCEPTION WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE delivery_channel AS ENUM ('portal', 'secure_email', 'print_mail', 'fax');
EXCEPTION WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE delivery_status AS ENUM (
        'draft', 'pending_review', 'released', 'delivered', 'failed', 'returned'
    );
EXCEPTION WHEN duplicate_object THEN null;
END $$;

CREATE TABLE IF NOT EXISTS pa_correspondence (
    notice_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    auth_request_id      TEXT REFERENCES pa_review_queue(auth_request_id),
    appeal_id            UUID REFERENCES pa_appeals(appeal_id),
    notice_type          notice_type NOT NULL,
    recipient            TEXT,                        -- member / provider
    recipient_role       TEXT,
    language             TEXT DEFAULT 'en',
    template_version     TEXT,
    subject              TEXT,
    body_markdown        TEXT,                         -- generated notice body
    body_redacted        BOOLEAN DEFAULT FALSE,        -- passed PHI redaction gate
    redaction_notes      TEXT,
    includes_appeal_rights BOOLEAN DEFAULT FALSE,
    criteria_citation    TEXT,                         -- policy/criteria version cited
    pdf_path             TEXT,                          -- UC Volume path
    delivery_channel     delivery_channel DEFAULT 'portal',
    delivery_status      delivery_status NOT NULL DEFAULT 'draft',
    generated_by         TEXT,                          -- reviewer or 'ai_query'
    generated_at         TIMESTAMPTZ DEFAULT now(),
    released_at          TIMESTAMPTZ,
    delivered_at         TIMESTAMPTZ,
    created_at           TIMESTAMPTZ DEFAULT now(),
    updated_at           TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_corr_request ON pa_correspondence(auth_request_id);
CREATE INDEX IF NOT EXISTS idx_corr_appeal  ON pa_correspondence(appeal_id) WHERE appeal_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_corr_status  ON pa_correspondence(delivery_status);

DROP TRIGGER IF EXISTS trg_corr_updated_at ON pa_correspondence;
CREATE TRIGGER trg_corr_updated_at
    BEFORE UPDATE ON pa_correspondence
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_pa();

-- ===========================================================================
-- BUSINESS RULES ENGINE  (RFI: Business Rules Engine + Workflow Engine)
--
-- No-code, data-driven adjudication/routing rules authored by business users.
-- PARALLEL-FIRST: this store runs ALONGSIDE the existing Tier-1 deterministic
-- SQL (gold_pa_tier1_evaluation); it does not replace it. Each rule carries a
-- JSON condition set, an action, priority, effective dating, versioning, and an
-- approval workflow — with an immutable version audit for full traceability.
-- ===========================================================================

DO $$ BEGIN
    CREATE TYPE rule_action AS ENUM ('auto_approve', 'auto_deny', 'pend', 'route');
EXCEPTION WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE rule_status AS ENUM ('draft', 'pending_approval', 'active', 'retired');
EXCEPTION WHEN duplicate_object THEN null;
END $$;

CREATE TABLE IF NOT EXISTS pa_business_rules (
    rule_id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name                 TEXT NOT NULL,
    description          TEXT,
    category             TEXT,                        -- e.g. 'auto-adjudication', 'routing', 'escalation'
    line_of_business     TEXT,                        -- scope; NULL = all LOBs
    service_type         TEXT,                        -- scope; NULL = all services
    conditions_json      JSONB NOT NULL,              -- {"all":[{"field","op","value"}, ...]}
    action               rule_action NOT NULL,
    action_detail        TEXT,                        -- e.g. route target queue / reviewer role
    priority             INT NOT NULL DEFAULT 100,    -- lower = evaluated first
    effective_start_date DATE NOT NULL DEFAULT CURRENT_DATE,
    effective_end_date   DATE,                        -- NULL = open-ended
    version              INT NOT NULL DEFAULT 1,
    status               rule_status NOT NULL DEFAULT 'draft',
    created_by           TEXT,
    approved_by          TEXT,
    approved_at          TIMESTAMPTZ,
    created_at           TIMESTAMPTZ DEFAULT now(),
    updated_at           TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_rules_status ON pa_business_rules(status);
CREATE INDEX IF NOT EXISTS idx_rules_active ON pa_business_rules(priority)
    WHERE status = 'active';

-- Immutable version history (every create/edit/activate/retire snapshots here).
CREATE TABLE IF NOT EXISTS pa_rule_versions (
    version_id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    rule_id              UUID NOT NULL REFERENCES pa_business_rules(rule_id),
    version              INT NOT NULL,
    change_type          TEXT NOT NULL,               -- created / updated / activated / retired
    snapshot_json        JSONB NOT NULL,              -- full rule state at this version
    changed_by           TEXT,
    change_reason        TEXT,
    created_at           TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_rule_versions_rule ON pa_rule_versions(rule_id, version DESC);

DROP TRIGGER IF EXISTS trg_rules_updated_at ON pa_business_rules;
CREATE TRIGGER trg_rules_updated_at
    BEFORE UPDATE ON pa_business_rules
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_pa();

-- ===========================================================================
-- QUALITY ASSURANCE  (RFI: Quality Assurance tab)
--
-- QA sampling + weighted scorecards over completed determinations. Supports
-- random + targeted sampling, weighted questions with critical-error logic,
-- reviewer quality trending, and IRR (inter-rater reliability) via a nullable
-- second-reviewer score on the same case.
-- ===========================================================================

DO $$ BEGIN
    CREATE TYPE qa_status AS ENUM (
        'Pending Score',       -- Sampled, awaiting QA reviewer
        'Scored',              -- Scorecard completed
        'Acknowledged',        -- Reviewer acknowledged findings
        'Rebutted'             -- Reviewer disputes findings
    );
EXCEPTION WHEN duplicate_object THEN null;
END $$;

-- Scorecard template: weighted questions, some flagged critical (auto-fail).
CREATE TABLE IF NOT EXISTS pa_qa_questions (
    question_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    question_text   TEXT NOT NULL,
    weight          INT NOT NULL DEFAULT 10,
    is_critical     BOOLEAN NOT NULL DEFAULT FALSE,
    sort_order      INT NOT NULL DEFAULT 0,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT now()
);

-- One QA review per sampled case (a second row w/ different qa_reviewer_id
-- enables IRR / consensus review of the same auth_request_id).
CREATE TABLE IF NOT EXISTS pa_qa_reviews (
    qa_id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    auth_request_id     TEXT NOT NULL REFERENCES pa_review_queue(auth_request_id),
    case_reviewer_id    UUID REFERENCES pa_reviewers(reviewer_id),   -- reviewer being audited
    qa_reviewer_id      UUID REFERENCES pa_reviewers(reviewer_id),   -- QA auditor
    sample_reason       TEXT,                                        -- 'random' | 'targeted'
    status              qa_status NOT NULL DEFAULT 'Pending Score',

    scores_json         JSONB,                 -- {question_id: points_awarded}
    total_score         NUMERIC(6,2),          -- weighted points earned
    max_score           NUMERIC(6,2),          -- weighted points possible
    score_pct           NUMERIC(5,2),
    passed              BOOLEAN,
    critical_error      BOOLEAN DEFAULT FALSE,  -- any critical question failed
    findings            TEXT,
    coaching_notes      TEXT,
    reviewer_rebuttal   TEXT,

    sampled_at          TIMESTAMPTZ DEFAULT now(),
    scored_at           TIMESTAMPTZ,
    created_at          TIMESTAMPTZ DEFAULT now(),
    updated_at          TIMESTAMPTZ DEFAULT now(),

    CONSTRAINT chk_qa_scored_has_date CHECK (status = 'Pending Score' OR scored_at IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS idx_qa_status   ON pa_qa_reviews(status);
CREATE INDEX IF NOT EXISTS idx_qa_request  ON pa_qa_reviews(auth_request_id);
CREATE INDEX IF NOT EXISTS idx_qa_reviewer ON pa_qa_reviews(case_reviewer_id);

DROP TRIGGER IF EXISTS trg_qa_updated_at ON pa_qa_reviews;
CREATE TRIGGER trg_qa_updated_at
    BEFORE UPDATE ON pa_qa_reviews
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_pa();

-- Reviewer quality rollup (RFI: track reviewer quality scores over time).
CREATE OR REPLACE VIEW v_qa_reviewer_scorecard AS
SELECT
    r.reviewer_id,
    r.display_name,
    r.role::text,
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

-- ===========================================================================
-- LONGITUDINAL CASE TIMELINE  (RFI: one longitudinal record across workflows)
-- Unions review actions, appeal actions, and correspondence into one stream.
-- ===========================================================================

CREATE OR REPLACE VIEW v_case_timeline AS
SELECT
    ra.auth_request_id,
    'review'        AS workflow,
    ra.action_type,
    ra.previous_status::text AS previous_status,
    ra.new_status::text      AS new_status,
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
    aa.previous_status::text,
    aa.new_status::text,
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
    c.notice_type::text AS action_type,
    NULL, c.delivery_status::text,
    c.subject,
    c.generated_by,
    c.created_at
FROM pa_correspondence c
WHERE c.auth_request_id IS NOT NULL
ORDER BY created_at DESC;
