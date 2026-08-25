-- ============================================================================
-- Lakebase (PostgreSQL) Schema — Denial Scrub
--
-- Operational store for the provider-facing Claim Scrubber / Denial Risk
-- Predictor app. Persists each pre-submission scrub, its per-reason findings,
-- and the remediation guidance — including the "resubmit clean" lineage that
-- links an amended draft back to the original scrub.
--
-- IDEMPOTENT: Safe to run multiple times (uses IF NOT EXISTS / DO blocks).
-- ============================================================================

-- ---------------------------------------------------------------------------
-- Enum types for controlled vocabularies
-- ---------------------------------------------------------------------------

DO $$ BEGIN
    CREATE TYPE scrub_request_type AS ENUM (
        'claim',            -- Post-service medical claim (837)
        'prior_auth'        -- Pre-service authorization request
    );
EXCEPTION WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE scrub_decision AS ENUM (
        'clean',            -- Low denial risk — safe to submit
        'at_risk',          -- Medium risk — review flagged issues
        'likely_denied'     -- High risk — fix before submitting
    );
EXCEPTION WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE scrub_reason_layer AS ENUM (
        'rule',             -- Deterministic pre-submission rule
        'ml',               -- Denial-prediction / reason classifier
        'rag'               -- Medical-policy retrieval-augmented reasoning
    );
EXCEPTION WHEN duplicate_object THEN null;
END $$;

-- ---------------------------------------------------------------------------
-- scrub_sessions — one row per scrub run
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS scrub_sessions (
    session_id        TEXT PRIMARY KEY,
    member_id         TEXT NOT NULL,
    member_name       TEXT,
    provider_npi      TEXT,
    date_of_service   DATE,
    request_type      scrub_request_type NOT NULL DEFAULT 'claim',
    risk_score        INT,
    decision          scrub_decision,
    ml_denial_prob    DOUBLE PRECISION,
    line_count        INT DEFAULT 0,
    dx_codes          TEXT,
    clinical_notes    TEXT,
    -- "resubmit clean" lineage: points at the original scrub this one amends.
    resubmitted_from  TEXT REFERENCES scrub_sessions(session_id),
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_scrub_sessions_member
    ON scrub_sessions (member_id);
CREATE INDEX IF NOT EXISTS idx_scrub_sessions_created
    ON scrub_sessions (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_scrub_sessions_resubmit
    ON scrub_sessions (resubmitted_from);

-- ---------------------------------------------------------------------------
-- scrub_line_findings — one row per predicted denial reason
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS scrub_line_findings (
    finding_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id        TEXT NOT NULL REFERENCES scrub_sessions(session_id) ON DELETE CASCADE,
    carc_code         TEXT NOT NULL,
    reason_category   TEXT,
    reason_label      TEXT,
    likelihood        DOUBLE PRECISION,
    layer             scrub_reason_layer,
    evidence          TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_scrub_findings_session
    ON scrub_line_findings (session_id);

-- ---------------------------------------------------------------------------
-- scrub_remediations — pre-submission fix guidance per finding
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS scrub_remediations (
    remediation_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id        TEXT NOT NULL REFERENCES scrub_sessions(session_id) ON DELETE CASCADE,
    carc_code         TEXT NOT NULL,
    remediation_text  TEXT,
    required_action   TEXT,
    doc_needed        TEXT,
    applied           BOOLEAN NOT NULL DEFAULT FALSE,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_scrub_remediations_session
    ON scrub_remediations (session_id);

-- ---------------------------------------------------------------------------
-- Link scrubs to their MLflow agent trace (for user-feedback assessments)
-- ---------------------------------------------------------------------------

ALTER TABLE scrub_sessions ADD COLUMN IF NOT EXISTS mlflow_trace_id TEXT;

-- ---------------------------------------------------------------------------
-- scrub_feedback — human feedback on the agent's denial reasoning.
-- Mirrors the MLflow Feedback assessment logged on the trace (governed artifact),
-- kept here for durable in-app display and Observability.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS scrub_feedback (
    feedback_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id        TEXT REFERENCES scrub_sessions(session_id) ON DELETE CASCADE,
    trace_id          TEXT,
    target            TEXT NOT NULL,          -- 'overall' or a CARC code
    value             INT NOT NULL,           -- +1 thumbs up, -1 thumbs down
    rationale         TEXT,
    source_id         TEXT,                   -- reviewer identity (app auth email)
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_scrub_feedback_session
    ON scrub_feedback (session_id);
CREATE INDEX IF NOT EXISTS idx_scrub_feedback_created
    ON scrub_feedback (created_at DESC);
