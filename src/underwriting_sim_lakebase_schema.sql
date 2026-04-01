-- ============================================================================
-- Lakebase (PostgreSQL) Schema — Underwriting Simulations
--
-- Run this against the uw-simulations Lakebase instance to create the
-- operational tables that power the Underwriting Simulation Portal app.
--
-- IDEMPOTENT: Safe to run multiple times (uses IF NOT EXISTS / DO blocks).
-- ============================================================================

-- ---------------------------------------------------------------------------
-- Enum types for controlled vocabularies
-- ---------------------------------------------------------------------------

DO $$ BEGIN
    CREATE TYPE simulation_status AS ENUM (
        'draft',            -- Created but not yet computed
        'computed',         -- Results calculated
        'approved',         -- Approved by management
        'archived'          -- No longer active
    );
EXCEPTION WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE simulation_type AS ENUM (
        'premium_rate',         -- Premium rate change
        'benefit_design',       -- Benefit design change (deductible/copay/coinsurance)
        'group_renewal',        -- Group renewal pricing
        'population_mix',       -- Population mix shift
        'medical_trend',        -- Medical trend sensitivity
        'stop_loss',            -- Stop-loss threshold change
        'risk_adjustment',      -- Risk adjustment / coding completeness
        'utilization_change',   -- Utilization change by category
        'new_group_quote',      -- New group quote
        'ibnr_reserve'          -- IBNR reserve adequacy
    );
EXCEPTION WHEN duplicate_object THEN null;
END $$;

-- ---------------------------------------------------------------------------
-- Simulations (core table)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS simulations (
    simulation_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    simulation_name     TEXT NOT NULL,
    simulation_type     simulation_type NOT NULL,
    created_by          TEXT NOT NULL,

    -- Scenario inputs and computed outputs (JSONB for flexibility per type)
    parameters          JSONB NOT NULL DEFAULT '{}',
    results             JSONB,
    baseline_snapshot   JSONB,

    -- Workflow
    status              simulation_status NOT NULL DEFAULT 'draft',

    -- Scope (optional narrowing)
    scope_lob           TEXT,           -- e.g., 'Commercial', 'Medicare Advantage'
    scope_group_id      TEXT,           -- e.g., 'GRP-001'

    -- Notes
    notes               TEXT,

    -- Audit
    created_at          TIMESTAMPTZ DEFAULT now(),
    updated_at          TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_sim_type     ON simulations(simulation_type);
CREATE INDEX IF NOT EXISTS idx_sim_status   ON simulations(status);
CREATE INDEX IF NOT EXISTS idx_sim_created  ON simulations(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_sim_lob      ON simulations(scope_lob) WHERE scope_lob IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_sim_group    ON simulations(scope_group_id) WHERE scope_group_id IS NOT NULL;

-- ---------------------------------------------------------------------------
-- Comparison sets (side-by-side analysis of 2-4 simulations)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS comparison_sets (
    comparison_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    comparison_name     TEXT NOT NULL,
    created_by          TEXT NOT NULL,
    simulation_ids      UUID[] NOT NULL,
    notes               TEXT,
    created_at          TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_comp_created ON comparison_sets(created_at DESC);

-- ---------------------------------------------------------------------------
-- Simulation audit log (immutable)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS simulation_audit_log (
    audit_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    simulation_id       UUID NOT NULL REFERENCES simulations(simulation_id),
    action              TEXT NOT NULL CHECK (action IN (
                            'created', 'computed', 'approved',
                            'shared', 'archived', 'note_added',
                            'added_to_comparison'
                        )),
    actor               TEXT NOT NULL,
    details             JSONB,
    created_at          TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_audit_sim ON simulation_audit_log(simulation_id, created_at DESC);

-- ---------------------------------------------------------------------------
-- Auto-update timestamps
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION update_simulation_timestamps()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_simulations_updated_at ON simulations;
CREATE TRIGGER trg_simulations_updated_at
    BEFORE UPDATE ON simulations
    FOR EACH ROW EXECUTE FUNCTION update_simulation_timestamps();

-- ---------------------------------------------------------------------------
-- Views
-- ---------------------------------------------------------------------------

-- Simulation list (what users see on the history page)
CREATE OR REPLACE VIEW v_simulation_list AS
SELECT
    s.simulation_id,
    s.simulation_name,
    s.simulation_type::text,
    s.status::text,
    s.scope_lob,
    s.scope_group_id,
    s.created_by,
    s.notes,
    -- Extract key result metrics for the list view
    (s.results->>'narrative')::text AS narrative,
    s.created_at,
    s.updated_at
FROM simulations s
ORDER BY s.created_at DESC;

-- Comparison detail (joins simulations into the comparison)
CREATE OR REPLACE VIEW v_comparison_detail AS
SELECT
    c.comparison_id,
    c.comparison_name,
    c.created_by,
    c.simulation_ids,
    c.notes,
    c.created_at
FROM comparison_sets c
ORDER BY c.created_at DESC;
