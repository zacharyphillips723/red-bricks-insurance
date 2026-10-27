-- Red Bricks Insurance — Risk Adjustment app: Lakebase (Postgres) schema.
-- Phase 2. The agent's SELF-AUTHORED memory (read/write) + case transactional state.
-- The org priority context (agent_policy) lives in governed Unity Catalog, NOT here.
-- Conversational memory REUSES the existing conversations / conversation_messages tables
-- in src/lakebase_schema.sql — do not duplicate them here.
--
-- pgvector: required for the semantic lessons tier. Lakebase is PG 16, which supports it,
-- but AVAILABILITY MUST BE CONFIRMED AT RUN (DESIGN.md §8.5). If unavailable, drop the
-- `embedding` column + its index and fall back to Databricks Vector Search over a Delta
-- mirror of agent_lessons, or structured scope/subject filters only.

CREATE EXTENSION IF NOT EXISTS vector;

-- Episodic memory: what happened on each case (written on case close).
CREATE TABLE IF NOT EXISTS agent_episodes (
  episode_id        TEXT PRIMARY KEY,
  case_id           TEXT,
  member_id         TEXT,
  user_id           TEXT,
  suspected_hccs    JSONB,          -- [{hcc, evidence_chunk_id, model_confidence}]
  coder_decisions   JSONB,          -- [{hcc, decision: code|reject|query, reason}]
  raf_delta         NUMERIC,
  outcome           TEXT,
  -- §15.5 reproducibility: pin what was active when the agent made its suggestions
  model_version     TEXT,
  policy_version    INT,
  grouper_version   TEXT,
  lesson_snapshot_id TEXT,
  created_at        TIMESTAMPTZ DEFAULT now()
);

-- Semantic / lessons-learned: distilled, reusable patterns the agent writes and recalls.
CREATE TABLE IF NOT EXISTS agent_lessons (
  lesson_id       TEXT PRIMARY KEY,
  scope           TEXT,             -- provider | coder | hcc | global
  subject_key     TEXT,             -- provider NPI | user_id | hcc_code | 'global'
  lesson_text     TEXT,
  embedding       VECTOR(1024),     -- pgvector; see note above
  provenance      JSONB,            -- episodes/decisions this was distilled from
  confidence      NUMERIC,
  status          TEXT DEFAULT 'provisional',  -- provisional | confirmed | retired
  times_recalled  INT  DEFAULT 0,
  created_at      TIMESTAMPTZ DEFAULT now(),
  updated_at      TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_lessons_scope ON agent_lessons (scope, subject_key, status);
-- ANN index (only if pgvector present):
-- CREATE INDEX idx_lessons_embedding ON agent_lessons USING hnsw (embedding vector_cosine_ops);

-- Inferred per-user preferences (agent's model of THIS coder).
CREATE TABLE IF NOT EXISTS agent_user_prefs (
  user_id     TEXT,
  pref_key    TEXT,
  pref_value  TEXT,
  source      TEXT DEFAULT 'inferred',   -- inferred | explicit
  updated_at  TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (user_id, pref_key)
);

-- Case transactional state + the adjudication audit trail (the compliance record).
CREATE TABLE IF NOT EXISTS ra_cases (
  case_id     TEXT PRIMARY KEY,
  member_id   TEXT,
  user_id     TEXT,
  status      TEXT DEFAULT 'open',   -- open|under_review|coded|submitted|radv_flagged|closed (DESIGN §16.1)
  created_at  TIMESTAMPTZ DEFAULT now(),
  updated_at  TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ra_hcc_candidates (
  candidate_id     TEXT PRIMARY KEY,
  case_id          TEXT,
  member_id        TEXT,
  hcc_code         TEXT,
  evidence_chunk_id TEXT,            -- -> documents.silver_case_notes_chunks.chunk_id
  evidence_state   TEXT,            -- documented|suspected|weak|non_hcc_noise
  ai_confidence    NUMERIC,
  raf_value        NUMERIC,
  created_at       TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ra_adjudications (
  adjudication_id  TEXT PRIMARY KEY,
  case_id          TEXT,
  member_id        TEXT,
  user_id          TEXT,
  hcc_code         TEXT,
  ai_suggested     BOOLEAN,
  ai_confidence    NUMERIC,
  decision         TEXT,            -- code | reject | query
  reason           TEXT,
  evidence_chunk_id TEXT,
  created_at       TIMESTAMPTZ DEFAULT now()
);
