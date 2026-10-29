# UW-Sim: Lakehouse → Lakebase writeback rewrite — SCOPE

**Goal:** move the underwriting-sim's simulation/app-state writeback off Delta
`{catalog}.app_state.*` back to **Lakebase Postgres** (`uw_sim` database), so the app is
fully Lakebase-backed (simulation state + the digital-twin memory on one DB). **Preserve all
Phase 1–4 vision-roadmap features** — this is a backend swap, not a feature rewrite.

Unlike FWA / command-center / prior-auth (clean single-commit reverts), uw-sim's Lakehouse
conversion (`d3177c7`) is buried under six later roadmap commits, so this is a *forward*
rewrite, not a `git checkout`.

## Current architecture (hybrid)
- `backend/database.py` = **LakehouseStore** → Delta `{catalog}.app_state.*` (13 tables).
  Translates Postgres-flavored SQL → Databricks SQL (`_translate`), auto-injects surrogate ids.
- `backend/twin_db.py` + `backend/twin_memory.py` = **Lakebase** `uw_sim` DB (decision_memory +
  pgvector). Already Lakebase — SQLAlchemy async engine + OAuth token-refresh (`do_connect`).
- Roadmap modules (`approvals`, `factor_governance`, `funding_store`, `scenarios`,
  `ops_analytics`, `router`) all use `database.py`'s `session()` surface and emit **neutral SQL**
  (no Databricks-isms — those live only in `database.py`/`twin_memory.py`).
- Env: `APP_STATE_SCHEMA=app_state` + `UC_CATALOG` + warehouse.

## Target architecture (all-Lakebase)
- `database.py` becomes a **Lakebase store with the identical public surface**
  (`initialize`, `ensure_tables(ddl)`, `session()`→execute/commit, `start_refresh`, `close`,
  `is_healthy`), reusing `twin_db.py`'s proven connection + token-refresh, pointed at `uw_sim`.
- All 13 tables live in Lakebase `uw_sim` (Postgres + pgvector). The twin's
  `decision_memory`/`decision_embeddings` already live there → consolidate (one owner).
- Env: drop `APP_STATE_SCHEMA`; add `LAKEBASE_PROJECT_ID=red-bricks-insurance`,
  `LAKEBASE_DATABASE_NAME=uw_sim`, `LAKEBASE_BRANCH=production` (matches `twin_db` + the other
  reverted apps). Keep `UC_CATALOG`/warehouse for reads that hit UC gold tables.

## Work items
1. **`src/underwriting_sim_lakebase_schema.sql`** — add the **5 roadmap tables** missing from
   the Postgres schema (present only in the Delta DDL): `approval_requests`,
   `approval_audit_log`, `factor_versions`, `funding_quotes`, `quote_revisions`. Port from the
   Delta `lakehouse_schema.sql` (STRING→TEXT, DOUBLE→DOUBLE PRECISION, TIMESTAMP→TIMESTAMPTZ,
   MAP/STRUCT/ARRAY→JSONB), keep PKs + `gen_random_uuid()`/`now()` defaults. Existing 8 tables +
   pgvector `decision_embeddings` stay as-is.
2. **`backend/database.py`** — replace LakehouseStore internals with a Lakebase-backed store
   modeled on `twin_db.py`'s `TwinLakebase`, preserving the public surface so roadmap modules are
   untouched. `ensure_tables` runs the Postgres schema (idempotent) on the Lakebase connection.
   Remove the `_translate` (Postgres→Databricks) layer and auto-id injection (native Postgres;
   `gen_random_uuid()` column defaults handle ids).
3. **Env** — `app-underwriting-sim/app.yml` + `resources/app_underwriting_sim.yml`: swap
   `APP_STATE_SCHEMA` → `LAKEBASE_*` (project/db/branch).
4. **`requirements.txt`** — restore the SQLAlchemy async + Postgres driver deps `d3177c7` removed
   (match the driver `twin_db.py` uses so there's one connection stack).
5. **Consolidate the twin** — point `database.py` and `twin_db.py` at the same `uw_sim` DB
   (twin already does); verify no table-ownership collisions on decision_memory/embeddings.
6. **Delete** `backend/lakehouse_schema.sql` once the Postgres schema is complete.
7. **Defaults** — port whatever currently seeds `factor_versions`/`org_guardrails` defaults
   (app-side `_ensure_default` or schema INSERT) so a fresh `uw_sim` gets them.
8. **Grants** — uw-sim app SP already has Lakebase `uw_sim` access (the twin works today); no
   new grant expected. Confirm during verification.

## Risks / watch-items
- **Low**: roadmap SQL silently relying on `_translate`'s Databricks rewrite — audited: Postgres-
  isms are confined to `database.py`/`twin_memory.py`, roadmap modules emit neutral SQL.
- **pgvector** must be available on `uw_sim` — the twin already uses it → confirmed.
- **Async driver parity** — reuse `twin_db.py`'s exact driver to avoid two stacks.
- **No data migration**: the current Delta `app_state` rows (1 `factor_versions` default + any
  interactive state) are **abandoned, not migrated** — acceptable (demo/interactive state; fresh
  start on Lakebase). Flagging explicitly.

## Verification plan
- Deploy (targeted import-dir + `apps deploy`, same as the other three); app RUNNING.
- `ensure_tables` creates all 13 tables in `uw_sim`.
- Smoke: create a simulation → row in `uw_sim.simulations`; approvals/funding/factor-governance
  flows write to their Lakebase tables; twin `decision_memory` + pgvector recall works.
- Confirm no `app_state` Delta reads remain in the app path.

## Deploy
Targeted import-dir + `apps deploy` (no full-job re-run needed — `ensure_tables` self-creates the
Postgres tables on startup; data is interactive). `setup_lakebase.py` already provisions `uw_sim`
from this schema file, so the 5 added tables flow into a future clean deploy automatically.
