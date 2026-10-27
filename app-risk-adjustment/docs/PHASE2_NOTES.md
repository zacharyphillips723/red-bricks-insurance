# Phase 2 — Lakebase memory + case state + org policy

Status: **2a done (UC) ; 2b built, pending a provisioning decision** (2026-10-22). See DESIGN.md §8 / §19 Phase 2.

## 2a — Org priority context (governed UC, read-only) — ✅ DONE on workspace
- `risk_adjustment.agent_policy` created + seeded with **8 hard policies**
  (never_auto_submit, human_adjudication, deterministic_grouping, high_value_signoff,
  memory_subordinate, allowlisted_schemas, phi_handling, suspect_requires_citation).
- This is the authoritative context the agent **reads** and that **wins all conflicts** with
  self-authored memory. Canonical artifact: `src/notebooks/setup_agent_policy.py`.
- TODO at app time: `GRANT SELECT ... TO <app service principal>` (read-only lock).

## 2b — Agent self-authored memory on Lakebase — ✅ DONE on workspace (2026-10-22)
- **Provisioned** a dedicated Lakebase instance **`red-bricks-ra-memory`** (capacity CU_1, PG 16,
  scale-to-zero; DNS `ep-frosty-cherry-d2b6zfc7.database.us-east-1.cloud.databricks.com`, db `databricks_postgres`).
- Applied canonical DDL `src/risk_adjustment_lakebase_schema.sql` → 6 tables created:
  `agent_episodes` (w/ §15.5 reproducibility pins), `agent_lessons` (pgvector `VECTOR(1024)`),
  `agent_user_prefs`, `ra_cases`, `ra_hcc_candidates`, `ra_adjudications`.
  Conversational tier **reuses** existing `conversations` / `conversation_messages`.
- **pgvector CONFIRMED** on Lakebase PG 16 — `CREATE EXTENSION vector` succeeded; `<=>` operator works
  (resolves the §8.5 verify item). No fallback needed.
- **Round-trip verified**: write/read across lessons + case + candidate + adjudication
  (`_c1 | MBR105480 | under_review | HCC19 | suspected | code`); test rows cleaned up (0 residue).
  Caveat: the throwaway similarity test used constant (collinear) vectors so both cosine distances
  were 0.0 — operator executes/orders fine; real embeddings will differentiate.
- Setup notebook `src/notebooks/setup_lakebase_ra_memory.py` is the canonical in-workspace artifact.
- TODO (Phase 5/6): seed a real case queue from `risk_adjustment.v_member_hcc_coherent`
  (UC→Lakebase bridge) once case generation / the app exists.

**Phase 2 complete.** Next: **Phase 3** — agents + tools + MLflow tracing + Unity Gateway.
