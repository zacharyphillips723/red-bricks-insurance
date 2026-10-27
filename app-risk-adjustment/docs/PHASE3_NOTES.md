# Phase 3 — Agents + tools + MLflow tracing + Unity Gateway

Status: **code built & syntax-checked ; not yet deployed** (2026-10-22). See DESIGN.md §7 / §19 Phase 3.

## Artifacts
- `src/agents/risk_adjustment_agent.py` — deployable `mlflow.pyfunc.ChatModel`, modeled on the FWA
  agent but **improved** (see below). 9 tools:
  `query_uc_table`, `search_clinical_notes` (VS over `documents.case_notes_vs_index`),
  **`verify_citation`**, `cms_hcc_grouper` (deterministic, `reference.cms_hcc_v28_grouper`),
  `compute_raf_delta`, `defensibility_score`, `memory_recall`, `memory_write_episode`, `memory_distill_lesson`.
- `src/notebooks/deploy_risk_adjustment_agent.py` — validate P0/P1/P2 deps → log_model (models-from-code)
  → register `{catalog}.analytics.risk_adjustment_agent` → alias `production`.

## Improvements over the FWA agent (per user steer)
1. **Parameterized SQL** for agent-controlled identifiers (`verify_citation`, `defensibility_score`) —
   the FWA agent interpolated identifiers into SQL (injection risk); unacceptable for a PHI app.
2. **`verify_citation` anti-hallucination guard** — a suspected HCC's cited `chunk_id` must be confirmed
   to exist *for that member* before it can be presented; else the agent must drop it. The FWA agent
   relied only on a "never fabricate" prompt. Directly serves the over-coding/RADV safety spine (§7.4).
3. **Per-step MLflow tracing** — `@mlflow.trace` on every tool/LLM/memory op (span types AGENT/LLM/TOOL/
   RETRIEVER). FWA traced only the top-level predict span.
4. **`model_config`-first config** — reads served-model `model_config`, then env, then default; FWA read
   only `os.environ`, which breaks on a served endpoint.
5. **Structured candidate output** — the agent appends a machine-readable ```json candidates block
   (hcc / evidence_state / chunk_id / raf / verified) so the app can drive adjudication + MLflow feedback.
   FWA returned free text only.
6. **MLflow 3** (`mlflow>=3.1.0`) + `psycopg2-binary` for Lakebase memory; graceful memory degradation.
7. Org policy (`agent_policy`) loaded read-only into the system prompt; Unity Gateway endpoint
   (`RA_SUSPECTING_ENDPOINT`) is config-driven — swap any FM with no code change.

## Not yet done (next live step)
Running `deploy_risk_adjustment_agent.py` needs: (a) `databricks bundle deploy` to sync `src/` to the
workspace, (b) run the deploy notebook (log+register), (c) create a Model Serving endpoint from
`...@production`, (d) grant the serving identity read on the allowlisted schemas + the
`red-bricks-ra-memory` Lakebase instance so memory works end-to-end. Alternatively verify in-process
(Phase 5 app, `AGENT_MODE=local`). **Held for go-ahead** — it's a multi-step live deploy.

Deferred: `hedis_measure_check` / Quality Gap tool (Stars module) — add in a later pass.

## Step B — in-process verification ✅ DONE (2026-10-22)
Ran the agent in-process (local, `databricks-claude-sonnet-5`) against the live workspace via
`~/ra_phase0/run_verify.py`. Results on member **MBR105480**:
- **Suspected HCC19** (diabetes) with **verified** citation `DOC103553_chunk_000`; correctly declined HCC18
  (no complication documented) — conservative, anti-over-coding.
- **Weak** coded HCC85/135/18 flagged at 8/10 RADV risk ("medication alone is not diagnostic evidence").
- Clean candidate JSON (potential_raf_gain 0.2, raf_at_radv_risk 1.1, verified flags).
- **20-span trace** AGENT→LLM→memory_recall→query_uc_table→search_clinical_notes×4→verify_citation→
  defensibility_score×3→compute_raf_delta×2→LLM. Every tool/LLM/memory op traced.
- Direct checks: grouper negative control (`I10`→no HCC) ✅; verify_citation true (real) / false (fake) ✅.

**Bugs found & fixed in B** (the reason to verify before deploying):
1. default endpoint `llama-4-maverick` doesn't exist here → `databricks-claude-sonnet-5`.
2. Claude 5 rejects `temperature` → omitted entirely.
3. member-id parse stripped `MBR-` but data uses `MBR######` (no dash).
4. 6 tool rounds too few → 10.
5. Claude returns `content` as a list of blocks → `_content_to_str` normalizes.

**Model dropdown (user request):** agent now takes a per-request override `params.custom_inputs.model_endpoint`
(DESIGN §6.1) — the app can switch any Unity-Gateway model with no redeploy.

## Exit gate
- [x] Agent answers a case end-to-end with VERIFIED cited evidence (Step B).
- [x] Full span tree (AGENT/LLM/TOOL/RETRIEVER/MEMORY) emitted (Step B; lands in `analytics.ra_agent_otel_spans` on deploy).
- [ ] Lesson written + recalled on a second case — memory degraded locally (no psycopg2); verify in Step A (served model has it).

## Step A — targeted deploy (in progress, 2026-10-23)
Approved: targeted import+run now; full `bundle deploy` to work in future; grants OK; add grants +
setup-notebook wiring to the full-job tasks for future runs.

Done:
- Local `log_model` works but local→UC **register** needs `mlflow[databricks]` (PEP 668 blocks local install)
  → used the intended in-workspace path instead.
- Imported `risk_adjustment_agent.py` (as a FILE) + `deploy_risk_adjustment_agent` (NOTEBOOK) to
  `/Users/zack.phillips@databricks.com/ra_deploy/src/...`.
- Submitted a serverless one-time run of the deploy notebook → **SUCCESS** (run 239814122409834).
- `red_bricks_insurance_catalog.analytics.risk_adjustment_agent` **v1 registered, alias `production` set**.
  v1 status was `PENDING_REGISTRATION` (UC finalizing) when last checked — expected to reach READY async.

Remaining (next session / when v1 READY):
1. Create Model Serving endpoint from `...risk_adjustment_agent` v1.
2. **Grant the serving identity** read on allowlisted schemas + the `red-bricks-ra-memory` Lakebase instance.
3. Verify the **served memory write/recall loop** (the one exit-gate item B couldn't cover locally).

## TODO — future full-job wiring (user-requested)
Add to `resources/full_demo_job.yml`: tasks `setup_hcc_grouper`, `setup_ra_coherent_data`,
`setup_agent_policy`, `setup_lakebase_ra_memory`, `deploy_risk_adjustment_agent` (+ the serving-identity
grants) in dependency order (DESIGN §19 DAG), so a full `bundle deploy` + job run builds RA end-to-end.
