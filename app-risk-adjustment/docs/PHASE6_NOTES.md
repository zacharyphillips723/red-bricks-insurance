# Phase 6 — End-to-end DAB, observability & demo seeding

Status: **started 2026-10-23** — app deployed live; DAB app resource authored; job-wiring + observability + seeding to do.

## App — DEPLOYED & RUNNING ✅
- Databricks App **`red-bricks-ra-workbench`** — https://red-bricks-ra-workbench-7474660722665158.aws.databricksapps.com
- `AGENT_MODE=local` (vendored `app-risk-adjustment/risk_adjustment_agent.py`). Source at
  `/Workspace/Users/zack.phillips@databricks.com/ra-workbench-src`.
- App SP `5afd32ea-25f5-4ef9-b22c-98a11c506ce4` granted: USE CATALOG + USE SCHEMA + SELECT on the catalog,
  warehouse CAN_USE, FM endpoint CAN_QUERY (via app.yml resource).
- **Follow-up grants still needed** for full function: CAN_USE on the Vector Search endpoint
  (`red-bricks-vs-endpoint`) and a Lakebase role on `red-bricks-ra-memory` (adjudication writes) — else those
  degrade gracefully.

## DAB app resource — authored
`resources/app_risk_adjustment.yml` (mirrors `app_fwa.yml`; `${var.catalog}`/`${var.warehouse_id}` for bundle deploys).

## Full-job wiring — to add to `resources/full_demo_job.yml` (user-requested, future bundle run)
Tasks in dependency order (DESIGN §19 DAG):
1. `setup_hcc_grouper` (P0) — notebook `src/notebooks/setup_hcc_grouper.py`
2. `setup_ra_coherent_data` (P1) — depends on documents_pipeline + clinical_pipeline + risk_adjustment_pipeline + setup_hcc_grouper
3. `setup_agent_policy` (P2a) — `src/notebooks/setup_agent_policy.py`
4. `setup_lakebase_ra_memory` (P2b) — `src/notebooks/setup_lakebase_ra_memory.py` (instance red-bricks-ra-memory)
5. `build_ra_eval_dataset` (P4) — depends on setup_ra_coherent_data
6. `deploy_risk_adjustment_agent` (P3) — depends on setup_ra_coherent_data + setup_agent_policy
   + **serving-identity grants** (SELECT on catalog, warehouse CAN_USE, VS endpoint CAN_USE, Lakebase role)
7. `evaluate_risk_adjustment_agent` (P4) — depends on deploy + build_ra_eval_dataset — QUALITY GATE
8. app `red_bricks_ra_workbench` (P5) — `resources/app_risk_adjustment.yml`

## Remaining Phase 6
- Insert the tasks above into `full_demo_job.yml` (careful edit of the big job DAG).
- **Observability dashboard** over `analytics.ra_agent_otel_spans` (reuse the FWA observability pattern):
  per-step latency, token cost, tool error rate, over-coding-rate trend, acceptance rate.
- **Demo seeding** — seed a real case queue into Lakebase from `v_member_hcc_coherent`, and optionally
  pre-confirm a lesson or two so the 3-case learning arc (DESIGN §11) is reliable live.

## Serving blocker (carried from Phase 3 A — investigation done)
`risk_adjustment_agent` v1 stuck `PENDING_REGISTRATION` ~40 min (survived re-register). Artifacts in the
**external S3** catalog-default location; UC model-registry can't finalize the copy — storage-credential /
external-location gap, likely specific to the MLflow-3 logged-model ("models from code") flow (FWA agents
registered fine under the older flow). **Fix:** register to a **UC Volume** or a Databricks-managed location,
or configure the external location's storage credential for the registry. Then `~/ra_phase0/run_serve.py`.
The app runs fine WITHOUT the served endpoint (AGENT_MODE=local), so this blocks only `AGENT_MODE=endpoint`.
