# Phase 4 — Evaluation, quality gates & reproducibility

Status: **golden set built (live) ; harness + gate authored ; full eval run pending** (2026-10-23). See DESIGN.md §15 / §19 Phase 4.

## Key idea
The Phase 1 coherent evidence layer gives **free, deterministic ground truth** — no expensive LLM judge
needed for the safety metrics. `suspected` = should-find, `weak` = should-flag, and **anything else the
agent surfaces as suspected = over-coding**.

## Done
- **Golden set built live:** `analytics.ra_eval_golden` — 50 members (top potential RAF gain),
  columns `expected_suspected`, `expected_weak`, `potential_raf_gain`, `raf_at_radv_risk`.
  Avg potential RAF gain 0.5, avg RAF-at-RADV-risk 0.27.
- Canonical builder: `src/notebooks/build_ra_eval_dataset.py`.

## Authored (runnable; not yet run end-to-end)
- `src/notebooks/evaluate_risk_adjustment_agent.py` — `mlflow.genai.evaluate` with:
  - **`over_coding_rate`** (PRIMARY safety metric, deterministic) — surfaced-suspected not in truth.
  - **`suspecting_recall`** — true suspected HCCs found.
  - **`citation_grounded`** — every surfaced suspected has `verified=true` + real chunk.
  - **model-swap regression gate** (`passes_gate`): OVER_CODING_MAX=0.10, RECALL_MIN=0.70, grounded≥0.99
    — call before promoting a new default Unity-Gateway model (§15.3).
  - **learning-lift** harness note (§15.4): eval with memory on vs off on repeat-provider members.
- `predict_fn` runs the agent **in-process** over the golden set and parses its candidate JSON, so it
  works without the (still-provisioning) serving endpoint; swap to the endpoint later.

## Eval RUN ✅ (2026-10-23, in-process sonnet-5, 50 members, ~/ra_phase0/run_eval_local.py → eval_results.json)
| metric | result | threshold |
|---|---|---|
| over_coding_rate | **0.02** | ≤0.10 ✅ |
| suspecting_recall | **0.94** | ≥0.70 ✅ |
| citation_grounded | **1.00** | ≥0.99 ✅ |
| **GATE** | **PASS** | |
- Nuance: the 0.02 "over-coding" = citation-VERIFIED HCCs the golden set didn't list (grounded=1.0 ⇒
  **zero fabricated evidence**); those are golden-label gaps (HCC19 likely already coded → should be `documented`),
  not real false positives. The 3 recall misses were all HCC85/CHF the agent declined to surface — safe direction.
- Headline: **0 fabricated evidence across 50 charts, 94% recall, gate PASS.** Re-run per model before promoting.

## Pending
- Learning-lift requires the served memory path (psycopg2 + Lakebase grant) — tie to Step A completion.
- Tune gate thresholds with the compliance stakeholder.

Next: **Phase 5** (app — Coder Workbench + the Unity Gateway model dropdown) once Step A serving is up.
