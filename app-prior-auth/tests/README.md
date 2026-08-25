# Prior Auth — Tier Logic Tests

Tests for the Red Bricks PA Review Portal's tiered auto-adjudication logic.

## The three tiers, and how each is tested

The "3-tier" decisioning is not one component — it lives in different places
with very different testability:

| Tier | Implementation | Deterministic? | Test layer |
|------|----------------|----------------|------------|
| **Tier 1 — deterministic rules** | `backend/documents.py::adjudicate()` (Python, runs live in the app) **and** `gold_pa_tier1_evaluation` (SQL view) | ✅ Yes | **Unit + regression** (offline) and a **SQL agreement** integration test |
| **Tier 2 — ML classification** | XGBoost `pa_adjudication_model` → `pa_ml_predictions` | ⚠️ Statistical | Integration: output class + confidence range sanity |
| **Tier 3 — LLM clinical review** | `pa_review_agent` (Claude Haiku 4.5), `backend/agent.py::query_pa_agent()` | ❌ No | Integration: structural smoke test (5 mandated sections) |

Tier 1 is the only fully deterministic tier, so it carries the real unit-test
weight. Tiers 2 and 3 get workspace-gated integration checks that assert
*contracts* (valid classes, confidence bounds, required briefing sections) —
never exact ML scores or LLM wording.

## Layout

- `conftest.py` — freezes config env vars so importing the backend never hits
  the network, and provides the `patch_sql` fixture that stubs the one I/O seam
  (`_execute_sql`) with synthetic medical-policy rules.
- `test_tier1_adjudication.py` — unit tests for `_split_codes`, `_has_value`,
  and the `adjudicate()` decision engine (Approve / Deny / Needs Review).
- `test_tier1_regression.py` — invariants that must never silently break, above
  all **exact code matching** (a superstring like `274470` must not match the
  covered `27447`). This is the specific bug `documents.py`'s docstring calls
  out; if the matcher ever reverts to substring/`LIKE` logic, these fail.
- `test_integration_tiers.py` — workspace-gated tests for the SQL view, the ML
  model, and the LLM agent. Skipped unless `RUN_DB_INTEGRATION=1`.

## Running

Run from the `app-prior-auth/` directory (so `import backend` resolves):

```bash
cd app-prior-auth

# Fast, offline unit + regression suite (no workspace needed) — this is the CI target
python3 -m pytest tests/test_tier1_adjudication.py tests/test_tier1_regression.py

# Everything, including workspace integration tests (needs a deployed workspace)
RUN_DB_INTEGRATION=1 UC_CATALOG=red_bricks_insurance SQL_WAREHOUSE_ID=<id> \
  python3 -m pytest tests
```

Requires: `pip install pytest` (the app's own deps are already in
`requirements.txt`; `mlflow`, `databricks-sdk`, and `sqlalchemy` must import).

## CI note

Only the two Tier-1 files belong in an offline CI gate — they run in ~2s with no
workspace. The integration file stays skipped in CI and is meant to run as a
post-deploy smoke check against a live Red Bricks workspace.
