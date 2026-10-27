# Phase 5 — Coder Workbench app (React + FastAPI, tabbed rework)

Status: **REWORKED, verified locally, deployed** (2026-10-23). The original single-file
vanilla-HTML MVP was replaced with a polished, tabbed React (Vite) + FastAPI app. See
DESIGN.md §5 / §6.1 / §19 Phase 5.

## Architecture
```
main.py                    FastAPI; lifespan = MLflow UC tracing; serves the React build (frontend/dist)
app.yml                    Databricks Apps config — CONCRETE env values (standalone deploy), AGENT_MODE=local
requirements.txt           fastapi, uvicorn, pydantic, databricks-sdk, mlflow>=3.1, psycopg2-binary
risk_adjustment_agent.py   vendored agent (runs in-process in AGENT_MODE=local)
backend/config.py          env resolution; ALLOWED_MODELS (dropdown), DEFAULT_MODEL, AGENT_MODE
backend/agent_runner.py    in-process agent runner + STEP CAPTURE wrapper + chat()
backend/memory.py          Lakebase: adjudications, conversations, memory CRUD (graceful degradation)
backend/router.py          all API routes
frontend/                  Vite + React + TS SPA (built to frontend/dist, served by FastAPI)
  src/App.tsx              header (Unity Gateway model dropdown) + 5 tabs + shared activity state
  src/tabs/{Overview,ChartReview,AgentActivity,Chat,Memory}.tsx
  src/Markdown.tsx         react-markdown + remark-gfm (ALL agent output rendered as Markdown)
```

## Tabs (Red Bricks branded — lava #FF3621 / navy #1B3139)
- **Overview** — what the app is, data scope (catalog + evidence-state counts), quick stats (worklist size,
  total potential RAF gain, RAF at RADV risk), default model.
- **Chart Review** (primary) — worklist from `v_member_hcc_coherent` (ranked by RAF gain, suspect/weak chips)
  → select member → evidence layer (`/api/member/{id}/evidence`) → **Run suspecting** → candidate table
  (HCC · state · RAF · **cited passage text** · verified ✓/✗ · Code/Reject/Query) → adjudications persist to
  Lakebase `ra_adjudications`.
- **Agent Activity** — the headline feature: the LIVE ordered list of tools the agent ran for the latest
  chart/turn (tool name + human purpose + result summary, e.g. `search_clinical_notes → 3 passages`,
  `verify_citation → verified ✓`, `cms_hcc_grouper → I50.9→HCC85`, `defensibility_score → risk 8/10 (weak)`).
- **Chat** — real conversational interface; turns persist to Lakebase `conversations`/`conversation_messages`,
  prior turns are threaded to the agent, replies render as Markdown, per-turn step list is expandable, and a
  member can be selected to ground the chat.
- **Memory** — surfaces this user's `agent_user_prefs` (standing instructions) + `agent_lessons` (provisional/
  confirmed, with agent-vs-user provenance badges); **ADD** a standing instruction (source='explicit') and
  **DELETE** any pref/lesson row.

The **Unity Gateway model dropdown** lives in the header (from `GET /api/models`) and is passed on every
agent call (`model_endpoint` → per-request override). Switch models with no redeploy.

## API endpoints
- `GET /api/health` (adds `lakebase` reachability), `GET /api/models`, `GET /api/whoami`, `GET /api/overview`
- `GET /api/cases`, `GET /api/member/{id}/evidence` (now also returns prior adjudications)
- `POST /api/suspect` → `{narrative, candidates[] (each enriched with cited passage text), steps[], meta}`
- `POST /api/adjudicate` → writes `ra_adjudications`
- `POST /api/chat` `{conversation_id?, member_id?, message, model_endpoint}` → loads/persists the conversation,
  runs the agent with history, returns `{conversation_id, reply(markdown), candidates?, steps[], meta}`
- `GET /api/conversations?user_id=`, `GET /api/conversations/{id}`
- `GET /api/memory?user_id=` → `{prefs[], lessons[]}`; `POST /api/memory` `{pref_key, pref_value}` (explicit);
  `DELETE /api/memory/{kind}/{id}` (kind = pref|lesson)

## Agent-step capture (how)
`backend/agent_runner._install_step_capture` wraps the vendored agent's `_execute_tool(name, args)`. Before
each in-process `predict()`, a per-call `agent._step_sink` list is set (under the existing lock); the wrapper
records `{tool, purpose, summary}` for every tool call by parsing its JSON result. `suspect()` and `chat()`
return that ordered `steps[]`. A small, guarded edit to the vendored `predict()` threads prior conversation
turns into the LLM context (empty for single-message suspecting → behavior unchanged).

## Grants applied (for the app SP `5afd32ea-25f5-4ef9-b22c-98a11c506ce4`)
- **Vector Search endpoint** `red-bricks-vs-endpoint`: PATCH `/api/2.0/permissions/vector-search-endpoints/{id}`
  adding the SP with `CAN_USE` (otherwise `search_clinical_notes` returns nothing).
- **Lakebase** `red-bricks-ra-memory`: registered the SP as a federated Postgres role via
  `w.database.create_database_instance_role(identity_type=SERVICE_PRINCIPAL, name=<client_id>)`, then (as owner)
  `GRANT USAGE, CREATE ON SCHEMA public` + `SELECT/INSERT/UPDATE/DELETE ON ALL TABLES` + `ALTER DEFAULT
  PRIVILEGES` so chat history, memory CRUD, and adjudications persist. Also created the `conversations` /
  `conversation_messages` tables (they did not pre-exist on this instance; `memory.py` self-heals them too).

## Deploy (targeted, NOT a bundle)
Frontend is built locally (`npm run build` → `frontend/dist`); FastAPI serves it (no node build in the Apps
runtime). Deploy flow — stash `.venv` + `frontend/node_modules` (too large), then:
```
databricks workspace import-dir . "/Users/zack.phillips@databricks.com/ra-workbench-src" --overwrite -p fe-vm-red-bricks-aws
databricks apps deploy red-bricks-ra-workbench --source-code-path "/Workspace/Users/zack.phillips@databricks.com/ra-workbench-src" -p fe-vm-red-bricks-aws
```
NOTE: `apps deploy --source-code-path` requires the **`/Workspace` prefix** even though `import-dir` does not.
Verified 2026-10-23: compute ACTIVE, app RUNNING, deployment SUCCEEDED.
URL: https://red-bricks-ra-workbench-7474660722665158.aws.databricksapps.com

## Verified (local devloop, DATABRICKS_CONFIG_PROFILE=fe-vm-red-bricks-aws, AGENT_MODE=local)
All 5 tabs render; Markdown rendering throughout; chart-review suspecting returns candidates with cited
passages + verified flags + a 11–15 step agent-activity list; adjudication writes persist; chat persists to
Lakebase and threads history; memory add/delete round-trip. Zero console errors; all API calls 200.

## Known gaps / to verify
- Deployed URL is OAuth/SSO-gated — not headless-testable; verified locally + confirmed deploy status only.
- Lakebase SP role was granted `DELETE` on all public tables (incl. agent memory) so the Memory-tab delete
  works; tighten if least-privilege is required for a customer handoff.
- Dashboards / RADV workbench / HEDIS tab are still later surfaces (not in this rework).
