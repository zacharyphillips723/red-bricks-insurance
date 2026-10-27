<!-- header Red Bricks Insurance — Confidential -->
<!-- footer Databricks | Data Intelligence Platform -->
<!-- toc -->
# Red Bricks Insurance — Deploying the Lakehouse Apps in Your Workspace

**What this is.** A branch of the `red-bricks-insurance` repo has already been prepared in
which the applications **no longer use Lakebase** — they persist all their operational state
to **Unity Catalog Delta tables (the Lakehouse)** instead. This guide walks you through
deploying that branch into your own Databricks workspace, pointing the apps at your own data,
and finishing the last workspace-specific steps. It gives you a copy‑paste **Genie Code**
prompt per application for the work that remains — every prompt makes Genie Code produce a
written plan and wait for your approval before it edits anything.

**Branch:** `feature/lakehouse-delta-writeback`.

---

## 0. What has already been done for you

The branch converts these apps off Lakebase and onto the Lakehouse:

| App | What it is | State store now |
|---|---|---|
| `app` | Population Health Command Center | Delta `app_state` schema |
| `app-fwa` | Fraud/Waste/Abuse Investigation Portal | Delta `app_state` schema |
| `app-prior-auth` | Prior Authorization Review Portal | Delta `app_state` schema |
| `app-underwriting-sim` | Underwriting Simulation Portal | Delta `app_state` schema |
| `app-group-reporting` | Employer Group Reporting | Read‑only (never used Lakebase) |
| `app-network-adequacy` | CMS Network Adequacy | Read‑only (never used Lakebase) |

**Features that are NOT used and require no action:**

- **Lakebase** — fully removed from the six apps above. Every write now goes to Delta.
- **Zerobus** — not referenced by any application (it appeared only in data‑generation
  tooling, which you are not deploying).
- **Online Feature Store / online tables** — used only by the MLOps training notebooks
  (which you are not deploying); no app calls a feature‑serving endpoint at runtime.

**Intentionally excluded:** `app-provider-scrub` (the provider claim scrubber) is **not part
of this branch's converted set** and still uses Lakebase — leave it out of your deployment.

**How the conversion works (for your engineers).** A single shared module,
`lib/shared_backend/database.py`, keeps the same `db.session()/execute(text(...))` interface
the apps always used, but routes every statement through the **Statement Execution API**
against a serverless SQL warehouse, writing to `{UC_CATALOG}.{APP_STATE_SCHEMA}`. It
translates Postgres SQL to Databricks SQL on the fly, coerces result types, and auto‑generates
surrogate ids. The per‑app Postgres schemas were translated to Delta DDL
(`<app>/backend/lakehouse_schema.sql`) by `scripts/pg_to_delta_ddl.py` and are created on app
startup. See the Appendix for the full mechanics and the Postgres→Delta type mapping.

> **Important caveat.** This conversion is **code‑complete and statically verified**
> (compiles clean, no residual Lakebase/Postgres constructs) but has **not been deploy‑tested
> in a live workspace**. Plan for one deploy‑and‑click smoke‑test pass — deploy, open each app,
> exercise a save/write path, and confirm rows land in the `app_state` Delta tables — before
> you put it in front of an audience.

---

## 1. The app-state model and the honest tradeoff

All app writeback now lands in one governed schema, `{your_catalog}.app_state` (Delta tables
like `pa_review_queue`, `fwa_investigations`, `scrub`‑style history, saved simulations, chat
history, alerts). One catalog, one governance model, full lineage — no separate Postgres to
run or pay for.

Be aware of what Delta is and isn't. It is an **analytical** store, not an OLTP engine. For
these apps' interactive single‑row writes that means: ~1–3 s write latency per statement, no
enforced primary/foreign keys (the app maintains integrity), small‑file accumulation (enable
Predictive Optimization or schedule `OPTIMIZE`), and possible write‑write conflicts under
heavy concurrency. That is entirely fine for a demo, POC, or low‑volume internal tool. If
`app-prior-auth` in particular will carry real multi‑reviewer production load, weigh keeping
that one on Lakebase — the branch makes the others independent, so you can mix.

---

## 2. Prerequisites

1. A Databricks workspace with Unity Catalog and **serverless SQL warehouses** enabled.
2. A target catalog you own — call it `<your_catalog>` — with rights to create schemas,
   tables, and views, plus a schema `app_state` (created for you at app startup).
3. **Databricks CLI v0.220+** authenticated, with a named profile for `databricks.yml`.
4. Your analytical data (see §4 for the tables each app reads).
5. Foundation Model API access for the LLM endpoints (`databricks-llama-4-maverick`,
   `databricks-claude-haiku-4-5`). Custom ML endpoints and Vector Search are optional to start
   (see §5).
6. **Genie Code** enabled, opened on the cloned repo at the `feature/lakehouse-delta-writeback`
   branch.

---

## 3. Deploy the branch (apps only)

### 3.1 Clone and check out the converted branch

```bash
git clone <your-copy-of-red-bricks-insurance>
cd red-bricks-insurance
git checkout feature/lakehouse-delta-writeback
```

### 3.2 Point the bundle at your workspace

In `databricks.yml`, add a default target using your profile and catalog:

```yaml
targets:
  customer:
    mode: development
    default: true
    workspace:
      profile: <your-cli-profile>
    variables:
      catalog: <your_catalog>
```

### 3.3 Deploy only the apps

Change the bundle's include from `resources/*.yml` to the six app files — this leaves out all
pipelines, jobs, dashboards, data generation, the AI Gateway, the Lakebase instance, **and the
excluded provider‑scrub app**:

```yaml
include:
  - resources/app.yml
  - resources/app_fwa.yml
  - resources/app_group_reporting.yml
  - resources/app_network_adequacy.yml
  - resources/app_prior_auth.yml
  - resources/app_underwriting_sim.yml
```

Then:

```bash
databricks bundle validate -t customer
databricks bundle deploy   -t customer
```

The apps come up but won't be fully functional until you point them at your data (§4) and
grant their service principals (§7). The `app_state` schema and its Delta tables are created
automatically the first time each app starts.

---

## 4. Point the apps at YOUR data — pick a strategy per app

Each app reads a set of Unity Catalog tables under fixed schema names (`analytics`, `claims`,
`members`, `fwa`, `prior_auth`, `network`, `clinical`, `documents`, `care_management`) from the
catalog in `UC_CATALOG`. You have two ways to satisfy those reads, and you can choose
differently per app — the per‑app prompts below let you say which.

**Strategy 1 — Compatibility views (recommended, least invasive).** Keep your tables as they
are; create Unity Catalog **views** in `<your_catalog>` whose names and columns match what each
app expects. The app code stays unchanged — you only set `UC_CATALOG`. Fastest to stand up and
easiest to re‑point later, at the cost of a thin view layer.

**Strategy 2 — Rewrite the app queries.** Have Genie Code edit each app's backend SQL to read
your actual table and column names directly. More invasive, no view layer. Best when your
model diverges a lot from the demo's.

When in doubt, start with Strategy 1 and rewrite only the queries that don't map cleanly.

---

## 5. External dependencies: LLMs, custom models, Vector Search

- **LLM endpoints** are Foundation Model APIs — usually already present. If named differently
  in your region, set each app's `LLM_ENDPOINT` / `*_AGENT_ENDPOINT` env vars.
- **Genie spaces** (`GENIE_SPACE_ID: auto`) auto‑detect by title, else first available. Create
  a Genie space over your gold tables; until then Genie panels return a friendly "not
  configured" message rather than failing.
- **Custom ML endpoints** (`readmission-scorer`, `fwa-fraud-scorer`) and **Vector Search**
  (`red-bricks-vs-endpoint`) don't exist until you build them on your data. To start, disable
  live scoring / RAG so the apps run on your batch gold tables — the per‑app prompts include
  this. Re‑enable once you've trained the equivalent model or built the index.

---

## 6. Genie Code prompts — one per application

Open Genie Code on the repo at the `feature/lakehouse-delta-writeback` branch. Paste the prompt
for each app. Every prompt makes Genie Code **plan first and wait** for your approval before
editing. Where a prompt says `[[CHOOSE: Strategy 1 views | Strategy 2 rewrite]]`, delete the
option you don't want. Replace `<your_catalog>` throughout.

> The Lakebase→Lakehouse conversion is already done on this branch, so these prompts are about
> **pointing the apps at your data** and **switching off dependencies you haven't built yet** —
> not about the database migration.

---

### Prompt 1 — `app` (Population Health Command Center)

```
This app is already converted to write its state to the Lakehouse app_state schema — do NOT
touch that. Your job is to point its READS at my data and disable live scoring I haven't built.

STEP 1 — PLAN ONLY, then STOP for my approval. Do not edit until I say "approved".

DATA REPOINTING — [[CHOOSE: Strategy 1 views | Strategy 2 rewrite]].
Reads (catalog = UC_CATALOG=<your_catalog>): analytics.gold_member_360,
clinical.silver_lab_results, care_management.silver_member_sdoh,
analytics.gold_member_readmission_risk, and a documents Vector Search index.
 - Strategy 1: list every table read and give me CREATE VIEW <your_catalog>.<schema>.<name>
   stubs (columns matching the app's SELECTs) for me to fill in from my tables.
 - Strategy 2: identify every file/query to rewrite and the columns each needs.

LIVE SCORING: set READMISSION_LIVE_SCORING=false and confirm the code falls back to the batch
analytics.gold_member_readmission_risk table when the readmission-scorer endpoint is absent.

ACCEPTANCE: app reads resolve against <your_catalog>; saving an alert/assignment/cohort still
writes to the app_state Delta tables. Show a diff summary. Do NOT deploy.

STEP 2 — EXECUTE only after I approve.
```

### Prompt 2 — `app-fwa` (FWA Investigation Portal)

```
Already converted to Lakehouse app_state — leave the writeback alone. Point reads at my data
and disable the custom fraud model.

STEP 1 — PLAN ONLY, then STOP for approval.

DATA REPOINTING — [[CHOOSE: Strategy 1 views | Strategy 2 rewrite]].
Reads: fwa.gold_fwa_provider_risk, fwa.gold_fwa_claim_flags, analytics.gold_fwa_member_risk,
analytics.gold_fwa_model_scores, fwa.gold_fwa_summary. View stubs or a rewrite list.

CUSTOM MODEL: the fwa-fraud-scorer endpoint won't exist in my workspace. Feature-flag live
scoring OFF and rely on the batch gold_fwa_model_scores / gold_fwa_provider_risk tables + Genie.
Make the missing endpoint non-fatal.

ACCEPTANCE: reads resolve against <your_catalog>; case create/assign/evidence still writes to
app_state; missing model endpoint doesn't crash the app. Diff summary. Do NOT deploy.

STEP 2 — EXECUTE after approval.
```

### Prompt 3 — `app-prior-auth` (Prior Authorization Review Portal)

```
Already converted to Lakehouse app_state (queue, appeals, correspondence, QA, rules) — leave
the writeback alone. Point its reads at my data.

STEP 1 — PLAN ONLY, then STOP for approval.

DATA REPOINTING — [[CHOOSE: Strategy 1 views | Strategy 2 rewrite]].
Reads: prior_auth.gold_pa_requests, prior_auth.silver_medical_policy_rules,
prior_auth.pa_ml_predictions, prior_auth.gold_pa_tier1_evaluation. View stubs or a rewrite list.
If my data lacks pa_ml_predictions, tell me how the app degrades (rules/manual only) and make
it non-fatal.

ACCEPTANCE: a reviewer can pull a case, make a determination, file/act on an appeal, and log
correspondence — all persisting to app_state — with reads resolving against <your_catalog>.
Diff summary. Do NOT deploy.

STEP 2 — EXECUTE after approval.
```

### Prompt 4 — `app-underwriting-sim` (Underwriting Simulation Portal)

```
Already converted to Lakehouse app_state (saved simulations/scenarios) — leave the writeback
alone. Point its baseline actuarial reads at my data.

STEP 1 — PLAN ONLY, then STOP for approval.

DATA REPOINTING — [[CHOOSE: Strategy 1 views | Strategy 2 rewrite]].
Reads: analytics.gold_pmpm, analytics.gold_mlr, members.gold_enrollment_summary,
analytics.gold_utilization_per_1000, analytics.gold_ibnr_completion_factors,
analytics.gold_ibnr_triangle, analytics.gold_risk_adjustment_analysis,
analytics.gold_coding_completeness, analytics.gold_tcoc_summary,
analytics.gold_group_experience, analytics.gold_group_stop_loss. View stubs or a rewrite list.

ACCEPTANCE: build a scenario, run the simulation, save and reload it (persists to app_state);
baseline tables resolve against <your_catalog>. Diff summary. Do NOT deploy.

STEP 2 — EXECUTE after approval.
```

### Prompt 5 — `app-group-reporting` (Group Reporting Portal)  *(read‑only)*

```
This app never used Lakebase — there is NO writeback migration. Only point its reads at my data.

STEP 1 — PLAN ONLY, then STOP for approval.

DATA REPOINTING — [[CHOOSE: Strategy 1 views | Strategy 2 rewrite]].
Reads (see backend/groups.py table constants): analytics.gold_group_report_card,
analytics.gold_group_experience, analytics.gold_group_stop_loss, analytics.gold_group_renewal,
analytics.gold_member_tcoc, members.silver_enrollment, analytics.gold_member_360,
claims.silver_claims_medical, claims.silver_claims_pharmacy. View stubs (Strategy 1) or a
rewrite list (Strategy 2). Confirm the write-back target analytics.group_renewal_scenarios
exists in <your_catalog> (create the Delta DDL if not). Slack/Salesforce/Glean env vars are
optional — leave blank.

ACCEPTANCE: report cards, drill-downs, and the renewal write-back resolve against <your_catalog>.
Diff summary. Do NOT deploy.

STEP 2 — EXECUTE after approval.
```

### Prompt 6 — `app-network-adequacy` (Network Adequacy Portal)  *(read‑only)*

```
This app never used Lakebase — no writeback migration. Only point reads at my data. It uses
spatial SQL (ST_DistanceSphere, H3) — preserve that logic; only the table sources change.

STEP 1 — PLAN ONLY, then STOP for approval.

DATA REPOINTING — [[CHOOSE: Strategy 1 views | Strategy 2 rewrite]].
Reads: network.gold_network_adequacy_compliance, network.silver_member_geo,
network.silver_provider_geo (member/provider lat-lon, specialty, network participation).
View stubs with the exact columns the spatial queries need, or a rewrite list. If my data
lacks lat/lon, flag it and propose how to derive it or which panels to disable.

ACCEPTANCE: adequacy compliance + geo-access panels render against <your_catalog>. Diff
summary. Do NOT deploy.

STEP 2 — EXECUTE after approval.
```

---

## 7. Deploy, grant, verify

After each app's plan is executed:

```bash
databricks bundle deploy -t customer
```

**Grants the app service principal needs** (the demo's bootstrap job did these; do them once by
hand or in a small setup script since you're not deploying it):

- `USE CATALOG <your_catalog>`; `USE SCHEMA` on each read schema **and** `app_state`.
- `SELECT` on the read tables/views; `SELECT` + `MODIFY` on the `app_state` tables.
- `CAN USE` on the serverless SQL warehouse; `CAN QUERY` on the LLM serving endpoints.

**Smoke test each app:** open it, confirm data panels populate from `<your_catalog>`, then
exercise a write (save/assign/log) and confirm a row appears:

```sql
SELECT * FROM <your_catalog>.app_state.<table> ORDER BY created_at DESC LIMIT 5;
```

**Maintenance:** enable **Predictive Optimization** on `app_state` (or schedule `OPTIMIZE`) so
the row‑at‑a‑time inserts don't degrade reads.

---

## 8. What is not included / caveats

- **`app-provider-scrub` is excluded** — it still uses Lakebase and is not on this branch's
  converted set. Don't add it to the include list.
- **Deploy‑test required.** The conversion is statically verified only; do the smoke‑test pass
  in §7 before any customer‑facing use.
- **Custom ML / Vector Search** must be rebuilt on your data to re‑enable live scoring / RAG;
  the apps run without them (batch gold + rules + Genie) once you flip the feature flags.
- **Transactions.** The Lakehouse writeback auto‑commits each statement — there are no
  multi‑statement transactions, so a mid‑sequence failure can leave partial writes. Acceptable
  for demo/POC volumes; revisit if you productionize `app-prior-auth`.

---

## Appendix — How the conversion works (for maintainers)

**Shared layer — `lib/shared_backend/database.py`.** A drop‑in `db` object and `text()` that
preserve the `async with db.session() as s: await s.execute(text(sql), params)` interface but
execute against a serverless SQL warehouse via the SDK Statement Execution API, with
`catalog`/`schema` defaulted to `{UC_CATALOG}.{APP_STATE_SCHEMA}` so unqualified table names
resolve. It (a) translates Postgres SQL to Databricks SQL, (b) coerces the all‑string
Statement Execution results back to native types using the result manifest, and (c) auto‑fills
`uuid()` surrogate ids for INSERTs that omit them.

**DDL — `scripts/pg_to_delta_ddl.py`** converts each `src/*_lakebase_schema.sql` into
`<app>/backend/lakehouse_schema.sql`, created on startup by `db.ensure_tables(...)`.

**Postgres → Delta type / construct mapping:**

| Postgres | Delta / Unity Catalog | Note |
|---|---|---|
| `TEXT`, `VARCHAR`, `UUID`, `JSONB`, enum types | `STRING` | JSON stored as text; json‑decoded on read |
| `TIMESTAMPTZ` | `TIMESTAMP` | UTC; `current_timestamp()` |
| `DOUBLE PRECISION` / `NUMERIC(p,s)` | `DOUBLE` / `DECIMAL(p,s)` | |
| `SERIAL` | `BIGINT` | no auto‑increment |
| `PRIMARY KEY` / `REFERENCES` (FK) / `CHECK` / indexes | dropped | not enforced in UC; integrity in app |
| `gen_random_uuid()` default | `uuid()` at write time | shim auto‑fills omitted ids |
| `now()` | `current_timestamp()` | |
| `::text` / `::uuid` / `::jsonb` / `::enum` casts | `::string` / stripped | translated at runtime |
| `EXTRACT(EPOCH FROM (a-b))` / `INTERVAL 'n unit'` | `timestampdiff(...)` / `INTERVAL n UNIT` | |
| `col->>'k'` (JSON) | `get_json_object(col,'$.k')` | |
| `INSERT ... RETURNING` | Python‑generated id + INSERT (+ SELECT) | |
| `INSERT ... ON CONFLICT DO NOTHING` | `INSERT ... WHERE NOT EXISTS` | |
| `to_jsonb(row)` | `to_json(struct(r.*))` | |
| `to_char(...)` | `date_format(...)` / `CONCAT`+`timestampdiff` | |

**To revert an app to Lakebase:** restore its `backend/database.py` and
`src/*_lakebase_schema.sql` from `main`, re‑add the `LAKEBASE_*` env in `app.yml` +
`resources/app_*.yml`, and re‑run the Lakebase bootstrap.
