# Red Bricks Insurance

Healthcare insurance company simulation — modular Databricks Asset Bundle (DAB). One deployable bundle that runs end-to-end: Synthea clinical generation → synthetic insurance data → bronze/silver/gold SDP pipelines → cross-domain analytics with AI classification → ML model training → intelligent agents → three purpose-built applications.

## Architecture

```
┌─────────────────────────┐
│  Synthea Generation     │  5,000 FHIR R4 bundles (NC residents)
│  (run_synthea_generation)│  + demographic extraction + MBR ID assignment
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│  Insurance Data Gen     │  Reads Synthea demographics → generates insurance domains
│  (run_data_generation)  │  Members, Enrollment, Groups, Claims, Providers,
│                         │  Benefits, Documents, Underwriting, Risk Adjustment, FWA
└───────────┬─────────────┘
            │
     ┌──────┴───────┐  (10 domain pipelines run in parallel)
     ▼              ▼
┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────────┐ ┌───────────────┐
│ Members  │ │Providers │ │ Claims   │ │ Clinical │ │ Underwriting │ │Risk Adjustment│
│& Enroll. │ │          │ │Med + Rx  │ │FHIR→Delta│ │              │ │ Member+Prov   │
│ B → S → G│ │ B → S → G│ │ B → S → G│ │ B → S → G│ │ B → S → G   │ │ B → S → G    │
└────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘ └──────┬──────┘ └──────┬────────┘
     │            │            │            │               │               │
     │  ┌──────────┐ ┌──────────┐ ┌─────────────┐          │               │
     │  │Documents │ │Benefits  │ │Member Months│          │               │
     │  │ B → S    │ │ B → S → G│ │ (notebook)  │          │               │
     │  └────┬─────┘ └────┬─────┘ └──────┬──────┘          │               │
     │       │            │              │                  │               │
     └───────┴────────────┴──────────────┴──────────────────┴───────────────┘
                                    │
                         ┌──────────▼──────────┐
                         │  Gold Analytics     │  Cross-domain metrics
                         │  Financial, Quality,│  Group Report Card
                         │  Risk, AI, Actuarial│  TCOC / TCI / FWA
                         │  Member 360, ML     │
                         └──────────┬──────────┘
                                    │
               ┌────────────────────┼────────────────────┐
               ▼                    ▼                    ▼
        ┌────────────┐       ┌────────────┐       ┌──────────────┐
        │ Dashboards │       │  Genie     │       │ AI Agents    │
        │ (AI/BI)    │       │  Spaces    │       │ Care Intel   │
        └────────────┘       └────────────┘       │ Sales Coach  │
                                                  │ FWA Agent    │
                                                  └──────┬───────┘
                                                         │
                       ┌─────────────────────────────────┼────────────────────┐
                       ▼                                 ▼                    ▼
                ┌──────────────┐                  ┌──────────────────┐ ┌──────────────────┐
                │  Command     │                  │  Group Reporting │ │  FWA Portal      │
                │  Center App  │                  │  Portal App      │ │  App             │
                │  (React+API) │                  │  (React+API)     │ │  (React+API)     │
                │  Clinical    │                  │  Sales Enablement│ │  Investigations  │
                └──────────────┘                  └──────────────────┘ └──────────────────┘
```

## Pipeline DAG

The full demo job (`red_bricks_full_demo`) orchestrates 25+ tasks:

```
synthea_generation (ROOT — generates FHIR bundles + extracts demographics + assigns MBR IDs)
  → data_generation (reads Synthea demographics, generates insurance domains + FWA signals)
      → [members, providers, claims, enrollment, benefits, underwriting,
         documents, risk_adjustment, fwa pipelines]
      → parse_fhir_with_dbignite (reads raw synthea_raw/fhir/, writes crosswalk Delta tables)
          → clinical_pipeline (bronze.sql JOINs crosswalk for MBR IDs + NPIs)
  → build_member_months (depends on members_pipeline)
  → fwa_pipeline (depends on data_generation — bronze/silver/gold FWA signals + provider profiles)
  → gold_analytics_pipeline (depends on all domain pipelines + member months + fwa_pipeline)
      → create_metric_views (governed semantic layer + FWA risk metrics)
  → train_fwa_model (depends on fwa_pipeline + gold_analytics — XGBoost fraud scorer)
  → setup_vector_search (depends on documents_pipeline)
      → deploy_member_agent (v1)
      → deploy_agent_v2 (v2 with benefits)
      → deploy_group_sales_agent (Sales Coach for group reporting)
      → deploy_fwa_agent (FWA Investigation agent with tool-calling)
          → evaluate_agents (v1 vs v2 vs sales coach comparison)
  → bootstrap_workspace (depends on gold_analytics + fwa_pipeline + train_fwa_model)
      — Creates Lakebase instances, applies UC/warehouse grants for app SPs, seeds operational data
```

A **refresh job** (`red_bricks_refresh`) runs the same DAG minus Synthea/FHIR/clinical — useful when only insurance data generation or downstream logic has changed. Both jobs include the `bootstrap_workspace` task, which automatically provisions Lakebase, discovers app service principals, grants UC + warehouse permissions, and seeds alerts/investigations from gold tables.

**Synthea as golden demographic source:** Synthea generates clinically realistic patients with names, DOBs, and addresses. These demographics flow INTO the insurance generators (members, enrollment), ensuring that searching for "Aaron Anderson" in FHIR returns the same person as in the member table. A lightweight `synthea_crosswalk` Delta table maps Synthea UUIDs to MBR IDs via JOIN in `bronze.sql`.

## Data Domains

| Domain | Records | Format | Description |
|--------|---------|--------|-------------|
| **Synthea Clinical** | ~5,000 FHIR R4 bundles | JSON | Encounters, conditions, observations, medications (Synthea-generated) |
| **Claims** | 150K medical + 50K pharmacy | Parquet | IP/OP/professional/ER; ICD-10/CPT, revenue codes, CARC denial codes, financials |
| **Members** | 5,000 | Parquet | Demographics from Synthea (name, DOB, gender, address, NC counties) |
| **Enrollment** | 5,000 | Parquet | LOB (Commercial/MA/Medicaid/ACA), plan, premium, risk_score; age-consistent LOB |
| **Groups** | 200 | Parquet | Employer groups with SIC codes, funding types, stop-loss, admin fees, renewal dates |
| **Benefits** | ~150K | Parquet | Plan benefit schedules, cost-sharing, actuarial values, utilization assumptions |
| **Documents** | ~15K | PDF + JSON | Case notes, call transcripts, claims summaries with full text |
| **Underwriting** | 5,000 | Parquet | Risk tier, smoker, BMI, occupation; correlated to risk_score |
| **Providers** | 500 | Parquet | NPI, specialty, network status, group practice |
| **Risk Adjustment** | 5K member + 10K provider | Parquet | RAF scores, HCC codes, provider attribution |
| **FWA Signals** | ~10K signals + 500 profiles + 75 cases | Parquet | Fraud signals (9 types), provider risk profiles, investigation cases |

**Data quality**: ~2% intentional defects (nulls, invalid codes, out-of-range dates) caught by SDP expectations at the silver layer.

## Schema Architecture

Tables are organized into **11 domain schemas** within the catalog, each owned by its domain pipeline:

| Schema | Contents | Example Tables |
|--------|----------|----------------|
| `raw` | Raw data volumes | `raw_sources/` (Parquet, JSON, PDF) |
| `members` | Member demographics & enrollment | `silver_members`, `silver_enrollment`, `gold_enrollment_summary`, `gold_member_demographics` |
| `claims` | Medical & pharmacy claims | `silver_claims_medical`, `silver_claims_pharmacy`, `gold_claims_summary`, `gold_pharmacy_summary` |
| `providers` | Provider directory | `silver_providers`, `gold_provider_directory` |
| `clinical` | Synthea FHIR clinical data | `bronze_encounters`, `silver_conditions`, `gold_clinical_summary` |
| `documents` | Case notes, call transcripts | `silver_case_notes`, `case_notes_vs_index` |
| `benefits` | Plan benefit schedules | `silver_benefits`, `gold_benefits_summary` |
| `underwriting` | Risk assessment | `silver_underwriting`, `gold_underwriting_summary` |
| `risk_adjustment` | RAF scores, HCC codes | `silver_risk_adjustment_member`, `gold_risk_adjustment_summary` |
| `fwa` | Fraud, Waste & Abuse detection | `silver_fwa_signals`, `gold_fwa_provider_risk`, `gold_fwa_claim_flags`, `gold_fwa_summary` |
| `analytics` | Cross-domain gold tables & metric views | `gold_pmpm`, `gold_mlr`, `gold_hedis_member`, `gold_member_360`, `gold_fwa_member_risk`, `fwa_model_inference`, `mv_financial_overview` |

**Key design principle**: Each domain pipeline writes bronze/silver/gold tables to its own schema. Only cross-domain gold analytics (tables that JOIN across multiple domains) land in the `analytics` schema.

## SDP Pipelines (Medallion Architecture)

Each domain has its own SDP pipeline with bronze → silver → gold tables:

| Layer | What It Does | Key Features |
|-------|-------------|--------------|
| **Bronze** | Raw ingestion from UC Volume via `read_files()` | Streaming tables, source lineage metadata |
| **Silver** | Cleansed, typed, validated | DQ expectations (DROP ROW for critical, track for soft), date casting, computed columns |
| **Gold** | Domain-level aggregates | Summary views in the domain's own schema |
| **Gold Analytics** | Cross-domain KPIs in `analytics` schema | Financial, quality, risk metrics + AI classification |

### Gold Analytics Tables

**Financial Metrics:** `gold_pmpm` (PMPM by LOB), `gold_mlr` (Medical Loss Ratio), `gold_ibnr_estimate` (payment lag / completion factors)

**Quality Metrics:** `gold_hedis_member` (HEDIS proxies), `gold_hedis_provider` (compliance rates), `gold_stars_provider` (CMS Stars-like composite)

**Risk Adjustment:** `gold_risk_adjustment_analysis` (RAF distributions, HCC counts, MA revenue), `gold_coding_completeness` (HCC coding gap detection)

**AI-Powered (via `ai_query()`):** `gold_denial_classification` (LLM-classified denial reasons), `gold_denial_analysis` (denial trends by AI category), `gold_member_risk_narrative` (AI-generated clinical summaries)

**Actuarial:** `gold_utilization_per_1000` (utilization benchmarks), `gold_ibnr_triangle` (chain-ladder development), `gold_ibnr_completion_factors`, `gold_mlr_ai_insights` (LLM-generated actuarial recommendations)

**Group Reporting:** `gold_group_experience` (claims PMPM, utilization per 1000 member-months, loss ratio by employer group), `gold_group_stop_loss` (specific & aggregate stop-loss tracking), `gold_group_renewal` (credibility-weighted renewal pricing), `gold_group_report_card` (single-row-per-group executive summary with peer percentile benchmarks and composite health score)

**Cost of Care:** `gold_member_tcoc` (member-level Total Cost of Care and Total Cost Index), `gold_tcoc_summary` (LOB-level TCOC distributions, cost tier breakdowns, spend concentration)

**Member 360:** `gold_member_360` (unified member view joining clinical, claims, enrollment, risk)

**FWA Analytics:** `gold_fwa_network_analysis` (provider referral ring detection), `gold_fwa_member_risk` (member-level fraud indicators: doctor shopping, pharmacy abuse), `gold_fwa_ai_classification` (AI-generated investigation narratives for top signals), `gold_fwa_model_scores` (AutoML model batch scoring of all claims)

**ML Model:** XGBoost claim-level fraud scorer (`fwa_scoring_model`), trained with 5-fold stratified CV and hyperparameter tuning, registered in Unity Catalog. Served via `fwa-fraud-scorer` endpoint with inference table logging. Predictions written to `analytics.fwa_model_inference`. An AutoML alternative (`train_fwa_model_automl.py`) is also available.

### Metric Views (Governed Semantic Layer)

Metric views (`CREATE VIEW ... WITH METRICS`) define governed measures and dimensions as YAML, ensuring every consumer — actuaries, dashboards, Genie, AI/BI — computes metrics the same way. Queried via the `MEASURE()` function.

| View | Source | Key Measures |
|------|--------|-------------|
| `mv_financial_overview` | `gold_pmpm` | PMPM Paid, PMPM Allowed, Total Paid, Member Months |
| `mv_mlr_compliance` | `gold_mlr` | MLR, Admin Ratio, Total Claims Paid, Total Premiums |
| `mv_utilization` | `gold_utilization_per_1000` | Claims/Patients/Cost per 1000, Admits per 1000 |
| `mv_enrollment` | `silver_member_months` | Member Months, Active Members, Premium Revenue, Avg Risk Score |
| `mv_ibnr` | `gold_ibnr_estimate` | Avg Payment Lag, Completion Rate, Claims Over 90 Days |
| `mv_denials` | `gold_denial_analysis` | Denial Count, Total Denied Amount, Avg Denied Amount |
| `mv_cost_of_care` | `gold_member_tcoc` | Avg TCOC, Avg TCI, Avg Actual PMPM, High Cost Members |
| `mv_fwa_risk` | `gold_fwa_summary` | Signal Count, Estimated Overpayment, Avg Fraud Score, High Severity Signals |

### Clinical Pipeline (Synthea → dbignite → SDP)

The clinical pipeline reads Synthea FHIR R4 bundles directly (no intermediate transformation):

1. **`parse_fhir_with_dbignite`** — Reads `synthea_raw/fhir/*.json`, uses dbignite to write `Patient`, `Encounter`, `Condition`, `Observation` Delta tables. Also writes `synthea_crosswalk` and `synthea_practitioner_crosswalk` Delta tables.
2. **`bronze.sql`** — Flattens FHIR structs, LEFT JOINs crosswalk tables to resolve Synthea UUIDs → MBR IDs and practitioner UUIDs → provider NPIs.
3. **`silver.sql`** / **`gold.sql`** — Standard cleansing and aggregation.

## AI Agents

Three agents are deployed and registered in Unity Catalog via MLflow:

| Agent | Description | Audience |
|-------|-------------|----------|
| **Care Intelligence v1** (`deploy_member_agent`) | Member lookup + document search | Clinical care teams |
| **Care Intelligence v2** (`deploy_agent_v2`) | v1 + benefits coverage analysis | Clinical care teams |
| **Sales Coach** (`deploy_group_sales_agent`) | Group report card analysis, renewal prep, roleplay negotiation simulation, care management program recommendations | Account executives, sales reps |
| **FWA Investigation** (`deploy_fwa_agent`) | Tool-calling agent that dynamically queries UC tables (provider risk, claims, ML predictions), generates structured investigation briefings | SIU analysts, compliance teams |

All agents are evaluated with `evaluate_agents.py`. The FWA Investigation agent uses a multi-turn tool-calling pattern — the LLM autonomously composes SQL queries against allowed Unity Catalog schemas, retrieves data, and synthesizes findings. The Sales Coach supports intent-based modes: full briefing ("prepare me for..."), renewal focus ("why rate increase"), care management ("what programs can I offer"), and negotiation roleplay ("simulate a renewal negotiation").

## Databricks Apps

### Command Center (`app/`)

Clinical-focused application for care management teams:

- **Backend**: FastAPI (Python), connects to Lakebase, SQL warehouse, and serving endpoints
- **Frontend**: React + Vite + Tailwind (Databricks-branded dark theme)
- **Features**: Member search, claims analysis, Genie-powered natural language queries, Care Intelligence agent chat
- **Config**: `app/app.yml`

### Group Reporting Portal (`app-group-reporting/`)

Sales enablement application for account executives preparing employer group renewals:

- **Backend**: FastAPI (Python), reads gold tables via Statement Execution API
- **Frontend**: React + Vite + Tailwind (Databricks-branded dark theme)
- **Pages**:
  - **Group Search** — filter/search 200 employer groups by industry, funding type, renewal action
  - **Report Card** — one-page executive summary with health score, peer percentile benchmarks, cost tier distribution, renewal projection
  - **Standard Reports** — 5 canned reports: High-Cost Members, Claims Trend (PMPM chart), Top Drugs, Utilization Summary, Risk & Care Gaps
  - **Sales Coach** — AI agent chat with negotiation roleplay and care management program recommendations
- **Context Enrichment** (optional): Slack (account channel history), Glean (internal knowledge base), Salesforce (CRM account data) feed into the Sales Coach agent's context for richer renewal prep
- **Config**: `app-group-reporting/app.yml`

### FWA Investigation Portal (`app-fwa/`)

SIU-focused application for fraud, waste, and abuse investigation:

- **Backend**: FastAPI (Python), connects to Lakebase (`fwa-investigations` instance), SQL warehouse (Statement Execution API for gold table queries), and Foundation Model API (Llama 4 Maverick)
- **Frontend**: React + Vite + Tailwind (Databricks-branded dark theme)
- **Pages**:
  - **Dashboard** — KPIs (total/open/critical/closed investigations), financial metrics (estimated overpayment, recovered, recovery rate), breakdowns by status/severity/type
  - **Investigation Queue** — filterable/searchable table with status, severity, type, investigator filters; sorted by severity + risk score
  - **Investigation Detail** — full case view with key metrics, fraud types, agent chat panel, evidence list, immutable audit trail, and action sidebar (assign investigator, update status, add notes, record recovery)
  - **Provider Analysis** — NPI search with risk scorecard, metrics grid (18 metrics), ML model predictions table, rules-based flagged claims table
  - **FWA Agent** — standalone AI agent chat with `[INV-XXXX]`/`[PRV-NPI]` prefix targeting; the agent dynamically queries Unity Catalog tables via tool-calling
  - **Genie Search** — natural language SQL exploration over FWA gold tables
  - **Caseload** — investigator capacity dashboard with utilization bars
- **Data Architecture**: Hybrid — Lakebase for transactional investigation state (status changes, assignments, audit log, evidence) + Statement Execution API for analytics (provider risk profiles, flagged claims, ML predictions from gold tables)
- **Config**: `app-fwa/app.yml`, DAB resource: `resources/app_fwa.yml`

## Dashboards

| Dashboard | Description |
|-----------|-------------|
| **Red Bricks Analytics** | Financial, quality, and risk metrics across all domains |
| **Agent Comparison** | Side-by-side v1 vs v2 agent evaluation results |

Both are deployed as AI/BI Lakeview dashboards with `CAN_READ` permissions for the users group.

## Project Structure

```
red-bricks-insurance/
├── databricks.yml                    # Bundle config, variables, targets (dev/e2-field-eng/prod)
├── app/                              # Command Center Databricks App
│   ├── app.yml                       #   App configuration (env vars, command)
│   ├── main.py                       #   FastAPI backend
│   ├── frontend/                     #   React + Vite + Tailwind source
│   └── static/                       #   Built frontend output
├── app-group-reporting/              # Group Reporting Portal Databricks App
│   ├── app.yml                       #   App config (SQL warehouse, LLM endpoint, enrichment tokens)
│   ├── main.py                       #   FastAPI backend
│   ├── backend/
│   │   ├── router.py                 #   API routes (groups, reports, agent, genie)
│   │   ├── models.py                 #   Pydantic models
│   │   ├── groups.py                 #   SQL queries via Statement Execution API
│   │   ├── agent.py                  #   Sales Coach (LLM + group data + enrichment context)
│   │   ├── enrichment.py             #   Slack, Glean, Salesforce context (each optional)
│   │   └── genie.py                  #   Genie space integration
│   ├── frontend/                     #   React + Vite + Tailwind source
│   └── static/                       #   Built frontend output
├── app-fwa/                             # FWA Investigation Portal Databricks App
│   ├── app.yml                       #   App config (Lakebase, SQL warehouse, LLM, ML model endpoint)
│   ├── main.py                       #   FastAPI backend
│   ├── backend/
│   │   ├── router.py                 #   API routes (dashboard, investigations, providers, agent, genie)
│   │   ├── models.py                 #   Pydantic models (8 investigation statuses, 9 fraud types)
│   │   ├── database.py               #   Lakebase connection with OAuth token refresh
│   │   ├── agent.py                  #   FWA agent (tool-calling with dynamic UC table queries)
│   │   └── genie.py                  #   Genie space integration
│   ├── frontend/                     #   React + Vite + Tailwind source
│   └── static/                       #   Built frontend output
├── resources/
│   ├── full_demo_job.yml             # End-to-end orchestration (25+ tasks)
│   ├── refresh_demo_job.yml          # Refresh without Synthea (data gen → all downstream)
│   ├── data_generation_job.yml       # Standalone data generation
│   ├── dashboard.yml                 # Analytics dashboard
│   ├── agent_comparison_dashboard.yml# Agent eval dashboard
│   ├── app_group_reporting.yml       # Group Reporting Portal app resource
│   ├── app_fwa.yml                  # FWA Investigation Portal app resource
│   ├── pipeline_members.yml          # Members & Enrollment SDP
│   ├── pipeline_providers.yml        # Providers SDP
│   ├── pipeline_claims.yml           # Claims SDP
│   ├── pipeline_clinical.yml         # Clinical SDP (Synthea → dbignite)
│   ├── pipeline_benefits.yml         # Benefits SDP
│   ├── pipeline_documents.yml        # Documents SDP
│   ├── pipeline_underwriting.yml     # Underwriting SDP
│   ├── pipeline_risk_adjustment.yml  # Risk Adjustment SDP
│   ├── pipeline_fwa.yml              # FWA domain SDP (signals, profiles, investigations)
│   └── pipeline_gold_analytics.yml   # Cross-domain analytics (10 SQL files, 25+ gold views)
├── src/
│   ├── data_generation/              # Modular synthetic data generators
│   │   ├── reference_data.py         #   ICD-10, CPT, DRG, HCC, CARC, LOB configs
│   │   ├── dq.py                     #   ~2% defect injection
│   │   ├── helpers.py                #   NPI generation, date utils, payment lag
│   │   └── domains/                  #   One generator per domain
│   │       ├── members.py            #     Demographics (Synthea-backed or Faker fallback)
│   │       ├── enrollment.py         #     Plans, LOB (age-consistent); min 5 members/group guaranteed
│   │       ├── claims.py             #     Medical + pharmacy claims
│   │       ├── providers.py          #     Provider directory
│   │       ├── benefits.py           #     Plan benefit schedules
│   │       ├── groups.py             #     Employer groups (stop-loss, funding, renewal)
│   │       ├── documents.py          #     Case notes, call transcripts, claims summaries
│   │       ├── underwriting.py       #     Risk assessment
│   │       ├── risk_adjustment.py    #     RAF scores, HCC codes
│   │       └── fwa.py               #     FWA signals, provider profiles, investigation cases
│   ├── notebooks/
│   │   ├── run_synthea_generation.py #   Synthea JAR → FHIR bundles → demographic crosswalk
│   │   ├── run_data_generation.py    #   Insurance domain generation (reads Synthea demographics)
│   │   ├── parse_fhir_with_dbignite.py # FHIR → Delta tables + crosswalk tables
│   │   ├── build_member_months.py    #   Member month enrollment spans
│   │   ├── create_metric_views.py    #   Governed semantic layer (DBR 17.2+)
│   │   ├── setup_vector_search.py    #   Document vector index for RAG
│   │   ├── deploy_member_agent.py    #   Care Intelligence v1 registration
│   │   ├── deploy_agent_v2.py        #   Care Intelligence v2 registration
│   │   ├── deploy_group_sales_agent.py #  Sales Coach agent registration
│   │   ├── train_fwa_model.py        #   XGBoost fraud scorer training + UC registration
│   │   ├── train_fwa_model_automl.py #   AutoML fraud scorer (alternative approach)
│   │   ├── deploy_fwa_agent.py       #   FWA Investigation agent registration
│   │   ├── bootstrap_workspace.py    #   Post-deploy setup: Lakebase, grants, seed data
│   │   ├── seed_fwa_lakebase.py      #   Seed FWA investigations into Lakebase (legacy, use bootstrap)
│   │   └── evaluate_agents.py        #   Agent evaluation
│   ├── pipelines/
│   │   ├── members/                  #   bronze.sql, silver.sql, gold.sql
│   │   ├── providers/
│   │   ├── claims/
│   │   ├── clinical/                 #   bronze.sql (with crosswalk JOINs), silver.sql, gold.sql
│   │   ├── benefits/
│   │   ├── documents/                #   bronze.sql, silver.sql
│   │   ├── underwriting/
│   │   ├── risk_adjustment/
│   │   ├── fwa/                      #   bronze.sql, silver.sql, gold.sql (FWA signals + provider risk)
│   │   ├── gold_analytics/           #   financial, quality, risk, ai, actuarial, groups,
│   │   │                             #   cost_of_care, member_360, group_report_card, fwa_analytics
│   │   └── python/                   #   Python alternatives for all pipelines
│   ├── dashboards/                   #   Lakeview dashboard JSON definitions
│   ├── fwa_lakebase_schema.sql       #   FWA Lakebase DDL (investigations, audit log, evidence)
│   └── agents/                       #   Agent model definitions (Care Intel v1/v2, Sales Coach, FWA)
├── config/                           #   Genie setup, Lakebase config
└── README.md
```

## Deployment

### Prerequisites

- Databricks CLI configured with workspace profile
- Unity Catalog workspace with catalog/schema permissions
- Foundation model endpoint (`databricks-meta-llama-3-3-70b-instruct`) for AI gold tables

### Compute

All tasks run on **serverless** compute except `synthea_generation` which requires a classic cluster (Java 17 for the Synthea JAR, DBR 16.x+). DLT pipelines are serverless SDP. Notebook tasks auto-provision and auto-scale — no cluster configuration needed.

### Targets

| Target | Profile | Catalog | Use Case |
|--------|---------|---------|----------|
| `dev` (default) | `fe-vm-red-bricks-insurance` | `red_bricks_insurance` | Development |
| `e2-field-eng` | `fe-demo-field-eng` | `zack_phillips_demos` | Field engineering demos (AWS) |
| `hls-financial` | `hls-financial-foundation` | `hls_financial_foundation_catalog` | HLS Financial Foundation workspace (AWS) |
| `prod` | `fe-vm-red-bricks-insurance` | `red_bricks_insurance` | Production |

### Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `catalog` | `main` | Unity Catalog catalog |
| `source_volume` | `/Volumes/{catalog}/raw/raw_sources` | Raw data volume path |
| `warehouse_id` | `""` | SQL warehouse (optional) |
| `node_type_small` | `Standard_DS3_v2` | Small compute (4 vCPU, 14GB) |
| `node_type_large` | `Standard_DS5_v2` | Large compute (16 vCPU, 56GB) |

### Deploying to a New Workspace

This bundle is designed to be fully portable — deploy to any workspace and everything auto-configures. No manual warehouse IDs, catalog names, or Genie space IDs to set.

**1. Add a new target** in `databricks.yml`:

```yaml
  my-workspace:
    mode: development
    workspace:
      profile: my-workspace-profile    # Databricks CLI profile name
    variables:
      catalog: my_catalog_name         # Unity Catalog catalog to use
      node_type_small: m5.xlarge       # AWS — or Standard_DS3_v2 for Azure
      node_type_large: m5.4xlarge      # AWS — or Standard_DS5_v2 for Azure
```

That's it. `warehouse_id` is optional — if omitted, the bootstrap task auto-detects a running warehouse and the apps auto-detect at startup.

**2. Deploy and run the full pipeline:**

```bash
databricks bundle deploy --target my-workspace
databricks bundle run red_bricks_full_demo --target my-workspace
```

**Let the full pipeline run to completion.** The `bootstrap_workspace` task runs at the end and automatically:
- Creates Lakebase instances and seeds operational data
- Discovers all deployed app service principals
- Grants UC permissions (USE CATALOG, USE SCHEMA, SELECT) on all domain schemas
- Grants SQL warehouse CAN_USE to all app SPs
- Creates 4 Genie spaces with dynamic table references based on the catalog
- Grants Genie space CAN_RUN to all app SPs

Once bootstrap completes, all three apps will be fully functional with no manual configuration.

**3. Runtime auto-detection** — apps self-configure at startup:

DAB resource config `${var.*}` values only resolve in resource definitions, not in source files uploaded to the workspace. Instead, each app uses an `env_config.py` module that auto-detects resources at startup:

| Resource | How It's Detected | Sentinel Value |
|----------|-------------------|----------------|
| **SQL Warehouse** | `w.warehouses.list()` → first RUNNING warehouse | `auto` |
| **UC Catalog** | `w.catalogs.list()` → finds catalog containing the app's `UC_SCHEMA` | `auto` |
| **Genie Space** | `GET /api/2.0/genie/spaces` → first available space the SP can see | `auto` |
| **LLM Endpoint** | Hardcoded default per app | — |

The `app.yml` files use `auto` as a sentinel value for `SQL_WAREHOUSE_ID`, `UC_CATALOG`, and `GENIE_SPACE_ID`. When `env_config.py` sees `auto`, it triggers runtime auto-detection using the Databricks SDK. This means the same source code works across any workspace without modification.

**Prerequisites for auto-detection to work:**
- At least one SQL warehouse exists and the app SP has `CAN_USE` (granted by bootstrap)
- The catalog contains the expected schema (e.g., `analytics`, `fwa`) — created by the SDP pipelines
- At least one Genie space exists with `CAN_RUN` for the SP (created by bootstrap)

**4. Configuration flow:**

| Layer | How Values Are Set | Example |
|-------|-------------------|---------|
| **DAB variables** (`databricks.yml`) | Set per target in `variables:` | `catalog: my_catalog` |
| **Job parameters** (`base_parameters`) | Auto-injected from `${var.catalog}`, `${var.warehouse_id}` | Notebook receives `catalog=my_catalog` |
| **Notebook widgets** (`dbutils.widgets`) | Populated by job params; defaults used for manual runs | `dbutils.widgets.get("catalog")` |
| **App env vars** (`app.yml`) | Set to `auto` → runtime auto-detection via `env_config.py` | `SQL_WAREHOUSE_ID=auto` → detected |
| **Bootstrap task** | Auto-detects warehouse, discovers app SPs, creates Genie spaces | Fully dynamic |

**5. Key environment variables** used by apps:

| Variable | Used By | Auto-Detected? |
|----------|---------|----------------|
| `SQL_WAREHOUSE_ID` | All apps | Yes — first running warehouse |
| `UC_CATALOG` | All apps | Yes — catalog containing target `UC_SCHEMA` |
| `GENIE_SPACE_ID` | Command Center | Yes — first visible Genie space |
| `LLM_ENDPOINT` | All apps | No — defaults per app (`databricks-llama-4-maverick` or `databricks-meta-llama-3-3-70b-instruct`) |
| `LAKEBASE_INSTANCE_NAME` | Command Center, FWA app | No — set in `app.yml` (instance names are fixed) |
| `LAKEBASE_DATABASE_NAME` | Command Center, FWA app | No — set in `app.yml` (database names are fixed) |
| `FWA_MODEL_ENDPOINT` | FWA app | No — defaults to `fwa-fraud-scorer` |

### Commands

```bash
# Validate bundle
databricks bundle validate --target e2-field-eng

# Deploy all resources (pipelines, jobs, dashboards, apps)
databricks bundle deploy --target e2-field-eng --force

# --- End-to-end demo (synthea → data gen → all pipelines → agents → eval) ---
databricks bundle run red_bricks_full_demo --target e2-field-eng

# --- Refresh without Synthea (data gen → pipelines → analytics → agents) ---
databricks bundle run red_bricks_refresh --target e2-field-eng

# --- Individual components ---
databricks bundle run red_bricks_data_generation   # Just generate insurance data
databricks bundle run claims_pipeline              # Just claims bronze → gold
databricks bundle run clinical_pipeline            # Just clinical (requires dbignite tables)
databricks bundle run members_pipeline             # Just members & enrollment
databricks bundle run providers_pipeline           # Just providers
databricks bundle run benefits_pipeline            # Just benefits
databricks bundle run documents_pipeline           # Just documents
databricks bundle run underwriting_pipeline        # Just underwriting
databricks bundle run risk_adjustment_pipeline     # Just risk adjustment
databricks bundle run gold_analytics_pipeline      # Just cross-domain analytics
databricks bundle run fwa_pipeline                 # Just FWA domain (signals, profiles, investigations)
```

## Apps — Frontend Build

All three apps (`app/` Command Center, `app-group-reporting/` Group Reporting Portal, `app-fwa/` FWA Investigation Portal) use React + Vite + Tailwind. **Frontends must be built before deploying the bundle** — the DAB deploys the pre-built `static/` directory, not the source.

```bash
# Command Center
cd app/frontend && npm install && npm run build   # → outputs to app/static/

# Group Reporting Portal
cd app-group-reporting/frontend && npm install && npm run build   # → outputs to app-group-reporting/static/

# FWA Investigation Portal
cd app-fwa/frontend && npm install && npm run build   # → outputs to app-fwa/static/
```

The `.bundleignore` excludes `node_modules/`, `src/`, and other frontend build artifacts from the bundle upload. Only the `static/` directories are deployed.

After building, deploy the bundle normally with `databricks bundle deploy`.

## Workspace Bootstrap — Post-Deploy Setup

The `bootstrap_workspace` task runs automatically at the end of both the full demo and refresh jobs. It handles all post-deploy provisioning and is fully **idempotent** — safe to re-run at any time.

1. **Lakebase instances** — Creates `red-bricks-command-center` and `fwa-investigations` instances, databases, and DDL schemas (skips if already exist)
2. **Staff seeding** — Inserts care managers and fraud investigators (`ON CONFLICT DO NOTHING`)
3. **App service principal discovery** — Auto-discovers SPs for all deployed apps matching `red-bricks-*` or `rb-*` name patterns
4. **Unity Catalog grants** — `USE CATALOG`, `BROWSE`, `USE SCHEMA`, `SELECT` on all 11 domain schemas for each app SP
5. **SQL Warehouse grants** — `CAN_USE` on the auto-detected (or configured) warehouse for each app SP
6. **Genie spaces** — Creates 4 Genie spaces (Analytics Assistant, FWA Analytics, Group Reporting, Financial Analytics) with dynamic table references based on the catalog. Validates tables exist before adding. Grants `CAN_RUN` to all app SPs. Skips spaces that already exist (matched by title).
7. **ML predictions table** — Pre-creates `analytics.fwa_ml_predictions` for gold MV compatibility
8. **Operational data seeding** — Populates Lakebase with risk alerts (from gold tables) and FWA investigation cases (from silver/gold FWA tables)

To run manually (e.g., after a fresh deploy without running the full job):

```bash
databricks jobs submit --json '{
  "run_name": "Bootstrap Workspace",
  "tasks": [{
    "task_key": "bootstrap",
    "notebook_task": {
      "notebook_path": "/Users/<your-email>/.bundle/red-bricks-insurance/dev/files/src/notebooks/bootstrap_workspace",
      "base_parameters": {"catalog": "red_bricks_insurance", "warehouse_id": "781064a3466c0984"}
    },
    "environment_key": "bootstrap_env"
  }],
  "environments": [{
    "environment_key": "bootstrap_env",
    "spec": {"client": "1", "dependencies": ["psycopg[binary]", "databricks-sdk>=0.30.0"]}
  }]
}'
```

### 3. FWA Model Training

Two model training notebooks are available:

| Notebook | Approach | Runtime |
|----------|----------|---------|
| `train_fwa_model.py` | XGBoost with manual CV + hyperparameter tuning | Serverless (ml_training_env) |
| `train_fwa_model_automl.py` | Databricks AutoML | Classic ML Runtime cluster |

The full demo job uses `train_fwa_model.py` (XGBoost) by default.

**Note:** Both notebooks cast all feature columns to `float64` before inference (`inference_pd[feature_cols].astype("float64")`). This is required because MLflow's schema enforcement rejects integer columns (e.g., `member_total_claims` as int64) when the model signature specifies double.

### 4. App Environment Variables

Set in `resources/app_fwa.yml`:

| Variable | Description |
|----------|-------------|
| `LAKEBASE_INSTANCE_NAME` | `fwa-investigations` |
| `LAKEBASE_DATABASE_NAME` | `fwa_cases` |
| `SQL_WAREHOUSE_ID` | Serverless SQL warehouse ID for Statement Execution API |
| `LLM_ENDPOINT` | Foundation Model API endpoint (e.g., `databricks-llama-4-maverick`) |
| `UC_CATALOG` | Unity Catalog name (e.g., `red_bricks_insurance`) |
| `FWA_MODEL_ENDPOINT` | Model serving endpoint for real-time scoring (optional) |
| `GENIE_SPACE_ID` | Genie space ID for natural language SQL queries |

## Customization

This demo is designed to be modular for customer-specific showings:

- **Remove a domain**: Delete its pipeline YAML from `resources/` and its task from `full_demo_job.yml`
- **Add a gold metric**: Add a new `CREATE OR REFRESH MATERIALIZED VIEW` to the appropriate SQL file in `gold_analytics/`
- **Switch to Python**: Update library paths in `resources/pipeline_*.yml` to point to `python/` files
- **Change AI model**: Update the model name in `ai_classification.sql`
- **Scale data**: Adjust `NUM_PATIENTS` in `run_synthea_generation.py` and record counts in `run_data_generation.py`
- **Different geography**: Change `STATE` in `run_synthea_generation.py` (Synthea supports all US states)

## Required Packages

| Package | Used By |
|---------|---------|
| `faker` | Insurance domain data generation |
| `fpdf2` | Document PDF generation |
| `dbignite` | FHIR R4 bundle parsing (installed at runtime) |
| `mlflow` | Agent registration and evaluation |
| `databricks-sdk` | Agent deployment, API calls |
| `databricks-automl-runtime` | FWA fraud scorer model training (AutoML) |
| `fastapi` / `uvicorn` | App backends (Command Center, Group Reporting, FWA Portal) |
| `psycopg` | Lakebase PostgreSQL connections (Command Center, FWA Portal) |
| `slack_sdk` | (Optional) Sales Coach Slack enrichment |
| `simple_salesforce` | (Optional) Sales Coach Salesforce enrichment |
