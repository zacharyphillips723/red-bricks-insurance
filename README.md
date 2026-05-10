# Red Bricks Insurance

Healthcare insurance company simulation — modular Databricks Asset Bundle (DAB). One deployable bundle that runs end-to-end: Synthea clinical generation → synthetic insurance data → bronze/silver/gold SDP pipelines → cross-domain analytics with AI classification → ML model training → intelligent agents → six purpose-built applications — all with MLflow 3 tracing and OpenTelemetry observability.

## Table of Contents

- [Architecture](#architecture)
- [Pipeline DAG](#pipeline-dag)
- [Data Domains](#data-domains)
- [Schema Architecture](#schema-architecture)
- [SDP Pipelines (Medallion Architecture)](#sdp-pipelines-medallion-architecture)
  - [Gold Analytics Tables](#gold-analytics-tables)
  - [Metric Views (Governed Semantic Layer)](#metric-views-governed-semantic-layer)
  - [Clinical Pipeline (Synthea → dbignite → SDP)](#clinical-pipeline-synthea--dbignite--sdp)
- [AI Agents](#ai-agents)
- [Databricks Apps](#databricks-apps)
  - [Command Center](#command-center-app)
  - [Group Reporting Portal](#group-reporting-portal-app-group-reporting)
  - [FWA Investigation Portal](#fwa-investigation-portal-app-fwa)
  - [Underwriting Simulation Portal](#underwriting-simulation-portal-app-underwriting-sim)
  - [Prior Authorization Portal](#prior-authorization-portal-app-prior-auth)
  - [Network Adequacy Portal](#network-adequacy-portal-app-network-adequacy)
- [Dashboards](#dashboards)
- [Project Structure](#project-structure)
- [Deployment](#deployment)
  - [Prerequisites](#prerequisites)
  - [Workspace Prerequisites](#workspace-prerequisites)
  - [Compute](#compute)
  - [Targets](#targets)
  - [Variables](#variables)
  - [Deploying to a New Workspace](#deploying-to-a-new-workspace)
  - [Commands](#commands)
- [Apps — Frontend Build](#apps--frontend-build)
- [Workspace Bootstrap — Post-Deploy Setup](#workspace-bootstrap--post-deploy-setup)
- [Lakebase & App Authentication](#lakebase--app-authentication)
- [Deployment Notes & Known Issues](#deployment-notes--known-issues)
- [Observability & Tracing](#observability--tracing)
- [Customization](#customization)
- [Required Packages](#required-packages)

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
│  (run_data_generation)  │  Members, Enrollment, Groups, Claims, Providers, Benefits,
│                         │  Documents, Underwriting, Risk Adjustment, FWA, Prior Auth,
│                         │  Network, ADT Events, Care Management
└───────────┬─────────────┘
            │
     ┌──────┴───────┐  (14 domain pipelines run in parallel)
     ▼              ▼
┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────────┐ ┌───────────────┐
│ Members  │ │Providers │ │ Claims   │ │ Clinical │ │ Underwriting │ │Risk Adjustment│
│& Enroll. │ │          │ │Med + Rx  │ │FHIR→Delta│ │              │ │ Member+Prov   │
│ B → S → G│ │ B → S → G│ │ B → S → G│ │ B → S → G│ │ B → S → G   │ │ B → S → G    │
└────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘ └──────┬──────┘ └──────┬────────┘
     │            │            │            │               │               │
     │  ┌──────────┐ ┌──────────┐ ┌─────────────┐ ┌──────────────────┐     │
     │  │Documents │ │Benefits  │ │Member Months│ │Network Adequacy  │     │
     │  │ B → S    │ │ B → S → G│ │ (notebook)  │ │H3 Geo, CMS 422  │     │
     │  └────┬─────┘ └────┬─────┘ └──────┬──────┘ │ B → S → G       │     │
     │       │            │              │         └───────┬──────────┘     │
     │       │            │              │                 │                │
     └───────┴────────────┴──────────────┴─────────────────┴────────────────┘
                                    │
                         ┌──────────▼──────────┐
                         │  Gold Analytics     │  Cross-domain metrics
                         │  Financial, Quality,│  Group Report Card
                         │  Risk, AI, Actuarial│  TCOC / TCI / FWA
                         │  Member 360, ML, PA │
                         └──────────┬──────────┘
                                    │
               ┌────────────────────┼────────────────────┐
               ▼                    ▼                    ▼
        ┌────────────┐       ┌────────────┐       ┌──────────────┐
        │ Dashboards │       │  Genie     │       │ AI Agents    │
        │ (AI/BI)    │       │  Spaces    │       │ Care Intel   │
        └────────────┘       └────────────┘       │ Sales Coach  │
                                                  │ FWA Agent    │
                                                  │ PA Review    │
                                                  └──────┬───────┘
                                                         │
     ┌──────────────┬──────────────┬─────────────────────┼─────────────────────┐
     ▼              ▼              ▼                      ▼                     ▼
┌──────────────┐┌──────────────┐┌──────────────────┐┌──────────────────┐┌──────────────────┐
│  Command     ││  PA Portal   ││  Group Reporting ││  FWA Portal      ││  Network         │
│  Center App  ││  App         ││  Portal App      ││  App             ││  Adequacy Portal │
│  (React+API) ││  (React+API) ││  (React+API)     ││  (React+API)     ││  (React+API)     │
│  Clinical    ││  Auto-Adjud. ││  Sales Enablement││  Investigations  ││  CMS Compliance  │
└──────────────┘└──────────────┘└──────────────────┘└──────────────────┘└──────────────────┘
                                     ┌──────────────────┐
                                     │  UW Simulation   │
                                     │  Portal App      │
                                     │  (React+API)     │
                                     │  What-If Analysis│
                                     └──────────────────┘
```

## Pipeline DAG

The full demo job (`red_bricks_full_demo`) orchestrates 36 tasks:

```
synthea_generation (ROOT — generates FHIR bundles + extracts demographics + assigns MBR IDs)
  → data_generation (reads Synthea demographics, generates insurance domains + FWA + PA signals)
      → [members, providers, claims, enrollment, benefits, underwriting,
         documents, risk_adjustment, fwa, prior_auth, care_management pipelines]
      → network_adequacy_pipeline (depends on data_generation + providers + members + claims)
      → seed_adt_feed → adt_pipeline (ADT event stream via Autoloader)
      → parse_fhir_with_dbignite (reads raw synthea_raw/fhir/, writes crosswalk Delta tables)
          → clinical_pipeline (bronze.sql JOINs crosswalk for MBR IDs + NPIs)
  → build_member_months (depends on members_pipeline)
  → fwa_pipeline (depends on data_generation — bronze/silver/gold FWA signals + provider profiles)
  → gold_analytics_pipeline (depends on all domain pipelines + member months + fwa_pipeline + network_adequacy_pipeline)
      → create_metric_views (governed semantic layer + FWA risk metrics)
  → train_fwa_model (depends on fwa_pipeline + gold_analytics — XGBoost fraud scorer)
  → train_pa_model (depends on prior_auth_pipeline — XGBoost 3-tier auto-adjudication model)
      → pa_model_governance (bias monitoring, drift detection, audit trail)
  → parse_medical_policies (depends on prior_auth_pipeline — LLM-based policy PDF extraction)
  → setup_vector_search (depends on documents_pipeline)
      → deploy_member_agent (v1)
      → deploy_agent_v2 (v2 with benefits)
      → deploy_group_sales_agent (Sales Coach for group reporting)
      → deploy_fwa_agent (FWA Investigation agent with tool-calling)
      → deploy_pa_agent (PA Review agent with clinical review tool-calling)
          → evaluate_agents (v1 vs v2 vs sales coach comparison)
          → evaluate_care_agent (MLflow GenAI evaluation: groundedness, relevance, safety, clinical completeness, actionability, HIPAA compliance)
  → bootstrap_workspace (depends on gold_analytics + fwa_pipeline + network_adequacy_pipeline + prior_auth_pipeline + train_fwa_model)
      — Creates Lakebase instances, applies UC/warehouse grants for app SPs, seeds operational data,
        seeds PA reviewer staff and review queue, creates Genie spaces (including Network Analytics)
      → deploy_app_source (deploys source code + starts compute for all 6 apps)
```

A **refresh job** (`red_bricks_refresh`) runs the same DAG minus Synthea/FHIR/clinical — useful when only insurance data generation or downstream logic has changed. Both jobs include the `bootstrap_workspace` task, which automatically provisions Lakebase, discovers app service principals, grants UC + warehouse permissions, seeds alerts/investigations from gold tables, and seeds the PA review queue with reviewer assignments.

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
| **Prior Authorization** | ~10K PA requests | Parquet | PA requests with service types, determination status, turnaround times, clinical summaries |
| **Medical Policies** | ~10 policies | PDF + Parquet | Prior auth policy PDFs with clinical criteria, CPT/ICD-10 codes, coverage rules |
| **Network Adequacy** | 500 providers + 5K members + 100 counties | Parquet + CSV | H3-geocoded provider/member locations, CMS time/distance standards, county classifications, enriched claims with OON leakage analysis |
| **ADT Events** | ~50 events/batch | JSON (Autoloader) | Admit/Discharge/Transfer hospital events streamed via Autoloader; triggers care management alerts for readmissions, high-acuity ED visits |
| **Care Management** | ~500 episodes + 6 programs + SDOH data | Parquet | Disease management programs, case episodes, care activities, SDOH screening, care gap tracking, transitions of care |

**Data quality**: ~2% intentional defects (nulls, invalid codes, out-of-range dates) caught by SDP expectations at the silver layer.

## Schema Architecture

Tables are organized into **15 domain schemas** within the catalog, each owned by its domain pipeline:

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
| `prior_auth` | Prior authorization requests & policies | `silver_pa_requests`, `gold_pa_summary`, `gold_pa_turnaround`, `parsed_medical_policies` |
| `network` | Network adequacy, ghost networks, OON leakage | `silver_provider_geo`, `silver_member_geo`, `gold_network_adequacy_compliance`, `gold_ghost_network_flags`, `gold_network_leakage`, `gold_provider_recruitment_targets`, `gold_network_gaps` |
| `adt` | ADT hospital event stream | `bronze_adt_events`, `silver_adt_events`, `gold_adt_alerts`, `gold_adt_readmission_risk` |
| `care_management` | Disease management & care coordination | `silver_care_programs`, `silver_program_enrollment`, `silver_case_episodes`, `silver_member_sdoh`, `gold_program_performance`, `gold_care_gap_closure` |
| `analytics` | Cross-domain gold tables & metric views | `gold_pmpm`, `gold_mlr`, `gold_hedis_member`, `gold_member_360`, `gold_fwa_member_risk`, `fwa_model_inference`, `mv_financial_overview` |

**Key design principle**: Each domain pipeline writes bronze/silver/gold tables to its own schema. Only cross-domain gold analytics (tables that JOIN across multiple domains) land in the `analytics` schema. The `network` schema is unique in that it cross-references providers, members, and claims data enriched with H3 geospatial indexing for CMS distance/time compliance calculations.

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

**ML Models:**
- **FWA Fraud Scorer** — XGBoost claim-level fraud scorer (`fwa_scoring_model`), trained with 5-fold stratified CV and hyperparameter tuning, registered in Unity Catalog. Served via `fwa-fraud-scorer` endpoint with inference table logging. Predictions written to `analytics.fwa_model_inference`. An AutoML alternative (`train_fwa_model_automl.py`) is also available.
- **PA Auto-Adjudication** — XGBoost 3-class classifier (`pa_adjudication_model`) for Tier 2 ML-based PA determinations (approve/deny/review). Trained with stratified CV, registered in Unity Catalog. Includes MLflow governance: bias monitoring (disparate impact by LOB, urgency, service type), drift detection (PSI + KS test), and feature importance logging. See `train_pa_model.py` and `pa_model_governance.py`.

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

Five agents are deployed and registered in Unity Catalog via MLflow:

| Agent | Description | Audience |
|-------|-------------|----------|
| **Care Intelligence v1** (`deploy_member_agent`) | Member lookup + document search | Clinical care teams |
| **Care Intelligence v2** (`deploy_agent_v2`) | v1 + benefits coverage analysis | Clinical care teams |
| **Sales Coach** (`deploy_group_sales_agent`) | Group report card analysis, renewal prep, roleplay negotiation simulation, care management program recommendations | Account executives, sales reps |
| **FWA Investigation** (`deploy_fwa_agent`) | Tool-calling agent that dynamically queries UC tables (provider risk, claims, ML predictions), generates structured investigation briefings | SIU analysts, compliance teams |
| **PA Review** (`deploy_pa_agent`) | Tool-calling agent that queries PA tables and medical policies, produces structured clinical review briefings with determination recommendations | UM nurses, PA reviewers |

All agents are evaluated with `evaluate_agents.py`. The FWA Investigation and PA Review agents use multi-turn tool-calling patterns — the LLM autonomously composes SQL queries against allowed Unity Catalog schemas, retrieves data, and synthesizes findings. The Sales Coach supports intent-based modes: full briefing ("prepare me for..."), renewal focus ("why rate increase"), care management ("what programs can I offer"), and negotiation roleplay ("simulate a renewal negotiation").

## Databricks Apps

### Command Center (`app/`)

Clinical-focused application for care management teams:

- **Backend**: FastAPI (Python), connects to Lakebase, SQL warehouse, and serving endpoints
- **Frontend**: React + Vite + Tailwind (Databricks-branded dark theme)
- **Observability**: MLflow 3 tracing (`@mlflow.trace`) on agent calls, Genie queries, and tool invocations; OpenTelemetry FastAPI auto-instrumentation
- **Pages**:
  - **Dashboard** — Real-time KPIs (active alerts, critical count, open cases, avg risk score), alert queue with filters
  - **Alert Queue** — Filterable/sortable alerts with risk tier, status, assignment tracking
  - **Alert Detail** — Full alert view with care manager assignment, status workflow, clinical context
  - **Member 360** — Unified member view (demographics, clinical summary, claims, care gaps, risk factors, agent chat)
  - **Care Plan** — AI-generated care plans with goals, interventions, milestones, timeline visualization
  - **Outreach Draft** — AI-generated personalized outreach scripts (phone, SMS, email) based on member profile and preferred communication
  - **Cohort Builder** — Population cohort definition with demographic, clinical, and utilization filters; cohort analytics
  - **Patient Search (Genie)** — Natural language SQL exploration via Genie space integration
  - **Caseload** — Care manager workload dashboard with assignment tracking
- **Agent Architecture**: LangGraph state machine skeleton (`agent_graph.py`) with specialist agents (Clinical, Financial, Care Management) and UC function tool-calling (`agent_tools.py`)
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

- **Backend**: FastAPI (Python), connects to Lakebase Autoscaling (`fwa_cases` database), SQL warehouse (Statement Execution API for gold table queries), and Foundation Model API (Llama 4 Maverick)
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

### Underwriting Simulation Portal (`app-underwriting-sim/`)

What-if analysis tool for actuaries and underwriters to model pricing scenarios in real time:

- **Backend**: FastAPI (Python), Lakebase PostgreSQL (async SQLAlchemy + OAuth token refresh), Statement Execution API with 15-min in-memory cache, Foundation Model API (Llama 4 Maverick)
- **Frontend**: React 19 + Vite 6 + Tailwind (Databricks-branded dark theme)
- **Simulation Engine**: 10 pure-Python simulation types with sub-100ms execution (no Spark required):
  - Premium Rate, Benefit Design, Group Renewal, Population Mix, Medical Trend, Stop-Loss, Risk Adjustment, Utilization Change, New Group Quote, IBNR Reserve
  - All return baseline vs. projected with delta, narrative summary, and warnings
- **Pages**:
  - **Dashboard** — KPI cards (total premium, claims, MLR, member count), baseline summary, recent simulations, quick-sim launcher
  - **Simulation Builder** — dynamic form per simulation type with parameter inputs, delta-colored results, save dialog
  - **Scenario Comparison** — side-by-side comparison of 2–4 simulations
  - **Simulation History** — list/detail view with status management (draft/computed/approved/archived), notes, and immutable audit trail
  - **Agent Chat** — conversational interface with tool-calling (`run_simulation`, `get_baseline`, `get_group_detail`, `query_uc_table`); SQL validation blocks write operations
- **Genie Integration**: natural language SQL exploration against UC gold tables (graceful degradation if GENIE_SPACE_ID not set)
- **Data Architecture**: Hybrid — Lakebase for transactional state (simulations, comparisons, audit log) + Statement Execution API for gold table aggregates (`gold_pmpm`, `gold_mlr`, `gold_enrollment_summary`, `gold_utilization_per_1000`, `gold_risk_adjustment_analysis`, etc.)
- **Config**: `app-underwriting-sim/app.yml`, DAB resource: `resources/app_underwriting_sim.yml`

### Prior Authorization Portal (`app-prior-auth/`)

Prior authorization review and auto-adjudication portal for UM nurses and PA reviewers:

- **Backend**: FastAPI (Python), connects to Lakebase (`pa_reviews` database), SQL warehouse (Statement Execution API for PA gold tables), and Foundation Model API (Llama 4 Maverick)
- **Frontend**: React + Vite + Tailwind (Databricks-branded dark theme)
- **Auto-Adjudication**: 3-tier determination model:
  - **Tier 1** — Deterministic rules (CPT/ICD-10 code matching against medical policies)
  - **Tier 2** — ML model (XGBoost classifier trained on historical PA decisions)
  - **Tier 3** — LLM clinical review (agent-generated briefings for complex cases)
- **Pages**:
  - **Dashboard** — KPI cards (total requests, approval rate, avg turnaround, pending queue depth), determination breakdown charts
  - **Review Queue** — filterable PA request queue sorted by urgency and turnaround SLA
  - **Request Detail** — full PA request view with clinical summary, service details, determination history, and reviewer notes
  - **Caseload View** — reviewer workload management with assignment tracking
  - **Policy Library** — medical policy reference with searchable clinical criteria
  - **Agent Chat** — PA Review agent interface for clinical review briefings and determination recommendations
- **Data Architecture**: Hybrid — Lakebase for transactional review state (assignments, status changes, reviewer notes, audit trail) + Statement Execution API for analytics (PA gold tables, medical policy lookups)
- **Config**: `app-prior-auth/app.yml`, DAB resource: `resources/app_prior_auth.yml`

### Network Adequacy Portal (`app-network-adequacy/`)

CMS network adequacy compliance monitoring for network operations teams:

- **Backend**: FastAPI (Python), reads network gold tables via Statement Execution API (no Lakebase — read-only analytics)
- **Frontend**: React + Vite + Tailwind (Databricks-branded dark theme)
- **Regulatory Framework**: CMS 42 CFR 422.116 — 29 provider specialties, 13 facility types, 5 county types (Large Metro/Metro/Micro/Rural/CEAC), 90% member compliance threshold
- **Geospatial Engine**: Haversine great-circle distance calculations in pure SQL for member-to-provider distance measurement. Pre-filters by 60-mile radius to keep cross-join complexity manageable (~2.5M calculations for 500 providers x 5K members). Location-agnostic — works for any state or geography.
- **Pages**:
  - **Dashboard** — 6 KPI cards (overall compliance %, ghost providers flagged, OON leakage cost, gap members, telehealth credits, ghost impact members), top recruitment targets table
  - **Geographic View** — Interactive Leaflet map with proportional circle markers at county centroids (computed from provider coordinates). 5 metric overlays (CMS Compliance, OON Leakage Cost, Gap Members, Ghost Network Flags, Provider Count) with diverging color scales, county type filters, click-to-inspect popups with full metric detail, auto-fit bounds (location-agnostic)
  - **CMS Compliance** — Filterable compliance table by county, CMS specialty type, and compliance status; shows distance thresholds, member counts, and gap analysis
  - **Ghost Network Detection** — Provider cards with multi-signal ghost detection (no claims 12m, not accepting patients, extreme wait times, expired credentials, panel at capacity), severity badges, impact member counts
  - **OON Leakage Analysis** — Leakage cost breakdowns by specialty, county, and reason with horizontal bar charts; detailed specialty table with OON provider counts
  - **Gaps & Recruitment** — Network gaps table with priority filtering (P1–P4) and gap status classification (Critical/Non-Compliant/At Risk/Marginal); OON provider recruitment targets ranked by priority score
  - **Network Analytics** — Genie-powered natural language SQL exploration over network gold tables
- **Data Architecture**: Read-only — Statement Execution API for all analytics queries against `network` schema gold tables
- **Config**: `app-network-adequacy/app.yml`, DAB resource: `resources/app_network_adequacy.yml`

## Dashboards

| Dashboard | Description |
|-----------|-------------|
| **Red Bricks Analytics** | Financial, quality, and risk metrics across all domains |
| **Agent Comparison** | Side-by-side v1 vs v2 agent evaluation results |
| **PA Operations** | Prior authorization turnaround times, approval/denial rates, reviewer workload, SLA compliance |
| **Network Adequacy** | CMS compliance overview, ghost network monitoring, OON leakage intelligence, network gaps & recruitment targets (4 pages, 17 datasets) |
| **Care Management Operations** | Program enrollment, case episode tracking, SDOH prevalence, care gap closure rates, care manager productivity |
| **Agent Observability** | Agent query volume, P50/P95 latencies, token costs, Genie usage, error rates (sourced from MLflow trace tables) |

All dashboards are deployed as AI/BI Lakeview dashboards with `CAN_READ` permissions for the users group.

## Project Structure

```
red-bricks-insurance/
├── databricks.yml                    # Bundle config, variables, targets (dev/e2-field-eng/prod)
├── app/                              # Command Center Databricks App
│   ├── app.yml                       #   App configuration (env vars, command)
│   ├── main.py                       #   FastAPI backend (MLflow tracing + OpenTelemetry)
│   ├── backend/
│   │   ├── router.py                 #   API routes (alerts, members, caseload, care plans, outreach)
│   │   ├── agent.py                  #   RAG agent (member lookup + Vector Search + LLM)
│   │   ├── agent_graph.py            #   LangGraph StateGraph skeleton with @mlflow.trace
│   │   ├── agent_tools.py            #   UC function tool-calling with tracing
│   │   ├── agents/                   #   Specialist agents (Clinical, Financial, CareManagement)
│   │   ├── genie.py                  #   Genie space integration with tracing
│   │   ├── models.py                 #   Pydantic models
│   │   ├── database.py               #   Lakebase connection with OAuth refresh
│   │   ├── conversation_store.py     #   LangGraph conversation persistence
│   │   ├── identity.py               #   User identity resolution
│   │   └── websocket.py              #   WebSocket notification support
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
├── app-underwriting-sim/                # Underwriting Simulation Portal Databricks App
│   ├── app.yml                       #   App config (Lakebase, SQL warehouse, LLM endpoint)
│   ├── main.py                       #   FastAPI backend
│   ├── backend/
│   │   ├── router.py                 #   API routes (simulations, comparisons, agent, genie)
│   │   ├── models.py                 #   Pydantic models (10 simulation types)
│   │   ├── simulation_engine.py      #   Pure-Python simulation functions
│   │   ├── data_loader.py            #   Statement Execution API + caching
│   │   ├── database.py               #   Lakebase connection with OAuth token refresh
│   │   ├── scenarios.py              #   Lakebase CRUD (simulations, comparisons, audit)
│   │   ├── agent.py                  #   UW agent (tool-calling with simulations + UC queries)
│   │   └── genie.py                  #   Genie space integration
│   ├── frontend/                     #   React + Vite + Tailwind source
│   └── static/                       #   Built frontend output
├── app-prior-auth/                      # Prior Authorization Portal Databricks App
│   ├── app.yml                       #   App config (Lakebase, SQL warehouse, LLM endpoint)
│   ├── main.py                       #   FastAPI backend
│   ├── backend/
│   │   ├── router.py                 #   API routes (dashboard, review queue, requests, agent)
│   │   ├── models.py                 #   Pydantic models (PA statuses, determination types)
│   │   ├── database.py               #   Lakebase connection with OAuth token refresh
│   │   ├── agent.py                  #   PA Review agent (tool-calling with clinical data)
│   │   └── env_config.py             #   Runtime auto-detection (warehouse, catalog, Genie)
│   ├── frontend/                     #   React + Vite + Tailwind source
│   └── static/                       #   Built frontend output
├── app-network-adequacy/                # Network Adequacy Portal Databricks App
│   ├── app.yml                       #   App config (SQL warehouse, catalog, Genie)
│   ├── main.py                       #   FastAPI backend (read-only, no Lakebase)
│   ├── backend/
│   │   ├── router.py                 #   API routes (compliance, ghost network, leakage, gaps, map, genie)
│   │   ├── models.py                 #   Pydantic models (compliance, ghost, leakage, recruitment, map)
│   │   ├── genie.py                  #   Genie space integration (Network Analytics)
│   │   └── env_config.py             #   Runtime auto-detection (warehouse, catalog, Genie)
│   ├── frontend/                     #   React + Vite + Tailwind source
│   └── static/                       #   Built frontend output
├── resources/
│   ├── full_demo_job.yml             # End-to-end orchestration (36 tasks)
│   ├── refresh_demo_job.yml          # Refresh without Synthea (data gen → all downstream)
│   ├── adt_feed_job.yml              # Scheduled ADT event generation (every 3 hours)
│   ├── data_generation_job.yml       # Standalone data generation
│   ├── dashboard.yml                 # Analytics dashboard
│   ├── agent_comparison_dashboard.yml# Agent eval dashboard
│   ├── dashboard_pa_operations.yml   # PA operations dashboard
│   ├── dashboard_network_adequacy.yml # Network adequacy dashboard (4 pages)
│   ├── lakebase_instances.yml        # Lakebase instance definitions
│   ├── app.yml                       # Command Center app resource
│   ├── app_prior_auth.yml            # Prior Auth Portal app resource
│   ├── app_group_reporting.yml       # Group Reporting Portal app resource
│   ├── app_fwa.yml                   # FWA Investigation Portal app resource
│   ├── app_underwriting_sim.yml      # Underwriting Simulation Portal app resource
│   ├── app_network_adequacy.yml     # Network Adequacy Portal app resource
│   ├── pipeline_members.yml          # Members & Enrollment SDP
│   ├── pipeline_providers.yml        # Providers SDP
│   ├── pipeline_claims.yml           # Claims SDP
│   ├── pipeline_clinical.yml         # Clinical SDP (Synthea → dbignite)
│   ├── pipeline_benefits.yml         # Benefits SDP
│   ├── pipeline_documents.yml        # Documents SDP
│   ├── pipeline_underwriting.yml     # Underwriting SDP
│   ├── pipeline_risk_adjustment.yml  # Risk Adjustment SDP
│   ├── pipeline_fwa.yml              # FWA domain SDP (signals, profiles, investigations)
│   ├── pipeline_prior_auth.yml       # Prior Auth domain SDP (PA requests, policies)
│   ├── pipeline_network.yml          # Network adequacy SDP (H3 geo, CMS compliance, ghost, leakage)
│   ├── pipeline_adt.yml              # ADT event stream SDP (Autoloader → bronze → silver → gold alerts)
│   ├── pipeline_care_management.yml  # Care management SDP (programs, episodes, SDOH, care gaps)
│   ├── dashboard_care_management.yml # Care management operations dashboard
│   ├── dashboard_observability.yml   # Agent observability dashboard (traces, latency, token costs)
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
│   │       ├── fwa.py               #     FWA signals, provider profiles, investigation cases
│   │       ├── prior_auth.py        #     PA requests with determinations + turnaround times
│   │       ├── medical_policies.py  #     Medical policy PDFs with clinical criteria
│   │       ├── network_adequacy.py  #     H3-geocoded providers/members, CMS standards, OON claims enrichment
│   │       ├── adt.py              #     ADT events (admit/discharge/transfer) for Autoloader
│   │       └── care_management.py  #     Disease management programs, case episodes, SDOH, care gaps
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
│   │   ├── train_pa_model.py        #   XGBoost PA auto-adjudication model + UC registration
│   │   ├── deploy_pa_agent.py       #   PA Review agent registration (tool-calling)
│   │   ├── parse_medical_policies.py#   LLM-based policy PDF extraction to structured rules
│   │   ├── pa_model_governance.py   #   PA model bias monitoring, drift detection, audit trail
│   │   ├── generate_adt_feed.py     #   ADT event batch generation + Lakebase alert seeding
│   │   ├── evaluate_care_agent.py  #   MLflow GenAI evaluation (6 scorers, 4 question types)
│   │   ├── create_uc_tools.py      #   Register UC tool functions for agent tool-calling
│   │   ├── deploy_app_source.py     #   Deploy source code + start compute for all 6 apps
│   │   ├── setup_lakebase.py        #   Lakebase DDL initialization (all instances)
│   │   ├── seed_lakebase_alerts.py  #   Seed risk alerts into Command Center Lakebase
│   │   ├── seed_fwa_lakebase.py     #   Seed FWA investigations into Lakebase (legacy, use bootstrap)
│   │   ├── bootstrap_workspace.py   #   Post-deploy setup: Lakebase, grants, seed data, PA queue
│   │   └── evaluate_agents.py       #   Agent evaluation
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
│   │   ├── prior_auth/              #   bronze.sql, silver.sql, gold.sql (PA requests + policies)
│   │   ├── network/                 #   bronze.sql, silver.sql, gold.sql (H3 geo, CMS compliance, ghost, leakage)
│   │   ├── adt/                     #   bronze.sql, silver.sql, gold.sql (ADT events, alerts, readmission risk)
│   │   ├── care_management/         #   bronze.sql, silver.sql, gold.sql (programs, episodes, SDOH, care gaps)
│   │   ├── gold_analytics/           #   financial, quality, risk, ai, actuarial, groups,
│   │   │                             #   cost_of_care, member_360, group_report_card, fwa_analytics
│   │   └── python/                   #   Python alternatives for all pipelines
│   ├── dashboards/                   #   Lakeview dashboard JSON definitions (6 dashboards)
│   ├── lakebase_schema.sql           #   Command Center Lakebase DDL (alerts, care managers)
│   ├── fwa_lakebase_schema.sql       #   FWA Lakebase DDL (investigations, audit log, evidence)
│   ├── pa_reviews_lakebase_schema.sql#   PA Lakebase DDL (review queue, reviewers, audit trail)
│   ├── underwriting_sim_lakebase_schema.sql # UW Sim Lakebase DDL (simulations, comparisons)
│   └── agents/                       #   Agent model definitions (Care Intel v1/v2, Sales Coach, FWA, PA)
├── config/                           #   Genie setup, Lakebase config
└── README.md
```

## Deployment

### Prerequisites

- Databricks CLI configured with workspace profile (v0.200+)
- Unity Catalog workspace with an existing catalog (defaults to `red_bricks_insurance`, configurable via `--var="catalog=your_catalog"`)
- All workspace features listed in [Workspace Prerequisites](#workspace-prerequisites) below

### Workspace Prerequisites

This bundle exercises a broad surface area of the Databricks platform. The following features must be enabled on the target workspace for the full demo to function. Features are grouped by criticality.

#### Required Platform Features

| Feature | What Uses It | Impact If Missing |
|---------|-------------|-------------------|
| **Unity Catalog** | All 13 domain schemas, model registry, volumes, governance | Everything fails |
| **Serverless Compute** | All 12 SDP/DLT pipelines (`serverless: true`, `channel: PREVIEW`) | All medallion pipelines fail |
| **SQL Warehouse** | Dashboards, Statement Execution API, agent validation, Genie | Dashboards, apps, and agents fail |
| **Databricks Apps** | 6 FastAPI + React portal applications | All app UIs unavailable |
| **Foundation Model API** | `ai_query()` in gold SQL, all 5 agents, medical policy parsing | AI gold tables, all agents, PA portal |
| **Vector Search** | RAG retrieval for Care Intelligence v1/v2 agents | Member/clinical document search fails |
| **Model Serving (Serverless)** | FWA fraud scorer real-time endpoint | Real-time FWA scoring unavailable |
| **Lakebase (Autoscaling)** | 4 PostgreSQL databases for transactional app state | All app operational data fails |
| **Genie / AI/BI** | Natural language SQL in all 5 apps + 3 Lakeview dashboards | NL query and dashboard features |
| **MLflow (UC Model Registry)** | 5 agents + 2 ML models registered via Models from Code | All agents and ML models |
| **UC Volumes** | Raw data storage; `read_files()` ingestion in bronze layers | All data ingestion fails |
| **Lakeview Dashboards** | 4 AI/BI dashboards (Analytics, Agent Comparison, PA Operations, Network Adequacy) | Analytics visualization unavailable |

#### Required Foundation Model Endpoints

| Endpoint | Used By |
|----------|---------|
| `databricks-meta-llama-3-3-70b-instruct` | `ai_query()` in gold SQL (denial classification, actuarial insights, FWA narratives, PA summaries) |
| `databricks-llama-4-maverick` | All 5 agents, medical policy PDF parsing |
| `databricks-bge-large-en` | Vector Search managed embeddings for document RAG |

#### Version Requirements

| Requirement | Minimum | Used By |
|-------------|---------|---------|
| `ai_query()` | DBR 15.4+ | Gold analytics AI tables |
| Metric Views (`WITH METRICS`) | DBR 17.2+ | `create_metric_views.py` |
| SDP pipelines | `channel: PREVIEW` | All 12 domain pipelines |
| XGBoost | >= 2.0 | FWA + PA model training |
| Databricks CLI | v0.200+ | Bundle deploy/run |

#### Required Permissions

The deploying user (or service principal) needs:
- **Catalog owner** on the target catalog (or `CREATE SCHEMA` + `USE CATALOG`)
- **Workspace admin** (or equivalent) to create Genie spaces, manage service principals, and create Lakebase projects
- The `bootstrap_workspace` task auto-discovers app service principals and grants them `USE CATALOG`, `USE SCHEMA`, `SELECT` (all 13 schemas), `CAN_USE` (warehouse), `CAN_QUERY` (all serving endpoints), and `CAN_RUN` (Genie spaces)

#### Optional Features (graceful degradation)

| Feature | What It Enables | Without It |
|---------|----------------|------------|
| **Classic Compute** | Synthea FHIR generation (Java JAR) | Use the refresh job instead; pre-load clinical JSON to the volume |
| **Slack SDK** | Sales Coach account channel context enrichment | Group Reporting app works without enrichment |
| **Glean API** | Internal knowledge base for Sales Coach | App works without enrichment |
| **Salesforce API** | CRM account data for Sales Coach | App works without enrichment |

#### Quick Validation Checklist

Before deploying to a new workspace, confirm:

1. A Unity Catalog catalog exists (or you have `CREATE CATALOG` privileges)
2. Serverless compute is enabled for the workspace
3. At least one SQL warehouse is running
4. Foundation Model API endpoints are accessible (`databricks-meta-llama-3-3-70b-instruct`, `databricks-llama-4-maverick`, `databricks-bge-large-en`)
5. Vector Search is enabled
6. Lakebase Autoscaling is enabled
7. Databricks Apps is enabled
8. Genie / AI/BI is enabled

### Compute

All tasks run on **serverless** compute. DLT pipelines are serverless SDP. Notebook tasks auto-provision and auto-scale — no cluster configuration needed.

> **Synthea on Serverless:** The `synthea_generation` task runs the Synthea JAR via `subprocess` and requires Java 11+. Serverless environment v5+ ships OpenJDK 17, which satisfies this requirement. The notebook auto-detects the Java version at runtime and fails with a clear error if the version is too old. The 300MB Synthea JAR is cached in a Unity Catalog Volume (`/Volumes/{catalog}/raw/raw_sources/synthea_cache/`) so subsequent runs skip the GitHub download.

### Targets

| Target | Profile | Cloud | Use Case |
|--------|---------|-------|----------|
| `dev` (default) | `fe-vm-red-bricks-aws` | AWS | Primary development (FEVM Stable Serverless) |
| `dev-azure` | `fe-vm-red-bricks-insurance` | Azure | Azure development |
| `e2-field-eng` | `fe-demo-field-eng` | AWS | Field engineering demos |
| `hls-financial` | `hls-financial-foundation` | AWS | HLS Financial Foundation workspace |
| `clinical-data-demo` | `clinical-data-demo` | AWS | Clinical data demo workspace |
| `prod` | `fe-vm-red-bricks-insurance` | Azure | Production |

> **Catalog:** Defaults to `red_bricks_insurance`. Override with `--var="catalog=your_catalog_name"` at deploy/run time, or set it per-target in `databricks.yml`. The `dev` target overrides to `red_bricks_insurance_catalog`. The catalog must already exist on the workspace — the pipelines create the 13 domain schemas automatically.

### Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `catalog` | `red_bricks_insurance` | Unity Catalog name — all pipelines, jobs, and apps use this |
| `source_volume` | `/Volumes/${var.catalog}/raw/raw_sources` | Raw data volume path (auto-derived from catalog) |
| `warehouse_id` | Lookup: `Serverless Starter Warehouse` | SQL warehouse for dashboards — auto-resolves by name; override with a specific ID |
| `num_patients` | `5000` | Number of Synthea patients to generate (1000 for quick demos, 5000 for full) |
| `node_type_small` | `Standard_DS3_v2` | Small compute (4 vCPU, 14GB) — AWS: `m5.xlarge` |
| `node_type_large` | `Standard_DS5_v2` | Large compute (16 vCPU, 56GB) — AWS: `m5.4xlarge` |
| `lakebase_project_id` | `red-bricks-insurance` | Lakebase Autoscaling project ID |

### Deploying to a New Workspace

This bundle is fully portable — deploy to any workspace with any catalog name and everything auto-configures.

**1. Ensure a catalog exists** on the target workspace. The default is `red_bricks_insurance`, but you can use any existing catalog by passing `--var="catalog=your_catalog"`.

**2. Add a new target** in `databricks.yml`:

```yaml
  my-workspace:
    mode: development
    workspace:
      profile: my-workspace-profile    # Databricks CLI profile name
    variables:
      catalog: my_catalog_name         # Must already exist on the workspace
      node_type_small: m5.xlarge       # AWS — or Standard_DS3_v2 for Azure
      node_type_large: m5.4xlarge      # AWS — or Standard_DS5_v2 for Azure
```

> **Warehouse auto-detection:** The `warehouse_id` variable uses a `lookup` block that resolves by warehouse name (`Serverless Starter Warehouse`). If your workspace uses a different warehouse name, override with `--var="warehouse_id=your_id"` or change the lookup name in `databricks.yml`.

**3. Two-phase deploy** (required for fresh workspaces):

On a fresh workspace, Lakebase instances and app database resources create a chicken-and-egg problem: apps reference databases that don't exist yet (databases are created by the `setup_lakebase` job task, but Terraform tries to create apps in the same apply as the instances). The solution is a two-phase deploy:

```bash
# Phase 1: Comment out `database` resources in app YAMLs, then deploy
# (Terraform creates Lakebase instances + apps without DB references)
databricks bundle deploy --target my-workspace --var="catalog=my_catalog"

# Run the job — setup_lakebase creates databases inside the new instances
databricks bundle run red_bricks_refresh --target my-workspace --var="catalog=my_catalog"
# Wait for setup_lakebase task to succeed, then cancel the run

# Phase 2: Uncomment `database` resources in app YAMLs, redeploy
# (Terraform adds DB resources + security labels to apps)
databricks bundle deploy --target my-workspace --var="catalog=my_catalog"

# Now run the full pipeline
databricks bundle run red_bricks_refresh --target my-workspace --var="catalog=my_catalog"
```

> **Subsequent deploys** don't need two phases — the databases already exist from the first run.

**4. Dashboard catalog replacement** — Lakeview dashboard JSON files hardcode SQL catalog references and don't support `${var.catalog}` interpolation. Use `prepare.sh` before deploying to a non-default catalog:

```bash
./prepare.sh my_catalog_name    # replaces red_bricks_insurance in dashboard JSONs
databricks bundle deploy --target my-workspace --var="catalog=my_catalog_name"
```

> Running `./prepare.sh` with no arguments (or `./prepare.sh red_bricks_insurance`) is a no-op.

**5. Pipeline automation** — the job runs end-to-end and automatically:
- Creates Lakebase databases + DDL schemas (`setup_lakebase` task)
- Generates synthetic data and runs all domain pipelines
- Trains the FWA fraud scoring model and PA auto-adjudication model
- Runs PA model governance (bias monitoring, drift detection)
- Parses medical policy PDFs with LLM extraction
- Deploys AI agents (Care Intelligence v1/v2, Sales Coach, FWA Investigation, PA Review)
- Creates Genie spaces, grants UC permissions, seeds operational data (`bootstrap_workspace`)
- Deploys app source code and starts compute (`deploy_app_source`)

Once the pipeline completes, all six apps are fully functional with no manual configuration.

**4. Runtime auto-detection** — apps self-configure at startup:

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

**5. Configuration flow:**

| Layer | How Values Are Set | Example |
|-------|-------------------|---------|
| **Catalog** | DAB variable `${var.catalog}`, defaults to `red_bricks_insurance` | `--var="catalog=my_catalog"` |
| **DAB variables** (`databricks.yml`) | Set per target in `variables:` or via CLI `--var` | `warehouse_id: abc123` |
| **Job parameters** (`base_parameters`) | Injected from DAB variables | `catalog: ${var.catalog}` |
| **App env vars** (`app.yml`) | Catalog from DAB variable; warehouse auto-detected if empty | `UC_CATALOG: ${var.catalog}` |
| **Bootstrap task** | Auto-detects warehouse, discovers app SPs, creates Genie spaces, restarts apps | Fully dynamic |

**6. Key environment variables** used by apps:

| Variable | Used By | Auto-Detected? |
|----------|---------|----------------|
| `SQL_WAREHOUSE_ID` | All apps | Yes — first running warehouse |
| `UC_CATALOG` | All apps | Set from `${var.catalog}`; auto-detected at runtime as fallback |
| `GENIE_SPACE_ID` | Command Center | Yes — first visible Genie space |
| `LLM_ENDPOINT` | All apps | No — defaults to `databricks-llama-4-maverick` |
| `LAKEBASE_PROJECT_ID` | Command Center, FWA, UW Sim apps | No — defaults to `red-bricks-insurance` |
| `LAKEBASE_DATABASE_NAME` | Command Center, FWA, UW Sim apps | No — set in `app.yml` per app |
| `FWA_MODEL_ENDPOINT` | FWA app | No — defaults to `fwa-fraud-scorer` |

### Commands

```bash
# Validate bundle
databricks bundle validate --target e2-field-eng

# Deploy all resources (pipelines, jobs, dashboards, apps, Lakebase instances)
databricks bundle deploy --target e2-field-eng

# Deploy with a custom catalog name
databricks bundle deploy --target hls-financial --var="catalog=hls_financial_foundation_catalog"

# --- End-to-end demo (synthea → data gen → all pipelines → agents → eval → app deploy) ---
# NOTE: Requires classic compute for Synthea. Use refresh job on serverless-only workspaces.
databricks bundle run red_bricks_full_demo --target e2-field-eng

# --- Refresh without Synthea (data gen → pipelines → analytics → agents → app deploy) ---
databricks bundle run red_bricks_refresh --target hls-financial --var="catalog=hls_financial_foundation_catalog"

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
databricks bundle run prior_auth_pipeline          # Just prior auth domain (PA requests, policies)
databricks bundle run network_pipeline             # Just network adequacy (H3 geo, CMS compliance, ghost, leakage)
```

## Apps — Frontend Build

All six apps (`app/` Command Center, `app-group-reporting/` Group Reporting Portal, `app-fwa/` FWA Investigation Portal, `app-underwriting-sim/` Underwriting Simulation Portal, `app-prior-auth/` PA Portal, `app-network-adequacy/` Network Adequacy Portal) use React + Vite + Tailwind. **Frontends must be built before deploying the bundle** — the DAB deploys the pre-built `static/` directory, not the source.

```bash
# Command Center
cd app/frontend && npm install && npm run build   # → outputs to app/static/

# Group Reporting Portal
cd app-group-reporting/frontend && npm install && npm run build   # → outputs to app-group-reporting/static/

# FWA Investigation Portal
cd app-fwa/frontend && npm install && npm run build   # → outputs to app-fwa/static/

# Underwriting Simulation Portal
cd app-underwriting-sim/frontend && npm install && npm run build   # → outputs to app-underwriting-sim/static/

# Prior Authorization Portal
cd app-prior-auth/frontend && npm install && npm run build   # → outputs to app-prior-auth/static/

# Network Adequacy Portal
cd app-network-adequacy/frontend && npm install && npm run build   # → outputs to app-network-adequacy/static/
```

The `.bundleignore` excludes `node_modules/`, `src/`, and other frontend build artifacts from the bundle upload. Only the `static/` directories are deployed.

After building, deploy the bundle normally with `databricks bundle deploy`.

## Workspace Bootstrap — Post-Deploy Setup

The `bootstrap_workspace` task runs automatically at the end of both the full demo and refresh jobs. It handles all post-deploy provisioning and is fully **idempotent** — safe to re-run at any time.

1. **Lakebase setup** — Creates the `red-bricks-insurance` Autoscaling project with 3 databases: `red_bricks_alerts` (Command Center), `fwa_cases` (FWA Portal), and `uw_sim` (Underwriting Sim). Runs DDL schemas and grants PUBLIC access (skips if already exist)
2. **Staff seeding** — Inserts care managers, fraud investigators, and PA reviewers (`ON CONFLICT DO NOTHING`)
3. **App service principal discovery** — Auto-discovers SPs for all deployed apps matching `red-bricks-*` or `rb-*` name patterns, resolving each SP's `service_principal_client_id` (UUID) for use in all subsequent grants
4. **Unity Catalog grants** — `USE CATALOG`, `BROWSE`, `USE SCHEMA`, `SELECT` on all 13 domain schemas for each app SP (using UUID)
5. **SQL Warehouse grants** — `CAN_USE` on the auto-detected (or configured) warehouse for each app SP
6. **Serving endpoint grants** — `CAN_QUERY` on all model serving endpoints (LLM, embedding, FWA scorer) for each app SP
7. **Vector search endpoint grants** — `CAN_USE` on the vector search endpoint (resolves endpoint UUID dynamically for Azure compatibility)
8. **Genie spaces** — Creates 5 Genie spaces (Analytics Assistant, FWA Analytics, Group Reporting, Financial Analytics, Network Analytics) with catalog table references. Validates tables exist before adding. Grants `CAN_RUN` to all app SPs. Skips spaces that already exist (matched by title).
9. **ML predictions table** — Pre-creates `analytics.fwa_ml_predictions` for gold MV compatibility
10. **Operational data seeding** — Populates Lakebase with risk alerts (from gold tables), FWA investigation cases (from silver/gold FWA tables), and PA review queue entries (from PA gold tables with reviewer assignments)
11. **App source code deployment** — Deploys source code to each of the 6 apps and restarts them so they pick up all grants and Lakebase connectivity

To run manually (e.g., after a fresh deploy without running the full job):

```bash
databricks jobs submit --json '{
  "run_name": "Bootstrap Workspace",
  "tasks": [{
    "task_key": "bootstrap",
    "notebook_task": {
      "notebook_path": "/Users/<your-email>/.bundle/red-bricks-insurance/dev/files/src/notebooks/bootstrap_workspace",
      "base_parameters": {"warehouse_id": "<your-warehouse-id>"}
    },
    "environment_key": "bootstrap_env"
  }],
  "environments": [{
    "environment_key": "bootstrap_env",
    "spec": {"client": "1", "dependencies": ["psycopg[binary]", "databricks-sdk>=0.30.0"]}
  }]
}'
```

### 3. ML Model Training

**FWA Fraud Scorer** — Two training notebooks are available:

| Notebook | Approach | Runtime |
|----------|----------|---------|
| `train_fwa_model.py` | XGBoost with manual CV + hyperparameter tuning | Serverless (ml_training_env) |
| `train_fwa_model_automl.py` | Databricks AutoML | Classic ML Runtime cluster |

The full demo job uses `train_fwa_model.py` (XGBoost) by default.

**Note:** Both FWA notebooks cast all feature columns to `float64` before inference (`inference_pd[feature_cols].astype("float64")`). This is required because MLflow's schema enforcement rejects integer columns (e.g., `member_total_claims` as int64) when the model signature specifies double.

**PA Auto-Adjudication Model** — `train_pa_model.py` trains a 3-class XGBoost classifier (approve/deny/review) for Tier 2 ML-based PA determinations. Features include service type, urgency, LOB, clinical indicators, and turnaround metrics. The model is registered in Unity Catalog as `{catalog}.prior_auth.pa_adjudication_model`. SHAP explainability is attempted with fallback to XGBoost native feature importance (XGBoost 2.x compatibility). Followed by `pa_model_governance.py` which runs bias monitoring (disparate impact ratios by LOB, urgency, service type), drift detection (PSI + KS test), and logs all governance checks to MLflow.

### 4. App Environment Variables

Set in `resources/app_fwa.yml`:

| Variable | Description |
|----------|-------------|
| `LAKEBASE_PROJECT_ID` | `red-bricks-insurance` |
| `LAKEBASE_DATABASE_NAME` | `fwa_cases` |
| `SQL_WAREHOUSE_ID` | Serverless SQL warehouse ID for Statement Execution API |
| `LLM_ENDPOINT` | Foundation Model API endpoint (e.g., `databricks-llama-4-maverick`) |
| `UC_CATALOG` | Hardcoded to `red_bricks_insurance` |
| `FWA_MODEL_ENDPOINT` | Model serving endpoint for real-time scoring (optional) |
| `GENIE_SPACE_ID` | Genie space ID for natural language SQL queries |

## Lakebase & App Authentication

A single **Lakebase Autoscaling** project (`red-bricks-insurance`) hosts all app databases. The project is managed by the `setup_lakebase` notebook (not DAB resources) because DAB does not yet support native Autoscaling project resources. The `resources/lakebase_instances.yml` file documents this.

| Database | Used By | Status |
|----------|---------|--------|
| `red_bricks_alerts` | Command Center app (alerts, care managers, care management data) | Active |
| `fwa_cases` | FWA Investigation Portal | Active |
| `uw_sim` | Underwriting Simulation Portal | Active |
| `pa_reviews` | Prior Authorization Portal | Active |

The Autoscaling project scales to zero when idle and wakes automatically on connection. The `setup_lakebase` job task creates databases, runs DDL, and grants PUBLIC access. It runs as part of both the full demo and refresh jobs.

### How Security Labels Work

Lakebase uses **security labels** to map a Databricks identity (user email or SP UUID) to a PostgreSQL role. Without a security label, OAuth authentication fails with: `"no role security label was configured in postgres for role"`.

Security labels are provisioned by declaring a `database` resource in the DAB app YAML with `CAN_CONNECT_AND_CREATE`. This automatically creates the PostgreSQL role and security label for the app's SP. See `resources/app.yml`, `resources/app_fwa.yml`, `resources/app_underwriting_sim.yml`, and `resources/app_prior_auth.yml`.

### Lifecycle & Deploy Order

On a **fresh workspace**, there's a chicken-and-egg problem:
- Terraform creates Lakebase instances + apps in the same apply
- Apps reference databases inside those instances
- But databases are created by `setup_lakebase` (a job task that runs after deploy)

This requires a **two-phase deploy** — see [Deploying to a New Workspace](#deploying-to-a-new-workspace) for the full procedure. On subsequent deploys, the databases already exist so a single `bundle deploy` works.

### Token Refresh

OAuth tokens expire after 1 hour. All Lakebase-connected apps implement a background refresh loop (every 50 minutes) using SQLAlchemy's `do_connect` event to inject fresh tokens. See `app/backend/database.py`, `app-fwa/backend/database.py`, and `app-underwriting-sim/backend/database.py`. The PA app (`app-prior-auth/backend/database.py`) uses the same Autoscaling pattern.

## Deployment Notes & Known Issues

### Clinical Tables Required for Gold Analytics

The `gold_analytics_pipeline` depends on `clinical.silver_lab_results` and other clinical tables. These are created by `parse_fhir_with_dbignite` → `clinical_pipeline`, which are part of the **Full Demo Pipeline** but not the Refresh job.

On a fresh workspace (or after `bundle destroy`), you must run these clinical tasks before the Refresh job will succeed:
1. Run the **Full Demo Pipeline** (which includes Synthea generation + FHIR parsing), or
2. Copy existing Synthea FHIR data to the volume and run `parse_fhir_with_dbignite` + `clinical_pipeline` manually
3. Then the Refresh job will work for all subsequent runs

### Serverless Synthea Generation

Synthea now runs fully on serverless compute (environment v5+, Java 17). The notebook includes:
- **UC Volume JAR caching** — downloads the 300MB JAR from GitHub once, caches in `/Volumes/{catalog}/raw/raw_sources/synthea_cache/`. Subsequent runs copy from cache (~2s vs ~60s download).
- **Robust Java version detection** — handles both legacy `1.8.x` and modern `17.x` version formats.
- **JVM tuning** — `-Xms2g -Xmx4g -XX:+UseG1GC` for optimal serverless performance.
- **Lab enrichment** — forces Metabolic Panel, Lipid Panel, and CBC modules for richer lab data with 10-year patient history.
- **dbignite patch** — `parse_fhir_with_dbignite.py` monkey-patches a bug in dbignite where `warnings.warn("Found " + count + ...)` concatenates `str` + `int`.

### `bundle destroy` Creates New Service Principals

Destroying and redeploying the bundle assigns **new** SP UUIDs to each app. The `bootstrap_workspace` task handles this dynamically by discovering current SPs at runtime. However, any manually applied Lakebase security labels from a prior deployment will reference stale UUIDs.

### SP Grants Use UUIDs (Not Display Names)

All grants in `bootstrap_workspace` use the SP's `service_principal_client_id` (UUID / `application_id`), not the display name. This is required for:
- SQL `GRANT` statements (`GRANT USE CATALOG ... TO '<uuid>'`)
- REST API permissions (warehouse, serving endpoints, vector search)

On Azure, the vector search endpoint permissions API requires the **endpoint UUID** (not name) in the URL path. Bootstrap dynamically resolves this via the VS endpoint API.

### `AutoCaptureConfigInput` Is Deprecated

The `AutoCaptureConfigInput` parameter for serving endpoint creation (legacy inference tables) now **blocks** the API call entirely. `train_fwa_model.py` creates the `fwa-fraud-scorer` endpoint without it and includes retry logic for transient failures.

### Model Version Registration Delay

After `mlflow.register_model()`, the model version may be in `PENDING_REGISTRATION` state. `train_fwa_model.py` polls for up to 5 minutes for the version to reach `READY` before creating the serving endpoint.

## Observability & Tracing

The Command Center app is instrumented with **MLflow 3 tracing** (OpenTelemetry-native) and **FastAPI auto-instrumentation** for production-grade observability.

### MLflow 3 Tracing

All agent interactions, Genie queries, and tool invocations are traced with `@mlflow.trace` decorators:

| Component | Traced Operations | Experiment |
|-----------|-------------------|------------|
| **Care Intelligence Agent** | End-to-end agent calls, SQL queries, Vector Search retrieval, LLM calls | `/Shared/red-bricks-insurance/agent-traces` |
| **Genie Integration** | Question submission, SQL generation, result retrieval, conversation flow | Same experiment |
| **UC Tool Calls** | Function invocations, SDK API requests, parameter/response logging | Same experiment |
| **LangGraph Nodes** | Route, dispatch, merge, and specialist agent nodes | Same experiment |

Traces are written to Unity Catalog Delta tables via MLflow's OpenTelemetry backend. View traces in the MLflow Experiment UI at `/Shared/red-bricks-insurance/agent-traces`.

### FastAPI OpenTelemetry

The FastAPI app is auto-instrumented with `opentelemetry-instrumentation-fastapi`, which captures:
- Request/response metadata for every API endpoint
- HTTP method, status code, route, and latency
- Correlation with MLflow trace spans for end-to-end visibility

### Agent Evaluation

The `evaluate_care_agent.py` notebook runs **MLflow GenAI evaluation** (`mlflow.genai.evaluate()`) with 6 scorers:

| Scorer | Type | What It Measures |
|--------|------|-----------------|
| `RetrievalGroundedness` | Built-in | Are claims supported by retrieved context? |
| `RelevanceToQuery` | Built-in | Does the response address the care manager's question? |
| `Safety` | Built-in | No harmful, biased, or inappropriate content? |
| `clinical_completeness` | Custom `Guidelines` | Covers diagnoses, risk factors, medications, care gaps, follow-up actions? |
| `actionability` | Custom `Guidelines` | At least 3 concrete, specific actions for a care manager? |
| `hipaa_compliance` | Custom `Guidelines` | PHI handled professionally in a need-to-know format? |

Results are persisted to `{catalog}.analytics.care_agent_eval_results` for dashboard consumption and tracked in the MLflow experiment for version comparison.

## Customization

This demo is designed to be modular for customer-specific showings:

- **Remove a domain**: Delete its pipeline YAML from `resources/` and its task from `full_demo_job.yml`
- **Add a gold metric**: Add a new `CREATE OR REFRESH MATERIALIZED VIEW` to the appropriate SQL file in `gold_analytics/`
- **Switch to Python**: Update library paths in `resources/pipeline_*.yml` to point to `python/` files
- **Change AI model**: Update the model name in `ai_classification.sql`
- **Scale data**: Set `--var="num_patients=1000"` (or any count) at run time, or change the default in `databricks.yml`. Also adjust record counts in `run_data_generation.py` if needed
- **Different geography**: Change `STATE` in `run_synthea_generation.py` (Synthea supports all US states)
- **Different catalog name**: Pass `--var="catalog=your_catalog"` at deploy and run time. Run `./prepare.sh your_catalog` first to update dashboard JSONs

## Required Packages

| Package | Used By |
|---------|---------|
| `faker` | Insurance domain data generation |
| `fpdf2` | Document PDF generation |
| `dbignite` | FHIR R4 bundle parsing (installed at runtime) |
| `mlflow` | Agent registration and evaluation |
| `databricks-sdk` | Agent deployment, API calls |
| `databricks-automl-runtime` | FWA fraud scorer model training (AutoML) |
| `xgboost` / `scikit-learn` | FWA fraud scorer + PA auto-adjudication model training |
| `fastapi` / `uvicorn` | App backends (Command Center, Group Reporting, FWA Portal, PA Portal, UW Sim, Network Adequacy) |
| `psycopg` | Lakebase PostgreSQL connections (Command Center, FWA Portal, PA Portal, UW Sim) |
| `opentelemetry-instrumentation-fastapi` | FastAPI auto-instrumentation for request tracing |
| `langgraph` | LangGraph state machine agent framework (Command Center) |
| `slack_sdk` | (Optional) Sales Coach Slack enrichment |
| `simple_salesforce` | (Optional) Sales Coach Salesforce enrichment |
