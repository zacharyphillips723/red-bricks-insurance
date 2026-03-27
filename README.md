# Red Bricks Insurance

Healthcare insurance company simulation — modular Databricks Asset Bundle (DAB). One deployable bundle that runs end-to-end: Synthea clinical generation → synthetic insurance data → bronze/silver/gold SDP pipelines → cross-domain analytics with AI classification → intelligent agents → Command Center app.

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
│                         │  Benefits, Documents, Underwriting, Risk Adjustment
└───────────┬─────────────┘
            │
     ┌──────┴───────┐  (9 domain pipelines run in parallel)
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
                         │  Financial, Quality,│
                         │  Risk, AI, Actuarial│
                         │  Member 360         │
                         └──────────┬──────────┘
                                    │
                    ┌───────────────┼───────────────┐
                    ▼               ▼               ▼
             ┌────────────┐ ┌────────────┐ ┌──────────────┐
             │ Dashboards │ │  Genie     │ │ AI Agents    │
             │ (AI/BI)    │ │  Space     │ │ v1 + v2      │
             └────────────┘ └────────────┘ └──────┬───────┘
                                                  │
                                           ┌──────▼───────┐
                                           │  Command     │
                                           │  Center App  │
                                           │  (React+API) │
                                           └──────────────┘
```

## Pipeline DAG

The full demo job (`red_bricks_full_demo`) orchestrates 18 tasks:

```
synthea_generation (ROOT — generates FHIR bundles + extracts demographics + assigns MBR IDs)
  → data_generation (reads Synthea demographics, generates insurance domains only)
      → [members, providers, claims, enrollment, benefits, underwriting,
         documents, risk_adjustment pipelines]
      → parse_fhir_with_dbignite (reads raw synthea_raw/fhir/, writes crosswalk Delta tables)
          → clinical_pipeline (bronze.sql JOINs crosswalk for MBR IDs + NPIs)
  → build_member_months (depends on members_pipeline)
  → gold_analytics_pipeline (depends on all domain pipelines + member months)
      → create_metric_views (governed semantic layer)
  → setup_vector_search (depends on documents_pipeline)
      → deploy_member_agent (v1)
      → deploy_agent_v2 (v2 with benefits)
          → evaluate_agents (v1 vs v2 comparison)
```

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

**Data quality**: ~2% intentional defects (nulls, invalid codes, out-of-range dates) caught by SDP expectations at the silver layer.

## SDP Pipelines (Medallion Architecture)

Each domain has its own SDP pipeline with bronze → silver → gold tables:

| Layer | What It Does | Key Features |
|-------|-------------|--------------|
| **Bronze** | Raw ingestion from UC Volume via `read_files()` | Streaming tables, source lineage metadata |
| **Silver** | Cleansed, typed, validated | DQ expectations (DROP ROW for critical, track for soft), date casting, computed columns |
| **Gold** | Domain-level aggregates | Summary views per domain |
| **Gold Analytics** | Cross-domain KPIs | Financial, quality, risk metrics + AI classification |

### Gold Analytics Tables

**Financial Metrics:** `gold_pmpm` (PMPM by LOB), `gold_mlr` (Medical Loss Ratio), `gold_ibnr_estimate` (payment lag / completion factors)

**Quality Metrics:** `gold_hedis_member` (HEDIS proxies), `gold_hedis_provider` (compliance rates), `gold_stars_provider` (CMS Stars-like composite)

**Risk Adjustment:** `gold_risk_adjustment_analysis` (RAF distributions, HCC counts, MA revenue), `gold_coding_completeness` (HCC coding gap detection)

**AI-Powered (via `ai_query()`):** `gold_denial_classification` (LLM-classified denial reasons), `gold_denial_analysis` (denial trends by AI category), `gold_member_risk_narrative` (AI-generated clinical summaries)

**Actuarial:** `gold_utilization_per_1000` (utilization benchmarks), `gold_ibnr_triangle` (chain-ladder development), `gold_ibnr_completion_factors`, `gold_mlr_ai_insights` (LLM-generated actuarial recommendations)

**Group Reporting:** `gold_group_experience` (claims PMPM, utilization, loss ratio by employer group), `gold_group_stop_loss` (specific & aggregate stop-loss tracking), `gold_group_renewal` (credibility-weighted renewal pricing)

**Cost of Care:** `gold_member_tcoc` (member-level Total Cost of Care and Total Cost Index), `gold_tcoc_summary` (LOB-level TCOC distributions, cost tier breakdowns, spend concentration)

**Member 360:** `gold_member_360` (unified member view joining clinical, claims, enrollment, risk)

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

### Clinical Pipeline (Synthea → dbignite → SDP)

The clinical pipeline reads Synthea FHIR R4 bundles directly (no intermediate transformation):

1. **`parse_fhir_with_dbignite`** — Reads `synthea_raw/fhir/*.json`, uses dbignite to write `Patient`, `Encounter`, `Condition`, `Observation` Delta tables. Also writes `synthea_crosswalk` and `synthea_practitioner_crosswalk` Delta tables.
2. **`bronze.sql`** — Flattens FHIR structs, LEFT JOINs crosswalk tables to resolve Synthea UUIDs → MBR IDs and practitioner UUIDs → provider NPIs.
3. **`silver.sql`** / **`gold.sql`** — Standard cleansing and aggregation.

## AI Agents

Two versions of the Care Intelligence Agent are deployed and evaluated:

| Agent | Description | Tools |
|-------|-------------|-------|
| **v1** (`deploy_member_agent`) | Member lookup + document search | SQL queries, vector search |
| **v2** (`deploy_agent_v2`) | v1 + benefits coverage analysis | SQL queries, vector search, benefits lookup |

Both are registered in Unity Catalog via MLflow and evaluated with `evaluate_agents.py`.

## Databricks App — Command Center

A full-stack application in `app/` providing a unified interface:

- **Backend**: FastAPI (Python), connects to Lakebase, SQL warehouse, and serving endpoints
- **Frontend**: React with modern UI
- **Features**: Member search, claims analysis, Genie-powered natural language queries, agent chat
- **Config**: `app/app.yml` (environment variables for catalog, schema, Genie Space ID, warehouse)

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
│   └── static/                       #   React frontend build
├── resources/
│   ├── full_demo_job.yml             # End-to-end orchestration (18 tasks)
│   ├── data_generation_job.yml       # Standalone data generation
│   ├── dashboard.yml                 # Analytics dashboard
│   ├── agent_comparison_dashboard.yml# Agent eval dashboard
│   ├── pipeline_members.yml          # Members & Enrollment SDP
│   ├── pipeline_providers.yml        # Providers SDP
│   ├── pipeline_claims.yml           # Claims SDP
│   ├── pipeline_clinical.yml         # Clinical SDP (Synthea → dbignite)
│   ├── pipeline_benefits.yml         # Benefits SDP
│   ├── pipeline_documents.yml        # Documents SDP
│   ├── pipeline_underwriting.yml     # Underwriting SDP
│   ├── pipeline_risk_adjustment.yml  # Risk Adjustment SDP
│   └── pipeline_gold_analytics.yml   # Cross-domain analytics (8 SQL files, 15+ gold views)
├── src/
│   ├── data_generation/              # Modular synthetic data generators
│   │   ├── reference_data.py         #   ICD-10, CPT, DRG, HCC, CARC, LOB configs
│   │   ├── dq.py                     #   ~2% defect injection
│   │   ├── helpers.py                #   NPI generation, date utils, payment lag
│   │   └── domains/                  #   One generator per domain
│   │       ├── members.py            #     Demographics (Synthea-backed or Faker fallback)
│   │       ├── enrollment.py         #     Plans, LOB (age-consistent when Synthea)
│   │       ├── claims.py             #     Medical + pharmacy claims
│   │       ├── providers.py          #     Provider directory
│   │       ├── benefits.py           #     Plan benefit schedules
│   │       ├── groups.py             #     Employer groups (stop-loss, funding, renewal)
│   │       ├── documents.py          #     Case notes, call transcripts, claims summaries
│   │       ├── underwriting.py       #     Risk assessment
│   │       └── risk_adjustment.py    #     RAF scores, HCC codes
│   ├── notebooks/
│   │   ├── run_synthea_generation.py #   Synthea JAR → FHIR bundles → demographic crosswalk
│   │   ├── run_data_generation.py    #   Insurance domain generation (reads Synthea demographics)
│   │   ├── parse_fhir_with_dbignite.py # FHIR → Delta tables + crosswalk tables
│   │   ├── build_member_months.py    #   Member month enrollment spans
│   │   ├── create_metric_views.py    #   Governed semantic layer (DBR 17.2+)
│   │   ├── setup_vector_search.py    #   Document vector index for RAG
│   │   ├── deploy_member_agent.py    #   Agent v1 registration
│   │   ├── deploy_agent_v2.py        #   Agent v2 registration
│   │   └── evaluate_agents.py        #   v1 vs v2 evaluation
│   ├── pipelines/
│   │   ├── members/                  #   bronze.sql, silver.sql, gold.sql
│   │   ├── providers/
│   │   ├── claims/
│   │   ├── clinical/                 #   bronze.sql (with crosswalk JOINs), silver.sql, gold.sql
│   │   ├── benefits/
│   │   ├── documents/                #   bronze.sql, silver.sql
│   │   ├── underwriting/
│   │   ├── risk_adjustment/
│   │   ├── gold_analytics/           #   financial, quality, risk, ai, actuarial, groups, cost_of_care, member_360
│   │   └── python/                   #   Python alternatives for all pipelines
│   ├── dashboards/                   #   Lakeview dashboard JSON definitions
│   └── agents/                       #   Agent tool definitions
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
| `dev` (default) | `fe-vm-red-bricks-insurance` | `catalog_insurance_vpx9o6` | Development |
| `e2-field-eng` | `fe-demo-field-eng` | `zack_phillips_demos` | Field engineering demos (AWS) |
| `prod` | `fe-vm-red-bricks-insurance` | `catalog_insurance_vpx9o6` | Production |

### Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `catalog` | `main` | Unity Catalog catalog |
| `schema` | `red_bricks_insurance` | Target schema (dev: `_dev` suffix) |
| `source_volume` | `/Volumes/{catalog}/{schema}/raw_sources` | Raw data volume |
| `warehouse_id` | `""` | SQL warehouse (optional) |
| `node_type_small` | `Standard_DS3_v2` | Small compute (4 vCPU, 14GB) |
| `node_type_large` | `Standard_DS5_v2` | Large compute (16 vCPU, 56GB) |

### Commands

```bash
# Validate bundle
databricks bundle validate --target e2-field-eng

# Deploy all resources (pipelines, jobs, dashboards, app)
databricks bundle deploy --target e2-field-eng --force

# --- End-to-end demo (synthea → data gen → all pipelines → agents → eval) ---
databricks bundle run red_bricks_full_demo --target e2-field-eng

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
```

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
