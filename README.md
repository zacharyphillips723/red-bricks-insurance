# Red Bricks Insurance

Healthcare insurance company simulation — modular Databricks Asset Bundle (DAB). One deployable bundle that can run end-to-end or as individual domain modules: data generation → bronze/silver/gold SDP pipelines → cross-domain analytics with AI classification.

## Architecture

```
┌──────────────────────┐
│   Data Generation    │  Synthetic data → UC Volume (raw_sources/)
│  (run_data_generation)│
└──────────┬───────────┘
           │
     ┌─────┴──────┐  (6 domain pipelines run in parallel)
     ▼            ▼
┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────────┐ ┌──────────────┐
│ Members  │ │Providers │ │ Claims   │ │ Clinical │ │ Underwriting │ │Risk Adjustment│
│& Enroll. │ │          │ │Med + Rx  │ │Enc/Lab/Vit│ │             │ │ Member+Prov  │
│ B → S → G│ │ B → S → G│ │ B → S → G│ │ B → S → G│ │ B → S → G  │ │ B → S → G   │
└─────┬────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘ └─────┬───────┘ └──────┬───────┘
      │           │            │            │              │               │
      └───────────┴────────────┴────────────┴──────────────┴───────────────┘
                                    │
                         ┌──────────▼──────────┐
                         │  Gold Analytics     │  Cross-domain metrics
                         │  ┌─────────────────┐│
                         │  │ Financial:      ││
                         │  │ PMPM, MLR, IBNR ││
                         │  ├─────────────────┤│
                         │  │ Quality:        ││
                         │  │ HEDIS, Stars    ││
                         │  ├─────────────────┤│
                         │  │ Risk:           ││
                         │  │ RAF Analysis,   ││
                         │  │ Coding Gaps     ││
                         │  ├─────────────────┤│
                         │  │ AI-Powered:     ││
                         │  │ Denial Classif.,││
                         │  │ Risk Narratives ││
                         │  └─────────────────┘│
                         └─────────────────────┘
```

## Data Domains

| Domain | Records | Format | Description |
|--------|---------|--------|-------------|
| **Claims** | 40K medical + 15K pharmacy | Parquet | IP/OP/professional/ER; ICD-10/CPT, revenue codes, CARC denial codes, financials |
| **Clinical** | 6K encounters + 12K labs + 10K vitals | JSON | Diagnosis-correlated labs; dbignite-ready |
| **Members** | 5,000 | Parquet | Demographics (name, DOB, gender, address, NC counties) |
| **Enrollment** | 5,000 | Parquet | LOB (Commercial/MA/Medicaid/ACA), plan, premium, risk_score |
| **Underwriting** | 5,000 | Parquet | Risk tier, smoker, BMI, occupation; correlated to risk_score |
| **Providers** | 500 | Parquet | NPI, specialty, network status, group practice |
| **Risk Adjustment** | 5K member + 2.5K provider | Parquet | RAF scores, HCC codes, provider attribution |

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

**Financial Metrics:**
- `gold_pmpm` — Per Member Per Month by LOB and service month
- `gold_mlr` — Medical Loss Ratio (medical + pharmacy vs premiums) with ACA targets
- `gold_ibnr_estimate` — Payment lag distribution and completion factors

**Quality Metrics:**
- `gold_hedis_member` — Simplified HEDIS proxies (diabetes HbA1c, breast/colorectal screening, preventive visits)
- `gold_hedis_provider` — HEDIS compliance rates by provider and specialty
- `gold_stars_provider` — CMS Stars-like 1-5 composite rating per provider

**Risk Adjustment:**
- `gold_risk_adjustment_analysis` — RAF distributions, HCC counts, estimated MA revenue
- `gold_coding_completeness` — HCC coding gap detection (diagnosis → expected HCC)

**AI-Powered (via `ai_query()`):**
- `gold_denial_classification` — LLM-classified denial reasons (Administrative/Clinical/Eligibility/Financial)
- `gold_denial_analysis` — Denial trends by AI category, claim type, LOB
- `gold_member_risk_narrative` — AI-generated clinical summaries for top 500 high-risk members

### SQL vs Python

All pipelines are implemented in both SQL (primary) and Python:
- **SQL**: `src/pipelines/{domain}/bronze.sql`, `silver.sql`, `gold.sql`
- **Python**: `src/pipelines/python/{domain}_pipeline.py`

Pipeline YAML configs point to SQL by default. Swap to Python by changing the library paths in `resources/pipeline_*.yml`.

## Project Structure

```
red-bricks-insurance/
├── databricks.yml                # Bundle config, variables, targets (dev/prod)
├── resources/
│   ├── data_generation_job.yml   # Standalone data generation job
│   ├── pipeline_claims.yml       # Claims SDP pipeline
│   ├── pipeline_clinical.yml     # Clinical SDP pipeline
│   ├── pipeline_members.yml      # Members & Enrollment SDP pipeline
│   ├── pipeline_providers.yml    # Providers SDP pipeline
│   ├── pipeline_risk_adjustment.yml
│   ├── pipeline_underwriting.yml
│   ├── pipeline_gold_analytics.yml  # Cross-domain analytics
│   └── full_demo_job.yml         # End-to-end orchestration job
├── src/
│   ├── data_generation/          # Modular synthetic data generators
│   │   ├── reference_data.py     # ICD-10, CPT, DRG, HCC, CARC codes
│   │   ├── dq.py                 # ~2% defect injection
│   │   ├── helpers.py            # NPI generation, date utils, payment lag
│   │   └── domains/              # One generator per domain
│   ├── notebooks/
│   │   └── run_data_generation.py
│   └── pipelines/
│       ├── claims/               # bronze.sql, silver.sql, gold.sql
│       ├── clinical/             # bronze.sql, silver.sql, gold.sql
│       ├── members/              # bronze.sql, silver.sql, gold.sql
│       ├── providers/            # bronze.sql, silver.sql, gold.sql
│       ├── risk_adjustment/      # bronze.sql, silver.sql, gold.sql
│       ├── underwriting/         # bronze.sql, silver.sql, gold.sql
│       ├── gold_analytics/       # financial, quality, risk, ai_classification
│       └── python/               # Python alternatives for all pipelines
└── README.md
```

## Deployment

### Prerequisites

- Databricks CLI configured (`databricks configure` or profile)
- Unity Catalog workspace with catalog/schema permissions
- Foundation model endpoint (`databricks-meta-llama-3-3-70b-instruct`) for AI tables

### Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `catalog` | `main` | Unity Catalog catalog |
| `schema` | `red_bricks_insurance` | Target schema (dev: `_dev` suffix) |
| `source_volume` | `/Volumes/{catalog}/{schema}/raw_sources` | Raw data volume |
| `warehouse_id` | `""` | SQL warehouse (optional) |

### Commands

```bash
# Validate bundle
databricks bundle validate

# Deploy all resources (pipelines + jobs)
databricks bundle deploy

# --- End-to-end demo (data gen → all pipelines → gold analytics) ---
databricks bundle run red_bricks_full_demo

# --- Individual components ---
databricks bundle run red_bricks_data_generation   # Just generate data
databricks bundle run claims_pipeline              # Just claims bronze → gold
databricks bundle run clinical_pipeline            # Just clinical
databricks bundle run members_pipeline             # Just members & enrollment
databricks bundle run providers_pipeline           # Just providers
databricks bundle run underwriting_pipeline        # Just underwriting
databricks bundle run risk_adjustment_pipeline     # Just risk adjustment
databricks bundle run gold_analytics_pipeline      # Just cross-domain analytics
```

## Customization

This demo is designed to be modular for customer-specific showings:

- **Remove a domain**: Delete its pipeline YAML from `resources/` and its task from `full_demo_job.yml`
- **Add a gold metric**: Add a new `CREATE OR REFRESH MATERIALIZED VIEW` to the appropriate SQL file in `gold_analytics/`
- **Switch to Python**: Update library paths in `resources/pipeline_*.yml` to point to `python/` files
- **Change AI model**: Update the model name in `ai_classification.sql` (e.g., swap to `databricks-dbrx-instruct`)
- **Scale data**: Adjust record counts in `run_data_generation.py` (e.g., 50K members → ~500K claims)

## Next Steps

- Dashboards (AI/BI Lakeview) for financial and quality metrics
- Data governance (UC policies, row/column filters, PII masking, tagging)
- Genie Space for natural language exploration
- Additional HEDIS measures and Stars domain weighting
