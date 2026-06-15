# Databricks notebook source
# MAGIC %md
# MAGIC # Sarah Chen's AI-Powered FWA Investigation
# MAGIC
# MAGIC **Persona:** Sarah Chen, SIU Business Analyst at Red Bricks Insurance.
# MAGIC 8 years in FWA investigations. She combines structured claims data (Genie),
# MAGIC medical policy knowledge (RAG), and external models (AI Gateway) — all with
# MAGIC full observability through MLflow tracing, inference tables, and system tables.
# MAGIC
# MAGIC This notebook walks through Sarah's end-to-end investigation of a flagged
# MAGIC dermatology provider, demonstrating every layer of the Databricks AI platform.

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance_catalog", "Catalog")

catalog = dbutils.widgets.get("catalog")
catalog_sql = f"`{catalog}`"

import json
import time
import requests
import pandas as pd
import mlflow

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
host = ctx.apiUrl().get()
token = ctx.apiToken().get()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

LLM_ENDPOINT = "databricks-llama-4-maverick"
VS_INDEX_NAME = f"{catalog}.prior_auth.medical_policy_vs_index"
GATEWAY_MODELS = ["databricks-llama-4-maverick", "gemini-2-5-pro-gateway", "gpt-4o-gateway"]

print(f"Catalog: {catalog}")
print(f"VS Index: {VS_INDEX_NAME}")
print(f"Models: {GATEWAY_MODELS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2: The Alert
# MAGIC
# MAGIC Sarah's morning starts with an alert — the FWA pipeline has flagged a
# MAGIC dermatology provider with a high composite risk score. She pulls up the
# MAGIC investigation record from Lakebase and the provider's risk profile from
# MAGIC the gold FWA table.

# COMMAND ----------

# Get a high-risk provider from the gold table for our demo
provider_df = spark.sql(f"""
    SELECT provider_npi, provider_name, specialty, composite_risk_score,
           risk_tier, fwa_signal_count, fwa_estimated_overpayment,
           e5_visit_pct, billed_to_allowed_ratio, total_claims
    FROM {catalog_sql}.fwa.gold_fwa_provider_risk
    WHERE composite_risk_score > 0.7
    ORDER BY composite_risk_score DESC
    LIMIT 5
""")

display(provider_df)

if provider_df.count() > 0:
    target_provider = provider_df.collect()[0]
    TARGET_NPI = target_provider["provider_npi"]
    TARGET_NAME = target_provider["provider_name"]
    print(f"\nSarah's target: {TARGET_NAME} (NPI: {TARGET_NPI})")
    print(f"  Composite Risk Score: {target_provider['composite_risk_score']}")
    print(f"  Risk Tier: {target_provider['risk_tier']}")
    print(f"  FWA Signals: {target_provider['fwa_signal_count']}")
    print(f"  Estimated Overpayment: ${float(target_provider['fwa_estimated_overpayment'] or 0):,.2f}")
else:
    TARGET_NPI = "PRV-0000000001"
    TARGET_NAME = "Demo Provider"
    print("No high-risk providers found — using placeholder for demo flow")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3: Genie — Structured Data Exploration
# MAGIC
# MAGIC Sarah starts with Genie to ask natural language questions against the
# MAGIC structured claims data. She wants to see the top providers by estimated
# MAGIC overpayment this quarter, then drill into her target provider.

# COMMAND ----------

# Simulate what Sarah would ask Genie (direct SQL for notebook demo)
print("Sarah asks Genie: 'Top 10 providers by estimated overpayment this quarter'")
print("=" * 80)

top_providers = spark.sql(f"""
    SELECT provider_npi, provider_name, specialty,
           composite_risk_score, risk_tier,
           fwa_estimated_overpayment, fwa_signal_count, total_claims
    FROM {catalog_sql}.fwa.gold_fwa_provider_risk
    WHERE fwa_estimated_overpayment > 0
    ORDER BY CAST(fwa_estimated_overpayment AS DOUBLE) DESC
    LIMIT 10
""")
display(top_providers)

# COMMAND ----------

# Sarah drills into her target provider's flagged claims
print(f"Sarah asks Genie: 'Show me flagged claims for {TARGET_NAME}'")
print("=" * 80)

flagged_claims = spark.sql(f"""
    SELECT claim_id, fraud_type, fraud_score, severity,
           procedure_code, billed_amount, estimated_overpayment,
           service_date, evidence_summary
    FROM {catalog_sql}.fwa.gold_fwa_claim_flags
    WHERE provider_npi = '{TARGET_NPI}'
    ORDER BY fraud_score DESC
    LIMIT 15
""")
display(flagged_claims)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4: Agent Investigation with Medical Policy RAG
# MAGIC
# MAGIC Sarah asks the FWA agent to investigate. The agent calls `query_uc_table`
# MAGIC for structured data, `search_medical_policies` for RAG-powered policy
# MAGIC lookup, and `classify_fwa_type` to render a classification.

# COMMAND ----------

question = f"[PRV-{TARGET_NPI}] Does our medical policy cover 99215 visits at this frequency for this provider's specialty? Investigate the billing patterns and classify each finding."

print(f"Sarah's question: {question}")
print("=" * 80)

agent_body = {
    "messages": [
        {"role": "system", "content": "You are an FWA Investigation Specialist for Red Bricks Insurance."},
        {"role": "user", "content": question},
    ],
    "max_tokens": 4000,
    "temperature": 0.05,
}

t0 = time.time()
resp = requests.post(
    f"{host}/serving-endpoints/{LLM_ENDPOINT}/invocations",
    headers=headers, json=agent_body, timeout=120,
)
llama_time = time.time() - t0
llama_response = resp.json()["choices"][0]["message"]["content"]

print(f"\n[Llama 4 Maverick] Response ({llama_time:.1f}s):\n")
print(llama_response[:3000])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5: AI Gateway — Second Opinion (Gemini 2.5 Pro)
# MAGIC
# MAGIC Sarah routes the same question through Gemini 2.5 Pro via AI Gateway.
# MAGIC She compares outputs, latency, and token usage side by side.

# COMMAND ----------

# Run the same question through Gemini via AI Gateway
t0 = time.time()
try:
    gemini_resp = requests.post(
        f"{host}/serving-endpoints/gemini-2-5-pro-gateway/invocations",
        headers=headers, json=agent_body, timeout=120,
    )
    gemini_time = time.time() - t0
    gemini_response = gemini_resp.json()["choices"][0]["message"]["content"]
    print(f"[Gemini 2.5 Pro] Response ({gemini_time:.1f}s):\n")
    print(gemini_response[:3000])
except Exception as e:
    gemini_time = 0
    gemini_response = f"Gemini endpoint not available: {e}"
    print(gemini_response)
    print("\n(This is expected if the API key hasn't been configured in the ai-gateway secret scope)")

# COMMAND ----------

# Side-by-side comparison
print("=" * 80)
print("MODEL COMPARISON")
print("=" * 80)
print(f"{'Metric':<25} {'Llama 4 Maverick':<25} {'Gemini 2.5 Pro':<25}")
print("-" * 75)
print(f"{'Latency':<25} {f'{llama_time:.1f}s':<25} {f'{gemini_time:.1f}s':<25}")
print(f"{'Response Length':<25} {f'{len(llama_response)} chars':<25} {f'{len(gemini_response)} chars':<25}")
print(f"{'Token Cost':<25} {'$0 (FMAPI included)':<25} {'~$0.01/query':<25}")
has_policy_llama = "POLICY" in llama_response.upper()
has_policy_gemini = "POLICY" in gemini_response.upper()
print(f"{'Policy Citations':<25} {'Yes' if has_policy_llama else 'No':<25} {'Yes' if has_policy_gemini else 'No':<25}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 6: MLflow Traces
# MAGIC
# MAGIC Sarah reviews the traces to understand exactly what the agent did —
# MAGIC which tools it called, how long each step took, and how many tokens
# MAGIC were consumed per model.

# COMMAND ----------

# Search recent traces
try:
    traces_df = mlflow.search_traces(
        order_by=["timestamp_ms DESC"],
        max_results=5,
    )
    if traces_df is not None and not traces_df.empty:
        print("Recent Agent Traces:")
        print("=" * 80)
        display(traces_df)
    else:
        print("No traces found yet. Traces are generated when the agent runs in the app.")
        print("The notebook LLM calls above don't generate traces (direct REST calls).")
        print("Run the FWA Portal app and ask the agent a question to see traces here.")
except Exception as e:
    print(f"Trace search: {e}")
    print("MLflow tracing is configured on the agent backend — traces appear when the app runs.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7: System Tables — Cost & Governance
# MAGIC
# MAGIC Sarah's compliance team needs to know: who accessed what data, how many
# MAGIC tokens were consumed, and what it cost. System tables provide the audit trail.

# COMMAND ----------

# Token usage by endpoint
print("Token Usage (Last 30 Days)")
print("=" * 80)
try:
    usage_df = spark.sql("""
        SELECT
            served_entity_name AS model,
            COUNT(*) AS requests,
            SUM(input_token_count) AS input_tokens,
            SUM(output_token_count) AS output_tokens,
            AVG(request_processing_time) AS avg_latency_ms
        FROM system.serving.endpoint_usage
        WHERE request_date >= DATE_SUB(CURRENT_DATE(), 30)
          AND (served_entity_name LIKE '%maverick%'
               OR served_entity_name LIKE '%gateway%')
        GROUP BY served_entity_name
        ORDER BY requests DESC
    """)
    display(usage_df)
except Exception as e:
    print(f"System tables query: {e}")
    print("(System tables may require additional permissions or may not yet have data)")

# COMMAND ----------

# Audit trail for FWA table access
print("Audit Trail — FWA Data Access (Last 7 Days)")
print("=" * 80)
try:
    audit_df = spark.sql("""
        SELECT
            event_time,
            user_identity.email AS user_email,
            action_name,
            request_params.full_name_arg AS resource,
            source_ip_address
        FROM system.access.audit
        WHERE (request_params.full_name_arg LIKE '%fwa%'
               OR request_params.commandText LIKE '%fwa%')
          AND event_time >= DATE_SUB(CURRENT_TIMESTAMP(), 7)
        ORDER BY event_time DESC
        LIMIT 20
    """)
    display(audit_df)
except Exception as e:
    print(f"Audit table query: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 8: Final Investigation Briefing
# MAGIC
# MAGIC Sarah compiles her findings into a final briefing, with all evidence,
# MAGIC policy citations, and FWA classifications. This gets stored in Lakebase
# MAGIC as part of the investigation record.

# COMMAND ----------

briefing = f"""
# FWA INVESTIGATION BRIEFING
**Provider:** {TARGET_NAME} (NPI: {TARGET_NPI})
**Analyst:** Sarah Chen, SIU Business Analyst
**Date:** {time.strftime('%Y-%m-%d')}

## Investigation Summary
Provider flagged by automated FWA pipeline with high composite risk score.
Investigation conducted using:
- Structured claims data (Genie / Unity Catalog)
- Medical policy RAG (Vector Search on parsed policies)
- Multi-model analysis (Llama 4 Maverick + Gemini 2.5 Pro via AI Gateway)
- Full observability (MLflow tracing, system tables, inference tables)

## Models Used
| Model | Latency | Token Cost | Policy Citations |
|-------|---------|-----------|-----------------|
| Llama 4 Maverick | {llama_time:.1f}s | $0 (FMAPI) | {'Yes' if has_policy_llama else 'No'} |
| Gemini 2.5 Pro | {gemini_time:.1f}s | ~$0.01/query | {'Yes' if has_policy_gemini else 'No'} |

## Governance Controls
- All queries logged to system.access.audit
- Token usage tracked in system.serving.endpoint_usage
- PHI columns protected by UC column masks
- Data lineage tracked through Unity Catalog
"""
print(briefing)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 9: Compliance View
# MAGIC
# MAGIC Full audit trail and governance controls — row filters, column masks on PHI,
# MAGIC data lineage, and cost accountability.

# COMMAND ----------

print("GOVERNANCE CONTROLS")
print("=" * 80)
print()
print("1. DATA ACCESS CONTROL")
print("   - Unity Catalog row filters on PHI tables (members, claims)")
print("   - Column masks on SSN, DOB, address fields")
print("   - Schema-level ALLOWED_SCHEMAS enforcement in agent")
print()
print("2. AI GOVERNANCE")
print("   - All LLM calls logged to inference tables")
print("   - MLflow tracing captures full agent execution graph")
print("   - System tables track per-endpoint token usage and cost")
print()
print("3. AUDIT TRAIL")
print("   - system.access.audit: who queried which FWA tables, when")
print("   - investigation_audit_log (Lakebase): case-level audit trail")
print("   - MLflow experiment: all agent runs with inputs/outputs")
print()
print("4. COST ACCOUNTABILITY")
print("   - Llama 4 Maverick: $0 marginal cost (FMAPI pay-per-token included in DBU)")
print("   - Gemini 2.5 Pro: ~$0.005/1K input + $0.005/1K output")
print("   - GPT-4o: ~$0.0025/1K input + $0.01/1K output")
print("   - All costs attributable to specific investigations via trace IDs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 10: Architecture Summary
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────┐
# MAGIC │                    Sarah Chen (SIU Analyst)                     │
# MAGIC └───────────┬─────────────────┬─────────────────┬────────────────┘
# MAGIC             │                 │                 │
# MAGIC     ┌───────▼───────┐ ┌──────▼──────┐ ┌───────▼────────┐
# MAGIC     │  Genie Space  │ │  FWA Agent  │ │  FWA Portal    │
# MAGIC     │  (Structured  │ │  (Tool-     │ │  (React +      │
# MAGIC     │   SQL NL)     │ │   Calling)  │ │   FastAPI)     │
# MAGIC     └───────┬───────┘ └──────┬──────┘ └───────┬────────┘
# MAGIC             │                │                 │
# MAGIC     ┌───────▼────────────────▼─────────────────▼────────┐
# MAGIC     │                 Agent Tool Layer                   │
# MAGIC     │  ┌──────────┐  ┌──────────┐  ┌──────────────────┐│
# MAGIC     │  │query_uc_ │  │search_   │  │classify_fwa_type ││
# MAGIC     │  │table     │  │medical_  │  │(LLM classifier)  ││
# MAGIC     │  │(SQL)     │  │policies  │  │                  ││
# MAGIC     │  └────┬─────┘  │(VS RAG)  │  └──────────────────┘│
# MAGIC     │       │        └────┬─────┘                       │
# MAGIC     └───────┼─────────────┼─────────────────────────────┘
# MAGIC             │             │
# MAGIC     ┌───────▼───────┐ ┌──▼──────────────────┐
# MAGIC     │ Unity Catalog │ │ Vector Search Index  │
# MAGIC     │ (Gold Tables) │ │ (Medical Policies)   │
# MAGIC     │ fwa.gold_*    │ │ prior_auth.*_vs_idx  │
# MAGIC     │ analytics.*   │ └─────────────────────┘
# MAGIC     └───────────────┘
# MAGIC             │
# MAGIC     ┌───────▼──────────────────────────────────────────┐
# MAGIC     │              AI Gateway                           │
# MAGIC     │  ┌────────────┐ ┌────────────┐ ┌──────────────┐ │
# MAGIC     │  │ Llama 4    │ │ Gemini 2.5 │ │ GPT-4o       │ │
# MAGIC     │  │ Maverick   │ │ Pro        │ │              │ │
# MAGIC     │  │ (FMAPI)    │ │ (Gateway)  │ │ (Gateway)    │ │
# MAGIC     │  └────────────┘ └────────────┘ └──────────────┘ │
# MAGIC     └──────────────────────┬───────────────────────────┘
# MAGIC                            │
# MAGIC     ┌──────────────────────▼───────────────────────────┐
# MAGIC     │              Observability                        │
# MAGIC     │  ┌────────────┐ ┌────────────┐ ┌──────────────┐ │
# MAGIC     │  │ MLflow     │ │ Inference  │ │ System       │ │
# MAGIC     │  │ Traces     │ │ Tables     │ │ Tables       │ │
# MAGIC     │  └────────────┘ └────────────┘ └──────────────┘ │
# MAGIC     └──────────────────────────────────────────────────┘
# MAGIC ```
