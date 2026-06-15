# Databricks notebook source

# MAGIC %md
# MAGIC # Red Bricks Insurance — System Tables Observability
# MAGIC
# MAGIC This notebook queries **system tables** and **inference tables** to provide full observability across
# MAGIC the FWA (Fraud, Waste & Abuse) investigation models deployed via Mosaic AI Gateway and Model Serving.
# MAGIC
# MAGIC **What's covered:**
# MAGIC - Served entity inventory
# MAGIC - Token usage by endpoint (last 30 days)
# MAGIC - Cost comparison across Llama 4 Maverick (FMAPI), Gemini 2.5 Pro, and GPT-4o
# MAGIC - Latency distribution (p50 / p95 / p99)
# MAGIC - Audit trail for FWA table access
# MAGIC - Inference table drill-down (request/response payloads)
# MAGIC - Token trending over time
# MAGIC - Cost-per-investigation summary

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance_catalog", "Catalog")
catalog = dbutils.widgets.get("catalog")
catalog_sql = f"`{catalog}`"

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

ENDPOINTS = ["databricks-llama-4-maverick", "gemini-2-5-pro-gateway", "gpt-4o-gateway"]
COST_PER_1K_TOKENS = {
    "databricks-llama-4-maverick": {"input": 0.0, "output": 0.0},  # included in DBU
    "gemini-2-5-pro-gateway": {"input": 0.00125, "output": 0.005},
    "gpt-4o-gateway": {"input": 0.0025, "output": 0.01},
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deployed Models — Served Entities

# COMMAND ----------

served_entities = spark.sql("""
    SELECT entity_name, entity_version, endpoint_name, endpoint_type,
           creation_timestamp, creator
    FROM system.serving.served_entities
    WHERE entity_name LIKE '%fwa%'
       OR entity_name LIKE '%gateway%'
       OR entity_name LIKE '%maverick%'
    ORDER BY creation_timestamp DESC
""")
display(served_entities)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Token Usage by Endpoint (Last 30 Days)

# COMMAND ----------

endpoint_list = ", ".join(f"'{e}'" for e in ENDPOINTS)
usage_df = spark.sql(f"""
    SELECT
        served_entity_name AS endpoint,
        DATE(request_date) AS dt,
        COUNT(*) AS request_count,
        SUM(input_token_count) AS total_input_tokens,
        SUM(output_token_count) AS total_output_tokens,
        SUM(input_token_count + output_token_count) AS total_tokens
    FROM system.serving.endpoint_usage
    WHERE served_entity_name IN ({endpoint_list})
      AND request_date >= DATE_SUB(CURRENT_DATE(), 30)
    GROUP BY served_entity_name, DATE(request_date)
    ORDER BY dt DESC, endpoint
""")
display(usage_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cost Comparison by Model

# COMMAND ----------

cost_data = spark.sql(f"""
    SELECT
        served_entity_name AS endpoint,
        SUM(input_token_count) AS total_input_tokens,
        SUM(output_token_count) AS total_output_tokens,
        COUNT(*) AS total_requests
    FROM system.serving.endpoint_usage
    WHERE served_entity_name IN ({endpoint_list})
      AND request_date >= DATE_SUB(CURRENT_DATE(), 30)
    GROUP BY served_entity_name
""").toPandas()

if not cost_data.empty:
    cost_data["input_cost"] = cost_data.apply(
        lambda r: (r["total_input_tokens"] / 1000) * COST_PER_1K_TOKENS.get(r["endpoint"], {}).get("input", 0), axis=1
    )
    cost_data["output_cost"] = cost_data.apply(
        lambda r: (r["total_output_tokens"] / 1000) * COST_PER_1K_TOKENS.get(r["endpoint"], {}).get("output", 0), axis=1
    )
    cost_data["total_cost"] = cost_data["input_cost"] + cost_data["output_cost"]
    cost_data["cost_per_request"] = cost_data["total_cost"] / cost_data["total_requests"].replace(0, 1)

    print("30-Day Cost Comparison:")
    print("=" * 80)
    for _, row in cost_data.iterrows():
        print(f"  {row['endpoint']:<35} | Requests: {row['total_requests']:>6} | "
              f"Tokens: {row['total_input_tokens'] + row['total_output_tokens']:>10,.0f} | "
              f"Est. Cost: ${row['total_cost']:>8,.2f} | Per-Request: ${row['cost_per_request']:>.4f}")
else:
    print("No usage data found for these endpoints in the last 30 days.")
    print("This is expected if the endpoints haven't been queried yet.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Latency Distribution by Model

# COMMAND ----------

latency_df = spark.sql(f"""
    SELECT
        served_entity_name AS endpoint,
        PERCENTILE(request_processing_time, 0.50) AS p50_ms,
        PERCENTILE(request_processing_time, 0.95) AS p95_ms,
        PERCENTILE(request_processing_time, 0.99) AS p99_ms,
        AVG(request_processing_time) AS avg_ms,
        COUNT(*) AS request_count
    FROM system.serving.endpoint_usage
    WHERE served_entity_name IN ({endpoint_list})
      AND request_date >= DATE_SUB(CURRENT_DATE(), 30)
    GROUP BY served_entity_name
    ORDER BY avg_ms
""")
display(latency_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Audit Trail — FWA Table Access

# COMMAND ----------

audit_df = spark.sql(f"""
    SELECT
        event_time,
        user_identity.email AS user_email,
        service_name,
        action_name,
        request_params.full_name_arg AS table_accessed,
        source_ip_address
    FROM system.access.audit
    WHERE action_name IN ('getTable', 'commandSubmit', 'executeStatement')
      AND (request_params.full_name_arg LIKE '%fwa%'
           OR request_params.commandText LIKE '%fwa%')
      AND event_time >= DATE_SUB(CURRENT_TIMESTAMP(), 7)
    ORDER BY event_time DESC
    LIMIT 50
""")
display(audit_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inference Table Drill-Down

# COMMAND ----------

# Check for inference table payloads
try:
    inference_df = spark.sql(f"""
        SELECT
            request_time,
            served_entity_name,
            request,
            response,
            request_processing_time
        FROM {catalog_sql}.analytics.`databricks-llama-4-maverick_payload`
        ORDER BY request_time DESC
        LIMIT 10
    """)
    display(inference_df)
except Exception as e:
    print(f"Inference table not yet populated: {e}")
    print("Run a few agent queries first, then re-run this cell.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Token Trending (Last 30 Days)

# COMMAND ----------

trend_df = spark.sql(f"""
    SELECT
        DATE(request_date) AS dt,
        served_entity_name AS endpoint,
        SUM(input_token_count + output_token_count) AS daily_tokens
    FROM system.serving.endpoint_usage
    WHERE served_entity_name IN ({endpoint_list})
      AND request_date >= DATE_SUB(CURRENT_DATE(), 30)
    GROUP BY DATE(request_date), served_entity_name
    ORDER BY dt
""").toPandas()

if not trend_df.empty:
    fig, ax = plt.subplots(figsize=(12, 5))
    for ep in trend_df["endpoint"].unique():
        ep_data = trend_df[trend_df["endpoint"] == ep]
        ax.plot(ep_data["dt"], ep_data["daily_tokens"], marker="o", markersize=3, label=ep)
    ax.set_xlabel("Date")
    ax.set_ylabel("Daily Tokens")
    ax.set_title("Daily Token Consumption by Endpoint (Last 30 Days)")
    ax.legend()
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    plt.xticks(rotation=45)
    plt.tight_layout()
    display(fig)
else:
    print("No token trending data available yet.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary — Cost per Investigation

# COMMAND ----------

if not cost_data.empty:
    avg_tokens_per_query = (cost_data["total_input_tokens"].sum() + cost_data["total_output_tokens"].sum()) / max(cost_data["total_requests"].sum(), 1)
    print("FWA Investigation Cost Analysis")
    print("=" * 60)
    print(f"  Avg tokens per agent query: {avg_tokens_per_query:,.0f}")
    print()
    for _, row in cost_data.iterrows():
        ep = row["endpoint"]
        rates = COST_PER_1K_TOKENS.get(ep, {"input": 0, "output": 0})
        est_cost = avg_tokens_per_query / 1000 * (rates["input"] + rates["output"]) / 2
        print(f"  {ep:<35} ~${est_cost:.4f}/query")
    print()
    print("Key Takeaway: Llama 4 Maverick (FMAPI) has $0 marginal token cost —")
    print("external models add per-token cost but may offer quality advantages.")
    print("Use the evaluation notebook to measure quality-vs-cost tradeoffs.")
else:
    print("No data yet. Run agent queries through all 3 models, then re-run.")
