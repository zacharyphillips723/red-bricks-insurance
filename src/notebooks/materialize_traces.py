# Databricks notebook source
# MAGIC %md
# MAGIC # Red Bricks Insurance — Materialize MLflow Traces to Unity Catalog
# MAGIC
# MAGIC Backfills MLflow traces from the FWA Investigation Agent experiment into a
# MAGIC queryable Delta table in Unity Catalog. This enables:
# MAGIC
# MAGIC - **SQL queryability** — query traces, spans, tool calls, and model reasoning via SQL
# MAGIC - **Dashboard integration** — power Genie spaces and Lakeview dashboards with trace data
# MAGIC - **Audit trail** — full observability of every agent interaction
# MAGIC
# MAGIC **Source experiment:** `/Shared/red-bricks-fwa-agent-traces`
# MAGIC **Destination table:** `{catalog}.analytics.fwa_agent_traces_detail`

# COMMAND ----------

# MAGIC %pip install mlflow>=2.14.0 --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance_catalog", "Catalog")
catalog = dbutils.widgets.get("catalog")

EXPERIMENT_NAME = "/Shared/red-bricks-fwa-agent-traces"
TRACES_TABLE = f"{catalog}.analytics.fwa_agent_traces_detail"
SPANS_TABLE = f"{catalog}.analytics.fwa_agent_spans"

print(f"Catalog:          {catalog}")
print(f"Experiment:       {EXPERIMENT_NAME}")
print(f"Traces table:     {TRACES_TABLE}")
print(f"Spans table:      {SPANS_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Ensure UC trace storage tag is set on the experiment
# MAGIC
# MAGIC This tag tells MLflow to write **new** traces directly to Unity Catalog,
# MAGIC enabling the Experiment Overview tab's tool-call metrics and detailed trace viewer.

# COMMAND ----------

import mlflow
import json

mlflow.set_tracking_uri("databricks")

exp = mlflow.get_experiment_by_name(EXPERIMENT_NAME)
if exp is None:
    mlflow.set_experiment(EXPERIMENT_NAME)
    exp = mlflow.get_experiment_by_name(EXPERIMENT_NAME)

experiment_id = exp.experiment_id
print(f"Experiment ID: {experiment_id}")

# Set UC trace table tag if not already present
client = mlflow.MlflowClient()
# exp.tags is a dict[str, str] in Databricks MLflow
existing_tags = exp.tags if isinstance(exp.tags, dict) else {}

trace_table_tag = f"{catalog}.analytics.fwa_agent_traces"
try:
    client.set_experiment_tag(experiment_id, "mlflow.experiment.traceTableName", trace_table_tag)
    print(f"Set mlflow.experiment.traceTableName = {trace_table_tag}")
except Exception as e:
    print(f"Tag may already be set: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read all traces from the experiment

# COMMAND ----------

import requests
import time

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
host = ctx.apiUrl().get()
token = ctx.apiToken().get()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def fetch_all_traces(experiment_id: str, max_traces: int = 1000) -> list:
    """Fetch all traces from the experiment via REST API."""
    all_traces = []
    page_token = None

    while len(all_traces) < max_traces:
        params = f"experiment_ids={experiment_id}&max_results=100"
        if page_token:
            params += f"&page_token={page_token}"

        resp = requests.get(
            f"{host}/api/2.0/mlflow/traces?{params}",
            headers=headers,
        )
        resp.raise_for_status()
        data = resp.json()

        traces = data.get("traces", [])
        all_traces.extend(traces)

        page_token = data.get("next_page_token")
        if not page_token or not traces:
            break

    return all_traces


traces = fetch_all_traces(experiment_id)
print(f"Fetched {len(traces)} traces from experiment {experiment_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Parse traces into structured rows
# MAGIC
# MAGIC Each trace becomes one row in the traces table. We extract:
# MAGIC - Request/response content
# MAGIC - Model used, agent type
# MAGIC - Tool calls, tables queried
# MAGIC - Execution time, token counts, span statistics

# COMMAND ----------

from datetime import datetime


def parse_trace(trace: dict) -> dict:
    """Parse a trace API response into a flat row for the traces table."""
    info = trace.get("trace_info", trace)

    # Extract metadata
    metadata = {}
    for item in info.get("request_metadata", info.get("trace_metadata", {}).items() if isinstance(info.get("trace_metadata"), dict) else []):
        if isinstance(item, dict):
            metadata[item["key"]] = item["value"]
        elif isinstance(item, tuple):
            metadata[item[0]] = item[1]

    # Extract tags
    tags = {}
    for item in info.get("tags", []):
        if isinstance(item, dict):
            tags[item["key"]] = item["value"]

    # Parse inputs/outputs
    trace_inputs = metadata.get("mlflow.traceInputs", "{}")
    trace_outputs = metadata.get("mlflow.traceOutputs", "{}")

    try:
        inputs = json.loads(trace_inputs) if isinstance(trace_inputs, str) else trace_inputs
    except (json.JSONDecodeError, TypeError):
        inputs = {"raw": str(trace_inputs)}

    try:
        outputs = json.loads(trace_outputs) if isinstance(trace_outputs, str) else trace_outputs
    except (json.JSONDecodeError, TypeError):
        outputs = {"raw": str(trace_outputs)}

    # Parse size stats
    size_stats = {}
    try:
        size_stats = json.loads(metadata.get("mlflow.trace.sizeStats", "{}"))
    except (json.JSONDecodeError, TypeError):
        pass

    # Extract response details
    response_data = metadata.get("mlflow.trace.response", "{}")
    try:
        resp = json.loads(response_data) if isinstance(response_data, str) else response_data
    except (json.JSONDecodeError, TypeError):
        resp = {}

    tools_used = resp.get("tools_used", [])
    tables_queried = resp.get("tables_queried", 0)
    model_used = resp.get("model", outputs.get("model", ""))
    agent_type = resp.get("agent", outputs.get("agent", ""))

    # Extract answer text
    answer_text = ""
    answer_list = resp.get("answer", outputs.get("answer", []))
    if isinstance(answer_list, list):
        for part in answer_list:
            if isinstance(part, dict) and part.get("type") == "text":
                answer_text += part.get("text", "")
    elif isinstance(answer_list, str):
        answer_text = answer_list

    # Build row
    request_id = info.get("request_id", info.get("trace_id", info.get("client_request_id", "")))
    execution_ms = info.get("execution_time_ms", 0)
    if not execution_ms:
        dur = info.get("execution_duration", "0s")
        if isinstance(dur, str) and dur.endswith("s"):
            try:
                execution_ms = int(float(dur.replace("s", "")) * 1000)
            except ValueError:
                execution_ms = 0

    request_time = info.get("request_time", info.get("timestamp_ms", ""))
    if isinstance(request_time, str) and "T" in request_time:
        try:
            request_time = datetime.fromisoformat(request_time.replace("Z", "+00:00")).strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError):
            pass

    return {
        "trace_id": request_id,
        "experiment_id": experiment_id,
        "request_time": str(request_time),
        "execution_ms": int(execution_ms),
        "status": info.get("status", info.get("state", "UNKNOWN")),
        "trace_name": tags.get("mlflow.traceName", ""),
        "requester": tags.get("mlflow.user", metadata.get("mlflow.user", "")),
        "target_id": inputs.get("target_id", ""),
        "target_type": inputs.get("target_type", ""),
        "question": inputs.get("question", ""),
        "model_used": model_used,
        "agent_type": agent_type,
        "tools_used": json.dumps(tools_used) if tools_used else "[]",
        "tables_queried": int(tables_queried) if tables_queried else 0,
        "num_spans": size_stats.get("num_spans", 0),
        "total_size_bytes": size_stats.get("total_size_bytes", int(metadata.get("mlflow.trace.sizeBytes", 0))),
        "answer_preview": answer_text[:2000] if answer_text else "",
        "has_policy_section": 1 if "POLICY COMPLIANCE" in answer_text.upper() or "policy" in answer_text.lower()[:500] else 0,
        "source": metadata.get("mlflow.source.name", ""),
    }


rows = [parse_trace(t) for t in traces]
print(f"Parsed {len(rows)} trace rows")

# Show sample
for r in rows[:3]:
    print(f"  {r['trace_id'][:30]}... | {r['model_used']} | {r['agent_type']} | {r['num_spans']} spans | {r['execution_ms']}ms | tools={r['tools_used'][:60]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Write traces summary table

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

schema = StructType([
    StructField("trace_id", StringType(), False),
    StructField("experiment_id", StringType(), True),
    StructField("request_time", StringType(), True),
    StructField("execution_ms", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("trace_name", StringType(), True),
    StructField("requester", StringType(), True),
    StructField("target_id", StringType(), True),
    StructField("target_type", StringType(), True),
    StructField("question", StringType(), True),
    StructField("model_used", StringType(), True),
    StructField("agent_type", StringType(), True),
    StructField("tools_used", StringType(), True),
    StructField("tables_queried", IntegerType(), True),
    StructField("num_spans", IntegerType(), True),
    StructField("total_size_bytes", LongType(), True),
    StructField("answer_preview", StringType(), True),
    StructField("has_policy_section", IntegerType(), True),
    StructField("source", StringType(), True),
])

traces_df = spark.createDataFrame(rows, schema)

(traces_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TRACES_TABLE))

row_count = spark.table(TRACES_TABLE).count()
print(f"Wrote {row_count} traces to {TRACES_TABLE}")
display(spark.table(TRACES_TABLE).limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Build span-level data from trace metadata
# MAGIC
# MAGIC Each trace contains multiple spans (AGENT, TOOL, RETRIEVER, LLM calls).
# MAGIC Since this workspace has DBFS root access disabled, `mlflow.search_traces()`
# MAGIC cannot read span artifacts from DBFS-backed storage. Instead, we extract
# MAGIC tool usage and span statistics from the trace metadata already fetched via
# MAGIC the REST API.
# MAGIC
# MAGIC **After UC trace storage is enabled** (tag set in step 1), new traces will
# MAGIC store spans directly in UC and `mlflow.search_traces()` will return full
# MAGIC span data for those new traces.

# COMMAND ----------

# Build span-like rows from the tools_used data in trace metadata
span_rows = []

for trace in traces:
    info = trace.get("trace_info", trace)
    request_id = info.get("request_id", info.get("trace_id", info.get("client_request_id", "")))

    # Extract metadata
    metadata = {}
    for item in info.get("request_metadata", info.get("trace_metadata", {}).items() if isinstance(info.get("trace_metadata"), dict) else []):
        if isinstance(item, dict):
            metadata[item["key"]] = item["value"]
        elif isinstance(item, tuple):
            metadata[item[0]] = item[1]

    # Parse the response to get tool usage
    response_data = metadata.get("mlflow.trace.response", "{}")
    try:
        resp = json.loads(response_data) if isinstance(response_data, str) else response_data
    except (json.JSONDecodeError, TypeError):
        resp = {}

    tools_used = resp.get("tools_used", [])
    model_used = resp.get("model", "")
    agent_type = resp.get("agent", "")

    # Parse size stats for span count
    try:
        size_stats = json.loads(metadata.get("mlflow.trace.sizeStats", "{}"))
    except (json.JSONDecodeError, TypeError):
        size_stats = {}

    # Parse execution time
    execution_ms = info.get("execution_time_ms", 0)
    if not execution_ms:
        dur = info.get("execution_duration", "0s")
        if isinstance(dur, str) and dur.endswith("s"):
            try:
                execution_ms = int(float(dur.replace("s", "")) * 1000)
            except ValueError:
                execution_ms = 0

    # Create a root agent span
    span_rows.append({
        "trace_id": str(request_id),
        "span_id": f"{request_id}_root",
        "parent_span_id": "",
        "span_name": agent_type or "fwa_agent",
        "span_type": "AGENT",
        "status": info.get("status", info.get("state", "OK")),
        "duration_ms": str(round(float(execution_ms), 1)),
        "model_name": model_used,
        "sql_query": "",
        "inputs_preview": metadata.get("mlflow.traceInputs", "")[:2000],
        "outputs_preview": "",
        "attributes_json": json.dumps({"num_spans": size_stats.get("num_spans", 0)}),
    })

    # Create a span row for each tool call
    for idx, tool_name in enumerate(tools_used):
        span_rows.append({
            "trace_id": str(request_id),
            "span_id": f"{request_id}_tool_{idx}",
            "parent_span_id": f"{request_id}_root",
            "span_name": tool_name,
            "span_type": "RETRIEVER" if "search" in tool_name.lower() else "TOOL",
            "status": "OK",
            "duration_ms": "0",
            "model_name": "",
            "sql_query": "",
            "inputs_preview": "",
            "outputs_preview": "",
            "attributes_json": json.dumps({"tool_index": idx}),
        })

print(f"Built {len(span_rows)} span rows from {len(traces)} traces")

# Try to enrich with real span data from mlflow.search_traces() (works for UC-backed traces)
try:
    traces_df_mlflow = mlflow.search_traces(
        experiment_ids=[experiment_id],
        max_results=500,
    )
    if traces_df_mlflow is not None and len(traces_df_mlflow) > 0 and "spans" in traces_df_mlflow.columns:
        real_span_count = 0
        for _, trace_row in traces_df_mlflow.iterrows():
            spans_data = trace_row.get("spans", None)
            if spans_data is None:
                continue
            if isinstance(spans_data, str):
                try:
                    spans_data = json.loads(spans_data)
                except (json.JSONDecodeError, TypeError):
                    continue
            if not isinstance(spans_data, list):
                continue
            trace_id = trace_row.get("request_id", trace_row.get("trace_id", ""))
            for span in spans_data:
                if not isinstance(span, dict):
                    continue
                span_name = span.get("name", "")
                span_type = span.get("span_type", span.get("type", ""))
                span_id = span.get("span_id", span.get("context", {}).get("span_id", ""))
                parent_id = span.get("parent_id", span.get("parent_span_id", ""))
                status = span.get("status", {})
                status_code = status.get("status_code", "OK") if isinstance(status, dict) else str(status)
                start_ns = span.get("start_time_ns", span.get("start_time", 0))
                end_ns = span.get("end_time_ns", span.get("end_time", 0))
                duration_ms = (end_ns - start_ns) / 1_000_000 if start_ns and end_ns else 0
                inputs_raw = json.dumps(span.get("inputs", "")) if isinstance(span.get("inputs"), dict) else str(span.get("inputs", ""))
                outputs_raw = json.dumps(span.get("outputs", "")) if isinstance(span.get("outputs"), dict) else str(span.get("outputs", ""))
                attributes = span.get("attributes", {})
                if isinstance(attributes, str):
                    try:
                        attributes = json.loads(attributes)
                    except (json.JSONDecodeError, TypeError):
                        attributes = {}
                span_rows.append({
                    "trace_id": str(trace_id),
                    "span_id": str(span_id),
                    "parent_span_id": str(parent_id) if parent_id else "",
                    "span_name": span_name,
                    "span_type": span_type,
                    "status": status_code,
                    "duration_ms": str(round(duration_ms, 1)),
                    "model_name": str(attributes.get("model", attributes.get("llm.model", ""))),
                    "sql_query": "",
                    "inputs_preview": inputs_raw[:2000] if inputs_raw else "",
                    "outputs_preview": outputs_raw[:2000] if outputs_raw else "",
                    "attributes_json": json.dumps(attributes)[:2000] if attributes else "{}",
                })
                real_span_count += 1
        if real_span_count > 0:
            print(f"Enriched with {real_span_count} real spans from mlflow.search_traces()")
except Exception as e:
    print(f"mlflow.search_traces() not available for span enrichment: {e}")
    print("This is expected when traces are stored in DBFS with root access disabled.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Write spans table

# COMMAND ----------

if span_rows:
    span_schema = StructType([
        StructField("trace_id", StringType(), False),
        StructField("span_id", StringType(), False),
        StructField("parent_span_id", StringType(), True),
        StructField("span_name", StringType(), True),
        StructField("span_type", StringType(), True),
        StructField("status", StringType(), True),
        StructField("duration_ms", StringType(), True),
        StructField("model_name", StringType(), True),
        StructField("sql_query", StringType(), True),
        StructField("inputs_preview", StringType(), True),
        StructField("outputs_preview", StringType(), True),
        StructField("attributes_json", StringType(), True),
    ])

    spans_df = spark.createDataFrame(span_rows, schema=span_schema)

    (spans_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(SPANS_TABLE))

    span_count = spark.table(SPANS_TABLE).count()
    print(f"Wrote {span_count} spans to {SPANS_TABLE}")
    display(spark.table(SPANS_TABLE).limit(10))
else:
    print("No span data available — spans may not be accessible from this storage backend.")
    print("New traces (after UC trace storage is enabled) will have queryable spans.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Example queries — trace observability via SQL
# MAGIC
# MAGIC These queries demonstrate how SAs and architects can analyze agent behavior
# MAGIC using standard SQL on the materialized trace data.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Agent usage by model

# COMMAND ----------

spark.sql(f"""
SELECT
    model_used,
    agent_type,
    COUNT(*) AS trace_count,
    AVG(execution_ms) AS avg_execution_ms,
    AVG(num_spans) AS avg_spans,
    AVG(tables_queried) AS avg_tables_queried,
    SUM(has_policy_section) AS traces_with_policy_analysis
FROM {TRACES_TABLE}
GROUP BY model_used, agent_type
ORDER BY trace_count DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tool usage frequency across all traces

# COMMAND ----------

spark.sql(f"""
SELECT
    tool,
    COUNT(*) AS usage_count
FROM (
    SELECT EXPLODE(FROM_JSON(tools_used, 'ARRAY<STRING>')) AS tool
    FROM {TRACES_TABLE}
    WHERE tools_used != '[]'
)
GROUP BY tool
ORDER BY usage_count DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Investigation targets and response quality

# COMMAND ----------

spark.sql(f"""
SELECT
    target_type,
    target_id,
    question,
    model_used,
    execution_ms,
    num_spans,
    tables_queried,
    has_policy_section,
    LEFT(answer_preview, 200) AS answer_start
FROM {TRACES_TABLE}
ORDER BY request_time DESC
LIMIT 20
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Span-level analysis — tool execution times

# COMMAND ----------

if spark.catalog.tableExists(SPANS_TABLE):
    spark.sql(f"""
    SELECT
        span_name,
        span_type,
        COUNT(*) AS call_count,
        AVG(CAST(duration_ms AS DOUBLE)) AS avg_duration_ms,
        MAX(CAST(duration_ms AS DOUBLE)) AS max_duration_ms,
        MIN(CAST(duration_ms AS DOUBLE)) AS min_duration_ms
    FROM {SPANS_TABLE}
    GROUP BY span_name, span_type
    ORDER BY avg_duration_ms DESC
    """).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL queries executed by the agent

# COMMAND ----------

if spark.catalog.tableExists(SPANS_TABLE):
    spark.sql(f"""
    SELECT
        trace_id,
        span_name,
        CAST(duration_ms AS DOUBLE) AS duration_ms,
        sql_query
    FROM {SPANS_TABLE}
    WHERE sql_query IS NOT NULL AND sql_query != ''
    ORDER BY CAST(duration_ms AS DOUBLE) DESC
    LIMIT 20
    """).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Trace data is now materialized to Unity Catalog:
# MAGIC
# MAGIC | Table | Content | Query Pattern |
# MAGIC |-------|---------|---------------|
# MAGIC | `analytics.fwa_agent_traces_detail` | One row per agent invocation — model, tools, timing, answer | `SELECT * FROM ...traces_detail WHERE model_used = 'databricks-gemini-2-5-pro'` |
# MAGIC | `analytics.fwa_agent_spans` | One row per span (AGENT, TOOL, RETRIEVER, LLM) | `SELECT * FROM ...spans WHERE span_type = 'TOOL'` |
# MAGIC
# MAGIC **UC Trace Storage** is now enabled on the experiment (`mlflow.experiment.traceTableName`).
# MAGIC New traces logged by the FWA app will go directly to UC, enabling:
# MAGIC - The MLflow Experiment **Overview** tab tool-call metrics
# MAGIC - **Detailed trace viewer** (click trace ID → full span tree)
# MAGIC - SQL queryability via the tables above

# COMMAND ----------

print("=" * 70)
print("TRACE MATERIALIZATION COMPLETE")
print("=" * 70)
traces_count = spark.table(TRACES_TABLE).count()
print(f"  Traces table:  {TRACES_TABLE} ({traces_count} rows)")
if spark.catalog.tableExists(SPANS_TABLE):
    spans_count = spark.table(SPANS_TABLE).count()
    print(f"  Spans table:   {SPANS_TABLE} ({spans_count} rows)")
print(f"  UC trace tag:  mlflow.experiment.traceTableName = {trace_table_tag}")
print()
print("  Next: Send a new query to the FWA agent, then check the Experiment")
print("        Overview tab — tool call metrics should now populate.")
