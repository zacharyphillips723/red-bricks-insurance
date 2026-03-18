# Databricks notebook source
# MAGIC %md
# MAGIC # Red Bricks Insurance — Deploy Member RAG Agent
# MAGIC
# MAGIC Deploys a care management assistant agent with two tools:
# MAGIC 1. **search_case_notes** — Vector Search over case notes, call transcripts, claims summaries
# MAGIC 2. **get_member_profile** — SQL query against `gold_member_360` for structured member data
# MAGIC
# MAGIC The agent synthesizes structured (demographics, claims, risk, HEDIS gaps) with
# MAGIC unstructured data (case notes, call transcripts) to help care managers prepare
# MAGIC for member outreach.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "red_bricks_insurance_dev", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

VS_INDEX_NAME = f"{catalog}.{schema}.case_notes_vs_index"
MEMBER_360_TABLE = f"{catalog}.{schema}.gold_member_360"
ENDPOINT_NAME = "red-bricks-member-agent"
MODEL_NAME = f"{catalog}.{schema}.member_rag_agent"

print(f"VS Index: {VS_INDEX_NAME}")
print(f"Member 360 Table: {MEMBER_360_TABLE}")
print(f"Serving Endpoint: {ENDPOINT_NAME}")
print(f"UC Model: {MODEL_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Agent Tools

# COMMAND ----------

import json
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()


def search_case_notes(member_id: str, query: str) -> str:
    """Search case notes, call transcripts, and claims summaries for a member.

    Args:
        member_id: The member ID (e.g., MBR100042)
        query: Natural language search query about the member's history

    Returns:
        Relevant document chunks with metadata
    """
    results = w.vector_search_indexes.query_index(
        index_name=VS_INDEX_NAME,
        columns=["chunk_id", "document_id", "member_id", "document_type", "title",
                 "created_date", "author", "chunk_text"],
        query_text=query,
        filters={"member_id": member_id},
        num_results=5,
    )

    if not results.result or not results.result.data_array:
        return json.dumps({"documents": [], "message": f"No documents found for member {member_id}"})

    columns = [c.name for c in results.manifest.columns]
    docs = []
    for row in results.result.data_array:
        doc = dict(zip(columns, row))
        docs.append(doc)

    return json.dumps({"documents": docs, "count": len(docs)})


def get_member_profile(member_id: str) -> str:
    """Get the full Member 360 profile with demographics, claims, risk, and HEDIS data.

    Args:
        member_id: The member ID (e.g., MBR100042)

    Returns:
        JSON with full member profile from gold_member_360
    """
    stmt = w.statement_execution.execute_statement(
        warehouse_id=spark.conf.get("warehouse_id", "781064a3466c0984"),
        statement=f"SELECT * FROM {MEMBER_360_TABLE} WHERE member_id = '{member_id}'",
        wait_timeout="30s",
    )

    if not stmt.result or not stmt.result.data_array or len(stmt.result.data_array) == 0:
        return json.dumps({"error": f"Member {member_id} not found"})

    columns = [c.name for c in stmt.manifest.schema.columns]
    row = dict(zip(columns, stmt.result.data_array[0]))
    return json.dumps(row, default=str)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Agent with LangChain

# COMMAND ----------

# MAGIC %pip install langchain langchain-community databricks-langchain mlflow
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# Re-read widgets after restart
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
VS_INDEX_NAME = f"{catalog}.{schema}.case_notes_vs_index"
MEMBER_360_TABLE = f"{catalog}.{schema}.gold_member_360"
ENDPOINT_NAME = "red-bricks-member-agent"
MODEL_NAME = f"{catalog}.{schema}.member_rag_agent"

# COMMAND ----------

from langchain_core.tools import tool
from langchain_community.chat_models import ChatDatabricks
from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
import json
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

SYSTEM_PROMPT = """You are a care management assistant for Red Bricks Insurance, a health plan serving commercial, Medicare Advantage, Medicaid, and ACA populations.

Your role is to help care managers prepare for member outreach by synthesizing both structured data (demographics, claims history, risk scores, HEDIS gaps) and unstructured data (clinical case notes, outreach call transcripts, claims summaries).

When answering questions about a member:
1. First retrieve the member's profile using get_member_profile to understand their demographics, risk level, and claims history
2. Then search their case notes using search_case_notes to find relevant clinical context
3. Synthesize both data sources into a clear, actionable summary

Always:
- Cite your sources (e.g., "According to the case note from January 15, 2025...")
- Highlight key risk factors and care gaps
- Suggest relevant follow-up actions when appropriate
- Use clinical terminology appropriately but explain for non-clinical audiences
- Flag any concerning trends (rising costs, worsening conditions, missed appointments)

Never:
- Make up information not in the data
- Provide medical advice or diagnoses
- Share PHI beyond what's needed for care coordination
"""


@tool
def search_case_notes_tool(member_id: str, query: str) -> str:
    """Search case notes, call transcripts, and claims summaries for a member.
    Use this to find clinical context, outreach history, and care documentation.

    Args:
        member_id: The member ID (e.g., MBR100042)
        query: Natural language search query about the member's history
    """
    results = w.vector_search_indexes.query_index(
        index_name=VS_INDEX_NAME,
        columns=["chunk_id", "document_id", "member_id", "document_type", "title",
                 "created_date", "author", "chunk_text"],
        query_text=query,
        filters={"member_id": member_id},
        num_results=5,
    )
    if not results.result or not results.result.data_array:
        return json.dumps({"documents": [], "message": f"No documents found for member {member_id}"})
    columns = [c.name for c in results.manifest.columns]
    docs = [dict(zip(columns, row)) for row in results.result.data_array]
    return json.dumps({"documents": docs, "count": len(docs)})


@tool
def get_member_profile_tool(member_id: str) -> str:
    """Get the full Member 360 profile including demographics, claims, risk scores, and HEDIS gaps.
    Use this as the first step when asked about any member.

    Args:
        member_id: The member ID (e.g., MBR100042)
    """
    stmt = w.statement_execution.execute_statement(
        warehouse_id=spark.conf.get("warehouse_id", "781064a3466c0984"),
        statement=f"SELECT * FROM {MEMBER_360_TABLE} WHERE member_id = :member_id",
        wait_timeout="30s",
        parameters=[{"name": "member_id", "value": member_id}],
    )
    if not stmt.result or not stmt.result.data_array or len(stmt.result.data_array) == 0:
        return json.dumps({"error": f"Member {member_id} not found"})
    columns = [c.name for c in stmt.manifest.schema.columns]
    row = dict(zip(columns, stmt.result.data_array[0]))
    return json.dumps(row, default=str)


tools = [search_case_notes_tool, get_member_profile_tool]

prompt = ChatPromptTemplate.from_messages([
    ("system", SYSTEM_PROMPT),
    MessagesPlaceholder("chat_history", optional=True),
    ("human", "{input}"),
    MessagesPlaceholder("agent_scratchpad"),
])

llm = ChatDatabricks(endpoint="databricks-meta-llama-3-3-70b-instruct")
agent = create_tool_calling_agent(llm, tools, prompt)
agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Agent Locally

# COMMAND ----------

test_response = agent_executor.invoke({
    "input": "Summarize the care history for member MBR100042",
    "chat_history": [],
})
print(test_response["output"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log and Register in Unity Catalog

# COMMAND ----------

import mlflow
from mlflow.models import infer_signature

mlflow.set_registry_uri("databricks-uc")

input_example = {"input": "Summarize care history for MBR100042", "chat_history": []}
output_example = agent_executor.invoke(input_example)

with mlflow.start_run(run_name="member_rag_agent"):
    model_info = mlflow.langchain.log_model(
        lc_model=agent_executor,
        artifact_path="agent",
        registered_model_name=MODEL_NAME,
        input_example=input_example,
        signature=infer_signature(input_example, output_example),
    )
    print(f"Model logged: {model_info.model_uri}")
    print(f"Registered as: {MODEL_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy to Model Serving

# COMMAND ----------

from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput

# Get latest model version
latest_version = max(
    w.model_registry.get_model(MODEL_NAME).latest_versions,
    key=lambda v: int(v.version),
).version

print(f"Deploying {MODEL_NAME} version {latest_version} to endpoint '{ENDPOINT_NAME}'...")

try:
    # Check if endpoint exists
    w.serving_endpoints.get(ENDPOINT_NAME)
    # Update existing
    w.serving_endpoints.update_config(
        name=ENDPOINT_NAME,
        served_entities=[
            ServedEntityInput(
                entity_name=MODEL_NAME,
                entity_version=latest_version,
                workload_size="Small",
                scale_to_zero_enabled=True,
            )
        ],
    )
    print(f"Updated existing endpoint '{ENDPOINT_NAME}'")
except Exception:
    # Create new
    w.serving_endpoints.create(
        name=ENDPOINT_NAME,
        config=EndpointCoreConfigInput(
            served_entities=[
                ServedEntityInput(
                    entity_name=MODEL_NAME,
                    entity_version=latest_version,
                    workload_size="Small",
                    scale_to_zero_enabled=True,
                )
            ]
        ),
    )
    print(f"Created endpoint '{ENDPOINT_NAME}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Deployment

# COMMAND ----------

import time

for i in range(60):
    ep = w.serving_endpoints.get(ENDPOINT_NAME)
    state = ep.state.ready if ep.state else None
    config_update = ep.state.config_update if ep.state else None
    print(f"  Endpoint state: ready={state}, config_update={config_update} ({i*10}s)")
    if state == "READY":
        break
    time.sleep(10)

print(f"\nEndpoint '{ENDPOINT_NAME}' is ready for queries.")

# COMMAND ----------

# Test via serving endpoint
response = w.serving_endpoints.query(
    name=ENDPOINT_NAME,
    dataframe_records=[{
        "input": "What are the key risk factors for member MBR100042?",
        "chat_history": [],
    }],
)
print("Test response:", response.predictions)
