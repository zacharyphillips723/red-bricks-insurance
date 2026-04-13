# Databricks notebook source
# MAGIC %md
# MAGIC # Red Bricks Insurance — Deploy PA Review Agent
# MAGIC
# MAGIC Registers the Prior Authorization Review Agent as an MLflow ChatModel in Unity Catalog.
# MAGIC The agent uses tool-calling to query PA tables and produce structured clinical review briefings.

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance", "Catalog")

catalog = dbutils.widgets.get("catalog")
catalog_sql = f"`{catalog}`"
PA_SCHEMA = "prior_auth"
MODEL_NAME = f"{catalog}.{PA_SCHEMA}.pa_review_agent"
LLM_ENDPOINT = "databricks-llama-4-maverick"

print(f"Catalog:      {catalog}")
print(f"Agent model:  {MODEL_NAME}")
print(f"LLM backend:  {LLM_ENDPOINT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Checks

# COMMAND ----------

# Verify PA gold tables are populated
catalog_sql = f"`{catalog}`"
for table in ["gold_pa_requests", "gold_pa_metrics", "gold_pa_provider_patterns"]:
    cnt = spark.table(f"{catalog_sql}.{PA_SCHEMA}.{table}").count()
    assert cnt > 0, f"{table} is empty!"
    print(f"  {table}: {cnt:,} rows")

# Verify Foundation Model API
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
from databricks.sdk.service.serving import ChatMessage as SdkChatMessage, ChatMessageRole
test_resp = w.serving_endpoints.query(
    name=LLM_ENDPOINT,
    messages=[SdkChatMessage(role=ChatMessageRole.USER, content="Say 'PA agent ready' in 3 words.")],
    max_tokens=10,
)
print(f"  LLM test: {test_resp.choices[0].message.content}")
print("All checks passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto-detect SQL Warehouse

# COMMAND ----------

warehouse_id = None
try:
    warehouse_id = dbutils.widgets.get("warehouse_id")
except Exception:
    pass

if not warehouse_id:
    warehouses = list(w.warehouses.list())
    running = [wh for wh in warehouses if str(wh.state) == "State.RUNNING"]
    if running:
        warehouse_id = running[0].id
        print(f"Using running warehouse: {running[0].name} ({warehouse_id})")
    elif warehouses:
        warehouse_id = warehouses[0].id
        print(f"Using warehouse: {warehouses[0].name} ({warehouse_id})")
    else:
        raise RuntimeError("No SQL warehouse found. Please provide warehouse_id parameter.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register Agent in Unity Catalog

# COMMAND ----------

import mlflow

mlflow.set_registry_uri("databricks-uc")

user = spark.sql("SELECT current_user()").first()[0]
experiment_path = f"/Users/{user}/{catalog}_pa_review_agent"
mlflow.set_experiment(experiment_path)

agent_code_path = "../agents/pa_review_agent.py"

model_config = {
    "UC_CATALOG": catalog,
    "SQL_WAREHOUSE_ID": warehouse_id,
    "LLM_ENDPOINT": LLM_ENDPOINT,
    "PA_SCHEMA": PA_SCHEMA,
}

with mlflow.start_run(run_name="pa_review_agent_registration") as run:
    mlflow.log_params({
        "catalog": catalog,
        "llm_endpoint": LLM_ENDPOINT,
        "warehouse_id": warehouse_id,
    })

    model_info = mlflow.pyfunc.log_model(
        name="pa_review_agent",
        python_model=agent_code_path,
        pip_requirements=[
            "databricks-sdk>=0.30.0",
            "mlflow>=2.14.0",
            "requests",
        ],
        model_config=model_config,
    )

    print(f"Logged model: {model_info.model_uri}")

# Register to UC
result = mlflow.register_model(model_info.model_uri, MODEL_NAME)
print(f"Registered: {MODEL_NAME} v{result.version}")

# Set production alias
client = mlflow.MlflowClient()
client.set_registered_model_alias(MODEL_NAME, "production", result.version)
print(f"Set 'production' alias → v{result.version}")

# Tag the model (skip 'domain' tag — restricted by UC tag policy)
client.set_model_version_tag(MODEL_NAME, result.version, "agent_type", "pa_review")
client.set_model_version_tag(MODEL_NAME, result.version, "llm_endpoint", LLM_ENDPOINT)

print("\nPA Review Agent deployed successfully.")
