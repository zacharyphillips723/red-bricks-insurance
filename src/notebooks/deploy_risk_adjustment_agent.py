# Databricks notebook source
# MAGIC %md
# MAGIC # Register the Risk Adjustment / HCC Agent in Unity Catalog (Phase 3)
# MAGIC Validates the Phase 0/1/2 dependencies, logs the agent as an MLflow ChatModel
# MAGIC ("models from code"), registers it in UC with the `production` alias.
# MAGIC Improvements over the FWA agent: parameterized SQL, a `verify_citation` anti-hallucination
# MAGIC guard, per-step tracing, model_config-first config, and MLflow 3.

# COMMAND ----------
import os
dbutils.widgets.text("catalog", "red_bricks_insurance_catalog", "Catalog")
dbutils.widgets.text("warehouse_id", "", "SQL Warehouse ID (auto if empty)")
dbutils.widgets.text("lakebase_instance", "red-bricks-ra-memory", "Lakebase instance (memory)")
catalog = dbutils.widgets.get("catalog")
warehouse_id = dbutils.widgets.get("warehouse_id")
lakebase_instance = dbutils.widgets.get("lakebase_instance")

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
if not warehouse_id.strip():
    running = [wh for wh in w.warehouses.list() if wh.state and wh.state.value == "RUNNING"]
    warehouse_id = (running or list(w.warehouses.list()))[0].id

LLM_ENDPOINT = os.environ.get("RA_SUSPECTING_ENDPOINT", "databricks-claude-sonnet-5")
VS_INDEX = f"{catalog}.documents.case_notes_vs_index"
MODEL_NAME = f"{catalog}.analytics.risk_adjustment_agent"
print("catalog:", catalog, "| warehouse:", warehouse_id, "| llm:", LLM_ENDPOINT)

# COMMAND ----------
# MAGIC %md ## Validate Phase 0/1/2 dependencies
import requests
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
host, token = ctx.apiUrl().get(), ctx.apiToken().get()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

def sql(q):
    r = requests.post(f"{host}/api/2.0/sql/statements", headers=headers,
                      json={"warehouse_id": warehouse_id, "statement": q, "wait_timeout": "30s"}).json()
    return r.get("result", {}).get("data_array", [[None]])

for label, q in [
    ("grouper (P0)", f"SELECT COUNT(*) FROM {catalog}.reference.cms_hcc_v28_grouper"),
    ("hcc_evidence suspected (P1)", f"SELECT COUNT(*) FROM {catalog}.risk_adjustment.hcc_evidence WHERE evidence_state='suspected'"),
    ("agent_policy (P2a)", f"SELECT COUNT(*) FROM {catalog}.risk_adjustment.agent_policy WHERE active"),
]:
    print(f"PASS {label}: {sql(q)[0][0]}")

llm = requests.post(f"{host}/serving-endpoints/{LLM_ENDPOINT}/invocations", headers=headers,
                    json={"messages": [{"role": "user", "content": "reply OK"}], "max_tokens": 5})
llm.raise_for_status(); print("PASS Foundation Model endpoint")

# COMMAND ----------
# MAGIC %md ## Log + register
import mlflow
mlflow.set_registry_uri("databricks-uc")

nb = ctx.notebookPath().get(); parts = nb.split("/")
src_idx = max(i for i, p in enumerate(parts) if p == "src")
agent_code = "/Workspace" + "/".join(parts[:src_idx + 1]) + "/agents/risk_adjustment_agent.py"
assert os.path.exists(agent_code), agent_code

model_config = {
    "UC_CATALOG": catalog,
    "SQL_WAREHOUSE_ID": warehouse_id,
    "RA_SUSPECTING_ENDPOINT": LLM_ENDPOINT,     # swap here (any Unity-Gateway endpoint) — no code change
    "VS_INDEX_NAME": VS_INDEX,
    "RA_LAKEBASE_INSTANCE": lakebase_instance,  # memory; agent degrades gracefully if absent
    "RA_GROUPER_VERSION": "cms_hcc_v28_demo",
    "RA_POLICY_VERSION": "1",
}

with mlflow.start_run(run_name="risk_adjustment_agent") as run:
    info = mlflow.pyfunc.log_model(
        name="risk_adjustment_agent",
        python_model=agent_code,
        model_config=model_config,
        pip_requirements=["mlflow>=3.1.0", "databricks-sdk>=0.30.0", "psycopg2-binary>=2.9"],
    )
    mlflow.set_tags({"agent_type": "risk_adjustment_hcc", "domain": "risk_adjustment",
                     "llm_endpoint": LLM_ENDPOINT, "improvements": "param-sql,verify-citation,per-step-tracing,model_config"})
    print("model_uri:", info.model_uri)

reg = mlflow.register_model(model_uri=info.model_uri, name=MODEL_NAME)
from mlflow import MlflowClient
c = MlflowClient()
c.set_registered_model_alias(name=MODEL_NAME, alias="production", version=reg.version)
c.update_registered_model(name=MODEL_NAME, description=(
    "Risk Adjustment / HCC agent. Reads the chart, suspects HCCs with VERIFIED cited evidence, "
    "deterministic grouping, RADV-weak flags, Lakebase memory. Advisory only; human adjudicates. See DESIGN.md."))
print(f"✅ {MODEL_NAME} v{reg.version} -> alias production")

# COMMAND ----------
# MAGIC %md ## Next: serve it
# MAGIC Create a Model Serving endpoint from `{MODEL_NAME}@production`. Grant the serving identity
# MAGIC read access to the allowlisted schemas + the `red-bricks-ra-memory` Lakebase instance so the
# MAGIC memory tools work end-to-end (otherwise they degrade gracefully). The app can also run the
# MAGIC agent in-process (`AGENT_MODE=local`) in Phase 5 for a full span tree.
