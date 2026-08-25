# Databricks notebook source
# MAGIC %md
# MAGIC # Denial-Agent Labeling Session (MLflow 3 Review App)
# MAGIC
# MAGIC Creates label schemas + a **labeling session** over the denial-reasoning agent's
# MAGIC traces and prints a **Review App URL** SMEs (UM nurses, coders) can open to give
# MAGIC structured human feedback. This is the in-UI feedback surface that does NOT depend
# MAGIC on the per-trace "Add assessment" control in the Experiments trace viewer.
# MAGIC
# MAGIC Requires `mlflow[databricks]` + `databricks-agents` (Review App is Databricks-backed).

# COMMAND ----------

# MAGIC %pip install --upgrade "mlflow[databricks]>=3.1" databricks-agents --quiet
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance_catalog", "Catalog")
dbutils.widgets.text("num_traces", "25", "How many recent traces to add")

catalog = dbutils.widgets.get("catalog")
num_traces = int(dbutils.widgets.get("num_traces"))

EXPERIMENT = "/Shared/red-bricks-denial-agent-traces-uc"

# COMMAND ----------

import os
import mlflow
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# UC-stored traces are read through a SQL warehouse — required for search_traces.
def _running_warehouse() -> str:
    for wh in w.warehouses.list():
        if wh.state and wh.state.value == "RUNNING":
            return wh.id
    whs = list(w.warehouses.list())
    return whs[0].id if whs else ""

wh_id = _running_warehouse()
os.environ["MLFLOW_TRACING_SQL_WAREHOUSE_ID"] = wh_id
mlflow.set_tracking_uri("databricks")
exp = mlflow.set_experiment(EXPERIMENT)
me = w.current_user.me().user_name
print(f"Experiment: {EXPERIMENT} (ID {exp.experiment_id}) | warehouse {wh_id} | user {me}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define label schemas (the reviewer questions)

# COMMAND ----------

import mlflow.genai.label_schemas as schemas


def _ensure_schema(name, **kwargs):
    """Reuse an existing schema (may be referenced by prior sessions) or create it."""
    try:
        existing = schemas.get_label_schema(name)
        print(f"  reusing existing schema: {name}")
        return existing.name
    except Exception:
        created = schemas.create_label_schema(name=name, **kwargs)
        print(f"  created schema: {name}")
        return created.name


try:
    _schema_names = [
        _ensure_schema(
            "overall_useful",
            type=schemas.LabelSchemaType.FEEDBACK,
            title="Was this scrub accurate and useful?",
            instruction="Would you trust this denial-risk assessment before submitting the claim?",
            input=schemas.InputCategorical(options=["Yes", "Partially", "No"]),
            enable_comment=True,
        ),
        _ensure_schema(
            "denial_reasoning_correct",
            type=schemas.LabelSchemaType.FEEDBACK,
            title="Were the flagged denial reasons correct?",
            instruction="Judge whether the CARC reasons + medical-necessity/experimental calls are right.",
            input=schemas.InputCategorical(options=["All correct", "Some wrong", "Mostly wrong"]),
            enable_comment=True,
        ),
    ]
    print("Label schemas ready:", _schema_names)
except Exception as e:
    import traceback; traceback.print_exc()
    raise RuntimeError(
        "Label-schema setup failed — the MLflow 3 GenAI labeling API may not be "
        f"available/enabled in this workspace: {e}"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the labeling session + attach recent agent traces

# COMMAND ----------

import mlflow.genai.labeling as labeling
from datetime import datetime

session = labeling.create_labeling_session(
    name=f"denial-agent-review-{datetime.now().strftime('%Y%m%d-%H%M')}",
    assigned_users=[me],
    label_schemas=_schema_names,
)
print("Labeling session:", session.name)

# Pull recent traces and attach them for review.
try:
    traces_df = mlflow.search_traces(experiment_ids=[exp.experiment_id], max_results=num_traces)
except TypeError:
    # Newer signature prefers `locations`.
    traces_df = mlflow.search_traces(locations=[exp.experiment_id], max_results=num_traces)

print(f"Found {len(traces_df)} traces; attaching to the session…")
if len(traces_df):
    session = session.add_traces(traces_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review App URL — share this with reviewers

# COMMAND ----------

review_app = labeling.get_review_app()
session_url = getattr(session, "url", "") or ""
print("=" * 70)
print("Session URL   :", session_url or "(open the Review App and pick the session)")
print("Review App URL:", review_app.url)
print("=" * 70)
print("Reviewers open the URL, answer the schema questions per trace, and their")
print("responses are written back as MLflow assessments on each trace.")

import json as _json
dbutils.notebook.exit(_json.dumps({
    "review_app_url": review_app.url,
    "session_url": session_url,
    "session_name": session.name,
    "traces_attached": int(len(traces_df)),
    "schemas": _schema_names,
}))
