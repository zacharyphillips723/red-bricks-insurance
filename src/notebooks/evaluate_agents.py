# Databricks notebook source
# MAGIC %md
# MAGIC # Red Bricks Insurance — Agent A/B Evaluation
# MAGIC
# MAGIC This notebook evaluates Care Intelligence Agent **v1** (`production`) vs **v2** (`champion`)
# MAGIC using MLflow's evaluation framework with custom clinical scorers and LLM-as-judge.
# MAGIC
# MAGIC ### Evaluation Pipeline
# MAGIC 1. Build evaluation dataset from high-risk members (`raf_score > 2.0`)
# MAGIC 2. Load both agents from Unity Catalog by alias
# MAGIC 3. Run `mlflow.evaluate()` with custom clinical scorers
# MAGIC 4. Combine results and persist to Delta tables
# MAGIC 5. Write aggregated summary for dashboard consumption
# MAGIC
# MAGIC ### Custom Scorers
# MAGIC - **clinical_completeness** — risk factors, care gaps, medications, actions (1-5)
# MAGIC - **citation_quality** — sources cited with dates and document types (1-5)
# MAGIC - **actionability** — concrete next steps for a care manager (1-5)
# MAGIC - **response_structure** — checks for SOAP section headers (0 or 1)
# MAGIC - Plus built-in: `relevance`, `groundedness`

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "red_bricks_insurance_dev", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

V1_MODEL_NAME = f"{catalog}.{schema}.care_intelligence_agent"
V2_MODEL_NAME = f"{catalog}.{schema}.care_intelligence_agent_v2"
RESULTS_TABLE = f"{catalog}.{schema}.agent_evaluation_results"
SUMMARY_TABLE = f"{catalog}.{schema}.agent_evaluation_summary"
JUDGE_ENDPOINT = "databricks-claude-sonnet-4"

print(f"v1 Model:       {V1_MODEL_NAME}")
print(f"v2 Model:       {V2_MODEL_NAME}")
print(f"Results Table:  {RESULTS_TABLE}")
print(f"Summary Table:  {SUMMARY_TABLE}")
print(f"Judge LLM:      {JUDGE_ENDPOINT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Build Evaluation Dataset

# COMMAND ----------

import pandas as pd

# Query high-risk members to build evaluation prompts
high_risk_df = spark.sql(f"""
    SELECT member_id, raf_score, hcc_count, line_of_business,
           top_diagnoses, hedis_gap_measures
    FROM {catalog}.{schema}.gold_member_360
    WHERE raf_score > 2.0
    ORDER BY raf_score DESC
    LIMIT 8
""").toPandas()

print(f"High-risk members for evaluation: {len(high_risk_df)}")
display(high_risk_df[["member_id", "raf_score", "hcc_count", "line_of_business"]])

# COMMAND ----------

# Generate evaluation questions for each member
question_templates = [
    ("outreach_prep", "Prepare me for outreach — what are the key risks and talking points?"),
    ("care_gaps", "What HEDIS care gaps should I prioritize for this member?"),
    ("clinical_trends", "Summarize recent clinical activity and flag concerning trends."),
]

eval_rows = []
for _, member in high_risk_df.iterrows():
    mid = member["member_id"]
    for q_type, q_text in question_templates:
        eval_rows.append({
            "inputs": f"[{mid}] {q_text}",
            "member_id": mid,
            "question_type": q_type,
            "raf_score": float(member["raf_score"]),
        })

# Add cost-context questions for the top 5 highest-risk members
for _, member in high_risk_df.head(5).iterrows():
    mid = member["member_id"]
    eval_rows.append({
        "inputs": f"[{mid}] What are the cost drivers for this member and how do they compare to peers?",
        "member_id": mid,
        "question_type": "cost_context",
        "raf_score": float(member["raf_score"]),
    })

eval_df = pd.DataFrame(eval_rows)
print(f"Total evaluation rows: {len(eval_df)}")
display(eval_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Define Custom Scorers

# COMMAND ----------

from mlflow.metrics.genai import make_genai_metric, EvaluationExample

# Custom scorer 1: Clinical Completeness
clinical_completeness = make_genai_metric(
    name="clinical_completeness",
    definition=(
        "Evaluates whether the response comprehensively covers the member's clinical picture: "
        "risk factors, active conditions, care gaps, current medications, and recommended actions."
    ),
    grading_prompt=(
        "Score the response on clinical completeness from 1-5:\n"
        "5: Covers risk factors, active conditions, medications, care gaps, AND recommended actions\n"
        "4: Covers 4 of the 5 clinical dimensions\n"
        "3: Covers 3 of the 5 clinical dimensions\n"
        "2: Covers only 1-2 clinical dimensions\n"
        "1: Missing most clinical context or provides vague/generic information"
    ),
    examples=[
        EvaluationExample(
            input="[MBR-0001] Prepare me for outreach — what are the key risks and talking points?",
            output=(
                "Member has diabetes (HCC 19), CHF (HCC 85), and CKD Stage 3. "
                "Currently on metformin and lisinopril. A1C of 9.2 from last lab. "
                "Open HEDIS gaps: diabetic eye exam, nephropathy screening. "
                "Recommend prioritizing A1C management and scheduling eye exam."
            ),
            score=5,
            justification="Covers all 5 dimensions: risk factors, conditions, medications, care gaps, and actions.",
        ),
    ],
    model=f"endpoints:/{JUDGE_ENDPOINT}",
    greater_is_better=True,
    parameters={"temperature": 0.0},
)

# Custom scorer 2: Citation Quality
citation_quality = make_genai_metric(
    name="citation_quality",
    definition=(
        "Evaluates whether the response properly cites its data sources with dates, "
        "document types, and author attribution where available."
    ),
    grading_prompt=(
        "Score the response on citation quality from 1-5:\n"
        "5: Most claims cite specific sources with dates and document types\n"
        "4: Multiple citations present but some claims lack attribution\n"
        "3: Some citations but many claims are unsourced\n"
        "2: Minimal citations — mostly unsourced assertions\n"
        "1: No citations or source references at all"
    ),
    examples=[
        EvaluationExample(
            input="[MBR-0001] Summarize recent clinical activity and flag concerning trends.",
            output=(
                "According to the case note from 2025-01-15 (authored by RN Johnson), "
                "the member reported increased shortness of breath. Lab results from "
                "2025-01-10 show BNP elevated at 450 pg/mL. The care plan note from "
                "2024-12-20 recommended cardiology referral."
            ),
            score=5,
            justification="Every claim cites a source with date, document type, and author.",
        ),
    ],
    model=f"endpoints:/{JUDGE_ENDPOINT}",
    greater_is_better=True,
    parameters={"temperature": 0.0},
)

# Custom scorer 3: Actionability
actionability = make_genai_metric(
    name="actionability",
    definition=(
        "Evaluates whether the response provides concrete, prioritized next steps "
        "that a care manager can act on immediately."
    ),
    grading_prompt=(
        "Score the response on actionability from 1-5:\n"
        "5: Provides specific, prioritized actions with timelines and talking points\n"
        "4: Provides specific actions but lacks prioritization or timelines\n"
        "3: Actions are present but somewhat generic\n"
        "2: Vague suggestions without concrete steps\n"
        "1: No actionable recommendations"
    ),
    examples=[
        EvaluationExample(
            input="[MBR-0001] What HEDIS care gaps should I prioritize?",
            output=(
                "Priority 1 (this week): Schedule diabetic eye exam — last exam was 14 months ago. "
                "Priority 2 (within 2 weeks): Order nephropathy screening given CKD progression. "
                "Talking point: 'Your diabetes management is important — let's get your eye exam "
                "scheduled. We can do it at the clinic on Main Street which is in-network.'"
            ),
            score=5,
            justification="Specific actions with clear priorities, timelines, and a member-facing talking point.",
        ),
    ],
    model=f"endpoints:/{JUDGE_ENDPOINT}",
    greater_is_better=True,
    parameters={"temperature": 0.0},
)

print("Custom GenAI scorers defined: clinical_completeness, citation_quality, actionability")

# COMMAND ----------

# Custom scorer 4: Response Structure (Python-based, checks for SOAP headers)
from mlflow.metrics import make_metric
from mlflow.metrics import MetricValue
import numpy as np


def _score_response_structure(predictions, targets=None, metrics=None):
    """Check if response contains SOAP section headers. Returns 1.0 if all present, 0.0 otherwise."""
    scores = []
    soap_headers = ["## SUBJECTIVE", "## OBJECTIVE", "## ASSESSMENT", "## PLAN"]
    for pred in predictions:
        if pred is None:
            scores.append(0.0)
            continue
        text = str(pred).upper()
        has_all = all(h.upper() in text for h in soap_headers)
        scores.append(1.0 if has_all else 0.0)
    return MetricValue(
        scores=scores,
        aggregate_results={"mean": np.mean(scores)},
    )


response_structure = make_metric(
    eval_fn=_score_response_structure,
    name="response_structure",
    greater_is_better=True,
)

print("Python scorer defined: response_structure (SOAP header check)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Load Models and Run Evaluation

# COMMAND ----------

import mlflow

mlflow.set_registry_uri("databricks-uc")

# Load v1 (production) and v2 (champion)
print(f"Loading v1: {V1_MODEL_NAME}@production")
v1_model = mlflow.pyfunc.load_model(f"models:/{V1_MODEL_NAME}@production")

print(f"Loading v2: {V2_MODEL_NAME}@champion")
v2_model = mlflow.pyfunc.load_model(f"models:/{V2_MODEL_NAME}@champion")

print("Both models loaded successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluate v1 (Production)

# COMMAND ----------

with mlflow.start_run(run_name="eval_v1_production"):
    v1_results = mlflow.evaluate(
        model=v1_model,
        data=eval_df,
        model_type="question-answering",
        extra_metrics=[
            clinical_completeness,
            citation_quality,
            actionability,
            response_structure,
        ],
        evaluator_config={
            "col_mapping": {"inputs": "inputs"},
        },
    )

print("v1 Evaluation Metrics:")
for k, v in v1_results.metrics.items():
    print(f"  {k}: {v}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluate v2 (Champion)

# COMMAND ----------

with mlflow.start_run(run_name="eval_v2_champion"):
    v2_results = mlflow.evaluate(
        model=v2_model,
        data=eval_df,
        model_type="question-answering",
        extra_metrics=[
            clinical_completeness,
            citation_quality,
            actionability,
            response_structure,
        ],
        evaluator_config={
            "col_mapping": {"inputs": "inputs"},
        },
    )

print("v2 Evaluation Metrics:")
for k, v in v2_results.metrics.items():
    print(f"  {k}: {v}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Combine Results and Persist to Delta

# COMMAND ----------

from datetime import datetime

eval_timestamp = datetime.now().isoformat()

# Extract per-row results from both evaluations
v1_per_row = v1_results.tables["eval_results_table"].copy()
v1_per_row["agent_version"] = "v1"
v1_per_row["model_name"] = V1_MODEL_NAME
v1_per_row["prompt_strategy"] = "narrative"
v1_per_row["llm_endpoint"] = "databricks-meta-llama-3-3-70b-instruct"
v1_per_row["retrieval_chunks"] = 5
v1_per_row["eval_timestamp"] = eval_timestamp

v2_per_row = v2_results.tables["eval_results_table"].copy()
v2_per_row["agent_version"] = "v2"
v2_per_row["model_name"] = V2_MODEL_NAME
v2_per_row["prompt_strategy"] = "soap_structured"
v2_per_row["llm_endpoint"] = "databricks-llama-4-maverick"
v2_per_row["retrieval_chunks"] = 10
v2_per_row["eval_timestamp"] = eval_timestamp

combined_df = pd.concat([v1_per_row, v2_per_row], ignore_index=True)
print(f"Combined results: {len(combined_df)} rows")

# COMMAND ----------

# Write per-row results to Delta
combined_spark_df = spark.createDataFrame(combined_df)
combined_spark_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(RESULTS_TABLE)

print(f"Per-row results written to {RESULTS_TABLE}")
print(f"  Rows: {combined_spark_df.count()}")

# COMMAND ----------

# Build and write aggregated summary
metric_cols = [
    "clinical_completeness/v1/mean", "citation_quality/v1/mean",
    "actionability/v1/mean", "response_structure/mean",
]
# Dynamically identify score columns from the combined dataframe
score_cols = [c for c in combined_df.columns if c.endswith("/score") or c in [
    "clinical_completeness", "citation_quality", "actionability", "response_structure"
]]

# Aggregate by agent version
summary_rows = []
for version in ["v1", "v2"]:
    version_df = combined_df[combined_df["agent_version"] == version]
    row = {
        "agent_version": version,
        "model_name": version_df["model_name"].iloc[0],
        "prompt_strategy": version_df["prompt_strategy"].iloc[0],
        "llm_endpoint": version_df["llm_endpoint"].iloc[0],
        "retrieval_chunks": int(version_df["retrieval_chunks"].iloc[0]),
        "eval_timestamp": eval_timestamp,
        "num_eval_rows": len(version_df),
    }
    # Compute mean for each score column
    for col in score_cols:
        if col in version_df.columns:
            row[f"avg_{col}"] = float(version_df[col].mean())
    summary_rows.append(row)

summary_df = pd.DataFrame(summary_rows)
summary_spark_df = spark.createDataFrame(summary_df)
summary_spark_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(SUMMARY_TABLE)

print(f"Summary written to {SUMMARY_TABLE}")
display(summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Evaluation Summary

# COMMAND ----------

print("=" * 70)
print("AGENT A/B EVALUATION — COMPLETE")
print("=" * 70)
print()
print("v1 (Production) Aggregate Metrics:")
for k, v in v1_results.metrics.items():
    print(f"  {k}: {v}")
print()
print("v2 (Champion) Aggregate Metrics:")
for k, v in v2_results.metrics.items():
    print(f"  {k}: {v}")
print()
print(f"Results persisted to:")
print(f"  Per-row:   {RESULTS_TABLE}")
print(f"  Summary:   {SUMMARY_TABLE}")
print()
print("Next steps:")
print("  - Open Agent A/B Comparison dashboard for visual comparison")
print("  - If v2 outperforms, promote champion -> production")
print("  - Deploy winning agent to Model Serving endpoint")
print("  - Set up ongoing evaluation with Databricks Monitoring")
