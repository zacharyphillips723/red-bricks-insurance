# Databricks notebook source
# MAGIC %md
# MAGIC # Red Bricks Insurance — Agent A/B Evaluation
# MAGIC
# MAGIC This notebook evaluates Care Intelligence Agent **v1** (`production`) vs **v2** (`champion`)
# MAGIC using direct model prediction + MLflow evaluation with custom scorers.
# MAGIC
# MAGIC ### Evaluation Pipeline
# MAGIC 1. Build evaluation dataset from high-risk members (`raf_score > 2.0`)
# MAGIC 2. Load both agents from Unity Catalog by alias
# MAGIC 3. Generate predictions from each agent
# MAGIC 4. Score outputs with LLM-as-judge and Python scorers
# MAGIC 5. Persist results to Delta tables for dashboard consumption
# MAGIC
# MAGIC ### Custom Scorers
# MAGIC - **clinical_completeness** — risk factors, care gaps, medications, actions (1-5)
# MAGIC - **citation_quality** — sources cited with dates and document types (1-5)
# MAGIC - **actionability** — concrete next steps for a care manager (1-5)
# MAGIC - **response_structure** — checks for SOAP section headers (0 or 1)

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")

catalog = dbutils.widgets.get("catalog")

V1_MODEL_NAME = f"{catalog}.analytics.care_intelligence_agent"
V2_MODEL_NAME = f"{catalog}.analytics.care_intelligence_agent_v2"
RESULTS_TABLE = f"{catalog}.analytics.agent_evaluation_results"
SUMMARY_TABLE = f"{catalog}.analytics.agent_evaluation_summary"
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
    FROM {catalog}.analytics.gold_member_360
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
            "prompt": f"[{mid}] {q_text}",
            "member_id": mid,
            "question_type": q_type,
            "raf_score": float(member["raf_score"]),
        })

# Add cost-context questions for the top 5 highest-risk members
for _, member in high_risk_df.head(5).iterrows():
    mid = member["member_id"]
    eval_rows.append({
        "prompt": f"[{mid}] What are the cost drivers for this member and how do they compare to peers?",
        "member_id": mid,
        "question_type": "cost_context",
        "raf_score": float(member["raf_score"]),
    })

eval_df = pd.DataFrame(eval_rows)
print(f"Total evaluation rows: {len(eval_df)}")
display(eval_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Load Models and Generate Predictions

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

def generate_predictions(model, prompts: list[str]) -> list[str]:
    """Call a ChatModel with each prompt and collect responses."""
    responses = []
    for i, prompt in enumerate(prompts):
        try:
            # ChatModel expects messages format via predict()
            input_data = {"messages": [{"role": "user", "content": prompt}]}
            result = model.predict(input_data)
            # Extract the assistant response from ChatCompletionResponse
            if isinstance(result, dict):
                content = (
                    result.get("choices", [{}])[0]
                    .get("message", {})
                    .get("content", "No response")
                )
            else:
                # ChatCompletionResponse object
                content = result.choices[0].message.content
            responses.append(content)
            print(f"  [{i+1}/{len(prompts)}] Generated response ({len(content)} chars)")
        except Exception as e:
            print(f"  [{i+1}/{len(prompts)}] ERROR: {e}")
            responses.append(f"Error: {e}")
    return responses

# COMMAND ----------

print("Generating v1 predictions...")
v1_responses = generate_predictions(v1_model, eval_df["prompt"].tolist())

# COMMAND ----------

print("Generating v2 predictions...")
v2_responses = generate_predictions(v2_model, eval_df["prompt"].tolist())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Score Responses with LLM-as-Judge

# COMMAND ----------

import json
import requests

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
host = ctx.apiUrl().get()
token = ctx.apiToken().get()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def llm_judge_score(prompt: str, response: str, criteria: str, rubric: str) -> int:
    """Use an LLM judge to score a response on a given criteria (1-5)."""
    judge_prompt = (
        f"You are evaluating a care management AI assistant's response.\n\n"
        f"## Evaluation Criteria: {criteria}\n\n"
        f"## Rubric\n{rubric}\n\n"
        f"## Input Question\n{prompt}\n\n"
        f"## Response to Evaluate\n{response}\n\n"
        f"Score the response from 1-5 based on the rubric above. "
        f"Return ONLY a JSON object: {{\"score\": <int>, \"justification\": \"<brief reason>\"}}"
    )
    try:
        resp = requests.post(
            f"{host}/serving-endpoints/{JUDGE_ENDPOINT}/invocations",
            headers=headers,
            json={
                "messages": [{"role": "user", "content": judge_prompt}],
                "max_tokens": 200,
                "temperature": 0.0,
            },
        )
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]
        # Parse score from JSON response
        parsed = json.loads(content.strip().removeprefix("```json").removesuffix("```").strip())
        return max(1, min(5, int(parsed["score"])))
    except Exception as e:
        print(f"    Judge error: {e}")
        return 3  # default middle score on failure

# COMMAND ----------

# Define scoring criteria
CRITERIA = {
    "clinical_completeness": {
        "criteria": "Clinical Completeness",
        "rubric": (
            "5: Covers risk factors, active conditions, medications, care gaps, AND recommended actions\n"
            "4: Covers 4 of the 5 clinical dimensions\n"
            "3: Covers 3 of the 5 clinical dimensions\n"
            "2: Covers only 1-2 clinical dimensions\n"
            "1: Missing most clinical context or provides vague/generic information"
        ),
    },
    "citation_quality": {
        "criteria": "Citation Quality",
        "rubric": (
            "5: Most claims cite specific sources with dates and document types\n"
            "4: Multiple citations present but some claims lack attribution\n"
            "3: Some citations but many claims are unsourced\n"
            "2: Minimal citations — mostly unsourced assertions\n"
            "1: No citations or source references at all"
        ),
    },
    "actionability": {
        "criteria": "Actionability",
        "rubric": (
            "5: Provides specific, prioritized actions with timelines and talking points\n"
            "4: Provides specific actions but lacks prioritization or timelines\n"
            "3: Actions are present but somewhat generic\n"
            "2: Vague suggestions without concrete steps\n"
            "1: No actionable recommendations"
        ),
    },
}


def score_response_structure(response: str) -> float:
    """Check if response contains SOAP section headers."""
    if not response:
        return 0.0
    text = response.upper()
    soap_headers = ["## SUBJECTIVE", "## OBJECTIVE", "## ASSESSMENT", "## PLAN"]
    return 1.0 if all(h.upper() in text for h in soap_headers) else 0.0

# COMMAND ----------

# MAGIC %md
# MAGIC ### Score v1 Responses

# COMMAND ----------

print("Scoring v1 responses...")
v1_scores = {metric: [] for metric in list(CRITERIA.keys()) + ["response_structure"]}

for i, (prompt, response) in enumerate(zip(eval_df["prompt"], v1_responses)):
    print(f"  Scoring [{i+1}/{len(v1_responses)}]: {prompt[:60]}...")
    for metric, config in CRITERIA.items():
        score = llm_judge_score(prompt, response, config["criteria"], config["rubric"])
        v1_scores[metric].append(score)
    v1_scores["response_structure"].append(score_response_structure(response))

print("v1 scoring complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Score v2 Responses

# COMMAND ----------

print("Scoring v2 responses...")
v2_scores = {metric: [] for metric in list(CRITERIA.keys()) + ["response_structure"]}

for i, (prompt, response) in enumerate(zip(eval_df["prompt"], v2_responses)):
    print(f"  Scoring [{i+1}/{len(v2_responses)}]: {prompt[:60]}...")
    for metric, config in CRITERIA.items():
        score = llm_judge_score(prompt, response, config["criteria"], config["rubric"])
        v2_scores[metric].append(score)
    v2_scores["response_structure"].append(score_response_structure(response))

print("v2 scoring complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Combine Results and Persist to Delta

# COMMAND ----------

from datetime import datetime

eval_timestamp = datetime.now().isoformat()

# Build per-row results for v1
v1_results_df = eval_df.copy()
v1_results_df["agent_version"] = "v1"
v1_results_df["model_name"] = V1_MODEL_NAME
v1_results_df["prompt_strategy"] = "narrative"
v1_results_df["llm_endpoint"] = "databricks-meta-llama-3-3-70b-instruct"
v1_results_df["retrieval_chunks"] = 5
v1_results_df["eval_timestamp"] = eval_timestamp
v1_results_df["response"] = v1_responses
for metric, scores in v1_scores.items():
    v1_results_df[metric] = scores

# Build per-row results for v2
v2_results_df = eval_df.copy()
v2_results_df["agent_version"] = "v2"
v2_results_df["model_name"] = V2_MODEL_NAME
v2_results_df["prompt_strategy"] = "soap_structured"
v2_results_df["llm_endpoint"] = "databricks-llama-4-maverick"
v2_results_df["retrieval_chunks"] = 10
v2_results_df["eval_timestamp"] = eval_timestamp
v2_results_df["response"] = v2_responses
for metric, scores in v2_scores.items():
    v2_results_df[metric] = scores

combined_df = pd.concat([v1_results_df, v2_results_df], ignore_index=True)

# Rename 'prompt' to 'inputs' for dashboard compatibility
combined_df = combined_df.rename(columns={"prompt": "inputs"})

print(f"Combined results: {len(combined_df)} rows")
display(combined_df[["agent_version", "inputs", "clinical_completeness", "citation_quality", "actionability", "response_structure"]].head(10))

# COMMAND ----------

# Write per-row results to Delta
combined_spark_df = spark.createDataFrame(combined_df)
combined_spark_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(RESULTS_TABLE)

print(f"Per-row results written to {RESULTS_TABLE}")
print(f"  Rows: {combined_spark_df.count()}")

# COMMAND ----------

# Build and write aggregated summary
summary_rows = []
score_metrics = ["clinical_completeness", "citation_quality", "actionability", "response_structure"]

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
    for metric in score_metrics:
        row[f"avg_{metric}"] = round(float(version_df[metric].mean()), 3)
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
for _, row in summary_df.iterrows():
    v = row["agent_version"]
    print(f"{v} ({row['prompt_strategy']}, {row['llm_endpoint']}):")
    for metric in score_metrics:
        print(f"  {metric:30s} {row[f'avg_{metric}']:.3f}")
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
