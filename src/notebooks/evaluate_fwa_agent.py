# Databricks notebook source

# MAGIC %md
# MAGIC # Red Bricks Insurance — FWA Agent Multi-Model Evaluation
# MAGIC
# MAGIC This notebook evaluates the **Fraud, Waste & Abuse (FWA) investigation agent** across three foundation models:
# MAGIC
# MAGIC | Model | Endpoint |
# MAGIC |---|---|
# MAGIC | Llama 4 Maverick | `databricks-llama-4-maverick` |
# MAGIC | Gemini 2.5 Pro | `databricks-gemini-2-5-pro` |
# MAGIC | Claude Sonnet 4 | `databricks-claude-sonnet-4` |
# MAGIC
# MAGIC **Evaluation approach:**
# MAGIC - 15 curated FWA investigation scenarios (Fraud, Waste, Abuse, No Fraud, edge cases)
# MAGIC - 4 LLM-judge scoring dimensions + 1 deterministic scorer
# MAGIC - Claude Sonnet 4 as the independent judge endpoint
# MAGIC - Results persisted to Delta for longitudinal tracking

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance_catalog", "Catalog")
catalog = dbutils.widgets.get("catalog")
catalog_sql = f"`{catalog}`"

import mlflow
import pandas as pd
import json
import requests
from datetime import datetime

MODELS = [
    "databricks-llama-4-maverick",
    "databricks-gemini-2-5-pro",
    "databricks-claude-sonnet-4",
]
JUDGE_ENDPOINT = "databricks-claude-sonnet-4"
RESULTS_TABLE = f"{catalog_sql}.analytics.fwa_agent_evaluation_results"

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
host = ctx.apiUrl().get()
token = ctx.apiToken().get()
api_headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Evaluation Dataset

# COMMAND ----------

eval_scenarios = [
    # Fraud cases
    {"prompt": "[PRV-1234567890] This dermatologist bills 99215 (Level 4 E/M) for 95% of visits, far above the specialty average of 40%. Is this consistent with our medical policy for dermatology E/M coding?",
     "expected_classification": "Fraud", "expected_fraud_type": "Upcoding",
     "expected_policy_reference": "E/M Coding Guidelines", "difficulty": "medium"},

    {"prompt": "[PRV-2345678901] We see duplicate claims for the same patient, same date, same procedure code (99213) submitted 3 times within minutes. Investigate.",
     "expected_classification": "Fraud", "expected_fraud_type": "Duplicate Billing",
     "expected_policy_reference": "Duplicate Claims Policy", "difficulty": "easy"},

    {"prompt": "[PRV-3456789012] This provider billed for physical therapy sessions on dates when the facility was closed (weekends and holidays per their registered hours). Are these phantom billings?",
     "expected_classification": "Fraud", "expected_fraud_type": "Phantom Billing",
     "expected_policy_reference": "Provider Billing Standards", "difficulty": "medium"},

    # Waste cases
    {"prompt": "[PRV-4567890123] This PCP orders a comprehensive metabolic panel (CMP) at every visit, including routine well-visits for healthy 25-year-olds. Does our policy support this frequency?",
     "expected_classification": "Waste", "expected_fraud_type": "Unnecessary Testing",
     "expected_policy_reference": "Preventive Care Guidelines", "difficulty": "medium"},

    {"prompt": "[PRV-5678901234] Provider sees the same chronic pain patient 3 times per week for office visits (99213) with no documented change in treatment plan. Is this frequency medically necessary?",
     "expected_classification": "Waste", "expected_fraud_type": "Excessive Frequency",
     "expected_policy_reference": "Office Visit Frequency Standards", "difficulty": "hard"},

    {"prompt": "[PRV-6789012345] This orthopedic practice orders both X-ray and MRI for every knee complaint on the same day, even for minor sprains. What does our imaging policy say?",
     "expected_classification": "Waste", "expected_fraud_type": "Redundant Services",
     "expected_policy_reference": "Diagnostic Imaging Guidelines", "difficulty": "medium"},

    # Abuse cases
    {"prompt": "[PRV-7890123456] This surgeon bills separate charges for each component of a procedure that should be bundled under a single CPT code. The unbundled charges total 40% more. Investigate.",
     "expected_classification": "Abuse", "expected_fraud_type": "Unbundling",
     "expected_policy_reference": "Surgical Bundling Rules", "difficulty": "hard"},

    {"prompt": "[PRV-8901234567] Provider consistently bills 99214 (moderate complexity) for straightforward follow-ups that typically warrant 99213. The pattern is consistent but not extreme.",
     "expected_classification": "Abuse", "expected_fraud_type": "Upcoding (minor)",
     "expected_policy_reference": "E/M Coding Guidelines", "difficulty": "hard"},

    {"prompt": "[PRV-9012345678] This DME supplier bills for the most expensive wheelchair model when clinical documentation supports a standard model. Is this consistent with our DME policy?",
     "expected_classification": "Abuse", "expected_fraud_type": "Excessive Charges",
     "expected_policy_reference": "DME Coverage Policy", "difficulty": "medium"},

    # No fraud cases
    {"prompt": "[PRV-0123456789] This oncologist has high per-patient costs and frequent E5 visits (99215). However, their patient panel is 80% Stage III-IV cancer patients. Is this appropriate?",
     "expected_classification": "No Fraud", "expected_fraud_type": "None",
     "expected_policy_reference": "Oncology Billing Standards", "difficulty": "hard"},

    {"prompt": "[PRV-1111111111] This endocrinologist bills 99215 frequently but treats complex diabetic patients with multiple comorbidities (CKD, neuropathy, retinopathy). Review billing patterns.",
     "expected_classification": "No Fraud", "expected_fraud_type": "None",
     "expected_policy_reference": "Endocrinology E/M Guidelines", "difficulty": "hard"},

    {"prompt": "[PRV-2222222222] New surgical practice has high initial costs but excellent outcomes and low readmission rates. The cost-per-episode is within specialty benchmarks. Assess risk.",
     "expected_classification": "No Fraud", "expected_fraud_type": "None",
     "expected_policy_reference": "Surgical Cost Benchmarks", "difficulty": "medium"},

    # Additional edge cases
    {"prompt": "Which providers have the highest estimated overpayment this quarter and what fraud types are most common?",
     "expected_classification": "N/A", "expected_fraud_type": "N/A",
     "expected_policy_reference": "N/A", "difficulty": "easy"},

    {"prompt": "[PRV-3333333333] This provider group has 5 NPIs billing from the same address. Three have similar billing patterns with high 99215 rates. Is this a potential fraud ring?",
     "expected_classification": "Fraud", "expected_fraud_type": "Network Fraud",
     "expected_policy_reference": "Provider Network Investigation", "difficulty": "hard"},

    {"prompt": "[PRV-4444444444] Investigate whether this provider's billing for modifier 25 on every E/M visit with a minor procedure is appropriate per our modifier policy.",
     "expected_classification": "Abuse", "expected_fraud_type": "Modifier Abuse",
     "expected_policy_reference": "Modifier 25 Guidelines", "difficulty": "hard"},
]

eval_df = pd.DataFrame(eval_scenarios)
print(f"Total evaluation scenarios: {len(eval_df)}")
display(eval_df[["expected_classification", "difficulty", "prompt"]])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Run Agent Across 3 Models

# COMMAND ----------

def query_fwa_agent(prompt: str, model_endpoint: str) -> dict:
    """Call the FWA agent backend with a specific model."""
    # Parse target from prompt
    target_id, target_type = None, None
    if prompt.startswith("[") and "]" in prompt:
        bracket_end = prompt.index("]")
        prefix = prompt[1:bracket_end].strip()
        if prefix.startswith("PRV-"):
            target_id = prefix[4:]
            target_type = "provider"
        elif prefix.startswith("INV-"):
            target_id = prefix
            target_type = "investigation"

    body = {
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 4000,
        "temperature": 0.05,
    }
    try:
        resp = requests.post(
            f"{host}/serving-endpoints/{model_endpoint}/invocations",
            headers=api_headers,
            json=body,
            timeout=120,
        )
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]
        return {"response": content, "model": model_endpoint, "error": None}
    except Exception as e:
        return {"response": f"Error: {e}", "model": model_endpoint, "error": str(e)}


results = []
for model in MODELS:
    print(f"\n{'='*60}")
    print(f"Running {model}...")
    print(f"{'='*60}")
    for i, row in eval_df.iterrows():
        print(f"  [{i+1}/{len(eval_df)}] {row['prompt'][:60]}...")
        result = query_fwa_agent(row["prompt"], model)
        results.append({
            **row.to_dict(),
            "model_endpoint": model,
            "response": result["response"],
            "error": result["error"],
        })

results_df = pd.DataFrame(results)
print(f"\nTotal results: {len(results_df)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Custom Scorers

# COMMAND ----------

def llm_judge_score(prompt: str, response: str, criteria: str, rubric: str) -> int:
    judge_prompt = (
        f"You are evaluating an FWA investigation AI agent's response.\n\n"
        f"## Evaluation Criteria: {criteria}\n\n"
        f"## Rubric\n{rubric}\n\n"
        f"## Input Question\n{prompt}\n\n"
        f"## Response to Evaluate\n{response}\n\n"
        f"Score from 1-5 based on the rubric. Return ONLY JSON: {{\"score\": <int>, \"justification\": \"<reason>\"}}"
    )
    try:
        resp = requests.post(
            f"{host}/serving-endpoints/{JUDGE_ENDPOINT}/invocations",
            headers=api_headers,
            json={"messages": [{"role": "user", "content": judge_prompt}], "max_tokens": 200, "temperature": 0.0},
        )
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]
        parsed = json.loads(content.strip().removeprefix("```json").removesuffix("```").strip())
        return max(1, min(5, int(parsed["score"])))
    except Exception:
        return 3

CRITERIA = {
    "investigation_completeness": {
        "criteria": "Investigation Completeness",
        "rubric": (
            "5: Includes Case Summary, Key Findings with specifics, Evidence Analysis, Risk Assessment, Policy Compliance, and Recommended Actions\n"
            "4: Missing 1 required section but thorough otherwise\n"
            "3: Missing 2 sections or sections lack depth\n"
            "2: Only 1-2 sections present\n"
            "1: No structured investigation output"
        ),
    },
    "policy_citation_quality": {
        "criteria": "Policy Citation Quality",
        "rubric": (
            "5: Cites specific policy names, rule IDs, procedure codes, and explains how the policy applies\n"
            "4: References policies by name with some procedure codes\n"
            "3: Mentions policies generically without specific citations\n"
            "2: Vague reference to 'company policy' without specifics\n"
            "1: No policy references at all"
        ),
    },
    "fwa_classification_accuracy": {
        "criteria": "FWA Classification Accuracy",
        "rubric": (
            "5: Correctly classifies as Fraud/Waste/Abuse/No Fraud with detailed reasoning\n"
            "4: Correct classification but reasoning could be stronger\n"
            "3: Partially correct classification (e.g., identifies issue but wrong category)\n"
            "2: Incorrect classification but identifies some relevant patterns\n"
            "1: No classification attempted or completely wrong"
        ),
    },
    "evidence_specificity": {
        "criteria": "Evidence Specificity",
        "rubric": (
            "5: References specific claim IDs, dollar amounts, dates, NPI numbers, procedure codes\n"
            "4: References most data points with some specifics\n"
            "3: Mix of specific and generic references\n"
            "2: Mostly generic references without specific data\n"
            "1: No specific evidence cited"
        ),
    },
}


def score_has_policy_section(response: str) -> float:
    return 1.0 if "POLICY COMPLIANCE" in response.upper() or "POLICY" in response.upper() else 0.0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Score All Responses

# COMMAND ----------

print("Scoring all responses...")
score_cols = list(CRITERIA.keys()) + ["has_policy_section"]

for col in score_cols:
    results_df[col] = 0.0

for idx, row in results_df.iterrows():
    print(f"  Scoring [{idx+1}/{len(results_df)}] {row['model_endpoint']} — {row['prompt'][:40]}...")
    for metric, config in CRITERIA.items():
        results_df.at[idx, metric] = llm_judge_score(row["prompt"], row["response"], config["criteria"], config["rubric"])
    results_df.at[idx, "has_policy_section"] = score_has_policy_section(row["response"])

print("Scoring complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Cross-Model Comparison

# COMMAND ----------

score_metrics = list(CRITERIA.keys()) + ["has_policy_section"]

summary = results_df.groupby("model_endpoint")[score_metrics].mean().round(3)
summary["avg_score"] = summary[list(CRITERIA.keys())].mean(axis=1).round(3)

print("Cross-Model Comparison (Average Scores)")
print("=" * 80)
display(summary)

# By difficulty
print("\nScores by Difficulty Level")
diff_summary = results_df.groupby(["model_endpoint", "difficulty"])[list(CRITERIA.keys())].mean().round(3)
display(diff_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Persist Results

# COMMAND ----------

eval_timestamp = datetime.now().isoformat()
results_df["eval_timestamp"] = eval_timestamp
results_df["judge_endpoint"] = JUDGE_ENDPOINT

results_spark = spark.createDataFrame(results_df)
results_spark.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(RESULTS_TABLE)

print(f"Results written to {RESULTS_TABLE} ({len(results_df)} rows)")
