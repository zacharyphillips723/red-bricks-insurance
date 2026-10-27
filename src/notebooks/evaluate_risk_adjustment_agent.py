# Databricks notebook source
# MAGIC %md
# MAGIC # Evaluate the Risk Adjustment / HCC Agent (Phase 4)
# MAGIC
# MAGIC Scores the agent against the coherent ground truth in `analytics.ra_eval_golden`:
# MAGIC - **over_coding_rate** (PRIMARY SAFETY METRIC) — suspected candidates the agent emits that are
# MAGIC   NOT in the member's true `expected_suspected` set → false positives / RADV exposure. Deterministic;
# MAGIC   no LLM judge needed.
# MAGIC - **suspecting_recall** — fraction of true suspected HCCs the agent found.
# MAGIC - **citation_grounded** — every surfaced suspected candidate has `verified=true` + a real chunk.
# MAGIC
# MAGIC Plus a **model-swap regression gate** (§15.3) and a **learning-lift** harness (§15.4).
# MAGIC See DESIGN.md §15 / §19 Phase 4.

# COMMAND ----------
dbutils.widgets.text("catalog", "red_bricks_insurance_catalog", "Catalog")
dbutils.widgets.text("model_endpoint", "databricks-claude-sonnet-5", "Unity Gateway model to evaluate")
catalog = dbutils.widgets.get("catalog")
MODEL_ENDPOINT = dbutils.widgets.get("model_endpoint")

import json, re
golden = spark.table(f"{catalog}.analytics.ra_eval_golden").toPandas()

def to_set(s):
    return set(x for x in (s or "").split(",") if x)

dataset = [{
    "inputs": {"member_id": r.member_id},
    "expectations": {"expected_suspected": to_set(r.expected_suspected),
                     "expected_weak": to_set(r.expected_weak)},
} for r in golden.itertuples()]

# COMMAND ----------
# MAGIC %md ## predict_fn — run the agent (in-process) and parse its candidate JSON
import sys
sys.path.append(f"/Workspace/Users/{spark.sql('select current_user()').first()[0]}/ra_deploy/src/agents")
import importlib, risk_adjustment_agent as mod
importlib.reload(mod)
import types as _t
agent = mod.RiskAdjustmentAgent()
agent.load_context(_t.SimpleNamespace(model_config={
    "UC_CATALOG": catalog, "RA_SUSPECTING_ENDPOINT": MODEL_ENDPOINT,
    "VS_INDEX_NAME": f"{catalog}.documents.case_notes_vs_index",
    "RA_LAKEBASE_INSTANCE": "red-bricks-ra-memory"}))
from mlflow.types.llm import ChatMessage

def predict_fn(member_id: str):
    resp = agent.predict(None, [ChatMessage(role="user", content=f"[{member_id}] Review chart; surface suspected HCCs and flag weak coded HCCs.")])
    ans = resp.choices[0].message.content
    m = re.search(r"```json\s*(\{.*?\})\s*```", ans, re.DOTALL)
    cands = json.loads(m.group(1)).get("candidates", []) if m else []
    return {"candidates": cands}

# COMMAND ----------
# MAGIC %md ## Scorers (deterministic for the safety metrics)
from mlflow.genai.scorers import scorer

@scorer
def over_coding_rate(outputs, expectations):
    sus = [c for c in outputs["candidates"] if c.get("evidence_state") == "suspected"]
    if not sus:
        return 0.0
    truth = expectations["expected_suspected"]
    false_pos = [c for c in sus if c.get("hcc") not in truth]
    return len(false_pos) / len(sus)   # 0 = no over-coding; lower is better

@scorer
def suspecting_recall(outputs, expectations):
    truth = expectations["expected_suspected"]
    if not truth:
        return 1.0
    found = {c.get("hcc") for c in outputs["candidates"] if c.get("evidence_state") == "suspected"}
    return len(truth & found) / len(truth)

@scorer
def citation_grounded(outputs, expectations):
    sus = [c for c in outputs["candidates"] if c.get("evidence_state") == "suspected"]
    if not sus:
        return 1.0
    ok = [c for c in sus if c.get("verified") is True and c.get("chunk_id")]
    return len(ok) / len(sus)

# COMMAND ----------
# MAGIC %md ## Run evaluation
import mlflow
results = mlflow.genai.evaluate(
    data=[{"inputs": d["inputs"], "expectations": d["expectations"]} for d in dataset],
    predict_fn=lambda member_id: predict_fn(member_id),
    scorers=[over_coding_rate, suspecting_recall, citation_grounded],
)
metrics = results.metrics
print("METRICS:", {k: round(v, 3) for k, v in metrics.items() if isinstance(v, (int, float))})

# COMMAND ----------
# MAGIC %md ## Model-swap regression gate (§15.3) — call before promoting a new default model
OVER_CODING_MAX = 0.10      # tune per compliance tolerance
RECALL_MIN = 0.70

def passes_gate(metrics: dict) -> bool:
    oc = metrics.get("over_coding_rate/mean", 1.0)
    rc = metrics.get("suspecting_recall/mean", 0.0)
    cg = metrics.get("citation_grounded/mean", 0.0)
    ok = oc <= OVER_CODING_MAX and rc >= RECALL_MIN and cg >= 0.99
    print(f"GATE for {MODEL_ENDPOINT}: over_coding={oc:.3f}(≤{OVER_CODING_MAX}) recall={rc:.3f}(≥{RECALL_MIN}) "
          f"grounded={cg:.3f} -> {'PASS' if ok else 'BLOCK'}")
    return ok

passes_gate({k: v for k, v in metrics.items() if isinstance(v, (int, float))})

# COMMAND ----------
# MAGIC %md ## Learning-lift money-metric (§15.4)
# MAGIC Run the eval twice on repeat-provider members — once with memory recall enabled, once disabled —
# MAGIC and compare suspecting_recall / over_coding_rate. Positive lift proves the self-improving loop is
# MAGIC signal, not noise. Requires the served memory path active (psycopg2 + Lakebase grant).
