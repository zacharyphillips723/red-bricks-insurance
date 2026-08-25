"""Denial-reasoning agent — Layer 3 of the scrub engine.

Retrieval-augmented reasoning over the shared medical-policy Vector Search
index (`prior_auth.medical_policy_vs_index`, built by setup_medical_policy_vs.py)
to judge the two policy-driven denial reasons that deterministic rules cannot:

  CO-50            — service not medically necessary (clinical criteria unmet)
  CO-55 / CO-96    — procedure is experimental / investigational, non-covered

Also hosts the shared Statement Execution + Foundation Model API helpers used
across the app (mirrors app-prior-auth/backend/agent.py).
"""

import json
import re
import traceback

import mlflow
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem

from .env_config import (
    UC_CATALOG, SQL_WAREHOUSE_ID, SCRUB_AGENT_ENDPOINT,
    VS_INDEX_NAME,
)

_CAT = f"`{UC_CATALOG}`"

# Schemas the app is allowed to read from via Statement Execution.
ALLOWED_SCHEMAS = ["claims", "members", "providers", "benefits", "prior_auth", "clinical", "analytics"]


# ---------------------------------------------------------------------------
# Statement Execution + Foundation Model API helpers (shared)
# ---------------------------------------------------------------------------

def _execute_sql(sql: str, params: list | None = None, poll_timeout_s: int = 120) -> list[dict]:
    import time as _time
    from databricks.sdk.service.sql import StatementState

    w = WorkspaceClient()
    kwargs = {
        "warehouse_id": SQL_WAREHOUSE_ID,
        "statement": sql,
        "wait_timeout": "30s",
    }
    if params:
        kwargs["parameters"] = [
            StatementParameterListItem(name=p["name"], value=p["value"], type=p.get("type", "STRING"))
            for p in params
        ]
    stmt = w.statement_execution.execute_statement(**kwargs)

    statement_id = stmt.statement_id
    deadline = _time.monotonic() + poll_timeout_s
    while stmt.status and stmt.status.state in (StatementState.PENDING, StatementState.RUNNING):
        if _time.monotonic() > deadline:
            raise TimeoutError(f"SQL statement {statement_id} did not finish within {poll_timeout_s}s")
        _time.sleep(1.5)
        stmt = w.statement_execution.get_statement(statement_id)

    if stmt.status and stmt.status.state != StatementState.SUCCEEDED:
        err = stmt.status.error.message if stmt.status.error else "unknown error"
        raise RuntimeError(f"SQL statement failed ({stmt.status.state}): {err}")

    if not stmt.result or not stmt.result.data_array:
        return []
    col_names = [c.name for c in stmt.manifest.schema.columns] if stmt.manifest and stmt.manifest.schema else []
    if not col_names:
        return []
    return [dict(zip(col_names, row)) for row in stmt.result.data_array]


def _sdk_request(method: str, path: str, body: dict | None = None) -> dict:
    w = WorkspaceClient()
    return w.api_client.do(method, path, body=body) if body else w.api_client.do(method, path)


def _content_to_text(content) -> str:
    """Normalize an LLM message `content` (str or typed-block list) to plain text."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for block in content:
            if isinstance(block, dict):
                parts.append(block.get("text") or block.get("content") or "")
            elif isinstance(block, str):
                parts.append(block)
        return "\n".join(p for p in parts if p)
    return str(content) if content is not None else ""


def _msg_content(data: dict) -> str:
    return _content_to_text(
        data.get("choices", [{}])[0].get("message", {}).get("content", "")
    )


def _parse_json_verdict(text: str) -> dict:
    """Extract the first JSON object from an LLM response."""
    if not text:
        return {}
    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if not match:
        return {}
    try:
        return json.loads(match.group(0))
    except json.JSONDecodeError:
        return {}


# ---------------------------------------------------------------------------
# Layer 3 — medical-policy RAG
# ---------------------------------------------------------------------------

@mlflow.trace(span_type="RETRIEVER", name="medical_policy_vector_search")
def _search_medical_policy(query: str, top_k: int = 5) -> list[dict]:
    """Retrieve the most relevant medical-policy chunks for a query."""
    try:
        vs_result = _sdk_request(
            "POST",
            f"/api/2.0/vector-search/indexes/{VS_INDEX_NAME}/query",
            {
                "query_text": query,
                "columns": ["policy_name", "service_category", "section", "chunk_text"],
                "num_results": top_k,
            },
        )
        data_array = vs_result.get("result", {}).get("data_array", [])
        columns = vs_result.get("manifest", {}).get("columns", [])
        col_names = [c.get("name", f"col_{i}") for i, c in enumerate(columns)]
        return [dict(zip(col_names, row)) for row in data_array]
    except Exception as e:
        print(f"[Denial Agent] VS search error: {e}")
        return []


_RAG_PROMPT = """You are a medical-policy reviewer for a health plan. A provider is about to
submit the request below. Using ONLY the retrieved medical-policy excerpts, judge two things:

1. EXPERIMENTAL / INVESTIGATIONAL — does policy list this procedure as experimental,
   investigational, or otherwise non-covered? (denial codes CO-55 / CO-96)
2. MEDICAL NECESSITY — do the submitted clinical notes satisfy the policy's clinical
   criteria for coverage? (denial code CO-50 if NOT met)

## Request
Procedure code(s): {procedures}
Diagnosis code(s): {diagnoses}
Clinical notes: {notes}

## Retrieved medical-policy excerpts
{context}

Respond with ONLY a JSON object:
{{"experimental": true|false,
  "experimental_confidence": 0.0-1.0,
  "experimental_evidence": "short quote from a policy excerpt, or empty",
  "medical_necessity_met": true|false,
  "medical_necessity_confidence": 0.0-1.0,
  "medical_necessity_evidence": "short quote from a policy excerpt, or empty",
  "cited_policy": "policy name"}}
If the excerpts do not cover this procedure, set both booleans conservatively
(experimental=false, medical_necessity_met=true) with low confidence."""


@mlflow.trace(span_type="AGENT", name="denial_policy_rag")
def assess_policy_rag(procedure_codes: list[str], diagnosis_codes: list[str],
                      clinical_notes: str) -> list[dict]:
    """Judge CO-50 / CO-55 / CO-96 against retrieved medical policy.

    Returns a list of finding dicts:
      {carc_code, reason_category, likelihood, evidence}
    """
    if not procedure_codes:
        return []

    query = (
        f"medical policy coverage clinical criteria for procedure "
        f"{' '.join(procedure_codes)} diagnosis {' '.join(diagnosis_codes)}. "
        f"{(clinical_notes or '')[:500]}"
    )
    chunks = _search_medical_policy(query, top_k=5)
    if not chunks:
        return []

    context = "\n\n".join(
        f"[{c.get('policy_name', '?')} — {c.get('section', '?')}]\n{c.get('chunk_text', '')}"
        for c in chunks
    )
    prompt = _RAG_PROMPT.format(
        procedures=", ".join(procedure_codes) or "none",
        diagnoses=", ".join(diagnosis_codes) or "none",
        notes=(clinical_notes or "none")[:1500],
        context=context[:6000],
    )

    try:
        data = _sdk_request(
            "POST",
            f"/serving-endpoints/{SCRUB_AGENT_ENDPOINT}/invocations",
            {"messages": [{"role": "user", "content": prompt}],
             "max_tokens": 800, "temperature": 0.0},
        )
        verdict = _parse_json_verdict(_msg_content(data))
    except Exception as e:
        print(f"[Denial Agent] RAG judgment error: {e}")
        traceback.print_exc()
        return []

    findings: list[dict] = []
    cited = verdict.get("cited_policy") or (chunks[0].get("policy_name") if chunks else None)

    if verdict.get("experimental") is True:
        conf = float(verdict.get("experimental_confidence") or 0.6)
        findings.append({
            "carc_code": "CO-55",
            "reason_category": "experimental",
            "likelihood": max(0.5, min(conf, 0.99)),
            "evidence": (verdict.get("experimental_evidence")
                         or f"Policy '{cited}' lists this procedure as experimental/investigational."),
        })

    if verdict.get("medical_necessity_met") is False:
        conf = float(verdict.get("medical_necessity_confidence") or 0.6)
        findings.append({
            "carc_code": "CO-50",
            "reason_category": "not_medically_necessary",
            "likelihood": max(0.5, min(conf, 0.99)),
            "evidence": (verdict.get("medical_necessity_evidence")
                         or f"Submitted clinical notes do not satisfy policy '{cited}' clinical criteria."),
        })

    return findings
