"""FWA Investigation Supervisor Agent — Agent-to-Agent Communication.

Implements a supervisor pattern where:
  1. Supervisor (Llama 4 Maverick) receives the analyst's question
  2. Routes to two sub-agents in parallel:
     - Genie sub-agent: structured claims data via natural language SQL
     - Gemini sub-agent: medical policy RAG + tool-calling analysis
  3. Supervisor synthesizes both responses into a unified investigation briefing

All calls are traced via MLflow with a full span hierarchy:
  AGENT (supervisor)
    ├── TOOL (genie_subagent)
    ├── AGENT (gemini_subagent)
    │     ├── TOOL (query_uc_table)
    │     ├── RETRIEVER (search_medical_policies)
    │     ├── TOOL (classify_fwa_type)
    │     └── LLM (gemini_call)
    └── LLM (supervisor_synthesis)
"""

import json
import os
import re
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

import mlflow
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem

# set_span_in_context / detach_span_from_context are semi-internal MLflow trace
# helpers. Import defensively so a future MLflow rename can't crash the agent —
# without them we still run sub-agents in parallel; only trace nesting degrades.
try:
    from mlflow.tracing.provider import (
        detach_span_from_context,
        set_span_in_context,
    )
except ImportError:  # pragma: no cover - version-dependent
    set_span_in_context = None
    detach_span_from_context = None

from .env_config import (
    UC_CATALOG, SQL_WAREHOUSE_ID, LLM_ENDPOINT, GEMINI_ENDPOINT,
    FWA_MODEL_ENDPOINT, VS_INDEX_NAME, GATEWAY_MODELS, GENIE_SPACE_ID,
)

_CAT = f"`{UC_CATALOG}`"
# Table references (used by direct API routes)
PROVIDER_RISK_TABLE = f"{_CAT}.fwa.gold_fwa_provider_risk"
CLAIM_FLAGS_TABLE = f"{_CAT}.fwa.gold_fwa_claim_flags"
MEMBER_RISK_TABLE = f"{_CAT}.analytics.gold_fwa_member_risk"
MODEL_INFERENCE_TABLE = f"{_CAT}.analytics.gold_fwa_model_scores"
FWA_SUMMARY_TABLE = f"{_CAT}.fwa.gold_fwa_summary"

# Allowed schemas the agent can query
ALLOWED_SCHEMAS = ["fwa", "analytics", "claims", "members", "providers", "pharmacy", "benefits", "prior_auth"]

# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

SUPERVISOR_SYSTEM_PROMPT = """You are the FWA Investigation Supervisor for Red Bricks Insurance.
You coordinate two specialized sub-agents to produce comprehensive investigation briefings:

1. **Genie** — queries structured claims data (billing totals, claim counts, procedure distributions)
2. **Gemini Analyst** — searches medical policies, analyzes compliance, classifies Fraud/Waste/Abuse

You receive their raw outputs and synthesize a unified briefing.

Your final response MUST include these sections:

## CASE SUMMARY
Brief overview of the investigation target, fraud types suspected, and current status.

## STRUCTURED DATA ANALYSIS
Key findings from claims data (from Genie): billing patterns, dollar amounts, claim volumes,
procedure code distributions. Cite specific numbers.

## POLICY COMPLIANCE ANALYSIS
Findings from medical policy review (from Gemini Analyst):
- Which policies were searched and what they say
- Whether billing practices comply with policy
- Classification of each finding as **Fraud**, **Waste**, or **Abuse** with reasoning
- Specific policy names, rule types, and procedure codes cited

## RISK ASSESSMENT
Risk rating: **Critical** / **High** / **Medium** / **Low** with justification.

## RECOMMENDED ACTIONS
Prioritized next steps with timeframes.

Always attribute which sub-agent provided each finding. Never fabricate evidence.

RESPONSE STYLE: Be concise. Use tight bullets with bolded key values (dollar
amounts, codes, risk rating). Keep each section brief and evidence-dense — no
filler, no restating the question, no generic disclaimers."""

GEMINI_SUBAGENT_PROMPT = """You are an FWA (Fraud, Waste & Abuse) Clinical Analyst for Red Bricks Insurance.
You specialize in medical policy compliance and clinical billing analysis.

You have tools to:
- Search medical policy documents via semantic similarity (Vector Search)
- Query Unity Catalog tables (only if the pre-fetched data below is missing something)
- Query the Lakebase investigation case database for investigation status and audit trail

## WORKFLOW — be efficient, minimize tool calls:

1. **Provider/claims data is already provided below.** It has been pre-fetched for you. Do NOT
   re-query it with query_uc_table. Only use query_uc_table if you need a specific detail that
   is genuinely absent from the provided data.

2. **Search medical policies** — You MUST call search_medical_policies for the procedure codes
   and billing patterns in the data. Make ONE combined search that covers the main procedure
   codes and fraud pattern (e.g. "upcoding E/M office visits 99214 99215"). Only make a second
   search if a genuinely distinct fraud type is involved. Do not over-search.

3. **Classify inline in your final answer** — Do NOT call a classification tool. Once you have
   the policy context, write your final analysis directly and, for each suspicious finding,
   state its classification as **Fraud**, **Waste**, or **Abuse** with a one-line rationale,
   a confidence level (High/Medium/Low), and the specific policy citation.

## CRITICAL RULES:
- NEVER skip search_medical_policies — our policies are the authoritative source for citing the
  specific rules being violated.
- Reference specific policy names, rule types, claim IDs, and dollar amounts.
- Be concise: tight, evidence-dense bullets with bolded key values, no filler.
- Aim to finish in 2 tool calls (one policy search, optionally one more) then answer."""

# ---------------------------------------------------------------------------
# Gemini sub-agent tools (same as before)
# ---------------------------------------------------------------------------

GEMINI_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "query_uc_table",
            "description": (
                f"Execute a read-only SQL SELECT query against Unity Catalog tables in the {_CAT} catalog. "
                "Key FWA tables:\n"
                f"- {_CAT}.fwa.gold_fwa_provider_risk: Provider risk scorecards\n"
                f"- {_CAT}.fwa.gold_fwa_claim_flags: Flagged claims with evidence\n"
                f"- {_CAT}.fwa.gold_fwa_summary: Aggregate FWA metrics\n"
                f"- {_CAT}.fwa.silver_fwa_signals: Individual FWA signals\n"
                f"- {_CAT}.analytics.gold_fwa_model_scores: ML model fraud predictions per claim\n"
                f"- {_CAT}.claims.silver_claims_medical: Medical claims detail\n"
                f"- {_CAT}.providers.silver_providers: Provider demographics\n"
                "Always include a LIMIT clause (max 50 rows). Only SELECT/WITH/DESCRIBE allowed."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "A read-only SQL SELECT query."},
                    "purpose": {"type": "string", "description": "Brief description of what this query looks up."},
                },
                "required": ["sql", "purpose"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_table_columns",
            "description": "List columns and types for a Unity Catalog table to understand its schema before querying.",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": f"Table name (e.g. 'fwa.gold_fwa_provider_risk'). Catalog '{_CAT}' is added automatically.",
                    },
                },
                "required": ["table_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "search_medical_policies",
            "description": (
                "Search Red Bricks Insurance medical policies using semantic similarity. "
                "Returns relevant policy sections with citations (policy name, rule ID, rule type, "
                "procedure codes, and the full policy text). Use this to determine if a provider's "
                "billing practices comply with company medical policy."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Natural language query describing the billing practice or policy question.",
                    },
                    "top_k": {
                        "type": "integer",
                        "description": "Number of policy sections to retrieve (default 5).",
                    },
                },
                "required": ["query"],
            },
        },
    },
    # NOTE: classify_fwa_type was removed as a tool — classification now happens
    # inline in the analyst's final answer, saving a full LLM round-trip (plus
    # the extra reasoning round the model spent processing each classify result).
    {
        "type": "function",
        "function": {
            "name": "query_lakebase_cases",
            "description": (
                "Query the Lakebase FWA investigation case database for operational data. "
                "Returns investigation details, audit trail, evidence records, and case status. "
                "Tables: fwa_investigations, investigation_audit_log, investigation_evidence, fraud_investigators. "
                "Use this to get the current status, timeline, assigned investigator, and prior actions on a case."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "SQL query against the fwa_cases Lakebase database. Key tables: fwa_investigations, investigation_audit_log, investigation_evidence, fraud_investigators.",
                    },
                    "purpose": {
                        "type": "string",
                        "description": "Brief description of what this query retrieves.",
                    },
                },
                "required": ["sql", "purpose"],
            },
        },
    },
]

MAX_TOOL_ROUNDS = 3


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------

def _run_with_parent_span(parent_span, fn, *args, **kwargs):
    """Run ``fn`` in the current thread with ``parent_span`` re-attached.

    MLflow keeps the active span in its own runtime context (contextvars),
    which does NOT automatically cross a ThreadPoolExecutor boundary — a naive
    contextvars.copy_context() silently splits the work into separate root
    traces. Re-attaching the parent span with set_span_in_context() ensures any
    @mlflow.trace spans created inside ``fn`` nest under the supervisor as
    children, preserving the single deep span tree this demo showcases.
    """
    if parent_span is None or set_span_in_context is None:
        return fn(*args, **kwargs)
    token = set_span_in_context(parent_span)
    try:
        return fn(*args, **kwargs)
    finally:
        detach_span_from_context(token)


def _execute_sql(sql: str, params: list | None = None) -> list[dict]:
    """Execute SQL via SDK Statement Execution API."""
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
    if not stmt.result or not stmt.result.data_array:
        return []
    col_names = [c.name for c in stmt.manifest.schema.columns] if stmt.manifest and stmt.manifest.schema else []
    if not col_names:
        return []
    return [dict(zip(col_names, row)) for row in stmt.result.data_array]


def _sdk_request(method: str, path: str, body: dict | None = None) -> dict:
    """Make an authenticated request using the SDK's api_client."""
    w = WorkspaceClient()
    return w.api_client.do(method, path, body=body) if body else w.api_client.do(method, path)


def _content_to_text(content) -> str:
    """Normalize an LLM message `content` to plain text.

    Some endpoints (e.g. Gemini via the Gateway) return content as a list of
    typed blocks — [{"type": "text", "text": "..."}] — rather than a bare
    string. Flatten those so downstream code (frontend markdown, synthesis
    context) always receives a string.
    """
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


def _validate_sql(sql: str) -> str | None:
    """Validate SQL is read-only and references allowed schemas."""
    sql_upper = sql.upper().lstrip()
    if not sql_upper.startswith("SELECT") and not sql_upper.startswith("WITH") and not sql_upper.startswith("DESCRIBE"):
        return "Only SELECT, WITH, and DESCRIBE statements are allowed."
    dangerous = re.compile(
        r'\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|GRANT|REVOKE|MERGE)\b',
        re.IGNORECASE,
    )
    if dangerous.search(sql):
        return "Write operations are not permitted."
    schema_found = any(s in sql.lower() for s in ALLOWED_SCHEMAS)
    if not schema_found and "information_schema" not in sql.lower():
        return f"Query must reference one of: {ALLOWED_SCHEMAS}"
    return None


def _execute_tool(tool_name: str, tool_args: dict) -> str:
    """Execute a tool call and return result as string."""
    if tool_name == "query_uc_table":
        sql = tool_args.get("sql", "").strip()
        purpose = tool_args.get("purpose", "")
        error = _validate_sql(sql)
        if error:
            return json.dumps({"error": error})
        if "LIMIT" not in sql.upper():
            sql = sql.rstrip(";") + " LIMIT 50"
        print(f"[Gemini Agent] Tool query ({purpose}): {sql[:200]}")
        try:
            rows = _execute_sql(sql)
            if not rows:
                return json.dumps({"result": [], "row_count": 0, "message": "No rows returned."})
            return json.dumps({"result": rows[:50], "row_count": len(rows)}, default=str)
        except Exception as e:
            return json.dumps({"error": f"SQL error: {str(e)}"})

    elif tool_name == "list_table_columns":
        table_name = tool_args.get("table_name", "").strip()
        table_name = table_name.replace(f"`{UC_CATALOG}`.", "").replace(f"{UC_CATALOG}.", "")
        table_name = f"{_CAT}.{table_name}"
        try:
            rows = _execute_sql(f"DESCRIBE TABLE {table_name}")
            return json.dumps({"table": table_name, "columns": rows}, default=str)
        except Exception as e:
            return json.dumps({"error": f"Could not describe table: {str(e)}"})

    elif tool_name == "search_medical_policies":
        query = tool_args.get("query", "").strip()
        top_k = tool_args.get("top_k", 5)
        if not query:
            return json.dumps({"error": "Query is required."})
        print(f"[Gemini Agent] Searching medical policies: {query[:100]}")
        try:
            with mlflow.start_span(name="search_medical_policies", span_type="RETRIEVER") as span:
                span.set_inputs({"query": query, "top_k": top_k})
                w = WorkspaceClient()
                vs_result = w.api_client.do(
                    "POST",
                    f"/api/2.0/vector-search/indexes/{VS_INDEX_NAME}/query",
                    body={
                        "query_text": query,
                        "columns": [
                            "chunk_id", "policy_name", "service_category",
                            "chunk_text",
                        ],
                        "num_results": top_k,
                    },
                )
                data_array = vs_result.get("result", {}).get("data_array", [])
                columns = vs_result.get("manifest", {}).get("columns", [])
                col_names = [c.get("name", f"col_{i}") for i, c in enumerate(columns)]
                policies = [dict(zip(col_names, row)) for row in data_array]
                # MLflow RETRIEVER span requires documents in outputs for trace viewer
                span.set_outputs({
                    "result_count": len(policies),
                    "documents": [
                        {
                            "page_content": p.get("chunk_text", ""),
                            "metadata": {
                                "chunk_id": p.get("chunk_id", ""),
                                "policy_name": p.get("policy_name", ""),
                                "service_category": p.get("service_category", ""),
                            },
                        }
                        for p in policies
                    ],
                })
                return json.dumps({"policies": policies, "result_count": len(policies)}, default=str)
        except Exception as e:
            print(f"[Gemini Agent] VS search error: {e}")
            return json.dumps({"error": f"Policy search failed: {str(e)}", "policies": []})

    elif tool_name == "classify_fwa_type":
        claim_summary = tool_args.get("claim_summary", "")
        policy_context = tool_args.get("policy_context", "")
        if not claim_summary:
            return json.dumps({"error": "claim_summary is required."})
        print(f"[Gemini Agent] Classifying FWA type for: {claim_summary[:80]}")
        try:
            classification_prompt = (
                "You are an FWA classification specialist. Based on the claim evidence and medical policy "
                "context below, classify the finding.\n\n"
                f"## Claim Summary\n{claim_summary}\n\n"
                f"## Relevant Medical Policies\n{policy_context}\n\n"
                "Respond with ONLY a JSON object:\n"
                '{"classification": "Fraud"|"Waste"|"Abuse"|"No Violation", '
                '"confidence": 0.0-1.0, '
                '"reasoning": "brief explanation", '
                '"policy_citations": ["policy name - rule type - procedure codes"]}'
            )
            resp = _sdk_request("POST", f"/serving-endpoints/{GEMINI_ENDPOINT}/invocations", {
                "messages": [{"role": "user", "content": classification_prompt}],
                "max_tokens": 500,
                "temperature": 0.0,
            })
            content = resp.get("choices", [{}])[0].get("message", {}).get("content", "{}")
            try:
                clean = content.strip().removeprefix("```json").removesuffix("```").strip()
                result = json.loads(clean)
            except json.JSONDecodeError:
                result = {"classification": "Unknown", "confidence": 0.0, "reasoning": content, "policy_citations": []}
            return json.dumps(result, default=str)
        except Exception as e:
            return json.dumps({"error": f"Classification failed: {str(e)}"})

    elif tool_name == "query_lakebase_cases":
        sql = tool_args.get("sql", "").strip()
        purpose = tool_args.get("purpose", "")
        if not sql:
            return json.dumps({"error": "SQL query is required."})
        # Safety: read-only queries only
        first_word = sql.split()[0].upper() if sql.split() else ""
        if first_word not in ("SELECT", "WITH"):
            return json.dumps({"error": "Only SELECT/WITH queries are allowed against Lakebase."})
        print(f"[Gemini Agent] Lakebase query ({purpose}): {sql[:100]}")
        try:
            import psycopg
            project_id = os.environ.get("LAKEBASE_PROJECT_ID", "red-bricks-insurance")
            branch = os.environ.get("LAKEBASE_BRANCH", "production")
            database_name = os.environ.get("LAKEBASE_DATABASE_NAME", "fwa_cases")
            endpoint_path = f"projects/{project_id}/branches/{branch}/endpoints/primary"
            w = WorkspaceClient()
            ep = w.postgres.get_endpoint(name=endpoint_path)
            host = ep.status.hosts.host
            cred = w.postgres.generate_database_credential(endpoint=endpoint_path)
            username = w.current_user.me().user_name
            conn = psycopg.connect(
                f"host={host} dbname={database_name} user={username} password={cred.token} sslmode=require"
            )
            cur = conn.cursor()
            cur.execute(sql)
            cols = [desc[0] for desc in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]
            cur.close()
            conn.close()
            return json.dumps({"purpose": purpose, "columns": cols, "rows": rows[:50], "row_count": len(rows)}, default=str)
        except Exception as e:
            return json.dumps({"error": f"Lakebase query failed: {str(e)}"})

    return json.dumps({"error": f"Unknown tool: {tool_name}"})


# ---------------------------------------------------------------------------
# Genie sub-agent
# ---------------------------------------------------------------------------

@mlflow.trace(span_type="TOOL", name="genie_subagent")
def _run_genie_subagent(target_id: str, target_type: str, question: str) -> dict:
    """Route structured data questions to Genie and return results."""
    from .genie import ask_genie

    # Build the Genie question deterministically. Genie is itself an NL->SQL
    # engine, so spending a full supervisor-LLM round-trip just to reword the
    # question adds latency for no benefit. For provider/investigation targets
    # we template a rich, multi-dimension question directly.
    if target_type == "provider" and target_id:
        genie_question = (
            f"For provider {target_id}: show total billed vs allowed amounts, "
            "claim counts, top procedure codes by volume, E/M visit level "
            "distribution (99211-99215), and denial rate. "
            f"Context: {question}"
        )
    elif target_type == "investigation" and target_id:
        genie_question = (
            f"For investigation {target_id}: summarize the associated claims — "
            "total billed, total paid, claim volume, top procedure codes, and "
            f"any billing anomalies. Context: {question}"
        )
    else:
        genie_question = question

    genie_questions = [genie_question]

    # Execute each Genie question
    results = []
    for gq in genie_questions[:1]:
        print(f"[Genie Sub-agent] Asking: {gq[:80]}")
        try:
            genie_result = ask_genie(
                question_text=gq,
                space_id=GENIE_SPACE_ID,
                warehouse_id=SQL_WAREHOUSE_ID,
            )
            results.append({
                "question": gq,
                "sql_query": genie_result.get("sql_query"),
                "columns": genie_result.get("columns", []),
                "rows": genie_result.get("rows", [])[:20],
                "row_count": genie_result.get("row_count", 0),
                "description": genie_result.get("description"),
            })
        except Exception as e:
            print(f"[Genie Sub-agent] Query failed: {e}")
            results.append({"question": gq, "error": str(e)})

    return {
        "agent": "genie",
        "model": "Genie (NL-to-SQL)",
        "questions_asked": len(results),
        "results": results,
    }


# ---------------------------------------------------------------------------
# Gemini sub-agent (tool-calling)
# ---------------------------------------------------------------------------

@mlflow.trace(span_type="TOOL", name="prefetch_provider_context")
def _prefetch_provider_context(npi: str) -> str | None:
    """Pre-fetch provider risk, flagged claims, and ML scores in parallel.

    These are deterministic lookups against fixed gold tables — there is no
    reason to spend serial Gemini tool rounds on them. Fetching them up front
    (concurrently) collapses ~2-3 LLM+tool round-trips into a single set of
    parallel SQL queries and lets the model jump straight to policy search.
    """
    # These three lookups are plain SQL and create no MLflow spans of their
    # own, so they can run on bare worker threads — no span context to carry.
    with ThreadPoolExecutor(max_workers=3) as pool:
        risk_f = pool.submit(get_provider_risk_profile, npi)
        claims_f = pool.submit(get_provider_flagged_claims, npi, 15)
        ml_f = pool.submit(get_provider_ml_scores, npi, 15)
        try:
            risk = risk_f.result()
            claims = claims_f.result()
            ml_scores = ml_f.result()
        except Exception as e:
            print(f"[Gemini Agent] Provider pre-fetch error: {e}")
            return None

    if not risk and not claims and not ml_scores:
        return None

    return (
        "Provider data (pre-fetched from Unity Catalog gold tables — no need to "
        "re-query these):\n"
        f"### Provider risk profile\n```json\n{json.dumps(risk, default=str, indent=1)}\n```\n"
        f"### Flagged claims (top {len(claims or [])} by fraud score)\n"
        f"```json\n{json.dumps(claims, default=str, indent=1)}\n```\n"
        f"### ML fraud model scores (top {len(ml_scores or [])})\n"
        f"```json\n{json.dumps(ml_scores, default=str, indent=1)}\n```\n\n"
    )


@mlflow.trace(span_type="AGENT", name="gemini_subagent")
def _build_policy_query(target_type: str, target_id: str, question: str, lb_json: str | None) -> str:
    """Derive a medical-policy search query from the case's fraud context."""
    terms: list[str] = []
    if lb_json:
        try:
            rec = json.loads(lb_json)
            ft = rec.get("fraud_types")
            if isinstance(ft, str):
                terms.append(ft.replace("|", " ").replace(",", " "))
            elif isinstance(ft, list):
                terms.extend(str(x) for x in ft)
            if rec.get("service_type"):
                terms.append(str(rec["service_type"]))
        except (json.JSONDecodeError, TypeError, AttributeError):
            pass
    base = " ".join(terms).strip()
    # Always include common E/M billing-integrity terms so the retrieval is useful
    # even when fraud_types is sparse.
    return (
        f"{base} upcoding unbundling duplicate billing E/M office visit coding standards"
        if base else
        f"{question} upcoding unbundling E/M office visit coding standards"
    ).strip()


def _prefetch_policy_chunks(query: str) -> list[dict]:
    """Deterministically run one medical-policy search and return raw chunks.

    Guarantees policy context is retrieved (and surfaced to the UI) regardless of
    whether the analyst model later chooses to call search_medical_policies.
    """
    try:
        result = _execute_tool("search_medical_policies", {"query": query, "top_k": 5})
        parsed = json.loads(result)
        return [
            {
                "chunk_id": p.get("chunk_id", ""),
                "policy_name": p.get("policy_name", ""),
                "service_category": p.get("service_category", ""),
                "chunk_text": p.get("chunk_text", ""),
            }
            for p in parsed.get("policies", [])
        ]
    except Exception as e:
        print(f"[Gemini Agent] policy pre-fetch failed: {e}")
        return []


def _run_gemini_subagent(target_id: str, target_type: str, question: str) -> dict:
    """Run the Gemini sub-agent with tool-calling for medical policy analysis."""
    context_hint = f"Question: {question}\nTarget: {target_id} (type: {target_type})\n\n"
    context_hint += f"The Unity Catalog is: {_CAT}\n"

    tables_queried = 0
    tools_used = []
    policy_chunks = []  # Collect raw VS retrieval results for frontend
    lb_data = None

    if target_type == "investigation":
        lb_data = _fetch_investigation_from_lakebase(target_id)
        if lb_data:
            context_hint += (
                f"Investigation record from Lakebase:\n```json\n{lb_data}\n```\n\n"
                "Query UC tables for flagged claims, provider risk, and ML model scores.\n"
            )
    elif target_type == "provider":
        prefetched = _prefetch_provider_context(target_id)
        if prefetched:
            context_hint += prefetched
        else:
            # Fall back to instructing the model to query the tables itself.
            context_hint += (
                f"Start by querying: SELECT * FROM {PROVIDER_RISK_TABLE} WHERE provider_npi = '{target_id}' LIMIT 1\n"
                f"Then query flagged claims from {CLAIM_FLAGS_TABLE} and ML predictions from {MODEL_INFERENCE_TABLE}.\n"
            )

    # Deterministically retrieve medical-policy context up front. This guarantees
    # the RAG step happens (and policy_chunks is populated for the UI) even if the
    # analyst model skips the search_medical_policies tool.
    policy_query = _build_policy_query(target_type, target_id, question, lb_data)
    policy_chunks = _prefetch_policy_chunks(policy_query)
    if policy_chunks:
        tools_used.append("search_medical_policies")
        _ctx = "\n".join(
            f"- [{c['policy_name']}] ({c['service_category']}): {c['chunk_text']}"
            for c in policy_chunks
        )
        context_hint += (
            f"\nRetrieved medical policy context (via Vector Search for '{policy_query}'):\n"
            f"{_ctx}\n\n"
            "Cite these specific policies in your POLICY COMPLIANCE ANALYSIS. You may call "
            "search_medical_policies again for a distinct fraud type if needed.\n"
        )
    else:
        context_hint += (
            "\nSearch medical policies for the procedure codes and fraud patterns, "
            "then classify each finding.\n"
        )

    messages = [
        {"role": "system", "content": GEMINI_SUBAGENT_PROMPT},
        {"role": "user", "content": context_hint},
    ]

    for _ in range(MAX_TOOL_ROUNDS):
        t0 = time.time()
        with mlflow.start_span(name="gemini_llm_call", span_type="LLM") as llm_span:
            llm_span.set_inputs({"model": GEMINI_ENDPOINT, "message_count": len(messages)})
            data = _sdk_request("POST", f"/serving-endpoints/{GEMINI_ENDPOINT}/invocations", {
                "messages": messages,
                "tools": GEMINI_TOOLS,
                "max_tokens": 4000,
                "temperature": 0.05,
            })
            usage = data.get("usage", {})
            llm_span.set_outputs({
                "tokens_input": usage.get("prompt_tokens", 0),
                "tokens_output": usage.get("completion_tokens", 0),
                "latency_ms": int((time.time() - t0) * 1000),
            })

        tool_calls = (
            data.get("choices", [{}])[0]
            .get("message", {})
            .get("tool_calls", [])
        )

        if not tool_calls:
            answer = _content_to_text(data.get("choices", [{}])[0].get("message", {}).get("content", "No response."))
            return {
                "agent": "gemini_analyst",
                "model": GEMINI_ENDPOINT,
                "answer": answer,
                "tables_queried": tables_queried,
                "tools_used": tools_used,
                "policy_chunks": policy_chunks,
            }

        messages.append(data["choices"][0]["message"])

        for tc in tool_calls:
            tool_name = tc.get("function", {}).get("name", "")
            try:
                tool_args = json.loads(tc.get("function", {}).get("arguments", "{}"))
            except json.JSONDecodeError:
                tool_args = {}
            tool_call_id = tc.get("id", "")

            span_type = "RETRIEVER" if tool_name == "search_medical_policies" else "TOOL"
            with mlflow.start_span(name=tool_name, span_type=span_type) as tool_span:
                tool_span.set_inputs(tool_args)
                result = _execute_tool(tool_name, tool_args)
                tool_span.set_outputs({"result_length": len(result)})

            # Capture raw policy chunks from VS retrieval (dedupe against the
            # deterministic pre-fetch by chunk_id).
            if tool_name == "search_medical_policies":
                try:
                    parsed = json.loads(result)
                    _seen = {c.get("chunk_id") for c in policy_chunks}
                    for p in parsed.get("policies", []):
                        if p.get("chunk_id") in _seen:
                            continue
                        policy_chunks.append({
                            "chunk_id": p.get("chunk_id", ""),
                            "policy_name": p.get("policy_name", ""),
                            "service_category": p.get("service_category", ""),
                            "chunk_text": p.get("chunk_text", ""),
                        })
                except (json.JSONDecodeError, TypeError):
                    pass

            tables_queried += 1
            tools_used.append(tool_name)

            messages.append({
                "role": "tool",
                "tool_call_id": tool_call_id,
                "content": result,
            })

    # Exhausted rounds — force final answer
    messages.append({"role": "user", "content": "Provide your final analysis now based on all data gathered."})
    data = _sdk_request("POST", f"/serving-endpoints/{GEMINI_ENDPOINT}/invocations", {
        "messages": messages,
        "max_tokens": 3000,
        "temperature": 0.05,
    })
    answer = _content_to_text(data.get("choices", [{}])[0].get("message", {}).get("content", "No response."))
    return {
        "agent": "gemini_analyst",
        "model": GEMINI_ENDPOINT,
        "answer": answer,
        "tables_queried": tables_queried,
        "tools_used": tools_used,
        "policy_chunks": policy_chunks,
    }


# ---------------------------------------------------------------------------
# Supervisor helpers (shared by blocking + streaming paths)
# ---------------------------------------------------------------------------

def _build_synthesis_context(target_id, target_type, question, genie_result, gemini_result) -> str:
    """Assemble the supervisor's synthesis prompt from sub-agent outputs."""
    ctx = f"Original question: {question}\nTarget: {target_id} ({target_type})\n\n"

    ctx += "## GENIE SUB-AGENT (Structured Claims Data)\n"
    if genie_result:
        for r in genie_result.get("results", []):
            ctx += f"\n### Question: {r.get('question', 'N/A')}\n"
            if r.get("error"):
                ctx += f"Error: {r['error']}\n"
            else:
                ctx += f"SQL: {r.get('sql_query', 'N/A')}\n"
                ctx += f"Rows returned: {r.get('row_count', 0)}\n"
                if r.get("rows"):
                    ctx += f"Data:\n```json\n{json.dumps(r['rows'][:10], default=str, indent=1)}\n```\n"
                if r.get("description"):
                    ctx += f"Description: {r['description']}\n"
    else:
        ctx += "Genie sub-agent did not return results.\n"

    ctx += "\n## GEMINI ANALYST SUB-AGENT (Medical Policy & Clinical Analysis)\n"
    if gemini_result:
        ctx += f"Model: {gemini_result.get('model', 'unknown')}\n"
        ctx += f"Tools used: {gemini_result.get('tools_used', [])}\n"
        ctx += f"Tables queried: {gemini_result.get('tables_queried', 0)}\n\n"
        ctx += f"Analysis:\n{gemini_result.get('answer', 'No analysis provided.')}\n"
    else:
        ctx += "Gemini sub-agent did not return results.\n"

    return ctx


def _build_sources(genie_result, gemini_result) -> list[dict]:
    return [
        {
            "type": "supervisor_agent",
            "supervisor_model": LLM_ENDPOINT,
            "gemini_model": GEMINI_ENDPOINT,
            "genie_questions": genie_result.get("questions_asked", 0) if genie_result else 0,
            "gemini_tables_queried": gemini_result.get("tables_queried", 0) if gemini_result else 0,
            "gemini_tools_used": gemini_result.get("tools_used", []) if gemini_result else [],
        }
    ]


def _synthesize(target_id, target_type, question, genie_result, gemini_result) -> str:
    """Run the supervisor synthesis LLM call and return the final briefing text."""
    synthesis_context = _build_synthesis_context(
        target_id, target_type, question, genie_result, gemini_result
    )
    with mlflow.start_span(name="supervisor_synthesis", span_type="LLM") as synth_span:
        synth_span.set_inputs({"model": LLM_ENDPOINT, "sub_agents": ["genie", "gemini"]})
        t0 = time.time()
        data = _sdk_request("POST", f"/serving-endpoints/{LLM_ENDPOINT}/invocations", {
            "messages": [
                {"role": "system", "content": SUPERVISOR_SYSTEM_PROMPT},
                {"role": "user", "content": synthesis_context},
            ],
            "max_tokens": 4000,
            "temperature": 0.1,
        })
        usage = data.get("usage", {})
        synth_span.set_outputs({
            "tokens_input": usage.get("prompt_tokens", 0),
            "tokens_output": usage.get("completion_tokens", 0),
            "latency_ms": int((time.time() - t0) * 1000),
        })
    return _content_to_text(data.get("choices", [{}])[0].get("message", {}).get("content", "No response."))


# ---------------------------------------------------------------------------
# Supervisor agent (orchestrator)
# ---------------------------------------------------------------------------

@mlflow.trace(span_type="AGENT", name="fwa_supervisor_agent")
def query_fwa_agent(target_id: str, target_type: str, question: str, model_endpoint: str | None = None) -> dict:
    """FWA Supervisor Agent — orchestrates Genie + Gemini sub-agents."""
    try:
        print(f"[Supervisor] Processing: {target_type} {target_id} — {question[:80]}...")

        # Phase 1: Dispatch to sub-agents in PARALLEL.
        # Genie and Gemini are independent — the supervisor only combines their
        # outputs at synthesis time — so we run them concurrently and pay
        # max(genie, gemini) instead of genie + gemini.
        #
        # Each sub-agent runs in its own thread with the supervisor span
        # re-attached (see _run_with_parent_span), so their @mlflow.trace spans
        # nest under the supervisor as siblings instead of splitting into
        # separate root traces.
        genie_result = None
        gemini_result = None

        parent_span = mlflow.get_current_active_span()

        def _dispatch(fn):
            return _run_with_parent_span(
                parent_span, fn, target_id, target_type, question
            )

        with ThreadPoolExecutor(max_workers=2) as pool:
            genie_future = pool.submit(_dispatch, _run_genie_subagent)
            gemini_future = pool.submit(_dispatch, _run_gemini_subagent)

            try:
                genie_result = genie_future.result()
            except Exception as e:
                print(f"[Supervisor] Genie sub-agent error: {e}")
                traceback.print_exc()

            try:
                gemini_result = gemini_future.result()
            except Exception as e:
                print(f"[Supervisor] Gemini sub-agent error: {e}")
                traceback.print_exc()

        # Phase 2 + 3: Synthesize the final briefing from both sub-agents.
        answer = _synthesize(target_id, target_type, question, genie_result, gemini_result)
        sources = _build_sources(genie_result, gemini_result)
        policy_chunks = gemini_result.get("policy_chunks", []) if gemini_result else []

        return {
            "answer": answer,
            "sources": sources,
            "model_used": f"{LLM_ENDPOINT} (supervisor) + {GEMINI_ENDPOINT} (analyst)",
            "policy_chunks": policy_chunks,
        }

    except Exception as e:
        print(f"[Supervisor] ERROR: {e}")
        traceback.print_exc()
        return {"answer": f"Error: {str(e)}", "sources": [], "model_used": LLM_ENDPOINT}


@mlflow.trace(span_type="AGENT", name="fwa_supervisor_agent_stream")
def stream_fwa_agent(target_id: str, target_type: str, question: str):
    """Streaming variant of query_fwa_agent — yields milestone events.

    Emits events as each stage completes so the UI can render progress and the
    early-finishing Gemini analysis long before the slower Genie query lands:

        status   → {"stage": "...", "message": "..."}
        gemini   → clinical analysis + policy_chunks (arrives ~18s)
        genie    → structured claims data (arrives ~40s)
        final    → synthesized briefing + sources
        error    → {"message": "..."}

    Each yielded value is a (event_type, payload) tuple; the route serializes
    them as Server-Sent Events.
    """
    try:
        yield ("status", {"stage": "dispatching",
                          "message": "Routing to Genie + Gemini sub-agents in parallel…"})

        parent_span = mlflow.get_current_active_span()

        def _dispatch(fn):
            return _run_with_parent_span(parent_span, fn, target_id, target_type, question)

        genie_result = None
        gemini_result = None

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = {
                pool.submit(_dispatch, _run_genie_subagent): "genie",
                pool.submit(_dispatch, _run_gemini_subagent): "gemini",
            }
            # Emit each sub-agent's result the moment it finishes, regardless of
            # order — Gemini (~18s) lands well before Genie (~40s).
            for fut in as_completed(futures):
                which = futures[fut]
                try:
                    res = fut.result()
                except Exception as e:
                    print(f"[Supervisor:stream] {which} sub-agent error: {e}")
                    traceback.print_exc()
                    yield ("status", {"stage": f"{which}_error",
                                      "message": f"{which} sub-agent failed: {e}"})
                    continue

                if which == "gemini":
                    gemini_result = res
                    yield ("gemini", {
                        "analysis": res.get("answer", "") if res else "",
                        "tools_used": res.get("tools_used", []) if res else [],
                        "tables_queried": res.get("tables_queried", 0) if res else 0,
                        "policy_chunks": res.get("policy_chunks", []) if res else [],
                        "model": res.get("model", GEMINI_ENDPOINT) if res else GEMINI_ENDPOINT,
                    })
                    yield ("status", {"stage": "gemini_complete",
                                      "message": "Clinical policy analysis ready — waiting on claims data…"})
                else:
                    genie_result = res
                    yield ("genie", {"results": res.get("results", []) if res else [],
                                     "questions_asked": res.get("questions_asked", 0) if res else 0})
                    yield ("status", {"stage": "genie_complete",
                                      "message": "Structured claims data retrieved."})

        yield ("status", {"stage": "synthesizing",
                          "message": "Synthesizing unified investigation briefing…"})

        answer = _synthesize(target_id, target_type, question, genie_result, gemini_result)

        yield ("final", {
            "answer": answer,
            "sources": _build_sources(genie_result, gemini_result),
            "model_used": f"{LLM_ENDPOINT} (supervisor) + {GEMINI_ENDPOINT} (analyst)",
            "policy_chunks": gemini_result.get("policy_chunks", []) if gemini_result else [],
        })

    except Exception as e:
        print(f"[Supervisor:stream] ERROR: {e}")
        traceback.print_exc()
        yield ("error", {"message": str(e)})


# ---------------------------------------------------------------------------
# Direct data access functions (used by API routes, not the agent)
# ---------------------------------------------------------------------------

def _fetch_investigation_from_lakebase(inv_id: str) -> str | None:
    """Pre-fetch investigation details from Lakebase for agent context."""
    try:
        import psycopg

        project_id = os.environ.get("LAKEBASE_PROJECT_ID", "red-bricks-insurance")
        branch = os.environ.get("LAKEBASE_BRANCH", "production")
        database_name = os.environ.get("LAKEBASE_DATABASE_NAME", "fwa_cases")
        endpoint_path = f"projects/{project_id}/branches/{branch}/endpoints/primary"

        w = WorkspaceClient()
        ep = w.postgres.get_endpoint(name=endpoint_path)
        host = ep.status.hosts.host
        cred = w.postgres.generate_database_credential(endpoint=endpoint_path)
        username = w.current_user.me().user_name
        conn_string = (
            f"host={host} "
            f"dbname={database_name} "
            f"user={username} "
            f"password={cred.token} "
            f"sslmode=require"
        )
        conn = psycopg.connect(conn_string)
        cur = conn.cursor()
        cur.execute("""
            SELECT i.investigation_id, i.investigation_type::text, i.target_type, i.target_id,
                   i.target_name, i.fraud_types, i.severity::text, i.status::text, i.source::text,
                   inv.display_name AS investigator_name,
                   i.estimated_overpayment, i.confirmed_overpayment, i.recovered_amount,
                   i.claims_involved_count, i.investigation_summary, i.evidence_summary,
                   i.recommendation, i.rules_risk_score, i.ml_risk_score, i.composite_risk_score,
                   i.created_at, i.updated_at, i.closed_at
            FROM fwa_investigations i
            LEFT JOIN fraud_investigators inv ON i.assigned_investigator_id = inv.investigator_id
            WHERE i.investigation_id = %s
        """, (inv_id,))
        cols = [desc[0] for desc in cur.description]
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            return None
        data = dict(zip(cols, row))
        return json.dumps(data, default=str, indent=2)
    except Exception as e:
        print(f"[Supervisor] Lakebase fetch error: {e}")
        return None


@mlflow.trace(name="fwa_supervisor_endpoint", span_type="AGENT")
def query_fwa_agent_via_endpoint(target_id: str, target_type: str, question: str, model_endpoint: str | None = None) -> dict:
    """Call the FWA supervisor agent via Model Serving endpoint.

    Uses the same input format as query_fwa_agent() but routes through
    the served model endpoint. The @mlflow.trace decorator on this function
    captures request/response/latency in the app's UC-enabled experiment,
    giving real-time trace visibility in the OTel spans table.
    """
    w = WorkspaceClient()

    endpoint_name = os.environ.get("FWA_AGENT_ENDPOINT", "fwa-supervisor-agent")

    # Format input as ChatModel expects: [TARGET_ID] question
    if target_id:
        prefix = target_id if target_id.startswith(("INV-", "PRV-", "MBR-")) else f"PRV-{target_id}"
        user_msg = f"[{prefix}] {question}"
    else:
        user_msg = question

    print(f"[Endpoint] Querying {endpoint_name}: {user_msg[:80]}...")

    try:
        response = w.serving_endpoints.query(
            name=endpoint_name,
            messages=[{"role": "user", "content": user_msg}],
            max_tokens=4000,
        )

        content = response.choices[0].message.content

        return {
            "answer": content,
            "sources": [{"type": "served_endpoint", "endpoint": endpoint_name}],
            "model_used": f"{endpoint_name} (served)",
            "policy_chunks": [],
        }
    except Exception as e:
        print(f"[Endpoint] Error calling {endpoint_name}: {e}")
        traceback.print_exc()
        # Fallback to in-process agent
        print("[Endpoint] Falling back to in-process agent...")
        return query_fwa_agent(target_id, target_type, question, model_endpoint)


def get_provider_risk_profile(npi: str) -> dict | None:
    try:
        rows = _execute_sql(
            f"SELECT * FROM {PROVIDER_RISK_TABLE} WHERE provider_npi = :npi LIMIT 1",
            [{"name": "npi", "value": npi}],
        )
        return rows[0] if rows else None
    except Exception as e:
        print(f"[FWA] Provider risk error: {e}")
        return None


def get_provider_flagged_claims(npi: str, limit: int = 30) -> list[dict]:
    try:
        return _execute_sql(
            f"""SELECT signal_id, claim_id, member_id, fraud_type, fraud_score, severity,
                       evidence_summary, estimated_overpayment, service_date,
                       procedure_code, billed_amount, claim_paid_amount, line_of_business
                FROM {CLAIM_FLAGS_TABLE}
                WHERE provider_npi = :npi
                ORDER BY fraud_score DESC LIMIT {limit}""",
            [{"name": "npi", "value": npi}],
        )
    except Exception as e:
        print(f"[FWA] Flagged claims error: {e}")
        return []


def get_provider_ml_scores(npi: str, limit: int = 20) -> list[dict]:
    try:
        return _execute_sql(
            f"""SELECT claim_id, ml_fraud_probability, ml_risk_tier,
                       billed_amount, paid_amount, procedure_code, claim_type
                FROM {MODEL_INFERENCE_TABLE}
                WHERE provider_npi = :npi
                ORDER BY ml_fraud_probability DESC LIMIT {limit}""",
            [{"name": "npi", "value": npi}],
        )
    except Exception as e:
        print(f"[FWA] ML scores error: {e}")
        return []


def get_provider_shap_values(npi: str) -> dict | None:
    try:
        provider_rows = _execute_sql(
            f"""SELECT provider_npi, composite_risk_score, e5_visit_pct,
                       billed_to_allowed_ratio, denial_rate, fwa_signal_count,
                       fwa_avg_score, total_claims, total_billed, total_paid
                FROM {PROVIDER_RISK_TABLE}
                WHERE provider_npi = :npi LIMIT 1""",
            [{"name": "npi", "value": npi}],
        )
        if not provider_rows:
            return None
        provider = provider_rows[0]
        pop_rows = _execute_sql(
            f"""SELECT
                    AVG(CAST(e5_visit_pct AS DOUBLE)) AS avg_e5_visit_pct,
                    AVG(CAST(billed_to_allowed_ratio AS DOUBLE)) AS avg_billed_to_allowed_ratio,
                    AVG(CAST(denial_rate AS DOUBLE)) AS avg_denial_rate,
                    AVG(CAST(fwa_signal_count AS DOUBLE)) AS avg_fwa_signal_count,
                    AVG(CAST(fwa_avg_score AS DOUBLE)) AS avg_fwa_avg_score,
                    AVG(CAST(total_claims AS DOUBLE)) AS avg_total_claims,
                    AVG(CAST(total_billed AS DOUBLE)) AS avg_total_billed
                FROM {PROVIDER_RISK_TABLE}"""
        )
        if not pop_rows:
            return None
        pop = pop_rows[0]

        def _sf(val) -> float:
            try:
                return float(val) if val is not None else 0.0
            except (ValueError, TypeError):
                return 0.0

        features = {
            "E5 Visit %": _sf(provider.get("e5_visit_pct")) - _sf(pop.get("avg_e5_visit_pct")),
            "Billed/Allowed Ratio": _sf(provider.get("billed_to_allowed_ratio")) - _sf(pop.get("avg_billed_to_allowed_ratio")),
            "Denial Rate": _sf(provider.get("denial_rate")) - _sf(pop.get("avg_denial_rate")),
            "FWA Signal Count": _sf(provider.get("fwa_signal_count")) - _sf(pop.get("avg_fwa_signal_count")),
            "Avg Fraud Score": _sf(provider.get("fwa_avg_score")) - _sf(pop.get("avg_fwa_avg_score")),
            "Total Claims Volume": _sf(provider.get("total_claims")) - _sf(pop.get("avg_total_claims")),
            "Total Billed Amount": _sf(provider.get("total_billed")) - _sf(pop.get("avg_total_billed")),
        }
        composite = _sf(provider.get("composite_risk_score"))
        abs_total = sum(abs(v) for v in features.values())
        if abs_total > 0 and composite > 0:
            scale_factor = composite / abs_total
            features = {k: round(v * scale_factor, 4) for k, v in features.items()}
        else:
            features = {k: round(v, 4) for k, v in features.items()}
        return features
    except Exception as e:
        print(f"[FWA] SHAP values error: {e}")
        return None


def get_dashboard_analytics() -> dict:
    try:
        summary = _execute_sql(f"""
            SELECT
                SUM(signal_count) AS total_signals,
                SUM(total_estimated_overpayment) AS total_overpayment,
                COUNT(DISTINCT fraud_type) AS fraud_types,
                SUM(distinct_providers) AS flagged_providers,
                SUM(distinct_members) AS flagged_members
            FROM (
                SELECT fraud_type, SUM(signal_count) AS signal_count,
                       SUM(total_estimated_overpayment) AS total_estimated_overpayment,
                       SUM(distinct_providers) AS distinct_providers,
                       SUM(distinct_members) AS distinct_members
                FROM {FWA_SUMMARY_TABLE}
                GROUP BY fraud_type
            )
        """)
        return summary[0] if summary else {}
    except Exception as e:
        print(f"[FWA] Dashboard analytics error: {e}")
        return {}
