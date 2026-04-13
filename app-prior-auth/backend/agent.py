"""PA Review Agent and analytics data integration module.

Tool-calling LLM agent for clinical PA review using Foundation Model API
+ Statement Execution API for dynamic UC table access.
"""

import json
import os
import re
import traceback

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem

from .env_config import UC_CATALOG, SQL_WAREHOUSE_ID, LLM_ENDPOINT

_CAT = f"`{UC_CATALOG}`"

# Allowed schemas the agent can query
ALLOWED_SCHEMAS = ["prior_auth", "members", "providers", "claims", "clinical", "analytics"]

SYSTEM_PROMPT = """You are a Prior Authorization Clinical Review Specialist for Red Bricks Insurance.
You help UM nurses and medical directors review PA requests by querying structured data
and synthesizing findings into actionable clinical review briefings.

You have access to tools that let you query Unity Catalog tables directly. Use them to:
- Look up PA request details, medical policy rules, and clinical criteria
- Retrieve ML model predictions and AI-extracted clinical facts
- Find member history, provider patterns, and related claims
- Check policy coverage and Tier 1 deterministic rule evaluations

Strategy: First gather all relevant data using tool calls, then synthesize.

Your final response MUST include these sections:

## REQUEST SUMMARY
Brief overview of the PA request: member, procedure, diagnosis, and requesting provider.

## CLINICAL EVIDENCE
Key clinical facts from the clinical summary and AI extraction. Include lab values,
diagnoses, treatments tried, and functional status.

## POLICY ANALYSIS
Which medical policy applies, what criteria must be met, and how the submitted
evidence maps to those criteria.

## AI ASSESSMENT
Summary of ML model prediction, confidence score, and Tier 1 rule evaluation results.

## RECOMMENDATION
Approve, deny, or request additional information — with specific justification
citing policy criteria and clinical evidence.

Always cite data sources. Never fabricate clinical evidence."""

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "query_uc_table",
            "description": (
                f"Execute a read-only SQL SELECT query against Unity Catalog tables in the {_CAT} catalog. "
                "Key PA tables:\n"
                f"- {_CAT}.prior_auth.gold_pa_requests: Enriched PA requests with determinations\n"
                f"- {_CAT}.prior_auth.gold_pa_metrics: Monthly PA metrics and compliance\n"
                f"- {_CAT}.prior_auth.gold_pa_provider_patterns: Provider PA patterns\n"
                f"- {_CAT}.prior_auth.gold_pa_policy_utilization: Policy usage metrics\n"
                f"- {_CAT}.prior_auth.gold_pa_tier1_evaluation: Deterministic rules results\n"
                f"- {_CAT}.prior_auth.gold_pa_clinical_analysis: AI clinical extraction\n"
                f"- {_CAT}.prior_auth.silver_medical_policy_rules: Medical policy rules\n"
                f"- {_CAT}.prior_auth.parsed_medical_policies: LLM-enriched policy rules\n"
                f"- {_CAT}.prior_auth.pa_ml_predictions: ML model predictions\n"
                f"- {_CAT}.prior_auth.policy_summaries: Policy narrative summaries\n"
                f"- {_CAT}.members.silver_enrollment: Member enrollment\n"
                f"- {_CAT}.providers.silver_providers: Provider demographics\n"
                f"- {_CAT}.clinical.silver_conditions: Patient conditions\n"
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
                        "description": f"Table name (e.g. 'prior_auth.gold_pa_requests'). Catalog '{_CAT}' is added automatically.",
                    },
                },
                "required": ["table_name"],
            },
        },
    },
]

MAX_TOOL_ROUNDS = 6


_KNOWN_HEADERS = [
    "REQUEST SUMMARY", "CLINICAL EVIDENCE", "POLICY ANALYSIS",
    "AI ASSESSMENT", "RECOMMENDATION",
]


def _clean_answer(text: str) -> str:
    """Strip raw tool-call artifacts and ensure markdown headers are formatted.

    If the response contains markdown headers (## ...), extract from the first
    header onward. Also ensure known section headers have ## prefix so
    react-markdown renders them as headings.
    """
    if not text or not text.strip():
        return text or "No response generated."

    # If response has structured markdown headers, extract from first one
    header_match = re.search(r'^#+\s+', text, flags=re.MULTILINE)
    if header_match:
        text = text[header_match.start():]

    # Ensure known section headers have ## prefix (model sometimes omits them)
    for h in _KNOWN_HEADERS:
        text = re.sub(rf'^(?!#)({re.escape(h)})\s*$', rf'## \1', text, flags=re.MULTILINE)

    return text.strip()


def _execute_sql(sql: str, params: list | None = None) -> list[dict]:
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
    w = WorkspaceClient()
    return w.api_client.do(method, path, body=body) if body else w.api_client.do(method, path)


def _validate_sql(sql: str) -> str | None:
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
    if tool_name == "query_uc_table":
        sql = tool_args.get("sql", "").strip()
        purpose = tool_args.get("purpose", "")

        error = _validate_sql(sql)
        if error:
            return json.dumps({"error": error})

        if "LIMIT" not in sql.upper():
            sql = sql.rstrip(";") + " LIMIT 50"

        print(f"[PA Agent] Tool query ({purpose}): {sql[:200]}")
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

    return json.dumps({"error": f"Unknown tool: {tool_name}"})


# ---------------------------------------------------------------------------
# Direct data access functions (used by API routes)
# ---------------------------------------------------------------------------

def get_pa_analytics() -> dict:
    """Get PA dashboard analytics from gold tables."""
    try:
        summary = _execute_sql(f"""
            SELECT
                COUNT(*) AS total_requests,
                SUM(CASE WHEN determination = 'approved' THEN 1 ELSE 0 END) AS approved,
                SUM(CASE WHEN determination = 'denied' THEN 1 ELSE 0 END) AS denied,
                ROUND(AVG(turnaround_hours), 1) AS avg_turnaround,
                ROUND(SUM(CASE WHEN cms_compliant THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS cms_rate,
                ROUND(SUM(CASE WHEN determination_tier = 'tier_1_auto' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS auto_rate
            FROM {_CAT}.prior_auth.gold_pa_requests
        """)
        return summary[0] if summary else {}
    except Exception as e:
        print(f"[PA] Analytics error: {e}")
        return {}


def get_policy_rules(policy_id: str) -> list[dict]:
    """Get medical policy rules for a specific policy."""
    try:
        return _execute_sql(
            f"""SELECT rule_id, rule_type, rule_text, procedure_codes, diagnosis_codes
                FROM {_CAT}.prior_auth.silver_medical_policy_rules
                WHERE policy_id = :pid
                ORDER BY rule_type""",
            [{"name": "pid", "value": policy_id}],
        )
    except Exception as e:
        print(f"[PA] Policy rules error: {e}")
        return []


def get_ml_prediction(auth_request_id: str) -> dict | None:
    """Get ML prediction for a specific PA request."""
    try:
        rows = _execute_sql(
            f"""SELECT predicted_determination, confidence, predicted_at
                FROM {_CAT}.prior_auth.pa_ml_predictions
                WHERE auth_request_id = :aid LIMIT 1""",
            [{"name": "aid", "value": auth_request_id}],
        )
        return rows[0] if rows else None
    except Exception as e:
        print(f"[PA] ML prediction error: {e}")
        return None


# ---------------------------------------------------------------------------
# Pre-fetch PA request from Lakebase for agent context
# ---------------------------------------------------------------------------

def _fetch_pa_request_from_lakebase(auth_request_id: str) -> str | None:
    try:
        import psycopg

        project_id = os.environ.get("LAKEBASE_PROJECT_ID", "red-bricks-insurance")
        branch = os.environ.get("LAKEBASE_BRANCH", "production")
        database_name = os.environ.get("LAKEBASE_DATABASE_NAME", "pa_reviews")
        endpoint_path = f"projects/{project_id}/branches/{branch}/endpoints/primary"

        w = WorkspaceClient()
        ep = w.postgres.get_endpoint(name=endpoint_path)
        host = ep.status.hosts.host
        cred = w.postgres.generate_database_credential(endpoint=endpoint_path)
        username = w.current_user.me().user_name
        conn = psycopg.connect(
            f"host={host} dbname={database_name} user={username} "
            f"password={cred.token} sslmode=require"
        )
        cur = conn.cursor()
        cur.execute("""
            SELECT q.auth_request_id, q.member_id, q.member_name,
                   q.requesting_provider_npi, q.provider_name,
                   q.service_type, q.procedure_code, q.procedure_description,
                   q.diagnosis_codes, q.policy_id, q.policy_name,
                   q.line_of_business, q.clinical_summary,
                   q.urgency::text, q.estimated_cost, q.status::text,
                   q.determination_tier::text, q.ai_recommendation, q.ai_confidence,
                   q.tier1_auto_eligible, q.clinical_extraction,
                   r.display_name AS reviewer_name,
                   q.request_date, q.determination_date, q.cms_deadline
            FROM pa_review_queue q
            LEFT JOIN pa_reviewers r ON q.assigned_reviewer_id = r.reviewer_id
            WHERE q.auth_request_id = %s
        """, (auth_request_id,))
        cols = [desc[0] for desc in cur.description]
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            return None
        data = dict(zip(cols, row))
        return json.dumps(data, default=str, indent=2)
    except Exception as e:
        print(f"[PA Agent] Lakebase fetch error: {e}")
        return None


# ---------------------------------------------------------------------------
# Tool-calling agent
# ---------------------------------------------------------------------------

def query_pa_agent(auth_request_id: str, question: str) -> dict:
    """PA review agent with tool-calling for dynamic UC table access."""
    try:
        print(f"[PA Agent] Processing query for {auth_request_id}: {question[:80]}...")

        context_hint = f"Question: {question}\n"
        context_hint += f"The Unity Catalog is: {_CAT}\n"
        context_hint += "Use the query_uc_table tool to gather all relevant data before generating your analysis.\n\n"

        if auth_request_id:
            lb_data = _fetch_pa_request_from_lakebase(auth_request_id)
            if lb_data:
                context_hint += (
                    f"Here is the PA request record from the operational database:\n"
                    f"```json\n{lb_data}\n```\n\n"
                    "Use this data as the basis for your review. "
                    "Now query UC tables for: medical policy rules, ML predictions, "
                    "provider PA patterns, tier1 evaluation results, and member clinical history.\n"
                )
            else:
                context_hint += (
                    f"PA request {auth_request_id} was not found in Lakebase. "
                    f"Try querying: SELECT * FROM {_CAT}.prior_auth.gold_pa_requests "
                    f"WHERE auth_request_id = '{auth_request_id}' LIMIT 1\n"
                )

        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": context_hint},
        ]

        tables_queried = 0

        for _ in range(MAX_TOOL_ROUNDS):
            data = _sdk_request("POST", f"/serving-endpoints/{LLM_ENDPOINT}/invocations", {
                "messages": messages,
                "tools": TOOLS,
                "max_tokens": 4000,
                "temperature": 0.05,
            })

            tool_calls = (
                data.get("choices", [{}])[0]
                .get("message", {})
                .get("tool_calls", [])
            )

            if not tool_calls:
                # Model stopped making tool calls — check if the response
                # is a real synthesis or just raw tool-call text
                answer = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                has_synthesis = bool(re.search(r'^#+\s+', answer, re.MULTILINE))

                if has_synthesis and tables_queried > 0:
                    # Model produced a proper structured answer
                    return {"answer": _clean_answer(answer), "sources": [{"type": "pa_tool_calling", "tables_queried": tables_queried}]}
                # Otherwise fall through to the forced synthesis call below
                break

            messages.append(data["choices"][0]["message"])

            for tc in tool_calls:
                tool_name = tc.get("function", {}).get("name", "")
                try:
                    tool_args = json.loads(tc.get("function", {}).get("arguments", "{}"))
                except json.JSONDecodeError:
                    tool_args = {}
                tool_call_id = tc.get("id", "")

                result = _execute_tool(tool_name, tool_args)
                tables_queried += 1

                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call_id,
                    "content": result,
                })

        # Force a final synthesis call WITHOUT tools so the model must
        # generate a structured text response instead of more tool calls
        print(f"[PA Agent] Forcing synthesis call ({tables_queried} tables queried)")
        messages.append({
            "role": "user",
            "content": (
                "Based on ALL the data gathered above, provide your final clinical review. "
                "Structure your response with these markdown sections:\n"
                "## REQUEST SUMMARY\n## CLINICAL EVIDENCE\n## POLICY ANALYSIS\n"
                "## AI ASSESSMENT\n## RECOMMENDATION\n"
                "Do NOT make any more tool calls. Synthesize the data you already have."
            ),
        })
        data = _sdk_request("POST", f"/serving-endpoints/{LLM_ENDPOINT}/invocations", {
            "messages": messages,
            "max_tokens": 3000,
            "temperature": 0.05,
        })
        answer = data.get("choices", [{}])[0].get("message", {}).get("content", "No response generated.")
        return {"answer": _clean_answer(answer), "sources": [{"type": "pa_tool_calling", "tables_queried": tables_queried}]}

    except Exception as e:
        print(f"[PA Agent] ERROR: {e}")
        traceback.print_exc()
        return {"answer": f"Error: {str(e)}", "sources": []}
