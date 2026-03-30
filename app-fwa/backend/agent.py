"""FWA Investigation Agent and analytics data integration module.

Implements the FWA investigation agent directly in the backend using
Foundation Model API + Statement Execution API with tool-calling for
dynamic UC table access. Uses the Databricks SDK for all data access.
"""

import json
import os
import re
import traceback

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem

UC_CATALOG = os.environ.get("UC_CATALOG", "red_bricks_insurance")
SQL_WAREHOUSE_ID = os.environ.get("SQL_WAREHOUSE_ID", "781064a3466c0984")
LLM_ENDPOINT = os.environ.get("LLM_ENDPOINT", "databricks-llama-4-maverick")
FWA_MODEL_ENDPOINT = os.environ.get("FWA_MODEL_ENDPOINT", "fwa-fraud-scorer")

# Table references (used by direct API routes)
PROVIDER_RISK_TABLE = f"{UC_CATALOG}.fwa.gold_fwa_provider_risk"
CLAIM_FLAGS_TABLE = f"{UC_CATALOG}.fwa.gold_fwa_claim_flags"
MEMBER_RISK_TABLE = f"{UC_CATALOG}.analytics.gold_fwa_member_risk"
MODEL_INFERENCE_TABLE = f"{UC_CATALOG}.analytics.gold_fwa_model_scores"
FWA_SUMMARY_TABLE = f"{UC_CATALOG}.fwa.gold_fwa_summary"

# Allowed schemas the agent can query
ALLOWED_SCHEMAS = ["fwa", "analytics", "claims", "members", "providers", "pharmacy", "benefits"]

SYSTEM_PROMPT = """You are an FWA (Fraud, Waste & Abuse) Investigation Specialist for Red Bricks Insurance.
You help SIU analysts investigate suspected fraud by querying structured data
and synthesizing findings into actionable investigation briefings.

You have access to tools that let you query Unity Catalog tables directly. Use them to:
- Look up investigation details, provider risk profiles, flagged claims
- Retrieve ML model fraud predictions from the inference table
- Find related claims, members, or providers
- Compare metrics against peers or benchmarks
- Discover patterns across multiple tables

Strategy: First gather all relevant data using tool calls, then synthesize.

Your final response MUST include these sections:

## CASE SUMMARY
Brief overview of the investigation target, fraud types suspected, and current status.

## KEY FINDINGS
Top 3-5 findings with supporting evidence (claim IDs, dollar amounts, dates).

## EVIDENCE ANALYSIS
Detailed analysis of billing patterns, anomalies, and red flags. Include both
rules-based flags AND ML model scores where available.

## RISK ASSESSMENT
Risk rating: **Critical** / **High** / **Medium** / **Low** with justification.

## RECOMMENDED ACTIONS
Prioritized next steps with timeframes.

Always cite data sources. Never fabricate evidence."""

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "query_uc_table",
            "description": (
                f"Execute a read-only SQL SELECT query against Unity Catalog tables in the {UC_CATALOG} catalog. "
                "Key FWA tables:\n"
                f"- {UC_CATALOG}.fwa.gold_fwa_provider_risk: Provider risk scorecards\n"
                f"- {UC_CATALOG}.fwa.gold_fwa_claim_flags: Flagged claims with evidence\n"
                f"- {UC_CATALOG}.fwa.gold_fwa_summary: Aggregate FWA metrics\n"
                f"- {UC_CATALOG}.fwa.silver_fwa_signals: Individual FWA signals\n"
                f"- {UC_CATALOG}.fwa.silver_fwa_investigation_cases: Investigation case records\n"
                f"- {UC_CATALOG}.analytics.gold_fwa_member_risk: Member-level fraud indicators\n"
                f"- {UC_CATALOG}.analytics.gold_fwa_network_analysis: Provider referral rings\n"
                f"- {UC_CATALOG}.analytics.gold_fwa_model_scores: ML model fraud predictions per claim\n"
                f"- {UC_CATALOG}.claims.silver_claims_medical: Medical claims detail\n"
                f"- {UC_CATALOG}.members.silver_enrollment: Member enrollment\n"
                f"- {UC_CATALOG}.providers.silver_providers: Provider demographics\n"
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
                        "description": f"Table name (e.g. 'fwa.gold_fwa_provider_risk'). Catalog '{UC_CATALOG}' is added automatically.",
                    },
                },
                "required": ["table_name"],
            },
        },
    },
]

MAX_TOOL_ROUNDS = 6


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


def _validate_sql(sql: str) -> str | None:
    """Validate SQL is read-only and references allowed schemas. Returns error message or None."""
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

        print(f"[FWA Agent] Tool query ({purpose}): {sql[:200]}")
        try:
            rows = _execute_sql(sql)
            if not rows:
                return json.dumps({"result": [], "row_count": 0, "message": "No rows returned."})
            return json.dumps({"result": rows[:50], "row_count": len(rows)}, default=str)
        except Exception as e:
            return json.dumps({"error": f"SQL error: {str(e)}"})

    elif tool_name == "list_table_columns":
        table_name = tool_args.get("table_name", "").strip()
        if not table_name.startswith(UC_CATALOG):
            table_name = f"{UC_CATALOG}.{table_name}"
        try:
            rows = _execute_sql(f"DESCRIBE TABLE {table_name}")
            return json.dumps({"table": table_name, "columns": rows}, default=str)
        except Exception as e:
            return json.dumps({"error": f"Could not describe table: {str(e)}"})

    return json.dumps({"error": f"Unknown tool: {tool_name}"})


# ---------------------------------------------------------------------------
# Direct data access functions (used by API routes, not the agent)
# ---------------------------------------------------------------------------

def get_provider_risk_profile(npi: str) -> dict | None:
    """Get provider risk profile from gold FWA table."""
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
    """Get flagged claims for a provider."""
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
    """Get ML model predictions for a provider's claims."""
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


def get_dashboard_analytics() -> dict:
    """Get FWA dashboard analytics from gold tables."""
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


# ---------------------------------------------------------------------------
# Tool-calling agent
# ---------------------------------------------------------------------------

def _fetch_investigation_from_lakebase(inv_id: str) -> str | None:
    """Pre-fetch investigation details from Lakebase for agent context."""
    try:
        from .database import db as _db
        import psycopg
        import uuid as _uuid
        from databricks.sdk import WorkspaceClient as _WC

        instance_name = os.environ.get("LAKEBASE_INSTANCE_NAME", "fwa-investigations")
        database_name = os.environ.get("LAKEBASE_DATABASE_NAME", "fwa_cases")

        w = _WC()
        instance = w.database.get_database_instance(name=instance_name)
        cred = w.database.generate_database_credential(
            request_id=str(_uuid.uuid4()),
            instance_names=[instance_name],
        )
        username = w.current_user.me().user_name
        conn_string = (
            f"host={instance.read_write_dns} "
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
        print(f"[FWA Agent] Lakebase fetch error: {e}")
        return None


def _parse_and_execute_text_tools(text: str) -> str | None:
    """Parse tool calls written as text by the model and execute them."""
    # Match patterns like: query_uc_table(sql=..., purpose=...)
    sql_pattern = re.compile(
        r'query_uc_table\(sql\s*=\s*(.+?),\s*purpose\s*=\s*(.+?)\)',
        re.DOTALL,
    )
    matches = sql_pattern.findall(text)
    if not matches:
        return None

    results = []
    for sql_raw, purpose_raw in matches:
        sql = sql_raw.strip().strip("'\"")
        purpose = purpose_raw.strip().strip("'\"")
        result = _execute_tool("query_uc_table", {"sql": sql, "purpose": purpose})
        results.append(f"Query ({purpose}):\n{result}")

    return "\n---\n".join(results) if results else None


def query_fwa_agent(target_id: str, target_type: str, question: str) -> dict:
    """FWA investigation agent with tool-calling for dynamic UC table access."""
    try:
        print(f"[FWA Agent] Processing query for {target_type} {target_id}: {question[:80]}...")

        # Build context hint
        context_hint = f"Question: {question}\n"
        if target_id:
            context_hint += f"Target: {target_id} (type: {target_type})\n\n"
        context_hint += f"The Unity Catalog is: {UC_CATALOG}\n"
        context_hint += "Use the query_uc_table tool to gather all relevant data before generating your analysis.\n\n"

        if target_type == "investigation":
            # Pre-fetch investigation details from Lakebase (the source of truth)
            lb_data = _fetch_investigation_from_lakebase(target_id)
            if lb_data:
                context_hint += (
                    f"Here is the investigation record from the Lakebase database (source of truth):\n"
                    f"```json\n{lb_data}\n```\n\n"
                    "Use this data as the basis for your analysis. "
                    "Now query UC tables for additional context: flagged claims, provider risk, "
                    "member risk, and ML model scores using the target_id and target_type above.\n"
                )
            else:
                context_hint += (
                    f"Investigation {target_id} was not found in Lakebase. "
                    f"Try querying: SELECT * FROM {UC_CATALOG}.fwa.silver_fwa_investigation_cases "
                    f"WHERE investigation_id = '{target_id}' LIMIT 1\n"
                )
        elif target_type == "provider":
            context_hint += (
                f"Start by querying: SELECT * FROM {PROVIDER_RISK_TABLE} WHERE provider_npi = '{target_id}' LIMIT 1\n"
                f"Then query flagged claims and ML predictions.\n"
            )

        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": context_hint},
        ]

        tables_queried = 0

        # Multi-turn tool-calling loop
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
                answer = data.get("choices", [{}])[0].get("message", {}).get("content", "No response generated.")
                # Check if the model returned tool calls as text instead of structured calls
                if "query_uc_table(" in answer or "list_table_columns(" in answer:
                    # Parse text-based tool calls and execute them directly
                    text_tool_results = _parse_and_execute_text_tools(answer)
                    if text_tool_results:
                        messages.append(data["choices"][0]["message"])
                        # Feed results back as if they were normal tool responses
                        messages.append({
                            "role": "user",
                            "content": f"Here are the query results:\n\n{text_tool_results}\n\nNow provide your final analysis based on all the data above. Do NOT output any more tool calls — just give the structured briefing.",
                        })
                        tables_queried += len(text_tool_results.split("---"))
                        continue
                return {"answer": answer, "sources": [{"type": "fwa_tool_calling", "tables_queried": tables_queried}]}

            # Append assistant message with tool calls
            messages.append(data["choices"][0]["message"])

            # Execute each tool call
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

        # Exhausted rounds — get final answer without tools
        messages.append({
            "role": "user",
            "content": "Please provide your final analysis now based on all the data gathered.",
        })
        data = _sdk_request("POST", f"/serving-endpoints/{LLM_ENDPOINT}/invocations", {
            "messages": messages,
            "max_tokens": 3000,
            "temperature": 0.05,
        })
        answer = data.get("choices", [{}])[0].get("message", {}).get("content", "No response generated.")
        return {"answer": answer, "sources": [{"type": "fwa_tool_calling", "tables_queried": tables_queried}]}

    except Exception as e:
        print(f"[FWA Agent] ERROR: {e}")
        traceback.print_exc()
        return {"answer": f"Error: {str(e)}", "sources": []}
