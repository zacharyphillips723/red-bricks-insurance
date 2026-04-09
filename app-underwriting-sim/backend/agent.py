"""Underwriting Simulation Agent with tool-calling.

Uses Foundation Model API for natural-language interaction and can run
simulations, query baseline data, look up group details, and query UC tables
on behalf of the underwriter/actuary.
"""

import json
import re
import traceback

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem

from .data_loader import data_cache
from .env_config import UC_CATALOG, SQL_WAREHOUSE_ID, LLM_ENDPOINT
from .simulation_engine import run_simulation, SIMULATION_FUNCTIONS

_CAT = f"`{UC_CATALOG}`"  # SQL-safe quoting (handles hyphens in catalog names)
ALLOWED_SCHEMAS = ["analytics", "claims", "members", "providers", "benefits", "pharmacy"]

SYSTEM_PROMPT = f"""You are an Underwriting & Actuarial Analysis Assistant for Red Bricks Insurance.
You help underwriters and actuaries evaluate risk, price renewals, and run what-if simulations
using structured data from Unity Catalog and the in-memory simulation engine.

You have four tools:
1. **run_simulation** — Execute one of {len(SIMULATION_FUNCTIONS)} simulation types with custom parameters.
2. **get_baseline** — Retrieve current book-level financials (total premium, claims, MLR, PMPM by LOB).
3. **get_group_detail** — Fetch experience, renewal, and stop-loss data for a specific group.
4. **query_uc_table** — Run read-only SQL against Unity Catalog gold/silver tables.

Strategy: gather data first, then synthesize. Provide clear, quantitative answers with
dollar amounts and percentages. Cite data sources. Do not fabricate numbers.

When running simulations, explain what the parameters mean in plain language and
summarise the projected impact with baseline vs. projected comparison.
"""

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "run_simulation",
            "description": (
                "Run an underwriting what-if simulation. Returns baseline, projected, delta, "
                "and narrative. Available types: " + ", ".join(SIMULATION_FUNCTIONS.keys())
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "simulation_type": {
                        "type": "string",
                        "enum": list(SIMULATION_FUNCTIONS.keys()),
                        "description": "The type of simulation to run.",
                    },
                    "parameters": {
                        "type": "object",
                        "description": "Simulation-specific input parameters (varies by type).",
                    },
                },
                "required": ["simulation_type", "parameters"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_baseline",
            "description": "Get current book-level financials: total premium, claims, MLR, PMPM by LOB, member counts.",
            "parameters": {
                "type": "object",
                "properties": {
                    "lob": {
                        "type": "string",
                        "description": "Optional line of business filter (e.g. 'Commercial', 'Medicare Advantage').",
                    },
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_group_detail",
            "description": "Fetch experience, renewal pricing, and stop-loss data for a specific group.",
            "parameters": {
                "type": "object",
                "properties": {
                    "group_id": {"type": "string", "description": "Group identifier (e.g. 'GRP-001')."},
                },
                "required": ["group_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "query_uc_table",
            "description": (
                f"Execute a read-only SQL SELECT query against Unity Catalog tables in {_CAT}. "
                f"Key tables:\n"
                f"- {_CAT}.analytics.gold_pmpm\n"
                f"- {_CAT}.analytics.gold_mlr\n"
                f"- {_CAT}.analytics.gold_enrollment_summary\n"
                f"- {_CAT}.analytics.gold_utilization_per_1000\n"
                f"- {_CAT}.analytics.gold_risk_adjustment_analysis\n"
                f"- {_CAT}.analytics.gold_tcoc_summary\n"
                f"- {_CAT}.analytics.gold_group_experience\n"
                f"- {_CAT}.claims.silver_claims_medical\n"
                f"- {_CAT}.members.silver_enrollment\n"
                "Always include LIMIT (max 50). Only SELECT/WITH/DESCRIBE allowed."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "Read-only SQL query."},
                    "purpose": {"type": "string", "description": "Brief description of what this query looks up."},
                },
                "required": ["sql", "purpose"],
            },
        },
    },
]

MAX_TOOL_ROUNDS = 6


# ---------------------------------------------------------------------------
# SQL helpers (shared with data_loader but independent for agent queries)
# ---------------------------------------------------------------------------

def _execute_sql(sql: str) -> list[dict]:
    w = WorkspaceClient()
    stmt = w.statement_execution.execute_statement(
        warehouse_id=SQL_WAREHOUSE_ID,
        statement=sql,
        wait_timeout="30s",
    )
    if not stmt.result or not stmt.result.data_array:
        return []
    col_names = [c.name for c in stmt.manifest.schema.columns] if stmt.manifest and stmt.manifest.schema else []
    return [dict(zip(col_names, row)) for row in stmt.result.data_array] if col_names else []


def _validate_sql(sql: str) -> str | None:
    sql_upper = sql.upper().lstrip()
    if not any(sql_upper.startswith(k) for k in ("SELECT", "WITH", "DESCRIBE")):
        return "Only SELECT, WITH, and DESCRIBE statements are allowed."
    dangerous = re.compile(
        r'\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|GRANT|REVOKE|MERGE)\b',
        re.IGNORECASE,
    )
    if dangerous.search(sql):
        return "Write operations are not permitted."
    return None


def _sdk_request(method: str, path: str, body: dict | None = None) -> dict:
    w = WorkspaceClient()
    return w.api_client.do(method, path, body=body) if body else w.api_client.do(method, path)


# ---------------------------------------------------------------------------
# Tool execution
# ---------------------------------------------------------------------------

def _execute_tool(tool_name: str, tool_args: dict) -> str:
    if tool_name == "run_simulation":
        sim_type = tool_args.get("simulation_type", "")
        sim_params = tool_args.get("parameters", {})
        print(f"[UW Agent] Running simulation: {sim_type}")
        try:
            result = run_simulation(data_cache, sim_type, sim_params)
            return json.dumps(result, default=str)
        except Exception as e:
            return json.dumps({"error": str(e)})

    elif tool_name == "get_baseline":
        lob = tool_args.get("lob")
        print(f"[UW Agent] Getting baseline summary (lob={lob})")
        try:
            result = data_cache.get_baseline_summary(lob=lob)
            return json.dumps(result, default=str)
        except Exception as e:
            return json.dumps({"error": str(e)})

    elif tool_name == "get_group_detail":
        group_id = tool_args.get("group_id", "")
        print(f"[UW Agent] Getting group detail for {group_id}")
        try:
            experience = data_cache.get_group_experience(group_id)
            renewal = data_cache.get_group_renewal(group_id)
            stop_loss = data_cache.get_group_stop_loss(group_id)
            return json.dumps({
                "group_id": group_id,
                "experience": experience,
                "renewal": renewal,
                "stop_loss": stop_loss,
            }, default=str)
        except Exception as e:
            return json.dumps({"error": str(e)})

    elif tool_name == "query_uc_table":
        sql = tool_args.get("sql", "").strip()
        purpose = tool_args.get("purpose", "")
        error = _validate_sql(sql)
        if error:
            return json.dumps({"error": error})
        if "LIMIT" not in sql.upper():
            sql = sql.rstrip(";") + " LIMIT 50"
        print(f"[UW Agent] SQL ({purpose}): {sql[:200]}")
        try:
            rows = _execute_sql(sql)
            return json.dumps({"result": rows[:50], "row_count": len(rows)}, default=str)
        except Exception as e:
            return json.dumps({"error": f"SQL error: {str(e)}"})

    return json.dumps({"error": f"Unknown tool: {tool_name}"})


# ---------------------------------------------------------------------------
# Agent entry point
# ---------------------------------------------------------------------------

def query_underwriting_agent(
    message: str,
    conversation_history: list[dict] | None = None,
) -> dict:
    """Underwriting agent with multi-turn tool-calling."""
    try:
        print(f"[UW Agent] Processing: {message[:80]}...")

        messages = [{"role": "system", "content": SYSTEM_PROMPT}]
        if conversation_history:
            messages.extend(conversation_history)
        messages.append({"role": "user", "content": message})

        tools_used = 0
        simulation_results = []

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
                answer = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                return {
                    "response": answer,
                    "simulation_results": simulation_results or None,
                }

            messages.append(data["choices"][0]["message"])

            for tc in tool_calls:
                fn_name = tc.get("function", {}).get("name", "")
                try:
                    fn_args = json.loads(tc.get("function", {}).get("arguments", "{}"))
                except json.JSONDecodeError:
                    fn_args = {}
                tool_call_id = tc.get("id", "")

                result_str = _execute_tool(fn_name, fn_args)
                tools_used += 1

                # Capture simulation results for structured return
                if fn_name == "run_simulation":
                    try:
                        sim_result = json.loads(result_str)
                        if "error" not in sim_result:
                            sim_result["simulation_type"] = fn_args.get("simulation_type", "")
                            simulation_results.append(sim_result)
                    except json.JSONDecodeError:
                        pass

                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call_id,
                    "content": result_str,
                })

        # Exhausted rounds
        messages.append({
            "role": "user",
            "content": "Please provide your final analysis based on all the data gathered.",
        })
        data = _sdk_request("POST", f"/serving-endpoints/{LLM_ENDPOINT}/invocations", {
            "messages": messages,
            "max_tokens": 3000,
            "temperature": 0.05,
        })
        answer = data.get("choices", [{}])[0].get("message", {}).get("content", "")
        return {
            "response": answer,
            "simulation_results": simulation_results or None,
        }

    except Exception as e:
        print(f"[UW Agent] ERROR: {e}")
        traceback.print_exc()
        return {"response": f"Error: {str(e)}", "simulation_results": None}
