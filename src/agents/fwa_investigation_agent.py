"""FWA Investigation Agent — Tool-Calling MLflow ChatModel for Unity Catalog.

Provides AI-assisted investigation briefings for SIU analysts.
Uses a tool-calling pattern so the LLM can dynamically query Unity Catalog
tables, retrieve provider/member/claim data, and synthesize findings into
structured investigation briefings.

Input format: [INV-XXXX] <question> or [PRV-XXXXXXXXXX] <question>
"""

import json
import os
import re
from typing import List, Optional

import mlflow
from mlflow.pyfunc import ChatModel
from mlflow.types.llm import (
    ChatMessage,
    ChatParams,
    ChatCompletionResponse,
    ChatChoice,
)

# Allowed schemas the agent can query (prevents arbitrary SQL execution)
ALLOWED_SCHEMAS = [
    "fwa",
    "analytics",
    "claims",
    "members",
    "providers",
    "pharmacy",
    "benefits",
]

# Tool definitions for the Foundation Model API function-calling interface
TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "query_uc_table",
            "description": (
                "Execute a read-only SQL query against Unity Catalog tables in the Red Bricks Insurance "
                "lakehouse. Use this to look up claims, providers, members, FWA signals, risk scores, "
                "ML model predictions, and any other data needed for the investigation. "
                "Only SELECT statements are allowed. Available schemas: "
                + ", ".join(ALLOWED_SCHEMAS)
                + ". All tables are in the catalog specified at runtime (e.g. red_bricks_insurance.fwa.gold_fwa_provider_risk). "
                "Key FWA tables:\n"
                "- fwa.gold_fwa_provider_risk: Provider risk scorecards\n"
                "- fwa.gold_fwa_claim_flags: Flagged claims with evidence\n"
                "- fwa.gold_fwa_summary: Aggregate FWA metrics\n"
                "- fwa.silver_fwa_signals: Individual FWA signals\n"
                "- fwa.silver_fwa_investigation_cases: Investigation case records\n"
                "- analytics.gold_fwa_member_risk: Member-level fraud indicators\n"
                "- analytics.gold_fwa_network_analysis: Provider referral ring detection\n"
                "- analytics.fwa_model_inference: ML model fraud predictions per claim\n"
                "- claims.silver_claims_medical: Medical claims detail\n"
                "- claims.silver_claims_pharmacy: Pharmacy claims detail\n"
                "- members.silver_enrollment: Member enrollment data\n"
                "- providers.silver_providers: Provider demographics\n"
                "Always include a LIMIT clause (max 50 rows)."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "A read-only SQL SELECT query to execute against Unity Catalog.",
                    },
                    "purpose": {
                        "type": "string",
                        "description": "Brief description of what this query is looking up and why.",
                    },
                },
                "required": ["sql", "purpose"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_table_columns",
            "description": (
                "List columns and their types for a Unity Catalog table. Use this to understand "
                "a table's schema before writing a query against it."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Fully qualified table name (e.g. 'fwa.gold_fwa_provider_risk'). The catalog prefix is added automatically.",
                    },
                },
                "required": ["table_name"],
            },
        },
    },
]

MAX_TOOL_ROUNDS = 6


class FWAInvestigationAgent(ChatModel):
    """FWA Investigation Agent with tool-calling for dynamic UC table access."""

    SYSTEM_PROMPT = (
        "You are an FWA (Fraud, Waste & Abuse) Investigation Specialist for Red Bricks Insurance. "
        "You help SIU analysts investigate suspected fraud by querying structured data "
        "(provider risk profiles, flagged claims, billing patterns, ML model scores) and synthesizing "
        "findings into actionable investigation briefings.\n\n"
        "You have access to tools that let you query Unity Catalog tables directly. Use them to:\n"
        "- Look up investigation details, provider risk profiles, flagged claims\n"
        "- Retrieve ML model fraud predictions from the inference table\n"
        "- Find related claims, members, or providers\n"
        "- Compare metrics against peers or benchmarks\n"
        "- Discover patterns across multiple tables\n\n"
        "Strategy: First gather all relevant data using tool calls, then synthesize into your response.\n\n"
        "Your final response MUST include the following sections:\n\n"
        "## CASE SUMMARY\n"
        "Brief overview of the investigation target, fraud types suspected, and current status.\n\n"
        "## KEY FINDINGS\n"
        "Top 3-5 findings from the data, each with supporting evidence. Include specific claim IDs, "
        "dollar amounts, dates, and provider/member identifiers.\n\n"
        "## EVIDENCE ANALYSIS\n"
        "Detailed analysis of billing patterns, anomalies, and red flags. Compare against peer "
        "benchmarks where available. Reference both rules-based flags AND ML model scores.\n\n"
        "## RISK ASSESSMENT\n"
        "Provide a risk rating: **Critical** / **High** / **Medium** / **Low**\n"
        "Justify the rating based on financial exposure, pattern severity, and confidence level. "
        "Note both the rules-based risk score and ML model confidence.\n\n"
        "## RECOMMENDED ACTIONS\n"
        "Provide prioritized, concrete next steps:\n"
        "1. Immediate actions (within 48 hours)\n"
        "2. Evidence gathering recommendations\n"
        "3. Recovery/referral recommendations\n\n"
        "Always cite data sources and table names. Never fabricate evidence. "
        "If data is missing or a query returns no results, say so explicitly."
    )

    def load_context(self, context) -> None:
        """Initialize SDK clients at model load time."""
        from databricks.sdk import WorkspaceClient

        self.w = WorkspaceClient()

        self.catalog = os.environ.get("UC_CATALOG", "red_bricks_insurance")
        self.warehouse_id = os.environ.get("SQL_WAREHOUSE_ID", "781064a3466c0984")
        self.llm_endpoint = os.environ.get("LLM_ENDPOINT", "databricks-llama-4-maverick")

    def predict(
        self, context, messages: List[ChatMessage], params: Optional[ChatParams] = None
    ) -> ChatCompletionResponse:
        """Process an FWA investigation query using tool-calling agent loop."""
        user_msg = ""
        for m in reversed(messages):
            if m.role == "user":
                user_msg = m.content
                break

        # Parse target from message format: [INV-XXXX] question
        target_id, target_type, question = self._parse_input(user_msg)

        # Build initial context hint so the LLM knows which tables to query
        context_hint = self._build_context_hint(target_id, target_type, question)

        # Start the agent loop with system prompt + user message
        llm_messages = [
            {"role": "system", "content": self.SYSTEM_PROMPT},
            {"role": "user", "content": context_hint},
        ]

        # Multi-turn tool-calling loop
        for _ in range(MAX_TOOL_ROUNDS):
            response = self._call_llm_with_tools(llm_messages)

            # Check if the model wants to call tools
            tool_calls = (
                response.get("choices", [{}])[0]
                .get("message", {})
                .get("tool_calls", [])
            )

            if not tool_calls:
                # No tool calls — model is done, extract final answer
                answer = (
                    response.get("choices", [{}])[0]
                    .get("message", {})
                    .get("content", "No response generated.")
                )
                break

            # Append the assistant message with tool calls
            assistant_msg = response["choices"][0]["message"]
            llm_messages.append(assistant_msg)

            # Execute each tool call and append results
            for tc in tool_calls:
                tool_name = tc.get("function", {}).get("name", "")
                tool_args_str = tc.get("function", {}).get("arguments", "{}")
                tool_call_id = tc.get("id", "")

                try:
                    tool_args = json.loads(tool_args_str)
                except json.JSONDecodeError:
                    tool_args = {}

                result = self._execute_tool(tool_name, tool_args)

                llm_messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call_id,
                    "content": result,
                })
        else:
            # Exhausted tool rounds — ask model for final answer without tools
            llm_messages.append({
                "role": "user",
                "content": "Please provide your final analysis now based on all the data gathered.",
            })
            answer = self._call_llm_no_tools(llm_messages)

        return ChatCompletionResponse(
            choices=[ChatChoice(index=0, message=ChatMessage(role="assistant", content=answer))],
            usage={},
            model=self.llm_endpoint,
        )

    def _parse_input(self, user_msg: str) -> tuple[str, str, str]:
        """Extract target_id, target_type, and question from user message."""
        target_id = ""
        target_type = ""
        question = user_msg

        if user_msg.startswith("[") and "]" in user_msg:
            bracket_end = user_msg.index("]")
            target_id = user_msg[1:bracket_end].strip()
            question = user_msg[bracket_end + 1:].strip()

            if target_id.startswith("INV-"):
                target_type = "investigation"
            elif target_id.startswith("PRV-") or (target_id.isdigit() and len(target_id) == 10):
                target_type = "provider"
                if target_id.startswith("PRV-"):
                    target_id = target_id[4:]
            elif target_id.startswith("MBR-"):
                target_type = "member"

        return target_id, target_type, question

    def _build_context_hint(self, target_id: str, target_type: str, question: str) -> str:
        """Build a context-rich prompt that guides the LLM on which tables to query."""
        hint = f"Question: {question}\n"

        if target_id:
            hint += f"Target: {target_id} (type: {target_type})\n\n"

        hint += f"The Unity Catalog is: {self.catalog}\n"
        hint += "Use the query_uc_table tool to gather all relevant data before generating your analysis.\n\n"

        if target_type == "investigation":
            hint += (
                "Suggested queries:\n"
                f"1. Get investigation details: SELECT * FROM {self.catalog}.fwa.silver_fwa_investigation_cases WHERE investigation_id = '{target_id}' LIMIT 1\n"
                f"2. After finding the target_id/target_type, query the relevant risk profile and flagged claims\n"
                f"3. Check ML model predictions from {self.catalog}.analytics.fwa_model_inference\n"
                f"4. Look for similar cases in silver_fwa_investigation_cases\n"
            )
        elif target_type == "provider":
            hint += (
                "Suggested queries:\n"
                f"1. Provider risk profile: SELECT * FROM {self.catalog}.fwa.gold_fwa_provider_risk WHERE provider_npi = '{target_id}' LIMIT 1\n"
                f"2. Flagged claims: SELECT * FROM {self.catalog}.fwa.gold_fwa_claim_flags WHERE provider_npi = '{target_id}' ORDER BY fraud_score DESC LIMIT 30\n"
                f"3. ML predictions: SELECT * FROM {self.catalog}.analytics.fwa_model_inference WHERE provider_npi = '{target_id}' ORDER BY ml_fraud_probability DESC LIMIT 20\n"
            )
        elif target_type == "member":
            hint += (
                "Suggested queries:\n"
                f"1. Member risk: SELECT * FROM {self.catalog}.analytics.gold_fwa_member_risk WHERE member_id = '{target_id}' LIMIT 1\n"
                f"2. Flagged claims: SELECT * FROM {self.catalog}.fwa.gold_fwa_claim_flags WHERE member_id = '{target_id}' ORDER BY fraud_score DESC LIMIT 30\n"
                f"3. ML predictions: SELECT * FROM {self.catalog}.analytics.fwa_model_inference WHERE member_id = '{target_id}' ORDER BY ml_fraud_probability DESC LIMIT 20\n"
            )
        else:
            hint += (
                "No specific target provided. Use query_uc_table and list_table_columns tools "
                "to explore the data and answer the question.\n"
            )

        return hint

    def _execute_tool(self, tool_name: str, tool_args: dict) -> str:
        """Execute a tool call and return the result as a string."""
        if tool_name == "query_uc_table":
            return self._tool_query_uc_table(tool_args)
        elif tool_name == "list_table_columns":
            return self._tool_list_table_columns(tool_args)
        else:
            return json.dumps({"error": f"Unknown tool: {tool_name}"})

    def _tool_query_uc_table(self, args: dict) -> str:
        """Execute a read-only SQL query against Unity Catalog."""
        sql = args.get("sql", "").strip()
        purpose = args.get("purpose", "")

        # Validate: only SELECT statements allowed
        sql_upper = sql.upper().lstrip()
        if not sql_upper.startswith("SELECT") and not sql_upper.startswith("WITH") and not sql_upper.startswith("DESCRIBE"):
            return json.dumps({"error": "Only SELECT, WITH, and DESCRIBE statements are allowed."})

        # Validate: block dangerous patterns
        dangerous = re.compile(
            r'\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|GRANT|REVOKE|MERGE)\b',
            re.IGNORECASE,
        )
        if dangerous.search(sql):
            return json.dumps({"error": "Write operations are not permitted. Only read-only queries allowed."})

        # Validate: ensure query references allowed schemas
        # (lightweight check — the warehouse permissions are the real guardrail)
        schema_found = False
        for schema in ALLOWED_SCHEMAS:
            if schema in sql.lower():
                schema_found = True
                break
        if not schema_found and "information_schema" not in sql.lower():
            return json.dumps({
                "error": f"Query must reference one of the allowed schemas: {ALLOWED_SCHEMAS}",
            })

        # Enforce LIMIT
        if "LIMIT" not in sql_upper:
            sql = sql.rstrip(";") + " LIMIT 50"

        print(f"[FWA Agent] Tool query ({purpose}): {sql[:200]}")

        try:
            rows = self._execute_sql(sql)
            if not rows:
                return json.dumps({"result": [], "row_count": 0, "message": "Query returned no rows."})
            return json.dumps({"result": rows[:50], "row_count": len(rows)}, default=str)
        except Exception as e:
            return json.dumps({"error": f"SQL execution error: {str(e)}"})

    def _tool_list_table_columns(self, args: dict) -> str:
        """List columns for a UC table."""
        table_name = args.get("table_name", "").strip()

        # Add catalog prefix if not present
        if not table_name.startswith(self.catalog):
            table_name = f"{self.catalog}.{table_name}"

        # Validate schema
        parts = table_name.split(".")
        if len(parts) != 3:
            return json.dumps({"error": "Table name must be schema.table_name or catalog.schema.table_name"})

        schema = parts[1] if len(parts) == 3 else parts[0]
        if schema not in ALLOWED_SCHEMAS:
            return json.dumps({"error": f"Schema '{schema}' not in allowed schemas: {ALLOWED_SCHEMAS}"})

        try:
            rows = self._execute_sql(f"DESCRIBE TABLE {table_name}")
            return json.dumps({"table": table_name, "columns": rows}, default=str)
        except Exception as e:
            return json.dumps({"error": f"Could not describe table: {str(e)}"})

    def _execute_sql(self, sql: str, params: list = None) -> list[dict]:
        """Execute SQL via Statement Execution API."""
        from databricks.sdk.service.sql import StatementParameterListItem

        kwargs = {
            "warehouse_id": self.warehouse_id,
            "statement": sql,
            "wait_timeout": "30s",
        }
        if params:
            kwargs["parameters"] = [
                StatementParameterListItem(name=p["name"], value=p["value"], type=p.get("type", "STRING"))
                for p in params
            ]
        stmt = self.w.statement_execution.execute_statement(**kwargs)
        if not stmt.result or not stmt.result.data_array:
            return []
        col_names = [c.name for c in stmt.manifest.schema.columns]
        return [dict(zip(col_names, row)) for row in stmt.result.data_array]

    def _call_llm_with_tools(self, messages: list) -> dict:
        """Call Foundation Model API with tool definitions."""
        try:
            return self.w.api_client.do(
                "POST",
                f"/serving-endpoints/{self.llm_endpoint}/invocations",
                body={
                    "messages": messages,
                    "tools": TOOLS,
                    "max_tokens": 4000,
                    "temperature": 0.05,
                },
            )
        except Exception as e:
            print(f"[FWA Agent] LLM error: {e}")
            return {"choices": [{"message": {"content": f"Error calling LLM: {e}"}}]}

    def _call_llm_no_tools(self, messages: list) -> str:
        """Call Foundation Model API without tools for final answer."""
        try:
            data = self.w.api_client.do(
                "POST",
                f"/serving-endpoints/{self.llm_endpoint}/invocations",
                body={
                    "messages": messages,
                    "max_tokens": 3000,
                    "temperature": 0.05,
                },
            )
            return (
                data.get("choices", [{}])[0]
                .get("message", {})
                .get("content", "No response generated.")
            )
        except Exception as e:
            return f"Error generating response: {e}"


# Required for MLflow code-based logging
mlflow.models.set_model(FWAInvestigationAgent())
