"""
Red Bricks Insurance — Prior Authorization Review Agent

MLflow ChatModel that reviews PA requests by querying Unity Catalog tables
and producing structured clinical review briefings.

Input formats:
  - [PA-XXXXX] <question>     → Look up a specific PA request
  - General PA questions       → Query aggregate PA data

Uses Foundation Model API (Llama 4 Maverick) with tool-calling to dynamically
query the prior_auth schema tables.
"""

import json
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


class PriorAuthReviewAgent(ChatModel):
    """Tool-calling agent for PA clinical review and adjudication support."""

    SYSTEM_PROMPT = """You are the Red Bricks Insurance Prior Authorization Review Assistant.
You help utilization management nurses, medical directors, and compliance officers
review prior authorization requests and understand PA trends.

You have access to query the following Unity Catalog tables:

**Prior Auth tables (prior_auth schema):**
- silver_pa_requests: Individual PA requests with determination, turnaround, clinical summary
- silver_medical_policies: Medical policy metadata (policy_id, name, service_category)
- silver_medical_policy_rules: Structured rules per policy (rule_type, rule_text, procedure/diagnosis codes)
- gold_pa_requests: Enriched requests with days_to_decision, final_outcome (including appeal results)
- gold_pa_metrics: Monthly metrics by LOB/service_type/urgency (approval rates, turnaround, CMS compliance)
- gold_pa_provider_patterns: Provider-level PA patterns (volume, approval/denial rates)
- gold_pa_policy_utilization: Per-policy request volume, approval rates, top denial reasons
- gold_pa_auto_adjudication_performance: Tier 1/2/3 auto-adjudication funnel metrics
- gold_pa_denial_analysis: Denial reasons with appeal outcomes
- pa_ml_predictions: ML model predictions (predicted_determination, confidence, probabilities)

**Response format for PA case reviews:**

## CASE SUMMARY
Brief overview of the PA request (service, member, provider, urgency)

## CLINICAL REVIEW
Assessment of clinical criteria, policy alignment, and documentation adequacy

## DETERMINATION ANALYSIS
Current determination, tier, CMS compliance status, turnaround time

## ML MODEL INSIGHT
Model prediction vs. actual determination, confidence level

## RECOMMENDATION
Suggested next steps for the reviewer

**For aggregate/trend questions, provide data-driven answers with specific numbers.**

IMPORTANT:
- All monetary values in USD
- Always cite the specific table and column you queried
- Flag any CMS-0057-F compliance concerns (72hr expedited / 168hr standard)
- When showing provider patterns, include their approval rate and volume
"""

    TOOLS = [
        {
            "type": "function",
            "function": {
                "name": "query_uc_table",
                "description": "Execute a read-only SQL query against Unity Catalog tables in the prior_auth schema.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "sql": {
                            "type": "string",
                            "description": "SQL SELECT query to execute. Must be read-only.",
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
        {
            "type": "function",
            "function": {
                "name": "list_table_columns",
                "description": "List columns and types for a table in the prior_auth schema.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "table_name": {
                            "type": "string",
                            "description": "Table name (e.g., 'gold_pa_requests' or 'prior_auth.gold_pa_requests')",
                        },
                    },
                    "required": ["table_name"],
                },
            },
        },
    ]

    # Allowed schemas for queries
    ALLOWED_SCHEMAS = {"prior_auth", "members", "providers", "claims", "analytics"}

    # Block non-SELECT statements
    BLOCKED_PATTERN = re.compile(
        r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|MERGE|GRANT|REVOKE)\b",
        re.IGNORECASE,
    )

    def load_context(self, context):
        """Load model config."""
        cfg = context.model_config or {}
        self.catalog = cfg.get("UC_CATALOG", "clinical-data-demo")
        self.warehouse_id = cfg.get("SQL_WAREHOUSE_ID", "")
        self.llm_endpoint = cfg.get("LLM_ENDPOINT", "databricks-llama-4-maverick")
        self.pa_schema = cfg.get("PA_SCHEMA", "prior_auth")

        from databricks.sdk import WorkspaceClient

        self.w = WorkspaceClient()

    # ------------------------------------------------------------------
    # Tool execution
    # ------------------------------------------------------------------

    def _execute_sql(self, sql: str) -> str:
        """Execute a read-only SQL query via Statement Execution API."""
        if self.BLOCKED_PATTERN.search(sql):
            return json.dumps({"error": "Only SELECT queries are allowed."})

        # Auto-append LIMIT if missing
        if "limit" not in sql.lower():
            sql = sql.rstrip(";") + " LIMIT 50"

        try:
            from databricks.sdk.service.sql import StatementState

            resp = self.w.statement_execution.execute_statement(
                warehouse_id=self.warehouse_id,
                statement=sql,
                wait_timeout="30s",
            )
            if resp.status and resp.status.state == StatementState.SUCCEEDED:
                cols = [c.name for c in resp.manifest.schema.columns]
                rows = [list(r) for r in (resp.result.data_array or [])]
                return json.dumps({"columns": cols, "rows": rows[:50]}, default=str)
            else:
                error = resp.status.error if resp.status else "Unknown error"
                return json.dumps({"error": str(error)})
        except Exception as e:
            return json.dumps({"error": str(e)})

    def _list_columns(self, table_name: str) -> str:
        """Describe a table's columns."""
        # Normalize table name
        if "." not in table_name:
            table_name = f"{self.pa_schema}.{table_name}"
        fqn = f"`{self.catalog}`.{table_name}"
        return self._execute_sql(f"DESCRIBE TABLE {fqn}")

    def _call_tool(self, name: str, args: dict) -> str:
        """Dispatch tool call."""
        if name == "query_uc_table":
            return self._execute_sql(args.get("sql", ""))
        elif name == "list_table_columns":
            return self._list_columns(args.get("table_name", ""))
        return json.dumps({"error": f"Unknown tool: {name}"})

    # ------------------------------------------------------------------
    # LLM interaction
    # ------------------------------------------------------------------

    def _call_llm(self, messages: List[dict], tools: Optional[list] = None) -> dict:
        """Call Foundation Model API via Databricks SDK api_client."""
        body = {
            "messages": messages,
            "max_tokens": 2048,
            "temperature": 0.1,
        }
        if tools:
            body["tools"] = tools

        try:
            return self.w.api_client.do(
                "POST",
                f"/serving-endpoints/{self.llm_endpoint}/invocations",
                body=body,
            )
        except Exception as e:
            return {"choices": [{"message": {"content": f"Error calling LLM: {e}"}}]}

    # ------------------------------------------------------------------
    # ChatModel interface
    # ------------------------------------------------------------------

    def predict(self, context, messages: List[ChatMessage], params: Optional[ChatParams] = None) -> ChatCompletionResponse:
        """Process a PA review request with tool-calling loop."""

        # Build message list
        conv = [{"role": "system", "content": self.SYSTEM_PROMPT}]
        for m in messages:
            conv.append({"role": m.role, "content": m.content})

        MAX_ROUNDS = 6
        for _ in range(MAX_ROUNDS):
            resp = self._call_llm(conv, tools=self.TOOLS)
            choice = resp["choices"][0]
            msg = choice["message"]

            # Check for tool calls
            tool_calls = msg.get("tool_calls")
            if not tool_calls:
                # Final answer
                return ChatCompletionResponse(
                    choices=[ChatChoice(index=0, message=ChatMessage(role="assistant", content=msg.get("content", "")))],
                    model=self.llm_endpoint,
                )

            # Execute tools
            conv.append(msg)
            for tc in tool_calls:
                fn = tc["function"]
                args = json.loads(fn["arguments"]) if isinstance(fn["arguments"], str) else fn["arguments"]
                result = self._call_tool(fn["name"], args)
                conv.append({"role": "tool", "content": result, "tool_call_id": tc["id"]})

        # Max rounds exceeded — return what we have
        return ChatCompletionResponse(
            choices=[ChatChoice(index=0, message=ChatMessage(
                role="assistant",
                content="I reached the maximum number of tool-calling rounds. Please refine your question.",
            ))],
            model=self.llm_endpoint,
        )


# Required for MLflow code-based logging
mlflow.models.set_model(PriorAuthReviewAgent())
