"""FWA Supervisor Agent — MLflow ChatModel for Model Serving Endpoint.

Wraps the supervisor agent pattern (Genie + Gemini sub-agents → synthesis)
into a ChatModel that can be deployed as a Model Serving endpoint with
inference tables enabled for automatic request/response/trace logging.

Input format: [TARGET_ID] <question>
  - [INV-XXXX] question → investigation briefing
  - [PRV-XXXXXXXXXX] question → provider analysis
  - [MBR-XXXX] question → member fraud history

All external dependencies (Genie API, FMAPI, Vector Search, Lakebase, SQL
warehouse) are accessed via WorkspaceClient() — auto-authenticated when
running inside a serving endpoint.
"""

import json
import os
import re
import time
import traceback
from typing import List, Optional

import mlflow
from mlflow.pyfunc import ChatModel
from mlflow.types.llm import (
    ChatMessage,
    ChatParams,
    ChatCompletionResponse,
    ChatChoice,
)


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

Always attribute which sub-agent provided each finding. Never fabricate evidence."""

GEMINI_SUBAGENT_PROMPT = """You are an FWA (Fraud, Waste & Abuse) Clinical Analyst for Red Bricks Insurance.
You specialize in medical policy compliance and clinical billing analysis.

You have tools to:
- Query Unity Catalog tables for provider risk profiles, flagged claims, and ML model scores
- Search medical policy documents via semantic similarity (Vector Search) using search_medical_policies_vs_index
- Classify findings as Fraud, Waste, or Abuse based on evidence and policy
- Query the Lakebase investigation case database for investigation status and audit trail

## MANDATORY WORKFLOW — Follow these steps in order:

1. **Query provider/investigation data** — Use query_uc_table or query_lakebase_cases to get
   the provider risk profile, flagged claims, ML model scores, and investigation details.

2. **ALWAYS search medical policies** — You MUST call search_medical_policies_vs_index at least once
   for EVERY investigation. This is non-negotiable. Our medical policy documents are the
   authoritative source for determining whether billing practices are compliant. Search for
   the specific procedure codes, service categories, or billing patterns found in step 1.

3. **Classify each finding** — After gathering both claims data AND policy context, use
   classify_fwa_type to formally classify each suspicious finding as Fraud, Waste, or Abuse.

## CRITICAL RULES:
- NEVER skip search_medical_policies_vs_index. Even if the fraud seems obvious from claims data alone,
  you MUST search our medical policies to cite the specific rules being violated.
- When you find procedure codes (CPT codes like 99213, 99215, etc.), ALWAYS search for the
  policy governing those codes.
- When you find fraud types (upcoding, unbundling, duplicate billing), ALWAYS search for our
  policy on that practice.
- Reference specific policy names, rule types, claim IDs, and dollar amounts in your analysis."""

GENIE_QUESTIONS_TEMPLATE = """You are generating natural language questions for a SQL analytics system (Genie) that queries FWA (Fraud, Waste & Abuse) claims data.

Target type: {target_type}
Target ID: {target_id}
Investigation ID: {investigation_id}
User question: "{question}"

NOTE: If "Investigation ID" is non-empty, the target was resolved from that investigation. The target_type and target_id above are the ACTUAL entity to query — use them directly.

## ID FORMAT RULES:
- **Provider NPI**: 10-digit numeric (e.g. 1544514776). Filter on `provider_npi`, `rendering_provider_npi`, or `billing_provider_npi`.
- **Claim ID**: Starts with "MC" followed by 9 digits (e.g. MC775156534). Filter on `claim_id`.
- **Member ID**: Starts with "MBR" followed by 6 digits (e.g. MBR105036). Filter on `member_id`.
- **Investigation ID**: Starts with "INV-" followed by 4 digits (e.g. INV-0038). Filter on `investigation_id`.

## AVAILABLE TABLES (key columns):
- `gold_fwa_provider_risk`: provider_npi, total_claims, total_billed, total_paid, denial_rate, risk_tier, composite_risk_score, fwa_primary_fraud_type, fwa_estimated_overpayment
- `gold_fwa_claim_flags`: claim_id, member_id, provider_npi, fraud_type, fraud_score, severity, estimated_overpayment, procedure_code, billed_amount, allowed_amount
- `silver_fwa_investigation_cases`: investigation_id, target_type, target_id, fraud_types, severity, status, estimated_overpayment, investigation_summary
- `silver_claims_medical`: claim_id, member_id, rendering_provider_npi, billing_provider_npi, procedure_code, billed_amount, allowed_amount, paid_amount, claim_status, denial_reason_code
- `gold_fwa_member_risk`: member_id, composite_member_fwa_score, doctor_shopping_score, pharmacy_abuse_score, fwa_signal_count
- `silver_fwa_signals`: claim_id, provider_npi, fraud_type, fraud_score, evidence_summary, estimated_overpayment
- `gold_fwa_summary`: fraud_type, severity, signal_count, total_estimated_overpayment (aggregate stats)
- `silver_providers`: npi, provider_name, specialty, network_status

## INSTRUCTIONS:
Generate exactly 2 questions. Each question should be simple and direct — filter on the target ID in the appropriate column.

For target_type = "provider":
- Question 1: Query the provider risk profile and FWA claim flags by `provider_npi`.
- Question 2: Query raw claims data by `rendering_provider_npi` or `billing_provider_npi` for billing patterns.

For target_type = "member":
- Question 1: Query the member risk profile from `gold_fwa_member_risk` by `member_id`, plus FWA claim flags by `member_id`.
- Question 2: Query raw medical claims from `silver_claims_medical` by `member_id` for billing details and denial patterns.

For target_type = "provider_ring":
- Question 1: Query FWA claim flags where `fraud_type = 'provider_ring'` and `provider_npi` matches the target.
- Question 2: Query FWA signals for that provider to find related providers in the ring.

If an investigation_id is present, include a mention of investigation {investigation_id} in Question 1 to also pull investigation case details from `silver_fwa_investigation_cases`.

For a claim ID: Question 1 should query the claim details and FWA flags. Question 2 should query the provider risk profile for the claim's provider.

## EXAMPLES:

Provider NPI:
["For provider NPI 1544514776, show the risk tier, composite risk score, total billed vs allowed amounts, denial rate, primary fraud type, and estimated overpayment from the provider risk table, plus any FWA claim flags with fraud type, severity, and procedure codes.", "For provider NPI 1544514776, show the top 10 claims by billed amount from the medical claims table including procedure codes, billed and paid amounts, claim status, and denial reasons."]

Member (resolved from investigation INV-0047):
["For member MBR101339, show the composite FWA score, doctor shopping score, pharmacy abuse score, and FWA signal count from the member risk table. Also show the investigation details for INV-0047 including fraud types, severity, status, and estimated overpayment.", "For member MBR101339, show all FWA claim flags including claim ID, provider NPI, fraud type, fraud score, severity, procedure code, billed amount, and estimated overpayment, ordered by fraud score descending."]

Member (direct):
["For member MBR105036, show the composite FWA score, doctor shopping score, pharmacy abuse score, FWA signal count from the member risk table.", "For member MBR105036, show all FWA claim flags including claim ID, provider NPI, fraud type, fraud score, severity, procedure code, billed amount, and estimated overpayment."]

Return ONLY a JSON array with exactly 2 question strings, nothing else."""

# Allowed schemas the agent can query
ALLOWED_SCHEMAS = ["fwa", "analytics", "claims", "members", "providers", "pharmacy", "benefits", "prior_auth"]

MAX_TOOL_ROUNDS = 6


class FWASupervisorAgent(ChatModel):
    """FWA Supervisor Agent — orchestrates Genie + Gemini sub-agents as a ChatModel."""

    def load_context(self, context) -> None:
        """Initialize SDK clients and config at model load time."""
        from databricks.sdk import WorkspaceClient

        self.w = WorkspaceClient()

        self.catalog = os.environ.get("UC_CATALOG", "red_bricks_insurance")
        self.warehouse_id = os.environ.get("SQL_WAREHOUSE_ID") or self._auto_detect_warehouse()
        _genie_env = os.environ.get("GENIE_SPACE_ID", "")
        self.genie_space_id = _genie_env if _genie_env not in ("", "auto") else self._auto_detect_genie_space()
        self.llm_endpoint = os.environ.get("LLM_ENDPOINT", "databricks-llama-4-maverick")
        self.gemini_endpoint = os.environ.get("GEMINI_ENDPOINT", "databricks-gemini-2-5-pro")
        self.vs_index_name = os.environ.get("VS_INDEX_NAME", f"{self.catalog}.prior_auth.medical_policy_vs_index")

        # Lakebase config
        self.lakebase_project_id = os.environ.get("LAKEBASE_PROJECT_ID", "red-bricks-insurance")
        self.lakebase_branch = os.environ.get("LAKEBASE_BRANCH", "production")
        self.lakebase_database_name = os.environ.get("LAKEBASE_DATABASE_NAME", "fwa_cases")

        self._cat = f"`{self.catalog}`"

        # Build Gemini tool definitions with catalog reference
        self._gemini_tools = self._build_gemini_tools()

        # Table references
        self.provider_risk_table = f"{self._cat}.fwa.gold_fwa_provider_risk"
        self.claim_flags_table = f"{self._cat}.fwa.gold_fwa_claim_flags"
        self.model_inference_table = f"{self._cat}.analytics.gold_fwa_model_scores"

    def _auto_detect_warehouse(self) -> str:
        try:
            for wh in self.w.warehouses.list():
                if wh.state and wh.state.value == "RUNNING":
                    return wh.id
            for wh in self.w.warehouses.list():
                return wh.id
        except Exception:
            pass
        return ""

    def _auto_detect_genie_space(self) -> str:
        try:
            resp = self.w.api_client.do("GET", "/api/2.0/genie/spaces")
            spaces = resp.get("spaces", [])
            target_title = "Red Bricks Insurance — FWA Analytics"
            for s in spaces:
                if s.get("title") == target_title:
                    return s["space_id"]
            if spaces:
                return spaces[0]["space_id"]
        except Exception:
            pass
        return ""

    def _build_gemini_tools(self) -> list:
        cat = self._cat
        return [
            {
                "type": "function",
                "function": {
                    "name": "query_uc_table",
                    "description": (
                        f"Execute a read-only SQL SELECT query against Unity Catalog tables in the {cat} catalog. "
                        "Key FWA tables:\n"
                        f"- {cat}.fwa.gold_fwa_provider_risk: Provider risk scorecards\n"
                        f"- {cat}.fwa.gold_fwa_claim_flags: Flagged claims with evidence\n"
                        f"- {cat}.fwa.gold_fwa_summary: Aggregate FWA metrics\n"
                        f"- {cat}.fwa.silver_fwa_signals: Individual FWA signals\n"
                        f"- {cat}.analytics.gold_fwa_model_scores: ML model fraud predictions per claim\n"
                        f"- {cat}.claims.silver_claims_medical: Medical claims detail\n"
                        f"- {cat}.providers.silver_providers: Provider demographics\n"
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
                                "description": f"Table name (e.g. 'fwa.gold_fwa_provider_risk'). Catalog '{cat}' is added automatically.",
                            },
                        },
                        "required": ["table_name"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "search_medical_policies_vs_index",
                    "description": (
                        "Search Red Bricks Insurance medical policy documents using semantic similarity "
                        "via Vector Search. Returns relevant policy text chunks with policy name and "
                        "service category. Use this for broad, natural-language policy questions where "
                        "exact procedure codes or rule IDs are not known."
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
            {
                "type": "function",
                "function": {
                    "name": "classify_fwa_type",
                    "description": (
                        "Classify an FWA finding as Fraud, Waste, or Abuse based on claim evidence and "
                        "retrieved medical policy context. Returns a structured classification with confidence "
                        "score, reasoning, and policy citations."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "claim_summary": {
                                "type": "string",
                                "description": "Summary of the claim or billing pattern being evaluated.",
                            },
                            "policy_context": {
                                "type": "string",
                                "description": "Relevant medical policy sections retrieved from search_medical_policies_vs_index.",
                            },
                        },
                        "required": ["claim_summary", "policy_context"],
                    },
                },
            },
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
                                "description": "SQL query against the fwa_cases Lakebase database.",
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

    # -------------------------------------------------------------------
    # ChatModel interface
    # -------------------------------------------------------------------

    @mlflow.trace(span_type="AGENT", name="fwa_supervisor_agent")
    def predict(
        self, context, messages: List[ChatMessage], params: Optional[ChatParams] = None
    ) -> ChatCompletionResponse:
        """Process an FWA investigation query using supervisor agent pattern."""
        # Extract last user message
        user_msg = ""
        for m in reversed(messages):
            if m.role == "user":
                user_msg = m.content
                break

        target_id, target_type, question = self._parse_input(user_msg)

        try:
            print(f"[Supervisor] Processing: {target_type} {target_id} — {question[:80]}...")

            # Phase 1: Dispatch to sub-agents sequentially
            genie_result = None
            gemini_result = None

            try:
                genie_result = self._run_genie_subagent(target_id, target_type, question)
            except Exception as e:
                print(f"[Supervisor] Genie sub-agent error: {e}")
                traceback.print_exc()

            try:
                gemini_result = self._run_gemini_subagent(target_id, target_type, question)
            except Exception as e:
                print(f"[Supervisor] Gemini sub-agent error: {e}")
                traceback.print_exc()

            # Phase 2: Build synthesis context
            synthesis_context = f"Original question: {question}\nTarget: {target_id} ({target_type})\n\n"

            synthesis_context += "## GENIE SUB-AGENT (Structured Claims Data)\n"
            if genie_result:
                for r in genie_result.get("results", []):
                    synthesis_context += f"\n### Question: {r.get('question', 'N/A')}\n"
                    if r.get("error"):
                        synthesis_context += f"Error: {r['error']}\n"
                    else:
                        synthesis_context += f"SQL: {r.get('sql_query', 'N/A')}\n"
                        synthesis_context += f"Rows returned: {r.get('row_count', 0)}\n"
                        if r.get("rows"):
                            synthesis_context += f"Data:\n```json\n{json.dumps(r['rows'][:10], default=str, indent=1)}\n```\n"
                        if r.get("description"):
                            synthesis_context += f"Description: {r['description']}\n"
            else:
                synthesis_context += "Genie sub-agent did not return results.\n"

            synthesis_context += "\n## GEMINI ANALYST SUB-AGENT (Medical Policy & Clinical Analysis)\n"
            if gemini_result:
                synthesis_context += f"Model: {gemini_result.get('model', 'unknown')}\n"
                synthesis_context += f"Tools used: {gemini_result.get('tools_used', [])}\n"
                synthesis_context += f"Tables queried: {gemini_result.get('tables_queried', 0)}\n\n"
                synthesis_context += f"Analysis:\n{gemini_result.get('answer', 'No analysis provided.')}\n"
            else:
                synthesis_context += "Gemini sub-agent did not return results.\n"

            # Phase 3: Supervisor synthesizes
            with mlflow.start_span(name="supervisor_synthesis", span_type="LLM") as synth_span:
                synth_span.set_inputs({"model": self.llm_endpoint, "sub_agents": ["genie", "gemini"]})
                t0 = time.time()
                data = self._sdk_request("POST", f"/serving-endpoints/{self.llm_endpoint}/invocations", {
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

            answer = data.get("choices", [{}])[0].get("message", {}).get("content", "No response.")

        except Exception as e:
            print(f"[Supervisor] ERROR: {e}")
            traceback.print_exc()
            answer = f"Error: {str(e)}"

        return ChatCompletionResponse(
            choices=[ChatChoice(index=0, message=ChatMessage(role="assistant", content=answer))],
            usage={},
            model=self.llm_endpoint,
        )

    # -------------------------------------------------------------------
    # Input parsing
    # -------------------------------------------------------------------

    def _parse_input(self, user_msg: str) -> tuple[str, str, str]:
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

    # -------------------------------------------------------------------
    # SDK helpers
    # -------------------------------------------------------------------

    def _sdk_request(self, method: str, path: str, body: dict | None = None) -> dict:
        return self.w.api_client.do(method, path, body=body) if body else self.w.api_client.do(method, path)

    def _execute_sql(self, sql: str, params: list | None = None) -> list[dict]:
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
        col_names = [c.name for c in stmt.manifest.schema.columns] if stmt.manifest and stmt.manifest.schema else []
        if not col_names:
            return []
        return [dict(zip(col_names, row)) for row in stmt.result.data_array]

    def _validate_sql(self, sql: str) -> str | None:
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

    # -------------------------------------------------------------------
    # Tool execution
    # -------------------------------------------------------------------

    def _execute_tool(self, tool_name: str, tool_args: dict) -> str:
        if tool_name == "query_uc_table":
            sql = tool_args.get("sql", "").strip()
            purpose = tool_args.get("purpose", "")
            error = self._validate_sql(sql)
            if error:
                return json.dumps({"error": error})
            if "LIMIT" not in sql.upper():
                sql = sql.rstrip(";") + " LIMIT 50"
            print(f"[Gemini Agent] Tool query ({purpose}): {sql[:200]}")
            try:
                rows = self._execute_sql(sql)
                if not rows:
                    return json.dumps({"result": [], "row_count": 0, "message": "No rows returned."})
                return json.dumps({"result": rows[:50], "row_count": len(rows)}, default=str)
            except Exception as e:
                return json.dumps({"error": f"SQL error: {str(e)}"})

        elif tool_name == "list_table_columns":
            table_name = tool_args.get("table_name", "").strip()
            table_name = table_name.replace(f"`{self.catalog}`.", "").replace(f"{self.catalog}.", "")
            table_name = f"{self._cat}.{table_name}"
            try:
                rows = self._execute_sql(f"DESCRIBE TABLE {table_name}")
                return json.dumps({"table": table_name, "columns": rows}, default=str)
            except Exception as e:
                return json.dumps({"error": f"Could not describe table: {str(e)}"})

        elif tool_name == "search_medical_policies_vs_index":
            query = tool_args.get("query", "").strip()
            top_k = tool_args.get("top_k", 5)
            if not query:
                return json.dumps({"error": "Query is required."})
            print(f"[Gemini Agent] Searching medical policies: {query[:100]}")
            try:
                with mlflow.start_span(name="search_medical_policies_vs_index", span_type="RETRIEVER") as span:
                    span.set_inputs({"query": query, "top_k": top_k})
                    vs_result = self.w.api_client.do(
                        "POST",
                        f"/api/2.0/vector-search/indexes/{self.vs_index_name}/query",
                        body={
                            "query_text": query,
                            "columns": ["chunk_id", "policy_name", "service_category", "chunk_text"],
                            "num_results": top_k,
                        },
                    )
                    data_array = vs_result.get("result", {}).get("data_array", [])
                    columns = vs_result.get("manifest", {}).get("columns", [])
                    col_names = [c.get("name", f"col_{i}") for i, c in enumerate(columns)]
                    policies = [dict(zip(col_names, row)) for row in data_array]
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
                resp = self._sdk_request("POST", f"/serving-endpoints/{self.gemini_endpoint}/invocations", {
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
            first_word = sql.split()[0].upper() if sql.split() else ""
            if first_word not in ("SELECT", "WITH"):
                return json.dumps({"error": "Only SELECT/WITH queries are allowed against Lakebase."})
            print(f"[Gemini Agent] Lakebase query ({purpose}): {sql[:100]}")
            try:
                import psycopg
                endpoint_path = f"projects/{self.lakebase_project_id}/branches/{self.lakebase_branch}/endpoints/primary"
                ep = self.w.postgres.get_endpoint(name=endpoint_path)
                host = ep.status.hosts.host
                cred = self.w.postgres.generate_database_credential(endpoint=endpoint_path)
                username = self.w.current_user.me().user_name
                conn = psycopg.connect(
                    f"host={host} dbname={self.lakebase_database_name} user={username} password={cred.token} sslmode=require"
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

    # -------------------------------------------------------------------
    # Genie sub-agent
    # -------------------------------------------------------------------

    @mlflow.trace(span_type="TOOL", name="genie_subagent")
    def _run_genie_subagent(self, target_id: str, target_type: str, question: str) -> dict:
        from databricks.sdk.errors import OperationFailed
        from datetime import timedelta

        # Resolve investigation IDs to their actual target before generating Genie questions.
        # This avoids asking Genie to do complex conditional joins.
        resolved_type = target_type
        resolved_id = target_id
        if target_type == "investigation":
            try:
                rows = self._execute_sql(
                    f"SELECT target_type, target_id FROM `{self.catalog}`.`fwa`.`silver_fwa_investigation_cases` WHERE investigation_id = :inv_id",
                    [{"name": "inv_id", "value": target_id}],
                )
                if rows:
                    resolved_type = rows[0].get("target_type", target_type)
                    resolved_id = rows[0].get("target_id", target_id)
                    print(f"[Genie Sub-agent] Resolved {target_id} -> {resolved_type} {resolved_id}")
            except Exception as e:
                print(f"[Genie Sub-agent] Investigation resolution failed: {e}, using raw IDs")

        # Generate Genie-appropriate questions using the supervisor LLM
        try:
            prompt = GENIE_QUESTIONS_TEMPLATE.format(
                target_type=resolved_type, target_id=resolved_id,
                investigation_id=target_id if target_type == "investigation" else "",
                question=question,
            )
            resp = self._sdk_request("POST", f"/serving-endpoints/{self.llm_endpoint}/invocations", {
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 500,
                "temperature": 0.0,
            })
            content = resp.get("choices", [{}])[0].get("message", {}).get("content", "[]")
            clean = content.strip().removeprefix("```json").removesuffix("```").strip()
            genie_questions = json.loads(clean)
            if not isinstance(genie_questions, list):
                genie_questions = [str(genie_questions)]
        except Exception as e:
            print(f"[Genie Sub-agent] Question generation failed: {e}, using original")
            genie_questions = [question]

        results = []
        for gq in genie_questions[:2]:
            print(f"[Genie Sub-agent] Asking: {gq[:80]}")
            try:
                genie_result = self._ask_genie(question_text=gq)
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

    @mlflow.trace(span_type="TOOL", name="genie_query")
    def _ask_genie(self, question_text: str) -> dict:
        """Send a question to the Genie Space and return structured results."""
        from databricks.sdk.errors import OperationFailed
        from datetime import timedelta

        if not self.genie_space_id:
            return {
                "conversation_id": "", "message_id": "", "sql_query": None,
                "columns": [], "rows": [], "row_count": 0,
                "description": "Genie space not configured.",
            }

        print(f"[Genie] Asking: {question_text[:80]}...")

        wait_obj = self.w.genie.start_conversation(
            space_id=self.genie_space_id,
            content=question_text,
        )

        try:
            msg = wait_obj.result(timeout=timedelta(seconds=120))
        except OperationFailed:
            conv_id = wait_obj.conversation_id
            msg_id = wait_obj.message_id
            msg = self.w.genie.get_message(
                space_id=self.genie_space_id,
                conversation_id=conv_id,
                message_id=msg_id,
            )

        sql_query = None
        columns: list[str] = []
        rows: list[dict] = []
        description = None
        query_attachment_id = None

        if msg.attachments:
            for att in msg.attachments:
                if att.query and att.query.query:
                    sql_query = att.query.query
                    query_attachment_id = att.attachment_id
                if att.text and att.text.content:
                    description = att.text.content

        if sql_query and query_attachment_id:
            try:
                qr = self.w.genie.get_message_query_result_by_attachment(
                    space_id=self.genie_space_id,
                    conversation_id=msg.conversation_id,
                    message_id=msg.message_id,
                    attachment_id=query_attachment_id,
                )
                stmt = qr.statement_response
                if stmt and stmt.result and stmt.result.data_array:
                    col_names = [
                        c.name for c in (stmt.manifest.schema.columns or [])
                    ] if stmt.manifest and stmt.manifest.schema else []
                    columns = col_names
                    rows = [dict(zip(col_names, row)) for row in stmt.result.data_array]
                print(f"[Genie] Query returned {len(rows)} rows")
            except Exception as e:
                print(f"[Genie] Query result fetch failed: {e}")
                if self.warehouse_id:
                    try:
                        stmt = self.w.statement_execution.execute_statement(
                            warehouse_id=self.warehouse_id,
                            statement=sql_query,
                            wait_timeout="30s",
                        )
                        if stmt.result and stmt.result.data_array:
                            col_names = [
                                c.name for c in (stmt.manifest.schema.columns or [])
                            ] if stmt.manifest and stmt.manifest.schema else []
                            columns = col_names
                            rows = [dict(zip(col_names, row)) for row in stmt.result.data_array]
                        print(f"[Genie] Fallback query returned {len(rows)} rows")
                    except Exception as e2:
                        print(f"[Genie] Fallback query execution failed: {e2}")

        return {
            "conversation_id": msg.conversation_id,
            "message_id": msg.message_id,
            "sql_query": sql_query,
            "columns": columns,
            "rows": rows,
            "row_count": len(rows),
            "description": description,
        }

    # -------------------------------------------------------------------
    # Gemini sub-agent (tool-calling)
    # -------------------------------------------------------------------

    @mlflow.trace(span_type="AGENT", name="gemini_subagent")
    def _run_gemini_subagent(self, target_id: str, target_type: str, question: str) -> dict:
        context_hint = f"Question: {question}\nTarget: {target_id} (type: {target_type})\n\n"
        context_hint += f"The Unity Catalog is: {self._cat}\n"

        if target_type == "investigation":
            lb_data = self._fetch_investigation_from_lakebase(target_id)
            if lb_data:
                context_hint += (
                    f"Investigation record from Lakebase:\n```json\n{lb_data}\n```\n\n"
                    "Query UC tables for flagged claims, provider risk, and ML model scores.\n"
                )
        elif target_type == "provider":
            context_hint += (
                f"Start by querying: SELECT * FROM {self.provider_risk_table} WHERE provider_npi = '{target_id}' LIMIT 1\n"
                f"Then query flagged claims from {self.claim_flags_table} and ML predictions from {self.model_inference_table}.\n"
                "After gathering data, search medical policies for relevant procedure codes.\n"
            )

        messages = [
            {"role": "system", "content": GEMINI_SUBAGENT_PROMPT},
            {"role": "user", "content": context_hint},
        ]

        tables_queried = 0
        tools_used = []
        policy_chunks = []

        for _ in range(MAX_TOOL_ROUNDS):
            t0 = time.time()
            with mlflow.start_span(name="gemini_llm_call", span_type="LLM") as llm_span:
                llm_span.set_inputs({"model": self.gemini_endpoint, "message_count": len(messages)})
                data = self._sdk_request("POST", f"/serving-endpoints/{self.gemini_endpoint}/invocations", {
                    "messages": messages,
                    "tools": self._gemini_tools,
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
                answer = data.get("choices", [{}])[0].get("message", {}).get("content", "No response.")
                return {
                    "agent": "gemini_analyst",
                    "model": self.gemini_endpoint,
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

                span_type = "RETRIEVER" if tool_name == "search_medical_policies_vs_index" else "TOOL"
                with mlflow.start_span(name=tool_name, span_type=span_type) as tool_span:
                    tool_span.set_inputs(tool_args)
                    result = self._execute_tool(tool_name, tool_args)
                    tool_span.set_outputs({"result_length": len(result)})

                if tool_name == "search_medical_policies_vs_index":
                    try:
                        parsed = json.loads(result)
                        for p in parsed.get("policies", []):
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
        data = self._sdk_request("POST", f"/serving-endpoints/{self.gemini_endpoint}/invocations", {
            "messages": messages,
            "max_tokens": 3000,
            "temperature": 0.05,
        })
        answer = data.get("choices", [{}])[0].get("message", {}).get("content", "No response.")
        return {
            "agent": "gemini_analyst",
            "model": self.gemini_endpoint,
            "answer": answer,
            "tables_queried": tables_queried,
            "tools_used": tools_used,
            "policy_chunks": policy_chunks,
        }

    # -------------------------------------------------------------------
    # Lakebase helper
    # -------------------------------------------------------------------

    def _fetch_investigation_from_lakebase(self, inv_id: str) -> str | None:
        try:
            import psycopg
            endpoint_path = f"projects/{self.lakebase_project_id}/branches/{self.lakebase_branch}/endpoints/primary"
            ep = self.w.postgres.get_endpoint(name=endpoint_path)
            host = ep.status.hosts.host
            cred = self.w.postgres.generate_database_credential(endpoint=endpoint_path)
            username = self.w.current_user.me().user_name
            conn = psycopg.connect(
                f"host={host} dbname={self.lakebase_database_name} user={username} password={cred.token} sslmode=require"
            )
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


# Required for MLflow code-based logging
mlflow.models.set_model(FWASupervisorAgent())
