"""Care Intelligence Agent v2 — Enhanced MLflow ChatModel for Unity Catalog.

Changes from v1:
  - SOAP-format structured responses (Subjective/Objective/Assessment/Plan)
  - Llama 4 Maverick model for improved clinical reasoning
  - 10 retrieval chunks (up from 5) for broader context
  - Benefit utilization data integration (cost/utilization context)
  - Lower temperature (0.05) for more deterministic clinical outputs
  - 2000 max tokens for longer structured responses
"""

import json
import os
from typing import List, Optional

import mlflow
from mlflow.pyfunc import ChatModel
from mlflow.types.llm import (
    ChatMessage,
    ChatParams,
    ChatCompletionResponse,
    ChatChoice,
)


class CareIntelligenceAgentV2(ChatModel):
    """Enhanced RAG agent with SOAP-format output and benefit utilization context."""

    SYSTEM_PROMPT = (
        "You are a care management assistant for Red Bricks Insurance. "
        "You help care managers prepare for member outreach by synthesizing structured data "
        "(demographics, claims, risk scores, HEDIS gaps, benefit utilization) with unstructured "
        "data (case notes, call transcripts, claims summaries).\n\n"
        "You will be given the member's profile, relevant case note excerpts, and benefit "
        "utilization data. Synthesize this information into a STRUCTURED SOAP-format response.\n\n"
        "Your response MUST include the following sections:\n\n"
        "## SUBJECTIVE\n"
        "Summarize the member's reported concerns, care manager observations from case notes, "
        "and any self-reported barriers to care (transportation, cost, adherence).\n\n"
        "## OBJECTIVE\n"
        "Present factual clinical data: diagnoses, lab values, medications, claims history, "
        "RAF/HCC scores, HEDIS gap status, and benefit utilization metrics. Cite sources with "
        "dates and document types (e.g., 'Case note from 2025-01-15, authored by RN Smith').\n\n"
        "## ASSESSMENT\n"
        "Provide a severity rating: **Critical** / **High** / **Moderate** / **Low**\n"
        "Justify the rating based on clinical risk factors, care gaps, cost trajectory, and "
        "social determinants. Flag concerning trends (rising costs, worsening conditions, "
        "missed appointments).\n\n"
        "## PLAN\n"
        "Provide prioritized, concrete next steps for the care manager:\n"
        "1. Immediate actions (within 48 hours)\n"
        "2. Short-term follow-ups (within 2 weeks)\n"
        "3. Ongoing monitoring recommendations\n"
        "Include specific talking points for member outreach.\n\n"
        "Always cite sources. Never make up information not in the provided data."
    )

    def load_context(self, context) -> None:
        """Initialize SDK clients at model load time (not __init__)."""
        from databricks.sdk import WorkspaceClient

        self.w = WorkspaceClient()

        self.catalog = os.environ.get("UC_CATALOG") or "red_bricks_insurance"
        self.warehouse_id = os.environ.get("SQL_WAREHOUSE_ID") or "781064a3466c0984"
        self.llm_endpoint = os.environ.get(
            "LLM_ENDPOINT", "databricks-llama-4-maverick"
        )
        self.vs_index = f"{self.catalog}.documents.case_notes_vs_index"
        self.member_360_table = f"{self.catalog}.analytics.gold_member_360"
        self.benefit_util_table = f"{self.catalog}.benefits.gold_member_benefit_utilization"

    def predict(
        self, context, messages: List[ChatMessage], params: Optional[ChatParams] = None
    ) -> ChatCompletionResponse:
        """Process a care management query using RAG over member data + benefit utilization."""
        # Extract the latest user message
        user_msg = ""
        member_id = ""
        for m in reversed(messages):
            if m.role == "user":
                user_msg = m.content
                break

        # Extract member_id from the message (expected format: "[MBR-XXXX] question")
        if user_msg.startswith("[") and "]" in user_msg:
            bracket_end = user_msg.index("]")
            member_id = user_msg[1:bracket_end].strip()
            question = user_msg[bracket_end + 1:].strip()
        else:
            question = user_msg

        # Step 1: Retrieve member profile
        profile = self._get_member_profile(member_id) if member_id else {}

        # Step 2: Search case notes via Vector Search (10 chunks)
        case_chunks = self._search_case_notes(member_id, question) if member_id else []

        # Step 3: Retrieve benefit utilization data
        benefit_util = self._get_benefit_utilization(member_id) if member_id else []

        # Step 4: Build context and call LLM
        profile_text = json.dumps(profile, indent=2, default=str) if profile else "No profile found."
        chunks_text = self._format_chunks(case_chunks)
        benefit_text = self._format_benefit_utilization(benefit_util)

        augmented_prompt = (
            f"Question: {question}\n\n"
            f"## Member Profile\n{profile_text}\n\n"
            f"## Relevant Case Notes and Documents\n{chunks_text}\n\n"
            f"## Benefit Utilization & Cost Context\n{benefit_text}"
        )

        llm_messages = [
            {"role": "system", "content": self.SYSTEM_PROMPT},
            {"role": "user", "content": augmented_prompt},
        ]

        answer = self._call_llm(llm_messages)

        return ChatCompletionResponse(
            choices=[ChatChoice(index=0, message=ChatMessage(role="assistant", content=answer))],
            usage={},
            model=self.llm_endpoint,
        )

    def _get_member_profile(self, member_id: str) -> dict:
        """Query gold_member_360 for the member profile."""
        from databricks.sdk.service.sql import StatementParameterListItem

        try:
            stmt = self.w.statement_execution.execute_statement(
                warehouse_id=self.warehouse_id,
                statement=f"SELECT * FROM {self.member_360_table} WHERE member_id = :member_id LIMIT 1",
                parameters=[
                    StatementParameterListItem(name="member_id", value=member_id, type="STRING")
                ],
                wait_timeout="30s",
            )
            if not stmt.result or not stmt.result.data_array:
                return {}
            col_names = [c.name for c in stmt.manifest.schema.columns]
            return dict(zip(col_names, stmt.result.data_array[0]))
        except Exception as e:
            print(f"[Agent v2] Profile retrieval error: {e}")
            return {}

    def _get_benefit_utilization(self, member_id: str) -> list[dict]:
        """Query gold_member_benefit_utilization for cost and utilization context."""
        from databricks.sdk.service.sql import StatementParameterListItem

        try:
            stmt = self.w.statement_execution.execute_statement(
                warehouse_id=self.warehouse_id,
                statement=(
                    f"SELECT benefit_category, utilization_ratio, "
                    f"total_allowed, total_paid, member_responsibility, "
                    f"risk_pool_segment, projected_annual_cost, "
                    f"peer_avg_cost, cost_variance_pct "
                    f"FROM {self.benefit_util_table} "
                    f"WHERE member_id = :member_id "
                    f"ORDER BY total_paid DESC"
                ),
                parameters=[
                    StatementParameterListItem(name="member_id", value=member_id, type="STRING")
                ],
                wait_timeout="30s",
            )
            if not stmt.result or not stmt.result.data_array:
                return []
            col_names = [c.name for c in stmt.manifest.schema.columns]
            return [dict(zip(col_names, row)) for row in stmt.result.data_array]
        except Exception as e:
            print(f"[Agent v2] Benefit utilization retrieval error: {e}")
            return []

    def _search_case_notes(self, member_id: str, query: str) -> list[dict]:
        """Search Vector Search index for relevant case note chunks (10 results)."""
        try:
            data = self.w.api_client.do(
                "POST",
                f"/api/2.0/vector-search/indexes/{self.vs_index}/query",
                body={
                    "columns": [
                        "chunk_id", "document_id", "member_id",
                        "document_type", "title", "created_date", "author", "chunk_text",
                    ],
                    "query_text": query,
                    "filters_json": json.dumps({"member_id": member_id}),
                    "num_results": 10,
                },
            )
            rows = data.get("result", {}).get("data_array", [])
            col_names = [c["name"] for c in data.get("manifest", {}).get("columns", [])]
            return [dict(zip(col_names, row)) for row in rows]
        except Exception as e:
            print(f"[Agent v2] Vector Search error: {e}")
            return []

    def _format_chunks(self, chunks: list[dict]) -> str:
        """Format case note chunks into context text."""
        if not chunks:
            return "No case notes or documents found for this member."
        parts = []
        for chunk in chunks:
            doc_type = chunk.get("document_type", "unknown")
            date = chunk.get("created_date", "unknown date")
            author = chunk.get("author", "unknown")
            text = chunk.get("chunk_text", "")
            parts.append(f"---\n[{doc_type}] Date: {date}, Author: {author}\n{text}")
        return "\n".join(parts)

    def _format_benefit_utilization(self, rows: list[dict]) -> str:
        """Format benefit utilization data into context text."""
        if not rows:
            return "No benefit utilization data available for this member."
        parts = ["| Category | Utilization Ratio | Total Paid | Member Cost | Risk Segment | Projected Annual | vs Peer Avg |"]
        parts.append("|---|---|---|---|---|---|---|")
        for r in rows:
            parts.append(
                f"| {r.get('benefit_category', 'N/A')} "
                f"| {r.get('utilization_ratio', 'N/A')} "
                f"| ${r.get('total_paid', 'N/A')} "
                f"| ${r.get('member_responsibility', 'N/A')} "
                f"| {r.get('risk_pool_segment', 'N/A')} "
                f"| ${r.get('projected_annual_cost', 'N/A')} "
                f"| {r.get('cost_variance_pct', 'N/A')}% |"
            )
        return "\n".join(parts)

    def _call_llm(self, messages: list[dict]) -> str:
        """Call Foundation Model API for chat completion."""
        try:
            data = self.w.api_client.do(
                "POST",
                f"/serving-endpoints/{self.llm_endpoint}/invocations",
                body={"messages": messages, "max_tokens": 2000, "temperature": 0.05},
            )
            return (
                data.get("choices", [{}])[0]
                .get("message", {})
                .get("content", "No response generated.")
            )
        except Exception as e:
            return f"Error generating response: {e}"


# Required for MLflow code-based logging
mlflow.models.set_model(CareIntelligenceAgentV2())
