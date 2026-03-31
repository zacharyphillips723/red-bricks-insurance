"""Care Intelligence Agent — MLflow ChatModel for Unity Catalog registration.

This file defines the RAG agent as an mlflow.pyfunc.ChatModel so it can be:
  1. Logged via MLflow "models from code" (no serialization needed)
  2. Registered in Unity Catalog for governance, versioning, and A/B testing
  3. Deployed to Model Serving endpoints for evaluation and production use

The agent retrieves a member profile (gold_member_360) and relevant case note
chunks (Vector Search), then synthesizes a response via Foundation Model API.
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


class CareIntelligenceAgent(ChatModel):
    """RAG agent for care management member outreach preparation."""

    SYSTEM_PROMPT = (
        "You are a care management assistant for Red Bricks Insurance. "
        "You help care managers prepare for member outreach by synthesizing structured data "
        "(demographics, claims, risk scores, HEDIS gaps) with unstructured data (case notes, "
        "call transcripts, claims summaries).\n\n"
        "You will be given the member's profile and relevant case note excerpts. Synthesize "
        "this information into a clear, actionable summary. Always:\n"
        "- Cite sources (e.g., 'According to the case note from January 15, 2025...')\n"
        "- Highlight key risk factors and care gaps\n"
        "- Suggest relevant follow-up actions\n"
        "- Flag concerning trends (rising costs, worsening conditions)\n\n"
        "Never make up information not in the provided data."
    )

    def load_context(self, context) -> None:
        """Initialize SDK clients at model load time (not __init__)."""
        from databricks.sdk import WorkspaceClient

        self.w = WorkspaceClient()

        self.catalog = os.environ.get("UC_CATALOG") or "red_bricks_insurance"
        self.warehouse_id = os.environ.get("SQL_WAREHOUSE_ID") or self._auto_detect_warehouse()

        self.llm_endpoint = os.environ.get(
            "LLM_ENDPOINT", "databricks-llama-4-maverick"
        )
        self.vs_index = f"{self.catalog}.documents.case_notes_vs_index"
        self.member_360_table = f"{self.catalog}.analytics.gold_member_360"

    def _auto_detect_warehouse(self) -> str:
        """Auto-detect a SQL warehouse when none is configured."""
        try:
            for wh in self.w.warehouses.list():
                if wh.state and wh.state.value == "RUNNING":
                    return wh.id
            for wh in self.w.warehouses.list():
                return wh.id
        except Exception:
            pass
        return ""

    def predict(
        self, context, messages: List[ChatMessage], params: Optional[ChatParams] = None
    ) -> ChatCompletionResponse:
        """Process a care management query using RAG over member data."""
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

        # Step 2: Search case notes via Vector Search
        case_chunks = self._search_case_notes(member_id, question) if member_id else []

        # Step 3: Build context and call LLM
        profile_text = json.dumps(profile, indent=2, default=str) if profile else "No profile found."
        chunks_text = self._format_chunks(case_chunks)

        augmented_prompt = (
            f"Question: {question}\n\n"
            f"## Member Profile\n{profile_text}\n\n"
            f"## Relevant Case Notes and Documents\n{chunks_text}"
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
            print(f"[Agent] Profile retrieval error: {e}")
            return {}

    def _search_case_notes(self, member_id: str, query: str) -> list[dict]:
        """Search Vector Search index for relevant case note chunks."""
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
                    "num_results": 5,
                },
            )
            rows = data.get("result", {}).get("data_array", [])
            col_names = [c["name"] for c in data.get("manifest", {}).get("columns", [])]
            return [dict(zip(col_names, row)) for row in rows]
        except Exception as e:
            print(f"[Agent] Vector Search error: {e}")
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

    def _call_llm(self, messages: list[dict]) -> str:
        """Call Foundation Model API for chat completion."""
        try:
            data = self.w.api_client.do(
                "POST",
                f"/serving-endpoints/{self.llm_endpoint}/invocations",
                body={"messages": messages, "max_tokens": 1500, "temperature": 0.1},
            )
            return (
                data.get("choices", [{}])[0]
                .get("message", {})
                .get("content", "No response generated.")
            )
        except Exception as e:
            return f"Error generating response: {e}"


# Required for MLflow code-based logging
mlflow.models.set_model(CareIntelligenceAgent())
