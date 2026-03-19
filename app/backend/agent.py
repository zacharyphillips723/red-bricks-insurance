"""Member RAG Agent and Member 360 data integration module.

Implements the care management RAG agent directly in the backend using
Foundation Model API + Vector Search REST API. Uses the Databricks SDK
for SQL execution (which handles auth natively in App context) and REST
API only for Vector Search (where SDK has deserialization bugs).
"""

import json
import os
import traceback

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem

UC_CATALOG = os.environ.get("UC_CATALOG", "catalog_insurance_vpx9o6")
UC_SCHEMA = os.environ.get("UC_SCHEMA", "red_bricks_insurance_dev")
SQL_WAREHOUSE_ID = os.environ.get("SQL_WAREHOUSE_ID", "781064a3466c0984")
LLM_ENDPOINT = os.environ.get("LLM_ENDPOINT", "databricks-meta-llama-3-3-70b-instruct")

MEMBER_360_TABLE = f"{UC_CATALOG}.{UC_SCHEMA}.gold_member_360"
CASE_NOTES_TABLE = f"{UC_CATALOG}.{UC_SCHEMA}.silver_case_notes"
VS_INDEX_NAME = f"{UC_CATALOG}.{UC_SCHEMA}.case_notes_vs_index"

SYSTEM_PROMPT = """You are a care management assistant for Red Bricks Insurance.
You help care managers prepare for member outreach by synthesizing structured data
(demographics, claims, risk scores, HEDIS gaps) with unstructured data (case notes,
call transcripts, claims summaries).

You will be given the member's profile and relevant case note excerpts. Synthesize
this information into a clear, actionable summary. Always:
- Cite sources (e.g., "According to the case note from January 15, 2025...")
- Highlight key risk factors and care gaps
- Suggest relevant follow-up actions
- Flag concerning trends (rising costs, worsening conditions)

Never make up information not in the provided data."""


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

    col_names = []
    if stmt.manifest and stmt.manifest.schema and stmt.manifest.schema.columns:
        col_names = [c.name for c in stmt.manifest.schema.columns]

    print(f"[SQL] col_names={col_names}, rows={len(stmt.result.data_array)}, first_row={stmt.result.data_array[0] if stmt.result.data_array else 'empty'}")

    if not col_names:
        print(f"[SQL] WARNING: No column names from manifest. manifest={stmt.manifest}")
        return []

    return [dict(zip(col_names, row)) for row in stmt.result.data_array]


def _sdk_request(method: str, path: str, body: dict | None = None) -> dict:
    """Make an authenticated request using the SDK's api_client (handles App OAuth)."""
    w = WorkspaceClient()
    if method == "GET":
        resp = w.api_client.do(method, path)
    else:
        resp = w.api_client.do(method, path, body=body)
    return resp


def search_members(query: str) -> list[dict]:
    """Search members by name or ID."""
    try:
        sql = f"""
            SELECT member_id, first_name, last_name, member_name, date_of_birth,
                   age, gender, line_of_business, risk_tier, raf_score, county
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY member_id) AS rn
                FROM {MEMBER_360_TABLE}
                WHERE member_id = :query
                   OR LOWER(member_name) LIKE CONCAT('%', LOWER(:query), '%')
                   OR LOWER(last_name) LIKE CONCAT('%', LOWER(:query), '%')
                   OR LOWER(first_name) LIKE CONCAT('%', LOWER(:query), '%')
            )
            WHERE rn = 1
            ORDER BY last_name, first_name
            LIMIT 25
        """
        rows = _execute_sql(sql, [{"name": "query", "value": query}])
        print(f"[Member Search] Query '{query}' returned {len(rows)} results")
        return rows
    except Exception as e:
        print(f"[Member Search] ERROR: {e}")
        traceback.print_exc()
        return []


def get_member_360(member_id: str) -> dict | None:
    """Get full Member 360 profile."""
    try:
        sql = f"SELECT * FROM {MEMBER_360_TABLE} WHERE member_id = :member_id LIMIT 1"
        rows = _execute_sql(sql, [{"name": "member_id", "value": member_id}])
        if rows:
            print(f"[Member 360] Found profile for {member_id}")
            return rows[0]
        print(f"[Member 360] No profile found for {member_id}")
        return None
    except Exception as e:
        print(f"[Member 360] ERROR: {e}")
        traceback.print_exc()
        return None


def get_case_notes(member_id: str, limit: int = 20) -> list[dict]:
    """Get recent case notes for a member."""
    try:
        sql = f"""
            SELECT document_id, member_id, document_type, title, created_date,
                   author, full_text, text_length
            FROM {CASE_NOTES_TABLE}
            WHERE member_id = :member_id
            ORDER BY created_date DESC
            LIMIT {limit}
        """
        rows = _execute_sql(sql, [{"name": "member_id", "value": member_id}])
        print(f"[Case Notes] Found {len(rows)} documents for {member_id}")
        return rows
    except Exception as e:
        print(f"[Case Notes] ERROR: {e}")
        traceback.print_exc()
        return []


def _search_vs_index(member_id: str, query: str) -> list[dict]:
    """Search Vector Search index for relevant case note chunks."""
    data = _sdk_request("POST", f"/api/2.0/vector-search/indexes/{VS_INDEX_NAME}/query", {
        "columns": ["chunk_id", "document_id", "member_id", "document_type",
                     "title", "created_date", "author", "chunk_text"],
        "query_text": query,
        "filters_json": json.dumps({"member_id": member_id}),
        "num_results": 5,
    })
    rows = data.get("result", {}).get("data_array", [])
    col_names = [c["name"] for c in data.get("manifest", {}).get("columns", [])]
    return [dict(zip(col_names, row)) for row in rows]


def _call_llm(messages: list[dict]) -> str:
    """Call Foundation Model API for chat completion via SDK api_client."""
    data = _sdk_request("POST", f"/serving-endpoints/{LLM_ENDPOINT}/invocations", {
        "messages": messages,
        "max_tokens": 1500,
        "temperature": 0.1,
    })
    return data.get("choices", [{}])[0].get("message", {}).get("content", "No response generated.")


def query_member_agent(member_id: str, question: str) -> dict:
    """RAG agent: retrieve member profile + relevant case notes, then synthesize with LLM."""
    try:
        print(f"[Agent] Processing query for {member_id}: {question[:80]}...")

        # Step 1: Get member profile via SDK
        sql = f"SELECT * FROM {MEMBER_360_TABLE} WHERE member_id = :member_id"
        profile_rows = _execute_sql(sql, [{"name": "member_id", "value": member_id}])
        profile = profile_rows[0] if profile_rows else {}

        # Step 2: Search case notes via Vector Search (SDK auth)
        case_chunks = []
        try:
            case_chunks = _search_vs_index(member_id, question)
        except Exception as vs_err:
            print(f"[Agent] Vector Search error (continuing without): {vs_err}")

        # Step 3: Build context for the LLM
        profile_text = json.dumps(profile, indent=2, default=str) if profile else "No profile found."

        chunks_text = ""
        sources = []
        for chunk in case_chunks:
            doc_type = chunk.get("document_type", "unknown")
            date = chunk.get("created_date", "unknown date")
            author = chunk.get("author", "unknown")
            text = chunk.get("chunk_text", "")
            chunks_text += f"\n---\n[{doc_type}] Date: {date}, Author: {author}\n{text}\n"
            sources.append({
                "document_id": chunk.get("document_id"),
                "document_type": doc_type,
                "created_date": date,
                "author": author,
            })

        if not chunks_text:
            chunks_text = "No case notes or documents found for this member."

        # Step 4: Call LLM
        user_message = f"""Question: {question}

## Member Profile
{profile_text}

## Relevant Case Notes and Documents
{chunks_text}"""

        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_message},
        ]

        answer = _call_llm(messages)
        print(f"[Agent] Response generated: {len(answer)} chars, {len(sources)} sources")
        return {"answer": answer, "sources": sources}

    except Exception as e:
        print(f"[Agent] ERROR: {e}")
        traceback.print_exc()
        return {
            "answer": f"I encountered an error processing your request: {str(e)}",
            "sources": [],
        }
