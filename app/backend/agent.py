"""Member RAG Agent and Member 360 data integration module.

Mirrors the genie.py pattern — synchronous SDK calls wrapped for async use
via asyncio.to_thread() in the router.
"""

import os
import traceback

from databricks.sdk import WorkspaceClient

AGENT_ENDPOINT_NAME = os.environ.get("AGENT_ENDPOINT_NAME", "red-bricks-member-agent")
UC_CATALOG = os.environ.get("UC_CATALOG", "catalog_insurance_vpx9o6")
UC_SCHEMA = os.environ.get("UC_SCHEMA", "red_bricks_insurance_dev")
SQL_WAREHOUSE_ID = os.environ.get("SQL_WAREHOUSE_ID", "781064a3466c0984")

MEMBER_360_TABLE = f"{UC_CATALOG}.{UC_SCHEMA}.gold_member_360"
CASE_NOTES_TABLE = f"{UC_CATALOG}.{UC_SCHEMA}.silver_case_notes"


def _execute_sql(w: WorkspaceClient, sql: str, params: list | None = None) -> tuple[list[str], list[dict]]:
    """Execute SQL via Statement Execution API and return (columns, rows)."""
    stmt = w.statement_execution.execute_statement(
        warehouse_id=SQL_WAREHOUSE_ID,
        statement=sql,
        wait_timeout="30s",
        parameters=params,
    )
    if not stmt.result or not stmt.result.data_array:
        return [], []
    col_names = [c.name for c in (stmt.manifest.schema.columns or [])] if stmt.manifest and stmt.manifest.schema else []
    rows = [dict(zip(col_names, row)) for row in stmt.result.data_array]
    return col_names, rows


def search_members(query: str) -> list[dict]:
    """Search members by name or ID. Returns list of member summaries."""
    try:
        w = WorkspaceClient()
        sql = f"""
            SELECT member_id, first_name, last_name, member_name, date_of_birth,
                   age, gender, line_of_business, risk_tier, raf_score, county
            FROM {MEMBER_360_TABLE}
            WHERE member_id = :query
               OR LOWER(member_name) LIKE CONCAT('%', LOWER(:query), '%')
               OR LOWER(last_name) LIKE CONCAT('%', LOWER(:query), '%')
               OR LOWER(first_name) LIKE CONCAT('%', LOWER(:query), '%')
            ORDER BY last_name, first_name
            LIMIT 25
        """
        _, rows = _execute_sql(w, sql, [{"name": "query", "value": query}])
        print(f"[Member Search] Query '{query}' returned {len(rows)} results")
        return rows
    except Exception as e:
        print(f"[Member Search] ERROR: {e}")
        traceback.print_exc()
        return []


def get_member_360(member_id: str) -> dict | None:
    """Get full Member 360 profile for a single member."""
    try:
        w = WorkspaceClient()
        sql = f"SELECT * FROM {MEMBER_360_TABLE} WHERE member_id = :member_id"
        _, rows = _execute_sql(w, sql, [{"name": "member_id", "value": member_id}])
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
        w = WorkspaceClient()
        sql = f"""
            SELECT document_id, member_id, document_type, title, created_date,
                   author, full_text, text_length
            FROM {CASE_NOTES_TABLE}
            WHERE member_id = :member_id
            ORDER BY created_date DESC
            LIMIT {limit}
        """
        _, rows = _execute_sql(w, sql, [{"name": "member_id", "value": member_id}])
        print(f"[Case Notes] Found {len(rows)} documents for {member_id}")
        return rows
    except Exception as e:
        print(f"[Case Notes] ERROR: {e}")
        traceback.print_exc()
        return []


def query_member_agent(member_id: str, question: str) -> dict:
    """Query the Member RAG Agent via Model Serving endpoint."""
    try:
        w = WorkspaceClient()
        print(f"[Agent] Querying '{AGENT_ENDPOINT_NAME}' for {member_id}: {question[:80]}...")

        # Build the question with member context
        full_question = f"For member {member_id}: {question}"

        response = w.serving_endpoints.query(
            name=AGENT_ENDPOINT_NAME,
            dataframe_records=[{
                "input": full_question,
                "chat_history": [],
            }],
        )

        # Extract the answer from predictions
        answer = ""
        sources = []
        if response.predictions:
            pred = response.predictions[0] if isinstance(response.predictions, list) else response.predictions
            if isinstance(pred, dict):
                answer = pred.get("output", str(pred))
            else:
                answer = str(pred)

        print(f"[Agent] Response length: {len(answer)} chars")
        return {"answer": answer, "sources": sources}

    except Exception as e:
        print(f"[Agent] ERROR: {e}")
        traceback.print_exc()
        return {
            "answer": f"Agent error: {str(e)}. The agent endpoint may not be deployed yet.",
            "sources": [],
        }
