"""Databricks Genie Conversation API integration.

Canonical shared implementation — synced to each app's backend/ directory by
sync_shared_backend.sh. Edit THIS file, then run the sync script.
"""

import traceback
from datetime import timedelta

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import OperationFailed

_TIMEOUT = timedelta(seconds=120)


def ask_genie(question_text: str, conversation_id: str = "", space_id: str = "",
              warehouse_id: str = "") -> dict:
    """Send a question to a Genie Space and return structured results.

    Returns a dict with: conversation_id, message_id, sql_query, columns, rows,
    row_count, description.

    This is a backend-agnostic function. Each app's genie.py wraps this with
    its own Pydantic models.
    """
    if not space_id:
        return {
            "conversation_id": "",
            "message_id": "",
            "sql_query": None,
            "columns": [],
            "rows": [],
            "row_count": 0,
            "description": "Genie space not configured. Set GENIE_SPACE_ID environment variable.",
        }

    try:
        w = WorkspaceClient()

        print(f"[Genie] Asking: {question_text[:80]}...")
        print(f"[Genie] Space ID: {space_id}")

        if conversation_id:
            wait_obj = w.genie.create_message(
                space_id=space_id,
                conversation_id=conversation_id,
                content=question_text,
            )
        else:
            wait_obj = w.genie.start_conversation(
                space_id=space_id,
                content=question_text,
            )

        try:
            msg = wait_obj.result(timeout=_TIMEOUT)
        except OperationFailed:
            conv_id = wait_obj.conversation_id
            msg_id = wait_obj.message_id
            msg = w.genie.get_message(
                space_id=space_id,
                conversation_id=conv_id,
                message_id=msg_id,
            )
            error_detail = getattr(msg.error, "message", None) if msg.error else None
            print(f"[Genie] Message FAILED: {error_detail}")

        result_conversation_id = msg.conversation_id
        result_message_id = msg.message_id
        print(f"[Genie] Conversation: {result_conversation_id}, Message: {result_message_id}")

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
                qr = w.genie.get_message_query_result_by_attachment(
                    space_id=space_id,
                    conversation_id=result_conversation_id,
                    message_id=result_message_id,
                    attachment_id=query_attachment_id,
                )
                stmt = qr.statement_response
                if stmt and stmt.result and stmt.result.data_array:
                    col_names = [
                        c.name for c in (stmt.manifest.schema.columns or [])
                    ] if stmt.manifest and stmt.manifest.schema else []
                    columns = col_names
                    rows = [
                        dict(zip(col_names, row))
                        for row in stmt.result.data_array
                    ]
                print(f"[Genie] Query returned {len(rows)} rows")
            except Exception as e:
                print(f"[Genie] Query result fetch failed: {e}")
                # Fallback: execute SQL directly via statement execution API
                if warehouse_id:
                    try:
                        stmt = w.statement_execution.execute_statement(
                            warehouse_id=warehouse_id,
                            statement=sql_query,
                            wait_timeout="30s",
                        )
                        if stmt.result and stmt.result.data_array:
                            col_names = [
                                c.name for c in (stmt.manifest.schema.columns or [])
                            ] if stmt.manifest and stmt.manifest.schema else []
                            columns = col_names
                            rows = [
                                dict(zip(col_names, row))
                                for row in stmt.result.data_array
                            ]
                        print(f"[Genie] Fallback query returned {len(rows)} rows")
                    except Exception as e2:
                        print(f"[Genie] Fallback query execution failed: {e2}")
                        description = f"Query generated but execution failed: {e2}"

        return {
            "conversation_id": result_conversation_id,
            "message_id": result_message_id,
            "sql_query": sql_query,
            "columns": columns,
            "rows": rows,
            "row_count": len(rows),
            "description": description,
        }

    except Exception as e:
        print(f"[Genie] ERROR: {e}")
        traceback.print_exc()
        return {
            "conversation_id": "",
            "message_id": "",
            "sql_query": None,
            "columns": [],
            "rows": [],
            "row_count": 0,
            "description": f"Genie error: {str(e)}",
        }
