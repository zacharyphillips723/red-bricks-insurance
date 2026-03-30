"""Databricks Genie Conversation API integration."""

import os
import time
import traceback

from databricks.sdk import WorkspaceClient

from .models import GenieQuestionIn, GenieResponseOut

from .env_config import SQL_WAREHOUSE_ID

GENIE_SPACE_ID = os.environ.get("GENIE_SPACE_ID") or ""


def _poll_for_result(w, space_id, conversation_id, message_id, timeout=60):
    """Poll Genie until the message is complete."""
    start = time.time()
    while time.time() - start < timeout:
        msg = w.genie.get_message(
            space_id=space_id,
            conversation_id=conversation_id,
            message_id=message_id,
        )
        status_val = getattr(msg.status, "value", str(msg.status)) if msg.status else None
        if status_val in ("COMPLETED", "FAILED", "EXECUTING_QUERY"):
            return msg
        time.sleep(2)
    return msg


def ask_genie(question_in: GenieQuestionIn) -> GenieResponseOut:
    """Send a question to the Genie Space and return structured results."""
    try:
        w = WorkspaceClient()
        space_id = GENIE_SPACE_ID

        print(f"[Genie] Asking: {question_in.question[:80]}...")
        print(f"[Genie] Space ID: {space_id}, Warehouse: {SQL_WAREHOUSE_ID}")

        if question_in.conversation_id:
            msg_resp = w.genie.create_message(
                space_id=space_id,
                conversation_id=question_in.conversation_id,
                content=question_in.question,
            )
            conversation_id = question_in.conversation_id
        else:
            conv_resp = w.genie.start_conversation(
                space_id=space_id,
                content=question_in.question,
            )
            conversation_id = conv_resp.conversation_id
            msg_resp = conv_resp

        message_id = msg_resp.message_id
        print(f"[Genie] Conversation: {conversation_id}, Message: {message_id}")

        result = _poll_for_result(w, space_id, conversation_id, message_id)

        sql_query = None
        columns: list[str] = []
        rows: list[dict] = []
        description = None

        if result.attachments:
            for attachment in result.attachments:
                if hasattr(attachment, "query") and attachment.query:
                    sql_query = getattr(attachment.query, "query", None)
                if hasattr(attachment, "text") and attachment.text:
                    description = getattr(attachment.text, "content", None)

        print(f"[Genie] SQL: {bool(sql_query)}, Description: {bool(description)}")

        if sql_query and SQL_WAREHOUSE_ID:
            try:
                stmt = w.statement_execution.execute_statement(
                    warehouse_id=SQL_WAREHOUSE_ID,
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
                print(f"[Genie] Query returned {len(rows)} rows")
            except Exception as e:
                print(f"[Genie] Query execution failed: {e}")
                description = f"Query generated but execution failed: {e}"

        return GenieResponseOut(
            conversation_id=conversation_id,
            message_id=message_id,
            sql_query=sql_query,
            columns=columns,
            rows=rows,
            row_count=len(rows),
            description=description,
        )

    except Exception as e:
        print(f"[Genie] ERROR: {e}")
        traceback.print_exc()
        return GenieResponseOut(
            conversation_id="",
            message_id="",
            description=f"Genie error: {str(e)}",
        )
