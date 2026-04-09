"""Databricks Genie Conversation API integration."""

import traceback
from datetime import timedelta

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import OperationFailed

from .models import GenieQuestionIn, GenieResponseOut

from .env_config import SQL_WAREHOUSE_ID, GENIE_SPACE_ID

_TIMEOUT = timedelta(seconds=120)


def _send_and_wait(w, space_id, question_in) -> "GenieMessage":
    """Send a Genie question and wait for completion, handling failures gracefully."""
    if question_in.conversation_id:
        wait_obj = w.genie.create_message(
            space_id=space_id,
            conversation_id=question_in.conversation_id,
            content=question_in.question,
        )
    else:
        wait_obj = w.genie.start_conversation(
            space_id=space_id,
            content=question_in.question,
        )

    try:
        return wait_obj.result(timeout=_TIMEOUT)
    except OperationFailed:
        # Message reached FAILED — fetch it to inspect error details
        conv_id = wait_obj.conversation_id
        msg_id = wait_obj.message_id
        msg = w.genie.get_message(
            space_id=space_id,
            conversation_id=conv_id,
            message_id=msg_id,
        )
        error_detail = getattr(msg.error, "message", None) if msg.error else None
        print(f"[Genie] Message FAILED: {error_detail}")
        print(f"[Genie] Full error obj: {msg.error}")
        return msg


def ask_genie(question_in: GenieQuestionIn) -> GenieResponseOut:
    """Send a question to the Genie Space and return structured results."""
    try:
        w = WorkspaceClient()
        space_id = GENIE_SPACE_ID

        print(f"[Genie] Asking: {question_in.question[:80]}...")
        print(f"[Genie] Space ID: {space_id}, Warehouse: {SQL_WAREHOUSE_ID}")

        msg = _send_and_wait(w, space_id, question_in)

        conversation_id = msg.conversation_id
        message_id = msg.message_id
        print(f"[Genie] Conversation: {conversation_id}, Message: {message_id}")
        print(f"[Genie] Status: {msg.status}, Attachments: {len(msg.attachments) if msg.attachments else 0}")

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

        print(f"[Genie] SQL: {bool(sql_query)}, Description: {bool(description)}")

        # Fetch query results via SDK if we have a query attachment
        if sql_query and query_attachment_id:
            try:
                qr = w.genie.get_message_query_result_by_attachment(
                    space_id=space_id,
                    conversation_id=conversation_id,
                    message_id=message_id,
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
                if SQL_WAREHOUSE_ID:
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
                        print(f"[Genie] Fallback query returned {len(rows)} rows")
                    except Exception as e2:
                        print(f"[Genie] Fallback query execution failed: {e2}")
                        description = f"Query generated but execution failed: {e2}"

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
