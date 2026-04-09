"""Genie Space integration for Underwriting Simulation Portal."""

import traceback
from datetime import timedelta

from databricks.sdk import WorkspaceClient

from .models import GenieQuestionIn, GenieResponseOut
from .env_config import GENIE_SPACE_ID
_TIMEOUT = timedelta(seconds=120)


def ask_genie(question_in: GenieQuestionIn) -> GenieResponseOut:
    """Send a natural language question to the underwriting Genie space."""
    if not GENIE_SPACE_ID:
        return GenieResponseOut(
            description="Genie space not configured. Set GENIE_SPACE_ID environment variable.",
        )

    try:
        w = WorkspaceClient()

        if question_in.conversation_id:
            msg = w.genie.create_message_and_wait(
                space_id=GENIE_SPACE_ID,
                conversation_id=question_in.conversation_id,
                content=question_in.question,
                timeout=_TIMEOUT,
            )
        else:
            msg = w.genie.start_conversation_and_wait(
                space_id=GENIE_SPACE_ID,
                content=question_in.question,
                timeout=_TIMEOUT,
            )

        conversation_id = msg.conversation_id
        message_id = msg.message_id

        sql_query = None
        columns: list[str] = []
        rows: list[list] = []
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
                    space_id=GENIE_SPACE_ID,
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
                    rows = [list(row) for row in stmt.result.data_array]
            except Exception as e:
                print(f"[Genie] Query result fetch failed: {e}")
                description = f"Query generated but result fetch failed: {e}"

        return GenieResponseOut(
            conversation_id=conversation_id,
            message_id=message_id,
            sql_query=sql_query,
            columns=columns,
            rows=rows,
            description=description,
        )

    except Exception as e:
        traceback.print_exc()
        return GenieResponseOut(
            description=f"Genie error: {str(e)}",
        )
