"""Genie Space integration for FWA Investigation Portal."""

import os
import time

from databricks.sdk import WorkspaceClient

from .models import GenieQuestionIn, GenieResponseOut

GENIE_SPACE_ID = os.environ.get("GENIE_SPACE_ID", "")


def ask_genie(question_in: GenieQuestionIn) -> GenieResponseOut:
    """Send a natural language question to a Genie space and return structured results."""
    if not GENIE_SPACE_ID:
        return GenieResponseOut(
            conversation_id="",
            message_id="",
            description="Genie space not configured. Set GENIE_SPACE_ID environment variable.",
        )

    try:
        w = WorkspaceClient()

        # Start conversation or continue existing one
        if question_in.conversation_id:
            msg = w.genie.create_message(
                space_id=GENIE_SPACE_ID,
                conversation_id=question_in.conversation_id,
                content=question_in.question,
            )
            conversation_id = question_in.conversation_id
        else:
            conv = w.genie.start_conversation(
                space_id=GENIE_SPACE_ID,
                content=question_in.question,
            )
            conversation_id = conv.conversation_id
            msg = conv

        message_id = msg.message_id if hasattr(msg, "message_id") else ""

        # Poll for completion
        for _ in range(30):
            result = w.genie.get_message(
                space_id=GENIE_SPACE_ID,
                conversation_id=conversation_id,
                message_id=message_id,
            )
            status_raw = getattr(result, "status", None)
            status_val = getattr(status_raw, "value", str(status_raw)) if status_raw else "UNKNOWN"
            if status_val in ("COMPLETED", "FAILED", "EXECUTING_QUERY",
                              "CANCELLED", "QUERY_RESULT_EXPIRED"):
                break
            time.sleep(1)

        # Extract SQL and results
        sql_query = None
        columns = []
        rows = []
        description = None

        for attachment in getattr(result, "attachments", []):
            if hasattr(attachment, "query") and attachment.query:
                sql_query = attachment.query.query
                if attachment.query.columns:
                    columns = [c.name for c in attachment.query.columns]
                if attachment.query.rows:
                    rows = [dict(zip(columns, row)) for row in attachment.query.rows]
            if hasattr(attachment, "text") and attachment.text:
                description = attachment.text.content

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
        return GenieResponseOut(
            conversation_id="",
            message_id="",
            description=f"Genie error: {str(e)}",
        )
