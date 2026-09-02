"""Lightweight conversation persistence using the Lakehouse (Delta) app-state store.

Replaces langgraph-checkpoint-postgres (which fails to install on Databricks
Apps) with direct SQL operations on the conversations / conversation_messages
tables.

Conversations older than RETENTION_DAYS are purged automatically on startup
and periodically via cleanup_expired_conversations().
"""

import json
import uuid
from typing import Optional

from .database import text

from .database import db

RETENTION_DAYS = 30


def _iso(value):
    """Normalize a timestamp column (datetime or ISO string) to an ISO string."""
    if value is None:
        return None
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def _as_dict(value):
    """Normalize a JSON column (dict or JSON string) to a dict."""
    if not value:
        return {}
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (ValueError, TypeError):
            return {}
    return value


async def get_or_create_conversation(
    member_id: str,
    user_email: str,
    conversation_id: Optional[str] = None,
) -> str:
    """Return an existing conversation_id or create a new thread."""
    async with db.session() as session:
        if conversation_id:
            # Verify it exists
            result = await session.execute(
                text("""
                    SELECT conversation_id::text
                    FROM conversations
                    WHERE conversation_id = CAST(:cid AS uuid)
                """),
                {"cid": conversation_id},
            )
            row = result.scalar_one_or_none()
            if row:
                return row

        # Create new conversation
        new_id = str(uuid.uuid4())
        await session.execute(
            text("""
                INSERT INTO conversations (conversation_id, member_id, user_email)
                VALUES (CAST(:cid AS uuid), :member_id, :user_email)
            """),
            {"cid": new_id, "member_id": member_id, "user_email": user_email},
        )
        await session.commit()
        return new_id


async def save_message(
    conversation_id: str,
    role: str,
    content: str,
    metadata: Optional[dict] = None,
) -> str:
    """Append a message to a conversation thread. Returns the message_id."""
    message_id = str(uuid.uuid4())
    meta_json = json.dumps(metadata or {})
    async with db.session() as session:
        await session.execute(
            text("""
                INSERT INTO conversation_messages
                    (message_id, conversation_id, role, content, metadata)
                VALUES
                    (CAST(:mid AS uuid), CAST(:cid AS uuid), :role, :content, CAST(:meta AS jsonb))
            """),
            {
                "mid": message_id,
                "cid": conversation_id,
                "role": role,
                "content": content,
                "meta": meta_json,
            },
        )
        await session.commit()
    return message_id


async def load_history(
    conversation_id: str,
    max_messages: int = 20,
) -> list[dict]:
    """Load the most recent messages for a conversation (oldest first)."""
    async with db.session() as session:
        result = await session.execute(
            text("""
                SELECT role, content, metadata, created_at
                FROM conversation_messages
                WHERE conversation_id = CAST(:cid AS uuid)
                ORDER BY created_at DESC
                LIMIT :lim
            """),
            {"cid": conversation_id, "lim": max_messages},
        )
        rows = result.mappings().all()
    # Reverse so oldest messages come first
    return [
        {
            "role": r["role"],
            "content": r["content"],
            "metadata": _as_dict(r["metadata"]),
            "created_at": _iso(r["created_at"]),
        }
        for r in reversed(rows)
    ]


async def list_conversations(
    user_email: str,
    member_id: Optional[str] = None,
    limit: int = 20,
) -> list[dict]:
    """List recent conversations for a user, optionally filtered by member."""
    query = """
        SELECT
            conversation_id::text,
            member_id,
            title,
            message_count,
            created_at,
            updated_at
        FROM conversations
        WHERE user_email = :email
    """
    params: dict = {"email": user_email, "lim": limit}
    if member_id:
        query += " AND member_id = :member_id"
        params["member_id"] = member_id
    query += " ORDER BY updated_at DESC LIMIT :lim"

    async with db.session() as session:
        result = await session.execute(text(query), params)
        rows = result.mappings().all()
    return [
        {
            "conversation_id": r["conversation_id"],
            "member_id": r["member_id"],
            "title": r["title"],
            "message_count": r["message_count"],
            "created_at": _iso(r["created_at"]),
            "updated_at": _iso(r["updated_at"]),
        }
        for r in rows
    ]


async def save_feedback(
    message_id: str,
    conversation_id: str,
    user_email: str,
    rating: str,
    comment: Optional[str] = None,
) -> str:
    """Save thumbs-up/down feedback on an agent response."""
    feedback_id = str(uuid.uuid4())
    async with db.session() as session:
        await session.execute(
            text("""
                INSERT INTO agent_feedback
                    (feedback_id, message_id, conversation_id, user_email, rating, comment)
                VALUES
                    (CAST(:fid AS uuid), CAST(:mid AS uuid), CAST(:cid AS uuid),
                     :email, :rating, :comment)
            """),
            {
                "fid": feedback_id,
                "mid": message_id,
                "cid": conversation_id,
                "email": user_email,
                "rating": rating,
                "comment": comment,
            },
        )
        await session.commit()
    return feedback_id


async def cleanup_expired_conversations() -> int:
    """Delete conversations older than RETENTION_DAYS.

    CASCADE on conversation_messages and agent_feedback handles child rows.
    Returns the number of conversations deleted.
    """
    async with db.session() as session:
        # No enforced CASCADE in Unity Catalog — delete children explicitly.
        result = await session.execute(
            text("""
                SELECT conversation_id::string AS cid
                FROM conversations
                WHERE datediff(current_timestamp(), updated_at) > :days
            """),
            {"days": RETENTION_DAYS},
        )
        ids = [str(r["cid"]) for r in result.mappings().all()]
        deleted = len(ids)
        if ids:
            safe = ",".join("'" + i.replace("'", "") + "'" for i in ids)
            for tbl in ("conversation_messages", "conversations"):
                await session.execute(
                    text(f"DELETE FROM {tbl} WHERE conversation_id IN ({safe})")
                )
        await session.commit()
    if deleted:
        print(f"[cleanup] Purged {deleted} conversations older than {RETENTION_DAYS} days")
    return deleted
