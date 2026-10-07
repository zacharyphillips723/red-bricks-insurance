"""Persistence for funding-arrangement quotes (Lakehouse Delta app-state).

Saved quotes feed the Phase 3 Rated→Sold→Implemented reconciliation and seed the
Phase 4 decision-memory digital twin, so the funnel keeps a durable lineage of
every priced arrangement.
"""

import json
import uuid
from datetime import datetime
from typing import Optional

from .database import text, _Session as AsyncSession


async def save_funding_quote(
    session: AsyncSession,
    *,
    group_name: str,
    funding_arrangement: str,
    inputs: dict,
    result: dict,
    created_by: str,
    lob: Optional[str] = None,
    scope_group_id: Optional[str] = None,
) -> dict:
    quote_id = str(uuid.uuid4())
    await session.execute(
        text("""
            INSERT INTO funding_quotes
                (quote_id, group_name, funding_arrangement, lob, scope_group_id,
                 inputs, result, total_annual_cost, dollar_impact, status, created_by)
            VALUES
                (:qid, :gname, :arr, :lob, :gid,
                 CAST(:inputs AS jsonb), CAST(:result AS jsonb),
                 :total, :impact, 'rated', :actor)
        """),
        {
            "qid": quote_id,
            "gname": group_name,
            "arr": funding_arrangement,
            "lob": lob,
            "gid": scope_group_id,
            "inputs": json.dumps(inputs),
            "result": json.dumps(result),
            "total": float(result.get("total_annual_cost") or 0.0),
            "impact": float(result.get("dollar_impact_vs_fi") or 0.0),
            "actor": created_by,
        },
    )
    await session.commit()
    return await get_funding_quote(session, quote_id)


async def get_funding_quote(session: AsyncSession, quote_id: str) -> Optional[dict]:
    result = await session.execute(
        text("SELECT * FROM funding_quotes WHERE quote_id = :qid"),
        {"qid": quote_id},
    )
    row = result.mappings().first()
    return _row_to_dict(row) if row else None


async def list_funding_quotes(
    session: AsyncSession,
    *,
    status: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
) -> list[dict]:
    conditions = []
    params: dict = {"lim": limit, "off": offset}
    if status:
        conditions.append("status = :status")
        params["status"] = status
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    result = await session.execute(
        text(f"""
            SELECT * FROM funding_quotes
            {where}
            ORDER BY created_at DESC
            LIMIT :lim OFFSET :off
        """),
        params,
    )
    return [_row_to_dict(r) for r in result.mappings().all()]


async def update_funding_quote_status(
    session: AsyncSession, quote_id: str, *, status: str
) -> Optional[dict]:
    """Advance a quote through the funnel: rated → sold → implemented (or lost)."""
    valid = {"rated", "sold", "implemented", "lost"}
    if status not in valid:
        raise ValueError(f"Invalid status '{status}'. Valid: {', '.join(sorted(valid))}")
    existing = await get_funding_quote(session, quote_id)
    if not existing:
        return None
    await session.execute(
        text("""
            UPDATE funding_quotes
            SET status = :status, updated_at = current_timestamp()
            WHERE quote_id = :qid
        """),
        {"status": status, "qid": quote_id},
    )
    await session.commit()
    return await get_funding_quote(session, quote_id)


async def save_quote_revision(
    session: AsyncSession,
    *,
    quote_id: str,
    instruction: str,
    param_changes: dict,
    result: dict,
    created_by: str,
) -> dict:
    """Record a negotiation re-rate against a quote (Phase 3 negotiation loop)."""
    revision_id = str(uuid.uuid4())
    await session.execute(
        text("""
            INSERT INTO quote_revisions
                (revision_id, quote_id, instruction, param_changes, result,
                 total_annual_cost, created_by)
            VALUES
                (:rid, :qid, :instr, CAST(:changes AS jsonb), CAST(:result AS jsonb),
                 :total, :actor)
        """),
        {
            "rid": revision_id,
            "qid": quote_id,
            "instr": instruction,
            "changes": json.dumps(param_changes),
            "result": json.dumps(result),
            "total": float(result.get("total_annual_cost") or 0.0),
            "actor": created_by,
        },
    )
    # Reflect the latest re-rate on the parent quote.
    await session.execute(
        text("""
            UPDATE funding_quotes
            SET result = CAST(:result AS jsonb),
                total_annual_cost = :total,
                dollar_impact = :impact,
                updated_at = current_timestamp()
            WHERE quote_id = :qid
        """),
        {
            "result": json.dumps(result),
            "total": float(result.get("total_annual_cost") or 0.0),
            "impact": float(result.get("dollar_impact_vs_fi") or 0.0),
            "qid": quote_id,
        },
    )
    await session.commit()
    result_rows = await list_quote_revisions(session, quote_id)
    return next((r for r in result_rows if r["revision_id"] == revision_id), {"revision_id": revision_id})


async def list_quote_revisions(session: AsyncSession, quote_id: str) -> list[dict]:
    result = await session.execute(
        text("""
            SELECT * FROM quote_revisions
            WHERE quote_id = :qid
            ORDER BY created_at ASC
        """),
        {"qid": quote_id},
    )
    return [_row_to_dict(r) for r in result.mappings().all()]


_JSON_COLS = {"inputs", "result", "param_changes"}


def _row_to_dict(row) -> dict:
    d = dict(row)
    for k, v in d.items():
        if k in _JSON_COLS and isinstance(v, str) and v:
            try:
                d[k] = json.loads(v)
                continue
            except (ValueError, TypeError):
                pass
        if isinstance(v, datetime):
            d[k] = v.isoformat()
        elif isinstance(v, uuid.UUID):
            d[k] = str(v)
    return d
