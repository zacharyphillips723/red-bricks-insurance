"""Lakebase CRUD operations for simulations, comparisons, and audit log.

All functions accept an AsyncSession from the database module and use raw SQL
against the Lakebase PostgreSQL tables defined in underwriting_sim_lakebase_schema.sql.
"""

import json
import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


# ---------------------------------------------------------------------------
# Simulations
# ---------------------------------------------------------------------------

async def save_simulation(
    session: AsyncSession,
    *,
    simulation_name: str,
    simulation_type: str,
    parameters: dict,
    results: dict,
    baseline_snapshot: dict,
    created_by: str,
    scope_lob: Optional[str] = None,
    scope_group_id: Optional[str] = None,
    notes: Optional[str] = None,
) -> dict:
    """Insert a new simulation and return its full row."""
    sim_id = str(uuid.uuid4())
    await session.execute(
        text("""
            INSERT INTO simulations
                (simulation_id, simulation_name, simulation_type, parameters,
                 results, baseline_snapshot, status, scope_lob, scope_group_id,
                 notes, created_by)
            VALUES
                (:sid, :name, CAST(:stype AS simulation_type), CAST(:params AS jsonb),
                 CAST(:results AS jsonb), CAST(:baseline AS jsonb), CAST('computed' AS simulation_status),
                 :lob, :gid, :notes, :actor)
        """),
        {
            "sid": sim_id,
            "name": simulation_name,
            "stype": simulation_type,
            "params": json.dumps(parameters),
            "results": json.dumps(results),
            "baseline": json.dumps(baseline_snapshot),
            "lob": scope_lob,
            "gid": scope_group_id,
            "notes": notes,
            "actor": created_by,
        },
    )
    # Audit entry
    await _log_audit(session, sim_id, "created", created_by, {"simulation_type": simulation_type})
    await _log_audit(session, sim_id, "computed", created_by)
    await session.commit()
    return await get_simulation(session, sim_id)


async def get_simulation(session: AsyncSession, simulation_id: str) -> Optional[dict]:
    """Fetch a single simulation by ID."""
    result = await session.execute(
        text("SELECT * FROM simulations WHERE simulation_id = :sid"),
        {"sid": simulation_id},
    )
    row = result.mappings().first()
    return _row_to_dict(row) if row else None


async def list_simulations(
    session: AsyncSession,
    *,
    simulation_type: Optional[str] = None,
    status: Optional[str] = None,
    lob: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
) -> list[dict]:
    """List simulations with optional filters."""
    conditions = []
    params: dict = {"lim": limit, "off": offset}

    if simulation_type:
        conditions.append("simulation_type = CAST(:stype AS simulation_type)")
        params["stype"] = simulation_type
    if status:
        conditions.append("status = CAST(:status AS simulation_status)")
        params["status"] = status
    if lob:
        conditions.append("scope_lob = :lob")
        params["lob"] = lob

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    result = await session.execute(
        text(f"""
            SELECT * FROM v_simulation_list
            {where}
            ORDER BY created_at DESC
            LIMIT :lim OFFSET :off
        """),
        params,
    )
    return [_row_to_dict(r) for r in result.mappings().all()]


async def update_simulation(
    session: AsyncSession,
    simulation_id: str,
    *,
    actor: str,
    status: Optional[str] = None,
    notes: Optional[str] = None,
) -> Optional[dict]:
    """Update simulation status and/or notes."""
    sets = []
    params: dict = {"sid": simulation_id}

    if status:
        sets.append("status = CAST(:status AS simulation_status)")
        params["status"] = status
    if notes is not None:
        sets.append("notes = :notes")
        params["notes"] = notes

    if not sets:
        return await get_simulation(session, simulation_id)

    await session.execute(
        text(f"UPDATE simulations SET {', '.join(sets)} WHERE simulation_id = :sid"),
        params,
    )
    if status:
        await _log_audit(session, simulation_id, status, actor)
    if notes is not None:
        await _log_audit(session, simulation_id, "note_added", actor, {"notes": notes})
    await session.commit()
    return await get_simulation(session, simulation_id)


async def delete_simulation(session: AsyncSession, simulation_id: str) -> bool:
    """Delete a simulation and its audit entries."""
    await session.execute(
        text("DELETE FROM simulation_audit_log WHERE simulation_id = :sid"),
        {"sid": simulation_id},
    )
    result = await session.execute(
        text("DELETE FROM simulations WHERE simulation_id = :sid"),
        {"sid": simulation_id},
    )
    await session.commit()
    return result.rowcount > 0


# ---------------------------------------------------------------------------
# Comparisons
# ---------------------------------------------------------------------------

async def create_comparison(
    session: AsyncSession,
    *,
    comparison_name: str,
    simulation_ids: list[str],
    created_by: str,
    notes: Optional[str] = None,
) -> dict:
    """Create a comparison set linking 2-4 simulations."""
    comp_id = str(uuid.uuid4())
    ids_array = "{" + ",".join(simulation_ids) + "}"
    await session.execute(
        text("""
            INSERT INTO comparison_sets
                (comparison_id, comparison_name, simulation_ids, created_by, notes)
            VALUES
                (:cid, :name, CAST(:sids AS uuid[]), :actor, :notes)
        """),
        {
            "cid": comp_id,
            "name": comparison_name,
            "sids": ids_array,
            "actor": created_by,
            "notes": notes,
        },
    )
    # Audit each simulation
    for sid in simulation_ids:
        await _log_audit(session, sid, "added_to_comparison", created_by, {"comparison_id": comp_id})
    await session.commit()
    return await get_comparison(session, comp_id)


async def get_comparison(session: AsyncSession, comparison_id: str) -> Optional[dict]:
    """Fetch a comparison set with its simulation details."""
    result = await session.execute(
        text("SELECT * FROM comparison_sets WHERE comparison_id = :cid"),
        {"cid": comparison_id},
    )
    row = result.mappings().first()
    if not row:
        return None

    comp = _row_to_dict(row)
    # Fetch linked simulations
    sim_ids = comp.get("simulation_ids", [])
    simulations = []
    for sid in sim_ids:
        sim = await get_simulation(session, str(sid))
        if sim:
            simulations.append(sim)
    comp["simulations"] = simulations
    return comp


async def list_comparisons(
    session: AsyncSession,
    limit: int = 20,
    offset: int = 0,
) -> list[dict]:
    """List comparison sets."""
    result = await session.execute(
        text("SELECT * FROM v_comparison_detail ORDER BY created_at DESC LIMIT :lim OFFSET :off"),
        {"lim": limit, "off": offset},
    )
    return [_row_to_dict(r) for r in result.mappings().all()]


# ---------------------------------------------------------------------------
# Audit Log
# ---------------------------------------------------------------------------

async def get_audit_log(
    session: AsyncSession,
    simulation_id: str,
) -> list[dict]:
    """Get audit trail for a simulation."""
    result = await session.execute(
        text("""
            SELECT * FROM simulation_audit_log
            WHERE simulation_id = :sid
            ORDER BY created_at DESC
        """),
        {"sid": simulation_id},
    )
    return [_row_to_dict(r) for r in result.mappings().all()]


async def _log_audit(
    session: AsyncSession,
    simulation_id: str,
    action: str,
    actor: str,
    details: Optional[dict] = None,
) -> None:
    """Insert an audit log entry (no commit — caller handles transaction)."""
    await session.execute(
        text("""
            INSERT INTO simulation_audit_log
                (audit_id, simulation_id, action, actor, details)
            VALUES
                (:aid, :sid, :action, :actor, CAST(:details AS jsonb))
        """),
        {
            "aid": str(uuid.uuid4()),
            "sid": simulation_id,
            "action": action,
            "actor": actor,
            "details": json.dumps(details) if details else None,
        },
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _row_to_dict(row) -> dict:
    """Convert a SQLAlchemy mapping row to a plain dict with JSON-safe types."""
    d = dict(row)
    for k, v in d.items():
        if isinstance(v, datetime):
            d[k] = v.isoformat()
        elif isinstance(v, uuid.UUID):
            d[k] = str(v)
        elif isinstance(v, list):
            d[k] = [str(x) if isinstance(x, uuid.UUID) else x for x in v]
    return d
