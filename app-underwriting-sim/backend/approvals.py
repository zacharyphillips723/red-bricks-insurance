"""Authority-matrix approval routing for underwriting decisions.

Phase 2 of the underwriting vision roadmap. Replicates the authority-matrix /
routing-rules / audit pattern used by app-prior-auth, kept self-contained for this
app (independent deployment) and backed by the same Lakehouse Delta app-state shim.

A proposed decision (a rate change or a funding quote) is routed to the lowest
approver tier whose dollar and rate-change limits both cover it. Every create and
decision writes an audit entry, so the "who approved what, and why" trail that the
NAIC AI governance story depends on is always present.
"""

import json
import uuid
from datetime import datetime
from typing import Optional

from .database import text, _Session as AsyncSession


# ---------------------------------------------------------------------------
# Authority matrix (deterministic, actuary-owned tiers)
# ---------------------------------------------------------------------------
# Each tier covers decisions up to BOTH its dollar limit and its rate-change limit.
# A decision routes to the lowest tier that covers it on both axes. `None` = no cap.

AUTHORITY_MATRIX: list[dict] = [
    {
        "tier": 1,
        "role": "Underwriter",
        "max_dollar_impact": 250_000,
        "max_rate_change_pct": 10.0,
        "description": "Routine renewals within standard parameters.",
    },
    {
        "tier": 2,
        "role": "Senior Underwriter",
        "max_dollar_impact": 1_000_000,
        "max_rate_change_pct": 20.0,
        "description": "Material rate actions and mid-market groups.",
    },
    {
        "tier": 3,
        "role": "Underwriting Manager",
        "max_dollar_impact": 5_000_000,
        "max_rate_change_pct": 35.0,
        "description": "Large-group and high-impact decisions.",
    },
    {
        "tier": 4,
        "role": "Chief Actuary / VP Underwriting",
        "max_dollar_impact": None,
        "max_rate_change_pct": None,
        "description": "Enterprise-material decisions beyond all lower limits.",
    },
]


def evaluate_authority(dollar_impact: float, rate_change_pct: float) -> dict:
    """Return the authority tier required to approve a decision.

    Routes to the lowest tier whose dollar and rate-change limits both cover the
    (absolute) decision magnitude.
    """
    amount = abs(dollar_impact or 0.0)
    rate = abs(rate_change_pct or 0.0)
    for tier in AUTHORITY_MATRIX:
        dollar_ok = tier["max_dollar_impact"] is None or amount <= tier["max_dollar_impact"]
        rate_ok = tier["max_rate_change_pct"] is None or rate <= tier["max_rate_change_pct"]
        if dollar_ok and rate_ok:
            return tier
    return AUTHORITY_MATRIX[-1]


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------

async def create_approval(
    session: AsyncSession,
    *,
    subject: str,
    decision_type: str,
    dollar_impact: float,
    rate_change_pct: float,
    requested_by: str,
    group_id: Optional[str] = None,
    lob: Optional[str] = None,
    context: Optional[dict] = None,
) -> dict:
    """Create and route an approval request to the required authority tier."""
    tier = evaluate_authority(dollar_impact, rate_change_pct)
    approval_id = str(uuid.uuid4())
    await session.execute(
        text("""
            INSERT INTO approval_requests
                (approval_id, subject, decision_type, group_id, lob,
                 dollar_impact, rate_change_pct, required_tier, required_role,
                 status, requested_by, context)
            VALUES
                (:aid, :subject, :dtype, :gid, :lob,
                 :impact, :rate, :tier, :role,
                 'pending', :actor, CAST(:context AS jsonb))
        """),
        {
            "aid": approval_id,
            "subject": subject,
            "dtype": decision_type,
            "gid": group_id,
            "lob": lob,
            "impact": float(dollar_impact or 0.0),
            "rate": float(rate_change_pct or 0.0),
            "tier": int(tier["tier"]),
            "role": tier["role"],
            "actor": requested_by,
            "context": json.dumps(context or {}),
        },
    )
    await _log_audit(session, approval_id, "requested", requested_by, {
        "required_tier": tier["tier"],
        "required_role": tier["role"],
        "dollar_impact": dollar_impact,
        "rate_change_pct": rate_change_pct,
    })
    await session.commit()
    return await get_approval(session, approval_id)


async def get_approval(session: AsyncSession, approval_id: str) -> Optional[dict]:
    result = await session.execute(
        text("SELECT * FROM approval_requests WHERE approval_id = :aid"),
        {"aid": approval_id},
    )
    row = result.mappings().first()
    return _row_to_dict(row) if row else None


async def list_approvals(
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
            SELECT * FROM approval_requests
            {where}
            ORDER BY created_at DESC
            LIMIT :lim OFFSET :off
        """),
        params,
    )
    return [_row_to_dict(r) for r in result.mappings().all()]


async def decide_approval(
    session: AsyncSession,
    approval_id: str,
    *,
    decision: str,
    decided_by: str,
    notes: Optional[str] = None,
) -> Optional[dict]:
    """Record an approver's decision (approved / rejected / needs_info)."""
    valid = {"approved", "rejected", "needs_info"}
    if decision not in valid:
        raise ValueError(f"Invalid decision '{decision}'. Valid: {', '.join(sorted(valid))}")

    existing = await get_approval(session, approval_id)
    if not existing:
        return None

    await session.execute(
        text("""
            UPDATE approval_requests
            SET status = :status, decided_by = :actor, decision_notes = :notes,
                decided_at = current_timestamp()
            WHERE approval_id = :aid
        """),
        {"status": decision, "actor": decided_by, "notes": notes, "aid": approval_id},
    )
    await _log_audit(session, approval_id, decision, decided_by, {"notes": notes} if notes else None)
    await session.commit()
    return await get_approval(session, approval_id)


async def get_approval_audit(session: AsyncSession, approval_id: str) -> list[dict]:
    result = await session.execute(
        text("""
            SELECT * FROM approval_audit_log
            WHERE approval_id = :aid
            ORDER BY created_at DESC
        """),
        {"aid": approval_id},
    )
    return [_row_to_dict(r) for r in result.mappings().all()]


async def _log_audit(
    session: AsyncSession,
    approval_id: str,
    action: str,
    actor: str,
    details: Optional[dict] = None,
) -> None:
    """Insert an approval audit entry (no commit — caller handles the transaction)."""
    await session.execute(
        text("""
            INSERT INTO approval_audit_log
                (audit_id, approval_id, action, actor, details)
            VALUES
                (:aid, :apid, :action, :actor, CAST(:details AS jsonb))
        """),
        {
            "aid": str(uuid.uuid4()),
            "apid": approval_id,
            "action": action,
            "actor": actor,
            "details": json.dumps(details) if details else None,
        },
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_JSON_COLS = {"context", "details"}


def _row_to_dict(row) -> dict:
    """Convert a result row to a plain dict, parsing JSON-text columns back to dict."""
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
