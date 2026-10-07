"""Operational analytics + quote-lifecycle reconciliation.

Phase 3 of the underwriting vision roadmap. Extends observability beyond agent
traces/cost to the underwriting operation itself: funnel conversion, approval
velocity, factor drift between published versions, quote cycle time, and the
Rated→Sold→Implemented reconciliation that "never existed before".

Aggregates the Phase 2/3 app-state tables (funding_quotes, approval_requests,
factor_versions, quote_revisions).
"""

import json
from datetime import datetime
from typing import Optional

from .database import text, _Session as AsyncSession


def _ts(value) -> Optional[datetime]:
    """Coerce a timestamp cell (datetime or ISO string) to datetime."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace(" ", "T", 1))
    except (ValueError, TypeError):
        return None


def _hours_between(a, b) -> Optional[float]:
    ta, tb = _ts(a), _ts(b)
    if ta and tb:
        return round((tb - ta).total_seconds() / 3600.0, 2)
    return None


async def _rows(session: AsyncSession, sql: str) -> list[dict]:
    result = await session.execute(text(sql))
    return [dict(r) for r in result.mappings().all()]


async def operational_analytics(session: AsyncSession) -> dict:
    quotes = await _rows(
        session,
        "SELECT status, total_annual_cost, created_at, updated_at FROM funding_quotes",
    )
    approvals = await _rows(
        session,
        "SELECT status, created_at, decided_at FROM approval_requests",
    )
    versions = await _rows(
        session,
        "SELECT version, status, factors, published_at FROM factor_versions ORDER BY version",
    )

    # ---- Funnel + conversion ----
    status_counts: dict[str, int] = {}
    for q in quotes:
        status_counts[q.get("status", "rated")] = status_counts.get(q.get("status", "rated"), 0) + 1
    total_quotes = len(quotes)
    won = status_counts.get("sold", 0) + status_counts.get("implemented", 0)
    quote_to_sold = round(won / total_quotes * 100, 1) if total_quotes else 0.0
    sold_to_impl = (
        round(status_counts.get("implemented", 0) / won * 100, 1) if won else 0.0
    )

    # ---- Cycle time (created -> updated for advanced quotes) ----
    cycle_hours = [
        h for q in quotes if q.get("status") != "rated"
        for h in [_hours_between(q.get("created_at"), q.get("updated_at"))] if h is not None
    ]
    avg_cycle = round(sum(cycle_hours) / len(cycle_hours), 2) if cycle_hours else None

    # ---- Approval velocity ----
    decided = [a for a in approvals if a.get("status") in ("approved", "rejected")]
    decision_hours = [
        h for a in decided
        for h in [_hours_between(a.get("created_at"), a.get("decided_at"))] if h is not None
    ]
    approved = sum(1 for a in approvals if a.get("status") == "approved")
    approval_rate = round(approved / len(decided) * 100, 1) if decided else 0.0

    # ---- Factor drift (latest published vs the prior published version) ----
    drift = _factor_drift(versions)

    return {
        "funnel": {
            "total_quotes": total_quotes,
            "by_status": status_counts,
            "quote_to_sold_pct": quote_to_sold,
            "sold_to_implemented_pct": sold_to_impl,
        },
        "cycle_time": {"avg_hours_to_advance": avg_cycle, "sample": len(cycle_hours)},
        "approvals": {
            "total": len(approvals),
            "pending": sum(1 for a in approvals if a.get("status") == "pending"),
            "decided": len(decided),
            "approval_rate_pct": approval_rate,
            "avg_decision_hours": (
                round(sum(decision_hours) / len(decision_hours), 2) if decision_hours else None
            ),
        },
        "factor_governance": drift,
    }


def _factor_drift(versions: list[dict]) -> dict:
    """Compare the two most recent published factor versions."""
    published = [v for v in versions if v.get("status") in ("published", "archived")]
    out: dict = {
        "published_versions": sum(1 for v in versions if v.get("status") == "published"),
        "total_versions": len(versions),
        "changed_factors": 0,
        "max_abs_pct_change": 0.0,
    }
    if len(published) < 2:
        return out
    prev, latest = published[-2], published[-1]
    prev_map = _factor_map(prev.get("factors"))
    latest_map = _factor_map(latest.get("factors"))
    changed = 0
    max_pct = 0.0
    for key, new_val in latest_map.items():
        old_val = prev_map.get(key)
        if old_val is None or old_val == 0:
            continue
        if abs(new_val - old_val) > 1e-9:
            changed += 1
            max_pct = max(max_pct, abs((new_val - old_val) / old_val) * 100)
    out["changed_factors"] = changed
    out["max_abs_pct_change"] = round(max_pct, 2)
    out["compared_versions"] = [prev.get("version"), latest.get("version")]
    return out


def _factor_map(factors) -> dict:
    """Flatten a factors payload (JSON string or list) into {type.key: value}."""
    if isinstance(factors, str):
        try:
            factors = json.loads(factors)
        except (ValueError, TypeError):
            factors = []
    out: dict = {}
    for f in factors or []:
        if isinstance(f, dict):
            out[f"{f.get('factor_type')}.{f.get('factor_key')}"] = float(f.get("factor_value", 0.0))
    return out


async def reconciliation(session: AsyncSession) -> dict:
    """Rated → Sold → Implemented lineage with per-stage totals."""
    quotes = await _rows(
        session,
        """SELECT quote_id, group_name, funding_arrangement, status,
                  total_annual_cost, dollar_impact, created_at, updated_at
           FROM funding_quotes ORDER BY created_at DESC""",
    )
    stages = ["rated", "sold", "implemented", "lost"]
    summary = {
        s: {
            "count": sum(1 for q in quotes if q.get("status") == s),
            "total_annual_cost": round(
                sum(float(q.get("total_annual_cost") or 0) for q in quotes if q.get("status") == s), 2
            ),
        }
        for s in stages
    }
    records = [
        {
            "quote_id": q.get("quote_id"),
            "group_name": q.get("group_name"),
            "funding_arrangement": q.get("funding_arrangement"),
            "status": q.get("status"),
            "total_annual_cost": float(q.get("total_annual_cost") or 0),
            "dollar_impact": float(q.get("dollar_impact") or 0),
            "created_at": _iso(q.get("created_at")),
            "updated_at": _iso(q.get("updated_at")),
        }
        for q in quotes
    ]
    return {"stage_summary": summary, "quotes": records}


def _iso(value) -> Optional[str]:
    ts = _ts(value)
    return ts.isoformat() if ts else None
