"""Actuarial factor governance — version, approve, and publish rating factors.

Phase 2 of the underwriting vision roadmap: closes the "actuaries govern, don't
compute" loop. The pricing engine already *consumes* a governed Unity Catalog
table (`analytics.gold_pricing_factors`); this module adds the in-app workflow to
version a proposed factor set, route it for actuarial approval, and publish an
approved version to that governed table (which `pricing_engine._load_governed_factors`
reads on a 15-minute TTL).

Version history lives in Lakehouse Delta app-state; publishing upserts the approved
factors into the governed gold table so every rate build-up thereafter uses them.
"""

import json
import uuid
from datetime import datetime
from typing import Optional

from .data_loader import _execute_sql, _CAT
from .database import text, _Session as AsyncSession
from .pricing_engine import _load_governed_factors

# factor_type values understood by pricing_engine._load_governed_factors.
_FACTOR_BUCKETS = {
    "base_rate": "base_rates",
    "age_factor": "age_factors",
    "area_factor": "area_factors",
    "industry_factor": "industry_factors",
    "experience_mod": "experience_mod",
}
_GOVERNED_TABLE = f"{_CAT}.analytics.gold_pricing_factors"


def snapshot_current_factors() -> list[dict]:
    """Flatten the pricing engine's current (governed or fallback) factor set into
    `{factor_type, factor_key, factor_value}` rows — the seed for a new version."""
    gf = _load_governed_factors()
    rows: list[dict] = []
    for ftype, bucket in _FACTOR_BUCKETS.items():
        for key, value in gf.get(bucket, {}).items():
            rows.append({"factor_type": ftype, "factor_key": str(key), "factor_value": float(value)})
    return rows


# ---------------------------------------------------------------------------
# Version lifecycle (app-state)
# ---------------------------------------------------------------------------

async def create_factor_version(
    session: AsyncSession,
    *,
    created_by: str,
    factors: Optional[list[dict]] = None,
    notes: Optional[str] = None,
    source: str = "manual",
) -> dict:
    """Create a draft factor version. Defaults to a snapshot of the current factors."""
    rows = factors if factors is not None else snapshot_current_factors()
    # Validate factor rows.
    clean: list[dict] = []
    for r in rows:
        ftype = r.get("factor_type")
        if ftype not in _FACTOR_BUCKETS:
            raise ValueError(f"Unknown factor_type '{ftype}'. Valid: {', '.join(_FACTOR_BUCKETS)}")
        clean.append({
            "factor_type": ftype,
            "factor_key": str(r.get("factor_key")),
            "factor_value": float(r.get("factor_value")),
        })

    result = await session.execute(text("SELECT COALESCE(MAX(version), 0) AS v FROM factor_versions"))
    next_version = int((result.mappings().first() or {}).get("v", 0)) + 1

    version_id = str(uuid.uuid4())
    await session.execute(
        text("""
            INSERT INTO factor_versions
                (version_id, version, status, factors, source, notes, created_by)
            VALUES
                (:vid, :ver, 'draft', CAST(:factors AS jsonb), :source, :notes, :actor)
        """),
        {
            "vid": version_id,
            "ver": next_version,
            "factors": json.dumps(clean),
            "source": source,
            "notes": notes,
            "actor": created_by,
        },
    )
    await session.commit()
    return await get_factor_version(session, version_id)


async def get_factor_version(session: AsyncSession, version_id: str) -> Optional[dict]:
    result = await session.execute(
        text("SELECT * FROM factor_versions WHERE version_id = :vid"),
        {"vid": version_id},
    )
    row = result.mappings().first()
    return _row_to_dict(row) if row else None


async def list_factor_versions(session: AsyncSession, limit: int = 50) -> list[dict]:
    result = await session.execute(
        text("SELECT * FROM factor_versions ORDER BY version DESC LIMIT :lim"),
        {"lim": limit},
    )
    return [_row_to_dict(r) for r in result.mappings().all()]


async def approve_factor_version(
    session: AsyncSession, version_id: str, *, approved_by: str
) -> Optional[dict]:
    existing = await get_factor_version(session, version_id)
    if not existing:
        return None
    if existing["status"] not in ("draft",):
        raise ValueError(f"Only draft versions can be approved (current status: {existing['status']}).")
    await session.execute(
        text("""
            UPDATE factor_versions
            SET status = 'approved', approved_by = :actor, approved_at = current_timestamp()
            WHERE version_id = :vid
        """),
        {"actor": approved_by, "vid": version_id},
    )
    await session.commit()
    return await get_factor_version(session, version_id)


async def publish_factor_version(
    session: AsyncSession, version_id: str, *, published_by: str
) -> dict:
    """Publish an approved version to the governed gold table and mark it published.

    Upserts each factor row into `analytics.gold_pricing_factors` so the pricing
    engine picks them up on its next refresh. Returns a dict with the publish result.
    """
    existing = await get_factor_version(session, version_id)
    if not existing:
        return {"published": False, "error": "version not found"}
    if existing["status"] != "approved":
        raise ValueError(f"Only approved versions can be published (current status: {existing['status']}).")

    factors = existing.get("factors") or []
    published_rows, publish_error = _write_governed_factors(factors)

    if publish_error is None:
        # Archive any previously-published version, then mark this one published.
        await session.execute(
            text("UPDATE factor_versions SET status = 'archived' WHERE status = 'published'")
        )
        await session.execute(
            text("""
                UPDATE factor_versions
                SET status = 'published', published_by = :actor, published_at = current_timestamp()
                WHERE version_id = :vid
            """),
            {"actor": published_by, "vid": version_id},
        )
        await session.commit()

    updated = await get_factor_version(session, version_id)
    return {
        "published": publish_error is None,
        "rows_written": published_rows,
        "error": publish_error,
        "version": updated,
    }


def _write_governed_factors(factors: list[dict]) -> tuple[int, Optional[str]]:
    """Upsert factor rows into the governed gold table. Returns (rows_written, error)."""
    if not factors:
        return 0, "no factor rows to publish"
    try:
        _execute_sql(f"""
            CREATE TABLE IF NOT EXISTS {_GOVERNED_TABLE} (
                factor_type STRING,
                factor_key STRING,
                factor_value DOUBLE,
                updated_at TIMESTAMP
            )
        """)
        # Build a VALUES source and MERGE (upsert) on (factor_type, factor_key).
        values = ", ".join(
            f"('{_sql_lit(r['factor_type'])}', '{_sql_lit(str(r['factor_key']))}', "
            f"{float(r['factor_value'])})"
            for r in factors
        )
        _execute_sql(f"""
            MERGE INTO {_GOVERNED_TABLE} AS t
            USING (SELECT * FROM VALUES {values} AS s(factor_type, factor_key, factor_value)) AS s
            ON t.factor_type = s.factor_type AND t.factor_key = s.factor_key
            WHEN MATCHED THEN UPDATE SET t.factor_value = s.factor_value, t.updated_at = current_timestamp()
            WHEN NOT MATCHED THEN INSERT (factor_type, factor_key, factor_value, updated_at)
                VALUES (s.factor_type, s.factor_key, s.factor_value, current_timestamp())
        """)
        return len(factors), None
    except Exception as e:  # pragma: no cover - depends on warehouse perms / pipeline ownership
        msg = str(e)
        if "pipeline" in msg.lower() or "materialized" in msg.lower() or "streaming" in msg.lower():
            msg = (
                "gold_pricing_factors appears to be pipeline-managed; publish from the app is "
                f"blocked. Govern factors in the SDP pipeline instead. ({msg})"
            )
        return 0, msg


def _sql_lit(value: str) -> str:
    """Escape single quotes for inline SQL string literals."""
    return value.replace("'", "''")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_JSON_COLS = {"factors"}


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
