"""Agentic memory + recall for the Underwriting Digital Twin (Phase 4).

Backed by **Lakebase (Postgres + pgvector)** — the twin's hot memory layer. Stores
every candidate/committed decision plus a 1024-dim embedding of its context, and
recalls relevant precedent with pgvector cosine ANN (`embedding <=> query`). Delta
remains system-of-record for the rest of the app; this module is the only Lakebase
consumer (via `twin_db`).

If the `vector` extension isn't available on the target Lakebase, `decision_memory`
and the structured tables still work and recall degrades to cohort + recency
ordering (no semantic similarity) — the function signatures and return shapes are
identical either way, so the preflight loop never changes.
"""

import json
import math
import uuid
from datetime import datetime
from typing import Optional

from databricks.sdk import WorkspaceClient
from sqlalchemy.ext.asyncio import AsyncSession

from .twin_db import twin_db, text

# Databricks FM API embedding endpoint (1024-dim).
EMBED_ENDPOINT = "databricks-gte-large-en"


# ---------------------------------------------------------------------------
# Embeddings
# ---------------------------------------------------------------------------

def embed(embed_text: str) -> list[float]:
    """Return an embedding vector for text, or [] if the endpoint is unavailable."""
    if not embed_text:
        return []
    try:
        w = WorkspaceClient()
        data = w.api_client.do(
            "POST",
            f"/serving-endpoints/{EMBED_ENDPOINT}/invocations",
            body={"input": [embed_text]},
        )
        if isinstance(data, dict) and data.get("data"):
            return [float(x) for x in data["data"][0].get("embedding", [])]
    except Exception as e:  # pragma: no cover - depends on endpoint availability
        print(f"[twin_memory] embedding failed, recall falls back to recency/cohort: {e}")
    return []


def _vec_literal(vector: list[float]) -> str:
    """pgvector text literal: [0.1,0.2,...]."""
    return "[" + ",".join(repr(float(x)) for x in vector) + "]"


def size_band(group_size: Optional[int]) -> str:
    n = group_size or 0
    if n < 100:
        return "<100"
    if n < 1000:
        return "100-999"
    if n < 5000:
        return "1000-4999"
    return "5000+"


def build_context_text(decision: dict) -> str:
    """Human-readable context string that gets embedded for semantic recall."""
    return (
        f"{decision.get('funding_arrangement', '')} {decision.get('lob', '')} group, "
        f"{decision.get('group_size', '')} lives ({decision.get('group_size_band', '')}), "
        f"industry {decision.get('industry', '')}; chose {decision.get('scenario_chosen', '')}; "
        f"projected margin {decision.get('projected_margin', '')}%, "
        f"MLR {decision.get('projected_mlr', '')}%, impact {decision.get('dollar_impact', '')}. "
        f"Rationale: {decision.get('rationale', '')}"
    )


# ---------------------------------------------------------------------------
# Write
# ---------------------------------------------------------------------------

async def record_decision(session: AsyncSession, decision: dict) -> str:
    """Persist a decision + (if pgvector is available) its embedding. Returns decision_id."""
    decision_id = decision.get("decision_id") or str(uuid.uuid4())
    gsize = decision.get("group_size")
    band = decision.get("group_size_band") or size_band(gsize)

    await session.execute(
        text("""
            INSERT INTO decision_memory
                (decision_id, analyst_id, funding_arrangement, lob, group_id, group_size,
                 group_size_band, industry, scenario_chosen, parameters, projected,
                 projected_margin, projected_mlr, dollar_impact, rationale, twin_verdict,
                 analyst_response, status)
            VALUES
                (:did, :analyst, :arr, :lob, :gid, :gsize,
                 :band, :industry, :scenario, (:params)::jsonb, (:projected)::jsonb,
                 :margin, :mlr, :impact, :rationale, :verdict,
                 :response, :status)
        """),
        {
            "did": decision_id,
            "analyst": decision.get("analyst_id") or "underwriter",
            "arr": decision.get("funding_arrangement"),
            "lob": decision.get("lob"),
            "gid": decision.get("group_id"),
            "gsize": int(gsize) if gsize is not None else None,
            "band": band,
            "industry": decision.get("industry"),
            "scenario": decision.get("scenario_chosen"),
            "params": json.dumps(decision.get("parameters") or {}),
            "projected": json.dumps(decision.get("projected") or {}),
            "margin": _f(decision.get("projected_margin")),
            "mlr": _f(decision.get("projected_mlr")),
            "impact": _f(decision.get("dollar_impact")),
            "rationale": decision.get("rationale"),
            "verdict": decision.get("twin_verdict"),
            "response": decision.get("analyst_response"),
            "status": decision.get("status") or "candidate",
        },
    )
    await session.commit()

    if await twin_db.pgvector_available():
        embed_text = build_context_text({**decision, "group_size_band": band})
        vector = embed(embed_text)
        if vector:
            try:
                await session.execute(
                    text("""
                        INSERT INTO decision_embeddings (decision_id, embedding, embed_text)
                        VALUES (:did, (:emb)::vector, :etext)
                        ON CONFLICT (decision_id) DO UPDATE
                            SET embedding = (:emb)::vector, embed_text = :etext
                    """),
                    {"did": decision_id, "emb": _vec_literal(vector), "etext": embed_text},
                )
                await session.commit()
            except Exception as e:  # pragma: no cover - vector write best-effort
                print(f"[twin_memory] embedding write skipped: {e}")
                await session.rollback()
    return decision_id


async def record_outcome(
    session: AsyncSession,
    *,
    decision_id: str,
    actual_mlr: Optional[float] = None,
    actual_margin: Optional[float] = None,
    retained: Optional[bool] = None,
    note: Optional[str] = None,
) -> dict:
    feedback_id = str(uuid.uuid4())
    await session.execute(
        text("""
            INSERT INTO outcome_feedback
                (feedback_id, decision_id, actual_mlr, actual_margin, retained, note)
            VALUES (:fid, :did, :mlr, :margin, :retained, :note)
        """),
        {
            "fid": feedback_id, "did": decision_id,
            "mlr": _f(actual_mlr), "margin": _f(actual_margin),
            "retained": retained, "note": note,
        },
    )
    await session.commit()
    return {"feedback_id": feedback_id, "decision_id": decision_id}


# ---------------------------------------------------------------------------
# Recall
# ---------------------------------------------------------------------------

# Latest outcome per decision, joined into recall results.
_OUTCOME_JOIN = """
    LEFT JOIN (
        SELECT DISTINCT ON (decision_id)
               decision_id, actual_mlr, actual_margin, retained
        FROM outcome_feedback
        ORDER BY decision_id, observed_at DESC
    ) oe ON oe.decision_id = dm.decision_id
"""


def _cohort_where(cohort: dict, params: dict) -> str:
    conds = ["dm.funding_arrangement = :arr"]
    params["arr"] = cohort.get("funding_arrangement")
    if cohort.get("group_size_band"):
        conds.append("dm.group_size_band = :band")
        params["band"] = cohort["group_size_band"]
    if cohort.get("lob"):
        conds.append("(dm.lob = :lob OR dm.lob IS NULL)")
        params["lob"] = cohort["lob"]
    return " AND ".join(conds)


async def recall_semantic(
    session: AsyncSession,
    query_text: str,
    cohort: dict,
    *,
    k: int = 6,
) -> list[dict]:
    """Top-K similar past decisions in the cohort via pgvector cosine ANN,
    outcome-annotated. Falls back to cohort+recency when pgvector is unavailable."""
    params: dict = {"k": k}
    where = _cohort_where(cohort, params)

    use_vector = await twin_db.pgvector_available()
    q_vec = embed(query_text) if use_vector else []

    if use_vector and q_vec:
        params["q"] = _vec_literal(q_vec)
        sql = f"""
            SELECT dm.*, oe.actual_mlr, oe.actual_margin, oe.retained,
                   1 - (de.embedding <=> (:q)::vector) AS similarity
            FROM decision_embeddings de
            JOIN decision_memory dm ON dm.decision_id = de.decision_id
            {_OUTCOME_JOIN}
            WHERE {where}
            ORDER BY de.embedding <=> (:q)::vector
            LIMIT :k
        """
    else:
        sql = f"""
            SELECT dm.*, oe.actual_mlr, oe.actual_margin, oe.retained,
                   NULL::double precision AS similarity
            FROM decision_memory dm
            {_OUTCOME_JOIN}
            WHERE {where}
            ORDER BY dm.created_at DESC
            LIMIT :k
        """
    result = await session.execute(text(sql), params)
    return [_recall_row(r) for r in result.mappings().all()]


async def recall_analyst(session: AsyncSession, analyst_id: str, *, limit: int = 5) -> list[dict]:
    result = await session.execute(
        text("""
            SELECT * FROM decision_memory
            WHERE analyst_id = :analyst
            ORDER BY created_at DESC
            LIMIT :lim
        """),
        {"analyst": analyst_id, "lim": limit},
    )
    return [_row_to_dict(r) for r in result.mappings().all()]


async def recent_memory(session: AsyncSession, limit: int = 25) -> list[dict]:
    result = await session.execute(
        text(f"""
            SELECT dm.*, oe.actual_mlr, oe.actual_margin, oe.retained,
                   NULL::double precision AS similarity
            FROM decision_memory dm
            {_OUTCOME_JOIN}
            ORDER BY dm.created_at DESC
            LIMIT :lim
        """),
        {"lim": limit},
    )
    return [_recall_row(r) for r in result.mappings().all()]


# ---------------------------------------------------------------------------
# Guardrails (actuary-owned, versioned)
# ---------------------------------------------------------------------------

DEFAULT_GUARDRAILS = [
    {"rule_type": "margin_floor", "threshold": 2.0, "severity": "block",
     "note": "Underwriting margin must be at least 2%."},
    {"rule_type": "mlr_ceiling", "threshold": 92.0, "severity": "block",
     "note": "Projected MLR may not exceed 92%."},
    {"rule_type": "mlr_ceiling", "threshold": 88.0, "severity": "warn",
     "note": "Projected MLR above 88% warrants senior review."},
    {"rule_type": "authority_limit", "threshold": 5_000_000.0, "severity": "warn",
     "note": "Dollar impact above $5M requires Chief Actuary sign-off."},
]


async def seed_default_guardrails(session: AsyncSession, approved_by: str = "chief_actuary") -> int:
    """Idempotently seed the default guardrail set if none exist. Returns rows added."""
    result = await session.execute(text("SELECT COUNT(*) FROM org_guardrails"))
    if int(result.scalar() or 0) > 0:
        return 0
    for g in DEFAULT_GUARDRAILS:
        await _insert_guardrail(session, g, version=1, approved_by=approved_by)
    await session.commit()
    return len(DEFAULT_GUARDRAILS)


async def list_guardrails(session: AsyncSession, *, active_only: bool = True) -> list[dict]:
    where = "WHERE active = true" if active_only else ""
    result = await session.execute(
        text(f"SELECT * FROM org_guardrails {where} ORDER BY rule_type, version DESC")
    )
    return [_row_to_dict(r) for r in result.mappings().all()]


async def create_guardrail(
    session: AsyncSession,
    *,
    rule_type: str,
    threshold: float,
    severity: str,
    scope: Optional[dict] = None,
    approved_by: str,
) -> dict:
    result = await session.execute(
        text("SELECT COALESCE(MAX(version), 0) FROM org_guardrails WHERE rule_type = :rt"),
        {"rt": rule_type},
    )
    version = int(result.scalar() or 0) + 1
    gid = await _insert_guardrail(
        session,
        {"rule_type": rule_type, "scope": scope or {}, "threshold": threshold,
         "severity": severity, "note": None},
        version=version, approved_by=approved_by,
    )
    await session.commit()
    result = await session.execute(
        text("SELECT * FROM org_guardrails WHERE guardrail_id = :g"), {"g": gid}
    )
    row = result.mappings().first()
    return _row_to_dict(row) if row else {"guardrail_id": gid}


async def _insert_guardrail(session: AsyncSession, g: dict, *, version: int, approved_by: str) -> str:
    gid = str(uuid.uuid4())
    scope = dict(g.get("scope") or {})
    if g.get("note"):
        scope.setdefault("note", g["note"])
    await session.execute(
        text("""
            INSERT INTO org_guardrails
                (guardrail_id, version, rule_type, scope, threshold, severity, active, approved_by)
            VALUES (:gid, :ver, :rt, (:scope)::jsonb, :thr, :sev, true, :by)
        """),
        {
            "gid": gid, "ver": version, "rt": g["rule_type"],
            "scope": json.dumps(scope), "thr": _f(g.get("threshold")),
            "sev": g.get("severity", "warn"), "by": approved_by,
        },
    )
    return gid


# ---------------------------------------------------------------------------
# Analyst profile
# ---------------------------------------------------------------------------

async def get_profile(session: AsyncSession, analyst_id: str) -> Optional[dict]:
    result = await session.execute(
        text("SELECT * FROM analyst_profile WHERE analyst_id = :a"), {"a": analyst_id}
    )
    row = result.mappings().first()
    return _row_to_dict(row) if row else None


async def update_analyst_profile(session: AsyncSession, analyst_id: str, decision: dict) -> None:
    """Upsert the learned per-analyst profile after a decision (continuous learning)."""
    existing = await get_profile(session, analyst_id)
    margin = _f(decision.get("projected_margin")) or 0.0
    if existing:
        count = int(existing.get("decision_count") or 0) + 1
        prev = float(existing.get("typical_margin") or margin)
        typical = round((prev * (count - 1) + margin) / count, 2)
    else:
        count, typical = 1, margin
    await session.execute(
        text("""
            INSERT INTO analyst_profile
                (analyst_id, risk_appetite, typical_margin, typical_overrides, decision_count)
            VALUES (:a, :ra, :tm, '{}'::jsonb, :c)
            ON CONFLICT (analyst_id) DO UPDATE SET
                risk_appetite = :ra, typical_margin = :tm,
                decision_count = :c, updated_at = now()
        """),
        {"a": analyst_id, "ra": _appetite(typical), "tm": typical, "c": count},
    )
    await session.commit()


def _appetite(typical_margin: float) -> str:
    if typical_margin >= 6:
        return "conservative"
    if typical_margin >= 3:
        return "balanced"
    return "aggressive"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _f(v) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _recall_row(row) -> dict:
    """Shape a recall row: base decision fields + nested outcome + similarity."""
    d = _row_to_dict(row)
    outcome = None
    if d.get("actual_mlr") is not None or d.get("retained") is not None or d.get("actual_margin") is not None:
        outcome = {
            "actual_mlr": d.get("actual_mlr"),
            "actual_margin": d.get("actual_margin"),
            "retained": d.get("retained"),
        }
    for k in ("actual_mlr", "actual_margin", "retained"):
        d.pop(k, None)
    d["outcome"] = outcome
    sim = d.get("similarity")
    d["similarity"] = round(float(sim), 4) if sim is not None else None
    return d


def _row_to_dict(row) -> dict:
    """RowMapping -> plain JSON-safe dict (psycopg already returns jsonb as dict/list)."""
    d = dict(row)
    for k, v in list(d.items()):
        if isinstance(v, datetime):
            d[k] = v.isoformat()
        elif isinstance(v, uuid.UUID):
            d[k] = str(v)
    return d
