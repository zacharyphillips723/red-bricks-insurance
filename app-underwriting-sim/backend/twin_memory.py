"""Agentic memory + recall for the Underwriting Digital Twin (Phase 4, optional).

Stores every candidate/committed underwriting decision and a semantic embedding of
its context, so the twin can recall relevant precedent before an analyst commits a
policy. The recall interface is deliberately abstracted: this implementation is
Delta-backed (embeddings stored as JSON, cosine computed in Python at demo scale),
which is the roadmap's sanctioned fallback for when Lakebase + pgvector isn't
provisioned. Swapping in a Lakebase/pgvector backend means reimplementing only the
`record_decision` / `recall_semantic` internals — the preflight loop is unchanged.
"""

import json
import math
import uuid
from datetime import datetime
from typing import Optional

from databricks.sdk import WorkspaceClient

from .database import text, _Session as AsyncSession

# Databricks FM API embedding endpoint (1024-dim). Overridable via env in env_config
# if a different model is standardized; kept local to the memory layer.
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
        print(f"[twin_memory] embedding failed, recall will fall back to recency/cohort: {e}")
    return []


def _cosine(a: list[float], b: list[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    return (dot / (na * nb)) if na and nb else 0.0


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
    """Persist a decision + its embedding. Returns the decision_id."""
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
                 :band, :industry, :scenario, CAST(:params AS jsonb), CAST(:projected AS jsonb),
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

    embed_text = build_context_text({**decision, "group_size_band": band})
    vector = embed(embed_text)
    if vector:
        await session.execute(
            text("""
                INSERT INTO decision_embeddings (decision_id, embedding, embed_text)
                VALUES (:did, :emb, :etext)
            """),
            {"did": decision_id, "emb": json.dumps(vector), "etext": embed_text},
        )
    await session.commit()
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
            "fid": feedback_id,
            "did": decision_id,
            "mlr": _f(actual_mlr),
            "margin": _f(actual_margin),
            "retained": retained,
            "note": note,
        },
    )
    await session.commit()
    return {"feedback_id": feedback_id, "decision_id": decision_id}


# ---------------------------------------------------------------------------
# Recall
# ---------------------------------------------------------------------------

async def recall_semantic(
    session: AsyncSession,
    query_text: str,
    cohort: dict,
    *,
    k: int = 6,
) -> list[dict]:
    """Top-K similar past decisions in the cohort, outcome-annotated.

    Falls back to recency ordering when embeddings are unavailable.
    """
    conditions = ["dm.funding_arrangement = :arr"]
    params: dict = {"arr": cohort.get("funding_arrangement")}
    if cohort.get("group_size_band"):
        conditions.append("dm.group_size_band = :band")
        params["band"] = cohort["group_size_band"]
    if cohort.get("lob"):
        conditions.append("(dm.lob = :lob OR dm.lob IS NULL)")
        params["lob"] = cohort["lob"]
    where = " AND ".join(conditions)

    rows = await _mappings(
        session,
        f"""
            SELECT dm.*, de.embedding AS _embedding
            FROM decision_memory dm
            LEFT JOIN decision_embeddings de ON dm.decision_id = de.decision_id
            WHERE {where}
            ORDER BY dm.created_at DESC
            LIMIT 100
        """,
        params,
    )
    outcomes = await _outcome_map(session)

    q_vec = embed(query_text)
    scored = []
    for r in rows:
        d = _row_to_dict(r)
        emb = d.pop("_embedding", None)
        vec = _parse_vec(emb)
        d["similarity"] = round(_cosine(q_vec, vec), 4) if (q_vec and vec) else None
        d["outcome"] = outcomes.get(d["decision_id"])
        scored.append(d)

    if q_vec and any(s["similarity"] is not None for s in scored):
        scored.sort(key=lambda s: (s["similarity"] or -1), reverse=True)
    # else: already recency-ordered from SQL
    return scored[:k]


async def recall_analyst(session: AsyncSession, analyst_id: str, *, limit: int = 5) -> list[dict]:
    rows = await _mappings(
        session,
        """
            SELECT * FROM decision_memory
            WHERE analyst_id = :analyst
            ORDER BY created_at DESC
            LIMIT :lim
        """,
        {"analyst": analyst_id, "lim": limit},
    )
    return [_row_to_dict(r) for r in rows]


async def recent_memory(session: AsyncSession, limit: int = 25) -> list[dict]:
    rows = await _mappings(
        session,
        "SELECT * FROM decision_memory ORDER BY created_at DESC LIMIT :lim",
        {"lim": limit},
    )
    outcomes = await _outcome_map(session)
    out = []
    for r in rows:
        d = _row_to_dict(r)
        d["outcome"] = outcomes.get(d["decision_id"])
        out.append(d)
    return out


# ---------------------------------------------------------------------------
# Guardrails (actuary-owned, versioned)
# ---------------------------------------------------------------------------

DEFAULT_GUARDRAILS = [
    {"rule_type": "margin_floor", "scope": {}, "threshold": 2.0, "severity": "block",
     "note": "Underwriting margin must be at least 2%."},
    {"rule_type": "mlr_ceiling", "scope": {}, "threshold": 92.0, "severity": "block",
     "note": "Projected MLR may not exceed 92%."},
    {"rule_type": "mlr_ceiling", "scope": {}, "threshold": 88.0, "severity": "warn",
     "note": "Projected MLR above 88% warrants senior review."},
    {"rule_type": "authority_limit", "scope": {}, "threshold": 5_000_000.0, "severity": "warn",
     "note": "Dollar impact above $5M requires Chief Actuary sign-off."},
]


async def seed_default_guardrails(session: AsyncSession, approved_by: str = "chief_actuary") -> int:
    """Idempotently seed the default guardrail set if none exist. Returns rows added."""
    existing = await _mappings(session, "SELECT COUNT(*) AS c FROM org_guardrails", {})
    count = int((existing[0] if existing else {}).get("c", 0) or 0)
    if count > 0:
        return 0
    for g in DEFAULT_GUARDRAILS:
        await _insert_guardrail(session, g, version=1, approved_by=approved_by)
    await session.commit()
    return len(DEFAULT_GUARDRAILS)


async def list_guardrails(session: AsyncSession, *, active_only: bool = True) -> list[dict]:
    where = "WHERE active = true" if active_only else ""
    rows = await _mappings(
        session, f"SELECT * FROM org_guardrails {where} ORDER BY rule_type, version DESC", {}
    )
    return [_row_to_dict(r) for r in rows]


async def create_guardrail(
    session: AsyncSession,
    *,
    rule_type: str,
    threshold: float,
    severity: str,
    scope: Optional[dict] = None,
    approved_by: str,
) -> dict:
    # New version = max existing version for this rule_type + 1.
    rows = await _mappings(
        session,
        "SELECT COALESCE(MAX(version), 0) AS v FROM org_guardrails WHERE rule_type = :rt",
        {"rt": rule_type},
    )
    version = int((rows[0] if rows else {}).get("v", 0) or 0) + 1
    gid = await _insert_guardrail(
        session,
        {"rule_type": rule_type, "scope": scope or {}, "threshold": threshold,
         "severity": severity, "note": None},
        version=version, approved_by=approved_by,
    )
    await session.commit()
    rows = await _mappings(session, "SELECT * FROM org_guardrails WHERE guardrail_id = :g", {"g": gid})
    return _row_to_dict(rows[0]) if rows else {"guardrail_id": gid}


async def _insert_guardrail(session: AsyncSession, g: dict, *, version: int, approved_by: str) -> str:
    gid = str(uuid.uuid4())
    scope = dict(g.get("scope") or {})
    if g.get("note"):
        scope.setdefault("note", g["note"])
    await session.execute(
        text("""
            INSERT INTO org_guardrails
                (guardrail_id, version, rule_type, scope, threshold, severity, active, approved_by)
            VALUES (:gid, :ver, :rt, CAST(:scope AS jsonb), :thr, :sev, true, :by)
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
    rows = await _mappings(
        session, "SELECT * FROM analyst_profile WHERE analyst_id = :a", {"a": analyst_id}
    )
    return _row_to_dict(rows[0]) if rows else None


async def update_analyst_profile(session: AsyncSession, analyst_id: str, decision: dict) -> None:
    """Upsert the learned per-analyst profile after a decision (continuous learning)."""
    existing = await get_profile(session, analyst_id)
    margin = _f(decision.get("projected_margin")) or 0.0
    if existing:
        count = int(existing.get("decision_count") or 0) + 1
        prev_margin = float(existing.get("typical_margin") or margin)
        typical = round((prev_margin * (count - 1) + margin) / count, 2)
        appetite = _appetite(typical)
        await session.execute(
            text("""
                UPDATE analyst_profile
                SET typical_margin = :tm, risk_appetite = :ra, decision_count = :c,
                    updated_at = current_timestamp()
                WHERE analyst_id = :a
            """),
            {"tm": typical, "ra": appetite, "c": count, "a": analyst_id},
        )
    else:
        await session.execute(
            text("""
                INSERT INTO analyst_profile
                    (analyst_id, risk_appetite, typical_margin, typical_overrides, decision_count)
                VALUES (:a, :ra, :tm, CAST(:ov AS jsonb), 1)
            """),
            {"a": analyst_id, "ra": _appetite(margin), "tm": margin, "ov": json.dumps({})},
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

_JSON_COLS = {"parameters", "projected", "scope", "typical_overrides"}


async def _mappings(session: AsyncSession, sql: str, params: dict) -> list:
    result = await session.execute(text(sql), params)
    return result.mappings().all()


async def _outcome_map(session: AsyncSession) -> dict:
    rows = await _mappings(
        session,
        "SELECT decision_id, actual_mlr, actual_margin, retained, observed_at FROM outcome_feedback "
        "ORDER BY observed_at ASC",
        {},
    )
    out: dict = {}
    for r in rows:
        d = dict(r)
        out[d["decision_id"]] = {
            "actual_mlr": d.get("actual_mlr"),
            "actual_margin": d.get("actual_margin"),
            "retained": d.get("retained"),
        }
    return out


def _parse_vec(raw) -> list[float]:
    if not raw:
        return []
    if isinstance(raw, list):
        return [float(x) for x in raw]
    try:
        return [float(x) for x in json.loads(raw)]
    except (ValueError, TypeError):
        return []


def _f(v) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _row_to_dict(row) -> dict:
    d = dict(row)
    for k, v in list(d.items()):
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
