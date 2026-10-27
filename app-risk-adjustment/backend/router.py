"""FastAPI routes for the Risk Adjustment Coder Workbench.

Surfaces: Overview stats, Chart Review (worklist + evidence + suspecting + adjudicate),
Agent Activity (step list returned by /suspect and /chat), Chat (Lakebase-persisted
conversations), and Memory (agent_user_prefs + agent_lessons CRUD).
"""
import asyncio

from fastapi import APIRouter
from pydantic import BaseModel

from . import agent_runner, config, memory

api = APIRouter(prefix="/api")


def _sql(stmt, params=None):
    # Poll to completion — a cold serverless warehouse may not finish within the initial
    # wait window; returning [] on a non-terminal statement is what caused the worklist to
    # intermittently show zero members. Distinguish "still starting" from "no rows".
    import time
    from databricks.sdk.service.sql import (StatementState, StatementParameterListItem,
                                            ExecuteStatementRequestOnWaitTimeout)
    se = config.workspace().statement_execution
    kwargs = {"warehouse_id": config.SQL_WAREHOUSE_ID, "statement": stmt,
              "wait_timeout": "50s", "on_wait_timeout": ExecuteStatementRequestOnWaitTimeout.CONTINUE}
    if params:
        kwargs["parameters"] = [StatementParameterListItem(name=p["name"], value=p["value"]) for p in params]
    r = se.execute_statement(**kwargs)
    terminal = {StatementState.SUCCEEDED, StatementState.FAILED, StatementState.CANCELED, StatementState.CLOSED}
    for _ in range(45):  # ~90s headroom for a cold-start
        state = r.status.state if r.status else None
        if state in terminal:
            break
        time.sleep(2)
        r = se.get_statement(r.statement_id)
    if not r.status or r.status.state != StatementState.SUCCEEDED or not r.result or not r.result.data_array:
        return []
    cols = [c.name for c in r.manifest.schema.columns]
    return [dict(zip(cols, row)) for row in r.result.data_array]


def _current_user():
    try:
        return config.workspace().current_user.me().user_name
    except Exception:
        return "coder@demo"


@api.get("/health")
def health():
    return {"ok": True, "agent_mode": config.AGENT_MODE, "catalog": config.UC_CATALOG,
            "lakebase": memory.available()}


@api.get("/models")
def models():
    return {"models": config.ALLOWED_MODELS, "default": config.DEFAULT_MODEL}


@api.get("/whoami")
def whoami():
    return {"user": _current_user()}


@api.get("/overview")
def overview():
    """Headline stats for the Overview tab."""
    try:
        agg = _sql(
            f"SELECT COUNT(*) AS worklist, ROUND(SUM(potential_raf_gain),1) AS total_gain, "
            f"ROUND(SUM(raf_at_radv_risk),1) AS total_radv_risk "
            f"FROM {config.UC_CATALOG}.risk_adjustment.v_member_hcc_coherent WHERE suspected_hccs <> ''")
        states = _sql(
            f"SELECT evidence_state, COUNT(*) AS n FROM {config.UC_CATALOG}.risk_adjustment.hcc_evidence "
            f"GROUP BY evidence_state ORDER BY n DESC")
        return {"stats": agg[0] if agg else {}, "evidence_states": states,
                "catalog": config.UC_CATALOG, "default_model": config.DEFAULT_MODEL,
                "models": config.ALLOWED_MODELS}
    except Exception as e:
        return {"stats": {}, "evidence_states": [], "error": str(e)}


@api.get("/cases")
def cases(limit: int = 30):
    rows = _sql(
        f"SELECT member_id, suspected_hccs, weak_hccs, potential_raf_gain, raf_at_radv_risk "
        f"FROM {config.UC_CATALOG}.risk_adjustment.v_member_hcc_coherent "
        f"WHERE suspected_hccs <> '' ORDER BY potential_raf_gain DESC LIMIT {int(limit)}")
    return {"cases": rows}


@api.get("/members/search")
def members_search(q: str = "", limit: int = 25):
    """Search members by member_id OR name (case-insensitive), LEFT JOINing the worklist so each
    result carries the same suspect/weak chips. Worklist members rank first, then by RAF gain."""
    term = (q or "").strip()
    if not term:
        return {"results": [], "query": ""}
    rows = _sql(
        f"SELECT m.member_id, m.member_name, "
        f"COALESCE(v.suspected_hccs,'') AS suspected_hccs, COALESCE(v.weak_hccs,'') AS weak_hccs, "
        f"COALESCE(v.potential_raf_gain,0) AS potential_raf_gain, "
        f"COALESCE(v.raf_at_radv_risk,0) AS raf_at_radv_risk, "
        f"CASE WHEN v.suspected_hccs IS NOT NULL AND v.suspected_hccs <> '' THEN 1 ELSE 0 END AS on_worklist "
        f"FROM {config.UC_CATALOG}.analytics.gold_member_360 m "
        f"LEFT JOIN {config.UC_CATALOG}.risk_adjustment.v_member_hcc_coherent v ON v.member_id = m.member_id "
        f"WHERE m.member_id ILIKE :q OR m.member_name ILIKE :q "
        f"ORDER BY on_worklist DESC, potential_raf_gain DESC "
        f"LIMIT {int(limit)}",
        params=[{"name": "q", "value": f"%{term}%"}])
    return {"results": rows, "query": term}


@api.get("/member/{member_id}/evidence")
def evidence(member_id: str):
    rows = _sql(
        f"SELECT hcc_code, hcc_label, raf_value, evidence_state, evidence_chunk_id, note_hits "
        f"FROM {config.UC_CATALOG}.risk_adjustment.hcc_evidence "
        f"WHERE member_id = :mid ORDER BY evidence_state", params=[{"name": "mid", "value": member_id}])
    # Attach the cited passage text (governed UC read) so the Overview can reveal a chunk on hover.
    texts = _chunk_texts(member_id, [r.get("evidence_chunk_id") for r in rows])
    for r in rows:
        r["passage"] = texts.get(r.get("evidence_chunk_id"), "")
    return {"member_id": member_id, "evidence": rows,
            "adjudications": memory.list_adjudications(member_id)}


def _chunk_texts(member_id, chunk_ids):
    """Fetch the cited passage text for a set of chunk_ids (governed UC read)."""
    ids = [c for c in chunk_ids if c]
    if not ids:
        return {}
    inlist = ",".join("'" + str(c).replace("'", "") + "'" for c in set(ids))
    try:
        rows = _sql(
            f"SELECT chunk_id, chunk_text FROM {config.UC_CATALOG}.documents.silver_case_notes_chunks "
            f"WHERE member_id = :mid AND chunk_id IN ({inlist})", params=[{"name": "mid", "value": member_id}])
        return {r["chunk_id"]: r["chunk_text"] for r in rows}
    except Exception:
        return {}


class SuspectIn(BaseModel):
    member_id: str
    model_endpoint: str | None = None


@api.post("/suspect")
async def suspect(body: SuspectIn):
    model = body.model_endpoint or config.DEFAULT_MODEL
    narrative, candidates, steps, meta = await asyncio.to_thread(agent_runner.suspect, body.member_id, model)
    texts = await asyncio.to_thread(_chunk_texts, body.member_id, [c.get("chunk_id") for c in candidates])
    for c in candidates:
        c["passage"] = texts.get(c.get("chunk_id"), "")
    return {"member_id": body.member_id, "model": model, "narrative": narrative,
            "candidates": candidates, "steps": steps, "meta": meta}


@api.post("/suspect/start")
def suspect_start(body: SuspectIn):
    """Begin a background suspecting run; the client polls /suspect/poll for live steps + result.
    Endpoint mode has no per-step feed, so the client should fall back to POST /suspect."""
    if config.AGENT_MODE == "endpoint":
        return {"streaming": False}
    model = body.model_endpoint or config.DEFAULT_MODEL
    run_id = agent_runner.start_run(body.member_id, model)
    return {"streaming": True, "run_id": run_id}


@api.get("/suspect/poll/{run_id}")
def suspect_poll(run_id: str):
    st = agent_runner.get_run(run_id)
    if st is None:
        return {"found": False}
    out = {"found": True, "done": st["done"], "steps": list(st["steps"])}
    if st["done"]:
        candidates = st["candidates"]
        if not st.get("enriched"):
            texts = _chunk_texts(st["member_id"], [c.get("chunk_id") for c in candidates])
            for c in candidates:
                c["passage"] = texts.get(c.get("chunk_id"), "")
            st["enriched"] = True
        out.update({"member_id": st["member_id"], "model": st["model"], "narrative": st["narrative"],
                    "candidates": candidates, "meta": st["meta"]})
    return out


class AdjudicateIn(BaseModel):
    member_id: str
    hcc: str
    decision: str          # code | reject | query
    reason: str = ""
    chunk_id: str | None = None
    user_id: str | None = None


@api.post("/adjudicate")
async def adjudicate(body: AdjudicateIn):
    user = body.user_id or _current_user()
    res = await asyncio.to_thread(
        memory.write_adjudication, body.member_id, body.hcc, body.decision,
        body.reason, body.chunk_id, user)
    return res


# -------------------------------------- chat --------------------------------------
class ChatIn(BaseModel):
    conversation_id: str | None = None
    member_id: str | None = None
    message: str
    model_endpoint: str | None = None


@api.post("/chat")
async def chat(body: ChatIn):
    model = body.model_endpoint or config.DEFAULT_MODEL
    user = _current_user()
    cid = body.conversation_id

    # Create a conversation on first turn (graceful if Lakebase down).
    if not cid:
        created = await asyncio.to_thread(memory.create_conversation, body.member_id, user, model,
                                          (body.message[:60] + "...") if len(body.message) > 60 else body.message)
        cid = created.get("conversation_id")

    history = await asyncio.to_thread(memory.load_messages, cid) if cid else []
    turns = [{"role": m["role"], "content": m["content"]} for m in history]
    turns.append({"role": "user", "content": body.message})

    reply, candidates, steps, meta = await asyncio.to_thread(agent_runner.chat, turns, body.member_id, model)

    if cid:
        await asyncio.to_thread(memory.save_message, cid, "user", body.message, {"member_id": body.member_id})
        await asyncio.to_thread(memory.save_message, cid, "assistant", reply,
                                {"steps": steps, "candidates": candidates, "model": model})
    return {"conversation_id": cid, "reply": reply, "candidates": candidates,
            "steps": steps, "meta": meta, "model": model}


@api.get("/conversations")
async def conversations(user_id: str | None = None):
    uid = user_id or _current_user()
    rows = await asyncio.to_thread(memory.list_conversations, uid)
    return {"conversations": rows}


@api.get("/conversations/{conversation_id}")
async def conversation(conversation_id: str):
    rows = await asyncio.to_thread(memory.load_messages, conversation_id)
    return {"conversation_id": conversation_id, "messages": rows}


# ------------------------------------- memory -------------------------------------
@api.get("/memory")
async def get_memory(user_id: str | None = None):
    uid = user_id or _current_user()
    res = await asyncio.to_thread(memory.get_memory, uid)
    res["user_id"] = uid
    return res


class PrefIn(BaseModel):
    pref_key: str
    pref_value: str
    user_id: str | None = None


@api.post("/memory")
async def add_memory(body: PrefIn):
    uid = body.user_id or _current_user()
    return await asyncio.to_thread(memory.add_pref, uid, body.pref_key, body.pref_value)


@api.delete("/memory/{kind}/{ident}")
async def delete_memory(kind: str, ident: str, user_id: str | None = None):
    uid = user_id or _current_user()
    return await asyncio.to_thread(memory.delete_memory, kind, ident, uid)
