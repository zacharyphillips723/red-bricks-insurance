"""Run the RA agent in-process (AGENT_MODE=local, full span tree) or via the served
Model Serving endpoint (AGENT_MODE=endpoint).

The in-process path also wraps the agent's `_execute_tool` so every tool invocation is
recorded as a human-readable *step* (tool name + purpose + result summary). That ordered
step list is the Agent Activity feature — it is returned alongside the narrative/candidates
from both `suspect()` and `chat()`.
"""
import importlib.util
import json
import re
import threading
import uuid

from . import config

_lock = threading.Lock()
_agent = None

# In-flight suspecting runs, for the poll-based live step feed (AGENT_MODE=local).
_RUNS: dict = {}
_RUNS_LOCK = threading.Lock()


def _load_local_agent():
    global _agent
    if _agent is not None:
        return _agent
    spec = importlib.util.spec_from_file_location("ra_agent", config.RA_AGENT_CODE_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    agent = mod.RiskAdjustmentAgent()
    import types
    agent.load_context(types.SimpleNamespace(model_config={
        "UC_CATALOG": config.UC_CATALOG,
        "SQL_WAREHOUSE_ID": config.SQL_WAREHOUSE_ID,
        "RA_SUSPECTING_ENDPOINT": config.DEFAULT_MODEL,
        "VS_INDEX_NAME": config.VS_INDEX_NAME,
        "RA_LAKEBASE_INSTANCE": config.LAKEBASE_INSTANCE,
    }))
    _install_step_capture(agent)
    _agent = agent
    return agent


# --------------------------- step capture (Agent Activity) ---------------------------
def _summarize_step(name: str, args: dict, result_str: str) -> dict:
    try:
        res = json.loads(result_str) if isinstance(result_str, str) else {}
        if not isinstance(res, dict):
            res = {}
    except Exception:
        res = {}
    purpose, summary = name, ""
    if name == "query_uc_table":
        purpose = args.get("purpose") or "Query governed Unity Catalog"
        summary = ("error: " + str(res["error"])[:120]) if "error" in res else f"{res.get('row_count', 0)} rows"
    elif name == "search_clinical_notes":
        purpose = f"Search the chart for “{args.get('query', '')}”"
        summary = "search failed" if "error" in res else f"{res.get('count', len(res.get('passages', [])))} passages"
    elif name == "verify_citation":
        purpose = f"Verify cited passage {args.get('chunk_id', '')}"
        summary = "verified ✓" if res.get("verified") else "NOT in chart ✗"
    elif name == "cms_hcc_grouper":
        purpose = f"Deterministic ICD-10 → HCC: {', '.join(args.get('icd10_codes', []))}"
        parts = [f"{g.get('icd10_code')}→{g.get('hcc_code') if g.get('is_hcc') else 'not-HCC'}"
                 for g in res.get("grouped", [])]
        summary = "; ".join(parts) or "no mapping found"
    elif name == "compute_raf_delta":
        purpose = f"Sum RAF for {', '.join(args.get('hcc_codes', []))}"
        summary = f"RAF delta {res.get('raf_delta', 0)}"
    elif name == "defensibility_score":
        purpose = f"RADV defensibility of {args.get('hcc_code', '')}"
        summary = f"risk {res.get('risk', '?')}/10 ({res.get('evidence_state', 'n/a')})"
    elif name == "memory_recall":
        purpose = "Recall agent memory (lessons / prefs / episodes)"
        summary = (f"{len(res.get('lessons', []))} lessons, {len(res.get('prefs', []))} prefs, "
                   f"{len(res.get('episodes', []))} episodes")
    elif name == "memory_write_episode":
        purpose = "Persist this case to episodic memory"
        summary = "written" if res.get("written") else f"skipped ({res.get('reason', res.get('error', ''))})"
    elif name == "memory_distill_lesson":
        purpose = f"Distill a provisional lesson ({args.get('scope', '')}/{args.get('subject_key', '')})"
        summary = "written (provisional)" if res.get("written") else f"skipped ({res.get('reason', res.get('error', ''))})"
    else:
        summary = str(res)[:120]
    return {"tool": name, "purpose": purpose, "summary": summary}


def _install_step_capture(agent):
    if getattr(agent, "_step_capture_installed", False):
        return
    original = agent._execute_tool

    def wrapped(name, args):
        result = original(name, args)
        try:
            step = _summarize_step(name, args, result)
        except Exception as e:
            step = {"tool": name, "purpose": name, "summary": f"(step capture error: {e})"}
        sink = getattr(agent, "_step_sink", None)
        if sink is not None:
            sink.append(step)   # live-appended; a poller may read this list concurrently
        return result

    agent._execute_tool = wrapped
    agent._step_capture_installed = True


def _parse(answer: str):
    m = re.search(r"```json\s*(\{.*?\})\s*```", answer, re.DOTALL)
    candidates, meta = [], {}
    if m:
        try:
            blob = json.loads(m.group(1))
            candidates = blob.get("candidates", [])
            meta = {k: v for k, v in blob.items() if k != "candidates"}
        except json.JSONDecodeError:
            pass
    narrative = re.sub(r"```json\s*\{.*?\}\s*```", "", answer, flags=re.DOTALL).strip()
    return narrative, candidates, meta


def _run(messages, model_endpoint):
    """Run the in-process agent over a ChatMessage list, capturing steps. Returns (answer, steps)."""
    with _lock:
        agent = _load_local_agent()
        agent._step_sink = []
        prev = agent.llm_endpoint
        agent.llm_endpoint = model_endpoint or prev
        try:
            resp = agent.predict(None, messages)
            steps = list(agent._step_sink)
        finally:
            agent.llm_endpoint = prev
            agent._step_sink = None
    return resp.choices[0].message.content, steps


def suspect(member_id: str, model_endpoint: str):
    prompt = _suspect_prompt(member_id)
    if config.AGENT_MODE == "endpoint":
        resp = config.workspace().serving_endpoints.query(
            name=config.RA_AGENT_ENDPOINT,
            messages=[{"role": "user", "content": prompt}],
            custom_inputs={"model_endpoint": model_endpoint})
        answer = resp.choices[0]["message"]["content"] if resp.choices else ""
        narrative, candidates, meta = _parse(answer if isinstance(answer, str) else str(answer))
        return narrative, candidates, [], meta

    from mlflow.types.llm import ChatMessage
    answer, steps = _run([ChatMessage(role="user", content=prompt)], model_endpoint)
    narrative, candidates, meta = _parse(answer)
    return narrative, candidates, steps, meta


def _suspect_prompt(member_id: str) -> str:
    return (f"[{member_id}] Review this member's chart. Surface SUSPECTED HCCs with cited evidence "
            "and flag RADV-weak coded HCCs. I will adjudicate each.")


def start_run(member_id: str, model_endpoint: str) -> str:
    """Kick off a suspecting run on a background thread and return a run_id. The agent appends
    steps to a live list the poller reads, so Agent Activity streams in as tools execute.
    Local (in-process) mode only — endpoint mode has no per-step visibility."""
    run_id = uuid.uuid4().hex
    state = {"steps": [], "done": False, "narrative": "", "candidates": [], "meta": {},
             "error": None, "member_id": member_id, "model": model_endpoint, "enriched": False}
    with _RUNS_LOCK:
        _RUNS[run_id] = state
        # Bound the map: drop finished older runs.
        done_ids = [k for k, v in _RUNS.items() if v["done"]]
        for k in done_ids[:-20]:
            _RUNS.pop(k, None)

    def worker():
        from mlflow.types.llm import ChatMessage
        try:
            with _lock:
                agent = _load_local_agent()
                agent._step_sink = state["steps"]
                prev = agent.llm_endpoint
                agent.llm_endpoint = model_endpoint or prev
                try:
                    resp = agent.predict(None, [ChatMessage(role="user", content=_suspect_prompt(member_id))])
                    narrative, candidates, meta = _parse(resp.choices[0].message.content)
                    state["narrative"], state["candidates"], state["meta"] = narrative, candidates, meta
                finally:
                    agent.llm_endpoint = prev
                    agent._step_sink = None
        except Exception as e:
            state["error"] = str(e)
            state["narrative"] = f"**Error:** {e}"
        finally:
            state["done"] = True

    threading.Thread(target=worker, daemon=True).start()
    return run_id


def get_run(run_id: str):
    with _RUNS_LOCK:
        return _RUNS.get(run_id)


def chat(history: list, member_id: str, model_endpoint: str):
    """history = [{role, content}, ...] ending with the new user turn. Returns (reply, candidates, steps, meta)."""
    from mlflow.types.llm import ChatMessage
    msgs = [ChatMessage(role=m["role"], content=m["content"]) for m in history if m.get("content")]
    # Ground the latest user turn on the selected member so the agent reads that chart.
    if member_id and msgs and msgs[-1].role == "user" and not msgs[-1].content.lstrip().startswith("["):
        msgs[-1] = ChatMessage(role="user", content=f"[{member_id}] {msgs[-1].content}")

    if config.AGENT_MODE == "endpoint":
        resp = config.workspace().serving_endpoints.query(
            name=config.RA_AGENT_ENDPOINT,
            messages=[{"role": m.role, "content": m.content} for m in msgs],
            custom_inputs={"model_endpoint": model_endpoint})
        answer = resp.choices[0]["message"]["content"] if resp.choices else ""
        narrative, candidates, meta = _parse(answer if isinstance(answer, str) else str(answer))
        return narrative, candidates, [], meta

    answer, steps = _run(msgs, model_endpoint)
    narrative, candidates, meta = _parse(answer)
    return narrative, candidates, steps, meta
