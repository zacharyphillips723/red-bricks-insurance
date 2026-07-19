"""Network Adequacy Agent — tool-calling over CMS network gold tables.

A conversational agent for network-operations teams that can query compliance,
ghost-network, leakage, and recruitment gold tables, and run a geospatial
what-if network simulation (recompute county compliance if specific OON
providers are recruited in-network) using in-warehouse st_distancesphere.
"""

import json
import re
import traceback

import mlflow
from databricks.sdk import WorkspaceClient

from .env_config import UC_CATALOG, SQL_WAREHOUSE_ID, NET_AGENT_ENDPOINT

_CAT = f"`{UC_CATALOG}`"
ALLOWED_SCHEMAS = ["network", "providers", "members", "analytics"]

SYSTEM_PROMPT = f"""You are a Network Adequacy Analyst for Red Bricks Insurance (a North Carolina health plan).
You help network-operations teams monitor CMS network-adequacy compliance (42 CFR 422.116),
identify ghost-network providers, quantify out-of-network leakage, and plan provider recruitment.

You have tools to:
- query_network_table: read-only SQL against the network gold tables
- get_compliance_summary: overall + by-county/specialty compliance rollup
- simulate_recruitment: geospatial what-if — recompute a county+specialty's compliance %
  if specific out-of-network providers are recruited in-network (uses real distance math)

Key tables (catalog {_CAT}, schema network):
- gold_network_adequacy_compliance (county x specialty compliance, gap_members, distances)
- gold_ghost_network_flags (providers listed but effectively unavailable)
- gold_network_leakage / gold_leakage_summary (OON cost leakage)
- gold_provider_recruitment_targets (ranked OON providers to recruit)
- gold_network_gaps (network gaps by priority)

Strategy: gather data via tools, then answer concisely with specific numbers
(compliance %, gap members, dollar leakage, distances). Cite the data. When a
user asks "what if we recruit X", use simulate_recruitment.

RESPONSE STYLE: Be concise. Lead with the answer, use tight bolded-number bullets,
no filler or restating the question."""

TOOLS = [
    {"type": "function", "function": {
        "name": "query_network_table",
        "description": (
            f"Execute a read-only SQL SELECT against Unity Catalog network tables in {_CAT}. "
            "Key tables: network.gold_network_adequacy_compliance, network.gold_ghost_network_flags, "
            "network.gold_network_leakage, network.gold_leakage_summary, "
            "network.gold_provider_recruitment_targets, network.gold_network_gaps. "
            "Always include a LIMIT (max 50). Only SELECT/WITH/DESCRIBE allowed."
        ),
        "parameters": {"type": "object", "properties": {
            "sql": {"type": "string", "description": "Read-only SQL query."},
            "purpose": {"type": "string", "description": "What this looks up."},
        }, "required": ["sql", "purpose"]}}},
    {"type": "function", "function": {
        "name": "get_compliance_summary",
        "description": "Overall network compliance rollup, optionally filtered to a county or CMS specialty type.",
        "parameters": {"type": "object", "properties": {
            "county": {"type": "string", "description": "Optional county name filter."},
            "specialty": {"type": "string", "description": "Optional CMS specialty type filter."},
        }}}},
    {"type": "function", "function": {
        "name": "simulate_recruitment",
        "description": (
            "Geospatial what-if: recompute a county + CMS specialty's compliance % if the given "
            "out-of-network provider NPIs are recruited in-network. If no NPIs are given, simulates "
            "recruiting ALL out-of-network providers of that specialty in the county. Returns baseline "
            "vs projected covered members and compliance %."
        ),
        "parameters": {"type": "object", "properties": {
            "county": {"type": "string", "description": "County name (e.g. 'Gaston')."},
            "specialty": {"type": "string", "description": "CMS specialty type (e.g. 'Primary Care')."},
            "npis": {"type": "array", "items": {"type": "string"},
                     "description": "Optional list of OON provider NPIs to recruit. Omit for all OON in county."},
        }, "required": ["county", "specialty"]}}},
]

MAX_TOOL_ROUNDS = 4


def _content_to_text(content) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "\n".join((b.get("text") or b.get("content") or "") if isinstance(b, dict) else str(b)
                         for b in content).strip()
    return str(content) if content is not None else ""


def _execute_sql(sql: str, params: list | None = None) -> list[dict]:
    from databricks.sdk.service.sql import StatementParameterListItem
    w = WorkspaceClient()
    kwargs = {"warehouse_id": SQL_WAREHOUSE_ID, "statement": sql, "wait_timeout": "50s"}
    if params:
        kwargs["parameters"] = [
            StatementParameterListItem(name=p["name"], value=p["value"], type=p.get("type", "STRING"))
            for p in params
        ]
    stmt = w.statement_execution.execute_statement(**kwargs)
    if not stmt.result or not stmt.result.data_array:
        return []
    cols = [c.name for c in stmt.manifest.schema.columns] if stmt.manifest and stmt.manifest.schema else []
    return [dict(zip(cols, row)) for row in stmt.result.data_array] if cols else []


def _validate_sql(sql: str) -> str | None:
    up = sql.upper().lstrip()
    if not any(up.startswith(k) for k in ("SELECT", "WITH", "DESCRIBE")):
        return "Only SELECT, WITH, and DESCRIBE statements are allowed."
    if re.search(r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|GRANT|REVOKE|MERGE)\b", sql, re.IGNORECASE):
        return "Write operations are not permitted."
    return None


@mlflow.trace(span_type="TOOL", name="simulate_recruitment")
def simulate_recruitment(county: str, specialty: str, npis: list[str] | None = None) -> dict:
    """Recompute county+specialty compliance if OON providers are recruited in-network."""
    npis = [str(n).strip() for n in (npis or []) if str(n).strip()]
    # Build the "recruited" predicate: specific NPIs, or all OON of the specialty in county.
    if npis:
        placeholders = ", ".join(f":npi{i}" for i in range(len(npis)))
        recruited_pred = f"network_status='Out-of-Network' AND npi IN ({placeholders})"
        extra_params = [{"name": f"npi{i}", "value": n} for i, n in enumerate(npis)]
    else:
        recruited_pred = "network_status='Out-of-Network'"
        extra_params = []

    sql = f"""
    WITH thresh AS (
      SELECT max_distance_miles md, total_members tm, pct_compliant pc, gap_members gm
      FROM {_CAT}.network.gold_network_adequacy_compliance
      WHERE county_name = :county AND cms_specialty_type = :spec LIMIT 1
    ),
    mem AS (SELECT member_latitude lat, member_longitude lon
            FROM {_CAT}.network.silver_member_geo WHERE county = :county),
    inn AS (SELECT provider_latitude lat, provider_longitude lon
            FROM {_CAT}.network.silver_provider_geo
            WHERE cms_specialty_type = :spec AND county = :county
              AND network_status='In-Network' AND is_active=true),
    recruited AS (SELECT provider_latitude lat, provider_longitude lon
            FROM {_CAT}.network.silver_provider_geo
            WHERE cms_specialty_type = :spec AND county = :county AND is_active=true AND {recruited_pred}),
    allp AS (SELECT lat, lon FROM inn UNION ALL SELECT lat, lon FROM recruited)
    SELECT
      (SELECT md FROM thresh) AS max_miles,
      COUNT(*) AS total_members,
      SUM(CASE WHEN EXISTS (SELECT 1 FROM inn WHERE st_distancesphere(st_point(inn.lon,inn.lat),st_point(mem.lon,mem.lat))/1609.34 <= (SELECT md FROM thresh)) THEN 1 ELSE 0 END) AS baseline_covered,
      SUM(CASE WHEN EXISTS (SELECT 1 FROM allp WHERE st_distancesphere(st_point(allp.lon,allp.lat),st_point(mem.lon,mem.lat))/1609.34 <= (SELECT md FROM thresh)) THEN 1 ELSE 0 END) AS projected_covered,
      (SELECT COUNT(*) FROM recruited) AS providers_recruited
    FROM mem
    """
    params = [{"name": "county", "value": county}, {"name": "spec", "value": specialty}] + extra_params
    rows = _execute_sql(sql, params)
    if not rows or not rows[0].get("total_members"):
        return {"error": f"No member/provider data for {specialty} in {county}.", "county": county, "specialty": specialty}
    r = rows[0]
    total = int(r.get("total_members") or 0)
    base = int(r.get("baseline_covered") or 0)
    proj = int(r.get("projected_covered") or 0)
    base_pct = round(base / total * 100, 1) if total else 0.0
    proj_pct = round(proj / total * 100, 1) if total else 0.0
    return {
        "county": county, "specialty": specialty,
        "max_distance_miles": int(r.get("max_miles") or 0),
        "total_members": total,
        "providers_recruited": int(r.get("providers_recruited") or 0),
        "baseline_covered": base, "baseline_pct_compliant": base_pct,
        "projected_covered": proj, "projected_pct_compliant": proj_pct,
        "members_gained": proj - base,
        "compliance_gain_pct": round(proj_pct - base_pct, 1),
        "meets_90pct_threshold": proj_pct >= 90.0,
    }


@mlflow.trace(span_type="TOOL", name="network_agent_tool")
def _execute_tool(name: str, args: dict) -> str:
    try:
        if name == "query_network_table":
            sql = (args.get("sql") or "").strip()
            err = _validate_sql(sql)
            if err:
                return json.dumps({"error": err})
            if "LIMIT" not in sql.upper():
                sql = sql.rstrip(";") + " LIMIT 50"
            if not any(s in sql.lower() for s in ALLOWED_SCHEMAS):
                return json.dumps({"error": f"Query must reference one of {ALLOWED_SCHEMAS}"})
            rows = _execute_sql(sql)
            return json.dumps({"result": rows[:50], "row_count": len(rows)}, default=str)
        if name == "get_compliance_summary":
            conds, params = [], []
            if args.get("county"):
                conds.append("county_name = :county"); params.append({"name": "county", "value": args["county"]})
            if args.get("specialty"):
                conds.append("cms_specialty_type = :spec"); params.append({"name": "spec", "value": args["specialty"]})
            where = f"WHERE {' AND '.join(conds)}" if conds else ""
            rows = _execute_sql(
                f"""SELECT COUNT(*) AS rows, ROUND(AVG(pct_compliant),1) AS avg_pct_compliant,
                           SUM(gap_members) AS total_gap_members,
                           SUM(CASE WHEN is_compliant THEN 1 ELSE 0 END) AS compliant_cells,
                           SUM(CASE WHEN NOT is_compliant THEN 1 ELSE 0 END) AS noncompliant_cells
                    FROM {_CAT}.network.gold_network_adequacy_compliance {where}""", params)
            return json.dumps(rows[0] if rows else {}, default=str)
        if name == "simulate_recruitment":
            return json.dumps(simulate_recruitment(
                args.get("county", ""), args.get("specialty", ""), args.get("npis")), default=str)
        return json.dumps({"error": f"Unknown tool: {name}"})
    except Exception as e:
        return json.dumps({"error": str(e)})


def _sdk_request(body: dict) -> dict:
    return WorkspaceClient().api_client.do("POST", f"/serving-endpoints/{NET_AGENT_ENDPOINT}/invocations", body=body)


def _run_agent(message: str, history: list[dict] | None):
    """Shared tool-calling loop. Yields status milestones then a final event."""
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    if history:
        messages.extend(history)
    messages.append({"role": "user", "content": message})

    _FRIENDLY = {"query_network_table": "Querying network data",
                 "get_compliance_summary": "Summarizing compliance",
                 "simulate_recruitment": "Running network what-if simulation"}

    for _ in range(MAX_TOOL_ROUNDS):
        yield ("status", {"stage": "thinking", "message": "Analyzing your question…"})
        data = _sdk_request({"messages": messages, "tools": TOOLS, "max_tokens": 3000, "temperature": 0.05})
        tool_calls = data.get("choices", [{}])[0].get("message", {}).get("tool_calls", [])
        if not tool_calls:
            yield ("final", {"response": _content_to_text(data["choices"][0]["message"].get("content", ""))})
            return
        messages.append(data["choices"][0]["message"])
        for tc in tool_calls:
            fn = tc.get("function", {}).get("name", "")
            try:
                fa = json.loads(tc.get("function", {}).get("arguments", "{}"))
            except json.JSONDecodeError:
                fa = {}
            yield ("status", {"stage": "tool", "message": _FRIENDLY.get(fn, fn)})
            result = _execute_tool(fn, fa)
            messages.append({"role": "tool", "tool_call_id": tc.get("id", ""), "content": result})

    yield ("status", {"stage": "synthesizing", "message": "Synthesizing analysis…"})
    data = _sdk_request({"messages": messages + [
        {"role": "user", "content": "Provide your final analysis based on the data gathered."}],
        "max_tokens": 2000, "temperature": 0.05})
    yield ("final", {"response": _content_to_text(data["choices"][0]["message"].get("content", ""))})


@mlflow.trace(span_type="AGENT", name="network_agent")
def query_network_agent(message: str, history: list[dict] | None = None) -> dict:
    try:
        result = {"response": "No response generated."}
        for et, payload in _run_agent(message, history):
            if et == "final":
                result = {"response": payload["response"]}
        return result
    except Exception as e:
        print(f"[NetAgent] ERROR: {e}"); traceback.print_exc()
        return {"response": f"Error: {e}"}


@mlflow.trace(span_type="AGENT", name="network_agent_stream")
def stream_network_agent(message: str, history: list[dict] | None = None):
    try:
        yield from _run_agent(message, history)
    except Exception as e:
        print(f"[NetAgent:stream] ERROR: {e}"); traceback.print_exc()
        yield ("error", {"message": str(e)})
