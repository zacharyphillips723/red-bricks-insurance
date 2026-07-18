"""Runtime environment configuration with auto-detection."""

import os
import traceback

from databricks.sdk import WorkspaceClient

_SENTINEL = {"", "auto"}


def _auto_detect_warehouse(w: WorkspaceClient) -> str:
    try:
        for wh in w.warehouses.list():
            if wh.state and wh.state.value == "RUNNING":
                print(f"[env_config] Auto-detected warehouse: {wh.id} ({wh.name})")
                return wh.id
        for wh in w.warehouses.list():
            print(f"[env_config] Using warehouse (state={wh.state}): {wh.id} ({wh.name})")
            return wh.id
        print("[env_config] WARNING: No SQL warehouses found")
    except Exception as e:
        print(f"[env_config] Warehouse auto-detection failed: {e}")
        traceback.print_exc()
    return ""


def _auto_detect_catalog(w: WorkspaceClient) -> str:
    target_schema = os.environ.get("UC_SCHEMA", "prior_auth")
    skip = {"system", "hive_metastore", "main", "samples", "__databricks_internal"}
    try:
        candidates = [
            cat.name for cat in w.catalogs.list()
            if (cat.name or "") not in skip
        ]
        for name in candidates:
            try:
                schemas = [s.name for s in w.schemas.list(catalog_name=name)]
                if target_schema in schemas:
                    print(f"[env_config] Auto-detected catalog: {name} (has schema '{target_schema}')")
                    return name
            except Exception:
                continue
        if candidates:
            print(f"[env_config] Auto-detected catalog (fallback): {candidates[0]}")
            return candidates[0]
        return "main"
    except Exception as e:
        print(f"[env_config] Catalog auto-detection failed: {e}")
        return "red_bricks_insurance"


def _auto_detect_genie_space(w: WorkspaceClient, target_title: str = "") -> str:
    try:
        resp = w.api_client.do("GET", "/api/2.0/genie/spaces")
        spaces = resp.get("spaces", [])
        if target_title:
            for s in spaces:
                if s.get("title") == target_title:
                    print(f"[env_config] Auto-detected Genie space: {s['space_id']} ({target_title})")
                    return s["space_id"]
        if spaces:
            space = spaces[0]
            print(f"[env_config] Auto-detected Genie space (fallback): {space['space_id']}")
            return space["space_id"]
    except Exception as e:
        print(f"[env_config] Genie space auto-detection failed: {e}")
    return ""


_w = WorkspaceClient()

_wh = os.environ.get("SQL_WAREHOUSE_ID", "")
SQL_WAREHOUSE_ID = _wh if _wh not in _SENTINEL else _auto_detect_warehouse(_w)

_cat = os.environ.get("UC_CATALOG", "")
UC_CATALOG = _cat if _cat not in _SENTINEL else _auto_detect_catalog(_w)

_genie = os.environ.get("GENIE_SPACE_ID", "")
GENIE_SPACE_ID = _genie if _genie not in _SENTINEL else _auto_detect_genie_space(
    _w, target_title="Red Bricks Insurance — PA Analytics"
)

LLM_ENDPOINT = os.environ.get("LLM_ENDPOINT") or "databricks-llama-4-maverick"

# PA review agent endpoint — Claude Haiku 4.5 is fast + reliable at tool
# calling and synthesis, and replaces the now-deprecated
# databricks-gemini-2-5-flash. Overridable via env.
PA_AGENT_ENDPOINT = os.environ.get("PA_AGENT_ENDPOINT") or "databricks-claude-haiku-4-5"

# MLflow UC trace storage — the app links its experiment to these UC OTel
# tables so PA agent + document-adjudication traces stream into Unity Catalog
# in real-time. Tables are `{UC_TRACE_TABLE_PREFIX}_otel_spans`, etc., in
# `{UC_CATALOG}.{UC_TRACE_SCHEMA}`. Provisioned by bootstrap_workspace.py; the
# app performs an idempotent re-link on startup. The experiment name must be a
# fresh one that has never had legacy trace-storage tags set on it.
UC_TRACE_SCHEMA = os.environ.get("UC_TRACE_SCHEMA", "analytics")
UC_TRACE_TABLE_PREFIX = os.environ.get("UC_TRACE_TABLE_PREFIX", "pa_agent")
MLFLOW_UC_EXPERIMENT = os.environ.get(
    "MLFLOW_UC_EXPERIMENT", "/Shared/red-bricks-pa-agent-traces-uc"
)

# UC Volume for uploaded medical records (PA document auto-adjudication feature).
UC_SCHEMA = os.environ.get("UC_SCHEMA", "prior_auth")
PA_DOC_VOLUME = os.environ.get("PA_DOC_VOLUME", f"{UC_CATALOG}.{UC_SCHEMA}.pa_documents")
PA_DOC_VOLUME_PATH = f"/Volumes/{PA_DOC_VOLUME.replace('.', '/')}"

print(f"[env_config] SQL_WAREHOUSE_ID={SQL_WAREHOUSE_ID}")
print(f"[env_config] UC_CATALOG={UC_CATALOG}")
print(f"[env_config] GENIE_SPACE_ID={GENIE_SPACE_ID}")
print(f"[env_config] LLM_ENDPOINT={LLM_ENDPOINT}")
print(f"[env_config] PA_AGENT_ENDPOINT={PA_AGENT_ENDPOINT}")
print(f"[env_config] PA_DOC_VOLUME_PATH={PA_DOC_VOLUME_PATH}")
print(f"[env_config] UC_TRACE_SCHEMA={UC_TRACE_SCHEMA}")
print(f"[env_config] UC_TRACE_TABLE_PREFIX={UC_TRACE_TABLE_PREFIX}")
print(f"[env_config] MLFLOW_UC_EXPERIMENT={MLFLOW_UC_EXPERIMENT}")
