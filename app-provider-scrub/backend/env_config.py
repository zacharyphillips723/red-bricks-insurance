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
    target_schema = os.environ.get("UC_SCHEMA", "claims")
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


_w = WorkspaceClient()

_wh = os.environ.get("SQL_WAREHOUSE_ID", "")
SQL_WAREHOUSE_ID = _wh if _wh not in _SENTINEL else _auto_detect_warehouse(_w)

_cat = os.environ.get("UC_CATALOG", "")
UC_CATALOG = _cat if _cat not in _SENTINEL else _auto_detect_catalog(_w)

# Schema holding the claims tables + denial-model reference tables.
UC_SCHEMA = os.environ.get("UC_SCHEMA", "claims")

LLM_ENDPOINT = os.environ.get("LLM_ENDPOINT") or "databricks-llama-4-maverick"

# Denial-reasoning agent endpoint — Claude Haiku 4.5 is fast + reliable at tool
# calling and grounded synthesis against medical-policy chunks. Overridable.
SCRUB_AGENT_ENDPOINT = os.environ.get("SCRUB_AGENT_ENDPOINT") or "databricks-claude-haiku-4-5"

# ML denial scorer (pyfunc serving both the binary denial-probability model and
# the multiclass reason classifier). Returns {denial_prob, reason_probs{}}.
DENIAL_ENDPOINT = os.environ.get("DENIAL_ENDPOINT") or "denial-risk-scorer"

# Vector Search for the medical-necessity / experimental RAG layer. Reuses the
# existing shared index built by setup_medical_policy_vs.py — no new index.
VS_ENDPOINT = os.environ.get("VS_ENDPOINT") or "red-bricks-vs-endpoint"
VS_INDEX_NAME = os.environ.get("VS_INDEX_NAME") or f"{UC_CATALOG}.prior_auth.medical_policy_vs_index"

# MLflow UC trace storage — the app links its experiment to these UC OTel tables
# so scrub-engine + denial-agent traces stream into Unity Catalog in real time.
# Tables are `{UC_TRACE_TABLE_PREFIX}_otel_spans`, etc., in
# `{UC_CATALOG}.{UC_TRACE_SCHEMA}`. Provisioned by bootstrap_workspace.py; the
# app performs an idempotent re-link on startup. The experiment name must be a
# fresh one that has never had legacy trace-storage tags set on it.
UC_TRACE_SCHEMA = os.environ.get("UC_TRACE_SCHEMA", "analytics")
UC_TRACE_TABLE_PREFIX = os.environ.get("UC_TRACE_TABLE_PREFIX", "denial_agent")
MLFLOW_UC_EXPERIMENT = os.environ.get(
    "MLFLOW_UC_EXPERIMENT", "/Shared/red-bricks-denial-agent-traces-uc"
)

print(f"[env_config] SQL_WAREHOUSE_ID={SQL_WAREHOUSE_ID}")
print(f"[env_config] UC_CATALOG={UC_CATALOG}")
print(f"[env_config] UC_SCHEMA={UC_SCHEMA}")
print(f"[env_config] LLM_ENDPOINT={LLM_ENDPOINT}")
print(f"[env_config] SCRUB_AGENT_ENDPOINT={SCRUB_AGENT_ENDPOINT}")
print(f"[env_config] DENIAL_ENDPOINT={DENIAL_ENDPOINT}")
print(f"[env_config] VS_ENDPOINT={VS_ENDPOINT}")
print(f"[env_config] VS_INDEX_NAME={VS_INDEX_NAME}")
print(f"[env_config] UC_TRACE_SCHEMA={UC_TRACE_SCHEMA}")
print(f"[env_config] UC_TRACE_TABLE_PREFIX={UC_TRACE_TABLE_PREFIX}")
print(f"[env_config] MLFLOW_UC_EXPERIMENT={MLFLOW_UC_EXPERIMENT}")
