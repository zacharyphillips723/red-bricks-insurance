"""Runtime environment configuration with auto-detection.

Canonical shared implementation — synced to each app's backend/ directory by
sync_shared_backend.sh. Edit THIS file, then run the sync script.

Config resolution order:
  1. Env var set by DAB resource config (via `databricks bundle deploy`)
  2. If env var is "auto" or empty -> auto-detect via Databricks SDK
  3. Hardcoded fallback if auto-detection fails

Per-app customization: each app's sync adds its own config block at the bottom
with target_schema, genie_space_title, and any extra exports.
"""

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


def _auto_detect_catalog(w: WorkspaceClient, target_schema: str = "analytics") -> str:
    """Find the catalog that contains our target UC_SCHEMA."""
    target_schema = os.environ.get("UC_SCHEMA", target_schema)
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
    """Find a Genie space by title, falling back to first available."""
    try:
        resp = w.api_client.do("GET", "/api/2.0/genie/spaces")
        spaces = resp.get("spaces", [])
        if target_title:
            for s in spaces:
                if s.get("title") == target_title:
                    print(f"[env_config] Auto-detected Genie space by title: {s['space_id']} ({target_title})")
                    return s["space_id"]
            print(f"[env_config] WARNING: No Genie space with title '{target_title}' found")
        if spaces:
            space = spaces[0]
            print(f"[env_config] Auto-detected Genie space (fallback): {space['space_id']} ({space.get('title', '')})")
            return space["space_id"]
        print("[env_config] WARNING: No Genie spaces found")
    except Exception as e:
        print(f"[env_config] Genie space auto-detection failed: {e}")
    return ""


def configure(target_schema: str = "analytics", genie_space_title: str = "") -> tuple:
    """Initialize and return (SQL_WAREHOUSE_ID, UC_CATALOG, GENIE_SPACE_ID, LLM_ENDPOINT).

    Call this once at module level in each app's env_config.py with app-specific values.
    """
    w = WorkspaceClient()

    wh = os.environ.get("SQL_WAREHOUSE_ID", "")
    warehouse_id = wh if wh not in _SENTINEL else _auto_detect_warehouse(w)

    cat = os.environ.get("UC_CATALOG", "")
    catalog = cat if cat not in _SENTINEL else _auto_detect_catalog(w, target_schema)

    genie = os.environ.get("GENIE_SPACE_ID", "")
    genie_space_id = genie if genie not in _SENTINEL else _auto_detect_genie_space(w, genie_space_title)

    llm_endpoint = os.environ.get("LLM_ENDPOINT") or "databricks-llama-4-maverick"

    print(f"[env_config] SQL_WAREHOUSE_ID={warehouse_id}")
    print(f"[env_config] UC_CATALOG={catalog}")
    print(f"[env_config] GENIE_SPACE_ID={genie_space_id}")
    print(f"[env_config] LLM_ENDPOINT={llm_endpoint}")

    return warehouse_id, catalog, genie_space_id, llm_endpoint
