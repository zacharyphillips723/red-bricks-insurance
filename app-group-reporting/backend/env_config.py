"""Runtime environment configuration with auto-detection.

Reads from env vars (set by DAB resource config via `databricks bundle deploy`).
If empty or missing, auto-detects SQL warehouse and catalog at startup.

Deployment flow:
  1. Set warehouse_id + catalog in databricks.yml target variables
  2. Run `databricks bundle deploy --target <name>`
  3. Resource YAMLs resolve ${var.warehouse_id} / ${var.catalog} automatically
  4. This module provides a safety net if env vars are empty
"""

import os
import traceback

from databricks.sdk import WorkspaceClient


def _auto_detect_warehouse(w: WorkspaceClient) -> str:
    """Find the first running SQL warehouse the SP has access to."""
    try:
        for wh in w.warehouses.list():
            if wh.state and wh.state.value == "RUNNING":
                print(f"[env_config] Auto-detected warehouse: {wh.id} ({wh.name})")
                return wh.id
        # No running warehouse — try any warehouse
        for wh in w.warehouses.list():
            print(f"[env_config] Auto-detected warehouse (not running): {wh.id} ({wh.name}, state={wh.state})")
            return wh.id
        print("[env_config] WARNING: No SQL warehouses found. Statement Execution API calls will fail.")
    except Exception as e:
        print(f"[env_config] Warehouse auto-detection failed: {e}")
        traceback.print_exc()
    return ""


def _auto_detect_catalog(w: WorkspaceClient) -> str:
    """Find the workspace's primary catalog (first non-system catalog)."""
    try:
        for cat in w.catalogs.list():
            name = cat.name or ""
            if name not in ("system", "hive_metastore", "main", "samples", "__databricks_internal"):
                print(f"[env_config] Auto-detected catalog: {name}")
                return name
        # Fallback to 'main' if nothing else found
        print("[env_config] No custom catalog found, using 'main'")
        return "main"
    except Exception as e:
        print(f"[env_config] Catalog auto-detection failed: {e}")
        return "red_bricks_insurance"


# Initialize SDK once
_w = WorkspaceClient()

SQL_WAREHOUSE_ID = os.environ.get("SQL_WAREHOUSE_ID") or _auto_detect_warehouse(_w)
UC_CATALOG = os.environ.get("UC_CATALOG") or _auto_detect_catalog(_w)
LLM_ENDPOINT = os.environ.get("LLM_ENDPOINT") or "databricks-llama-4-maverick"

print(f"[env_config] SQL_WAREHOUSE_ID={SQL_WAREHOUSE_ID}")
print(f"[env_config] UC_CATALOG={UC_CATALOG}")
print(f"[env_config] LLM_ENDPOINT={LLM_ENDPOINT}")
