"""Runtime environment configuration with auto-detection.

Reads from env vars (set by DAB resource config). If empty or missing,
auto-detects SQL warehouse and falls back to defaults for catalog/LLM.
"""

import os

from databricks.sdk import WorkspaceClient


def _auto_detect_warehouse() -> str:
    """Find the first running SQL warehouse on this workspace."""
    try:
        w = WorkspaceClient()
        for wh in w.warehouses.list():
            if wh.state and wh.state.value == "RUNNING":
                print(f"[env_config] Auto-detected warehouse: {wh.id} ({wh.name})")
                return wh.id
    except Exception as e:
        print(f"[env_config] Warehouse auto-detection failed: {e}")
    return ""


SQL_WAREHOUSE_ID = os.environ.get("SQL_WAREHOUSE_ID") or _auto_detect_warehouse()
UC_CATALOG = os.environ.get("UC_CATALOG") or "red_bricks_insurance"
LLM_ENDPOINT = os.environ.get("LLM_ENDPOINT") or "databricks-meta-llama-3-3-70b-instruct"

print(f"[env_config] SQL_WAREHOUSE_ID={SQL_WAREHOUSE_ID}")
print(f"[env_config] UC_CATALOG={UC_CATALOG}")
print(f"[env_config] LLM_ENDPOINT={LLM_ENDPOINT}")
