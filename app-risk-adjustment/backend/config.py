"""Runtime config + resolution for the Risk Adjustment Coder Workbench app."""
import os
from functools import lru_cache

from databricks.sdk import WorkspaceClient

_SENTINEL = {None, ""}


@lru_cache(maxsize=1)
def workspace() -> WorkspaceClient:
    return WorkspaceClient()


def _auto_warehouse() -> str:
    try:
        w = workspace()
        running = [wh for wh in w.warehouses.list() if wh.state and wh.state.value == "RUNNING"]
        return (running or list(w.warehouses.list()))[0].id
    except Exception:
        return ""


UC_CATALOG = os.environ.get("UC_CATALOG") or "red_bricks_insurance_catalog"
SQL_WAREHOUSE_ID = os.environ.get("SQL_WAREHOUSE_ID") or _auto_warehouse()
VS_INDEX_NAME = os.environ.get("VS_INDEX_NAME") or f"{UC_CATALOG}.documents.case_notes_vs_index"
LAKEBASE_INSTANCE = os.environ.get("RA_LAKEBASE_INSTANCE") or "red-bricks-ra-memory"

AGENT_MODE = os.environ.get("AGENT_MODE", "local")            # local (in-process) | endpoint (served)
RA_AGENT_ENDPOINT = os.environ.get("RA_AGENT_ENDPOINT", "ra-agent")
# Prefer the vendored copy at the app root (present when deployed as a Databricks App),
# else the repo src path (local dev from the repo).
_vendored = os.path.join(os.path.dirname(os.path.dirname(__file__)), "risk_adjustment_agent.py")
_src = os.path.join(os.path.dirname(__file__), "..", "..", "src", "agents", "risk_adjustment_agent.py")
RA_AGENT_CODE_PATH = os.environ.get("RA_AGENT_CODE_PATH") or (_vendored if os.path.exists(_vendored) else _src)

# Unity Gateway model dropdown — curated subset of workspace FM endpoints.
DEFAULT_MODEL = os.environ.get("RA_SUSPECTING_ENDPOINT", "databricks-claude-sonnet-5")
ALLOWED_MODELS = [m.strip() for m in os.environ.get(
    "RA_ALLOWED_MODELS",
    "databricks-claude-sonnet-5,databricks-claude-opus-5,databricks-claude-haiku-5-5,"
    "databricks-gpt-5-5,databricks-gemini-3-8-flash",
).split(",") if m.strip()]
