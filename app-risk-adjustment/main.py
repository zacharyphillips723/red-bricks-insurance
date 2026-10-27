"""Risk Adjustment / HCC Coder Workbench — FastAPI app entrypoint.

Serves the React (Vite) build from frontend/dist and the API. Runs the RA agent in-process
(AGENT_MODE=local) for a full MLflow span tree, or calls the served endpoint (AGENT_MODE=endpoint).
"""
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from backend.router import api


@asynccontextmanager
async def lifespan(app: FastAPI):
    # MLflow tracing to Unity Catalog (every LLM/TOOL/RETRIEVER/MEMORY span).
    try:
        import mlflow
        from mlflow.tracing.destination import UnityCatalog
        mlflow.set_experiment(os.environ.get("MLFLOW_UC_EXPERIMENT", "/Shared/red-bricks-ra-agent-traces-uc"))
        mlflow.tracing.set_destination(UnityCatalog(
            catalog_name=os.environ.get("UC_CATALOG", "red_bricks_insurance_catalog"),
            schema_name=os.environ.get("UC_TRACE_SCHEMA", "analytics"),
            table_prefix=os.environ.get("UC_TRACE_TABLE_PREFIX", "ra_agent")))
    except Exception as e:
        print(f"[RA App] MLflow UC tracing setup skipped: {e}")
    yield


app = FastAPI(title="Red Bricks — Risk Adjustment Coder Workbench", lifespan=lifespan)
app.include_router(api)

_DIST = Path(__file__).parent / "frontend" / "dist"
_LEGACY = Path(__file__).parent / "frontend" / "index.html"


@app.get("/")
def index():
    if (_DIST / "index.html").exists():
        return FileResponse(str(_DIST / "index.html"))
    return FileResponse(str(_LEGACY))


# SPA: serve built assets; fall back to index.html for client-side routes.
if _DIST.exists():
    app.mount("/assets", StaticFiles(directory=str(_DIST / "assets")), name="assets")

    @app.get("/{full_path:path}")
    def spa(full_path: str):
        candidate = _DIST / full_path
        if full_path and candidate.exists() and candidate.is_file():
            return FileResponse(str(candidate))
        return FileResponse(str(_DIST / "index.html"))
