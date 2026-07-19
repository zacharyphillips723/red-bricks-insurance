"""Network Adequacy Portal — FastAPI application entry point."""

import os
import traceback
from contextlib import asynccontextmanager
from pathlib import Path

import mlflow
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from backend.router import api


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: link MLflow tracing to Unity Catalog OTel tables."""
    from backend.env_config import (
        UC_CATALOG, SQL_WAREHOUSE_ID, UC_TRACE_SCHEMA,
        UC_TRACE_TABLE_PREFIX, MLFLOW_UC_EXPERIMENT,
    )
    mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "databricks"))
    if SQL_WAREHOUSE_ID:
        os.environ.setdefault("MLFLOW_TRACING_SQL_WAREHOUSE_ID", SQL_WAREHOUSE_ID)
    try:
        from mlflow.entities import UnityCatalog
        exp = mlflow.set_experiment(
            MLFLOW_UC_EXPERIMENT,
            trace_location=UnityCatalog(
                catalog_name=UC_CATALOG, schema_name=UC_TRACE_SCHEMA,
                table_prefix=UC_TRACE_TABLE_PREFIX,
            ),
        )
        print(f"[main] MLflow UC traces enabled — {MLFLOW_UC_EXPERIMENT} (ID {exp.experiment_id})")
    except Exception as e:
        print(f"[main] WARNING: UC trace linking failed ({e})")
        traceback.print_exc()
        try:
            mlflow.set_experiment(os.environ.get("MLFLOW_EXPERIMENT_NAME", "/Shared/red-bricks-network-agent-traces"))
        except Exception as e2:
            print(f"[main] Fallback experiment setup also failed: {e2}")
    yield


app = FastAPI(
    title="Network Adequacy Portal",
    description="CMS compliance monitoring, ghost network detection, and OON leakage intelligence",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(api)

# Serve React frontend from pre-built static directory
static_dir = Path(__file__).parent / "static"
if static_dir.exists():
    app.mount("/", StaticFiles(directory=str(static_dir), html=True), name="frontend")
