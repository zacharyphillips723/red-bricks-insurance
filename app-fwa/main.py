"""FWA Investigation Portal — FastAPI application entry point."""

import traceback
from contextlib import asynccontextmanager
from pathlib import Path

import mlflow
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from backend.database import db
from backend.router import api


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup/shutdown lifecycle — initialize Lakebase and MLflow tracing."""
    import os
    from backend.env_config import UC_CATALOG, SQL_WAREHOUSE_ID

    agent_mode = os.environ.get("AGENT_MODE", "local")

    # Configure MLflow tracing with UC trace storage for both modes.
    # In local mode, @mlflow.trace decorators on agent internals capture the
    # full span tree (supervisor → genie + gemini → tools).
    # In endpoint mode, @mlflow.trace on query_fwa_agent_via_endpoint() captures
    # the request/response/latency as a single span from the app side.
    # Both write to the same UC OTel tables in real-time.
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "databricks")
    mlflow.set_tracking_uri(tracking_uri)

    uc_experiment = os.environ.get(
        "MLFLOW_EXPERIMENT_NAME", "/Shared/red-bricks-fwa-agent-traces"
    ) + "-uc"
    try:
        exp = mlflow.set_experiment(uc_experiment)
        print(f"Agent mode: {agent_mode} — MLflow UC traces enabled")
        print(f"  Experiment: {uc_experiment} (ID: {exp.experiment_id})")
        print(f"  Spans table: {UC_CATALOG}.analytics.fwa_agent_otel_spans")
    except Exception as e:
        base_experiment = os.environ.get(
            "MLFLOW_EXPERIMENT_NAME", "/Shared/red-bricks-fwa-agent-traces"
        )
        mlflow.set_experiment(base_experiment)
        print(f"WARNING: UC experiment setup failed ({e})")
        print(f"  Using fallback: {base_experiment}")

    try:
        db.initialize()
        db.start_refresh()
    except Exception as e:
        print(f"ERROR: Lakebase initialization failed: {e}")
        traceback.print_exc()
    yield
    await db.close()


app = FastAPI(
    title="FWA Investigation Portal",
    description="Fraud, Waste & Abuse detection and investigation for SIU analysts",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(api)

# Serve React frontend from pre-built static directory
static_dir = Path(__file__).parent / "static"
if static_dir.exists():
    app.mount("/", StaticFiles(directory=str(static_dir), html=True), name="frontend")
