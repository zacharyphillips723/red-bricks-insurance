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
    from backend.env_config import (
        UC_CATALOG, SQL_WAREHOUSE_ID, UC_TRACE_SCHEMA,
        UC_TRACE_TABLE_PREFIX, MLFLOW_UC_EXPERIMENT,
    )

    agent_mode = os.environ.get("AGENT_MODE", "local")

    # Configure MLflow tracing to stream traces into Unity Catalog OTel tables
    # in real-time. @mlflow.trace decorators on the agent internals capture the
    # full span tree (supervisor → genie + gemini → tools) and each completed
    # turn is exported to `{catalog}.{schema}.{prefix}_otel_spans` within seconds.
    #
    # We link the experiment to the UC location via the modern
    # `trace_location=UnityCatalog(...)` API. The backing OTel tables are
    # provisioned once by bootstrap_workspace.py (running as a principal with
    # CREATE TABLE); this call then hits the fast idempotent re-link path.
    # MLFLOW_TRACING_SQL_WAREHOUSE_ID is required for the link/provision call.
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "databricks")
    mlflow.set_tracking_uri(tracking_uri)
    if SQL_WAREHOUSE_ID:
        os.environ.setdefault("MLFLOW_TRACING_SQL_WAREHOUSE_ID", SQL_WAREHOUSE_ID)

    try:
        from mlflow.entities import UnityCatalog

        exp = mlflow.set_experiment(
            MLFLOW_UC_EXPERIMENT,
            trace_location=UnityCatalog(
                catalog_name=UC_CATALOG,
                schema_name=UC_TRACE_SCHEMA,
                table_prefix=UC_TRACE_TABLE_PREFIX,
            ),
        )
        spans_table = f"{UC_CATALOG}.{UC_TRACE_SCHEMA}.{UC_TRACE_TABLE_PREFIX}_otel_spans"
        print(f"Agent mode: {agent_mode} — MLflow UC traces enabled")
        print(f"  Experiment: {MLFLOW_UC_EXPERIMENT} (ID: {exp.experiment_id})")
        print(f"  Spans table: {spans_table}")
    except Exception as e:
        # Fall back to a plain experiment so tracing still works via the MLflow
        # backend (artifact-backed) even if UC linking is unavailable.
        print(f"WARNING: UC trace linking failed ({e})")
        traceback.print_exc()
        try:
            fallback = os.environ.get(
                "MLFLOW_EXPERIMENT_NAME", "/Shared/red-bricks-fwa-agent-traces"
            )
            mlflow.set_experiment(fallback)
            print(f"  Using fallback experiment (no UC tables): {fallback}")
        except Exception as e2:
            print(f"  Fallback experiment setup also failed: {e2}")

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
