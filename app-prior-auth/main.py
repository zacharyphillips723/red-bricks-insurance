"""PA Review Portal — FastAPI application entry point."""

import os
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
    """Startup/shutdown lifecycle — initialize MLflow tracing and Lakebase."""
    from backend.env_config import (
        UC_CATALOG, SQL_WAREHOUSE_ID, UC_TRACE_SCHEMA,
        UC_TRACE_TABLE_PREFIX, MLFLOW_UC_EXPERIMENT,
    )

    # Stream PA agent + document-adjudication traces into Unity Catalog OTel
    # tables in real-time. @mlflow.trace spans on the agent/doc internals are
    # exported to `{catalog}.{schema}.{prefix}_otel_spans` within seconds. The
    # backing tables are provisioned by bootstrap_workspace.py; this call hits
    # the idempotent re-link path. MLFLOW_TRACING_SQL_WAREHOUSE_ID is required.
    mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "databricks"))
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
        print(f"MLflow UC traces enabled — experiment {MLFLOW_UC_EXPERIMENT} (ID {exp.experiment_id})")
        print(f"  Spans table: {spans_table}")
    except Exception as e:
        print(f"WARNING: UC trace linking failed ({e})")
        traceback.print_exc()
        try:
            fallback = os.environ.get("MLFLOW_EXPERIMENT_NAME", "/Shared/red-bricks-pa-agent-traces")
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
    title="PA Review Portal",
    description="Prior Authorization Review & Auto-Adjudication Portal for UM nurses and medical directors",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(api)

# Serve React frontend from pre-built static directory
static_dir = Path(__file__).parent / "static"
if static_dir.exists():
    app.mount("/", StaticFiles(directory=str(static_dir), html=True), name="frontend")
