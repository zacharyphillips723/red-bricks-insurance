"""Population Health Command Center — FastAPI application entry point."""

import os
import traceback
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

# ---------------------------------------------------------------------------
# MLflow tracing → Unity Catalog OTel tables (matches the other Red Bricks apps).
# The care-intelligence multi-agent supervisor is decorated with @mlflow.trace;
# linking the experiment to a UC trace location streams spans to
# analytics.care_agent_otel_* so the in-app Observability page can query them.
# ---------------------------------------------------------------------------
try:
    import mlflow
    from backend.env_config import (
        UC_CATALOG, SQL_WAREHOUSE_ID, UC_TRACE_SCHEMA,
        UC_TRACE_TABLE_PREFIX, MLFLOW_UC_EXPERIMENT,
    )
    mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "databricks"))
    if SQL_WAREHOUSE_ID:
        os.environ.setdefault("MLFLOW_TRACING_SQL_WAREHOUSE_ID", SQL_WAREHOUSE_ID)
    try:
        from mlflow.entities import UnityCatalog
        _exp = mlflow.set_experiment(
            MLFLOW_UC_EXPERIMENT,
            trace_location=UnityCatalog(
                catalog_name=UC_CATALOG, schema_name=UC_TRACE_SCHEMA,
                table_prefix=UC_TRACE_TABLE_PREFIX,
            ),
        )
        print(f"[main] MLflow UC traces enabled — {MLFLOW_UC_EXPERIMENT} (ID {_exp.experiment_id})")
    except Exception as e:
        print(f"[main] WARNING: UC trace linking failed ({e}) — falling back to workspace experiment")
        mlflow.set_experiment("/Shared/red-bricks-insurance/agent-traces")
    mlflow.langchain.autolog()
except ImportError:
    print("[main] mlflow not available — tracing disabled")
except Exception as e:
    print(f"[main] mlflow setup warning: {e}")
    traceback.print_exc()
    # Best-effort recovery: keep autologging on even if UC-experiment linking above
    # failed before reaching autolog() — degrade to default workspace tracing.
    try:
        import mlflow
        mlflow.set_tracking_uri("databricks")
        mlflow.langchain.autolog()
        print("[main] MLflow autolog enabled (workspace tracing fallback)")
    except Exception:
        pass

# ---------------------------------------------------------------------------
# OpenTelemetry FastAPI instrumentation (request-level spans)
# ---------------------------------------------------------------------------
try:
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    _otel_available = True
    print("[main] OpenTelemetry FastAPI instrumentation available")
except ImportError:
    _otel_available = False
    print("[main] OpenTelemetry not installed — request-level tracing disabled")

from fastapi import WebSocket, WebSocketDisconnect

from backend.database import db
from backend.conversation_store import cleanup_expired_conversations
from backend.router import api
from backend.websocket import notifications


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup/shutdown lifecycle — initialize and tear down Lakebase connection."""
    try:
        db.initialize()
        db.start_refresh()
    except Exception as e:
        print(f"ERROR: Lakebase initialization failed: {e}")
        traceback.print_exc()
    try:
        await cleanup_expired_conversations()
    except Exception as e:
        print(f"[main] conversation cleanup warning: {e}")
    yield
    await db.close()


app = FastAPI(
    title="Population Health Command Center",
    description="Care gap management and patient risk stratification for Blues health plans",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(api)


@app.websocket("/ws/notifications")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time alert notifications."""
    await notifications.connect(websocket)
    try:
        while True:
            # Keep connection alive — client sends pings, we just listen
            await websocket.receive_text()
    except WebSocketDisconnect:
        await notifications.disconnect(websocket)

# Instrument FastAPI with OpenTelemetry (request-level spans)
if _otel_available:
    FastAPIInstrumentor.instrument_app(app)
    print("[main] FastAPI OpenTelemetry instrumentation active")

# Serve React frontend from pre-built static directory
static_dir = Path(__file__).parent / "static"
if static_dir.exists():
    app.mount("/", StaticFiles(directory=str(static_dir), html=True), name="frontend")
