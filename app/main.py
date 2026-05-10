"""Population Health Command Center — FastAPI application entry point."""

import traceback
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

# ---------------------------------------------------------------------------
# MLflow tracing (agent-level spans — writes to Unity Catalog via Zerobus)
# ---------------------------------------------------------------------------
try:
    import mlflow
    mlflow.set_tracking_uri("databricks")
    mlflow.set_experiment("/Shared/red-bricks-insurance/agent-traces")
    mlflow.langchain.autolog()
    print("[main] MLflow tracing enabled (tracking to Databricks workspace)")
except ImportError:
    print("[main] mlflow not available — tracing disabled")
except Exception as e:
    print(f"[main] mlflow setup warning: {e}")
    try:
        mlflow.set_tracking_uri("databricks")
        mlflow.langchain.autolog()
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
