"""PA Review Portal — FastAPI application entry point."""

import traceback
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from backend.database import db
from backend.router import api


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup/shutdown lifecycle — initialize and tear down Lakebase connection."""
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
