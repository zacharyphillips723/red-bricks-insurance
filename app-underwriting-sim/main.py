"""Underwriting Simulation Portal — FastAPI application entry point."""

import traceback
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from backend.database import db
from backend.router import api


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: initialise Lakebase connection and token refresh."""
    try:
        db.initialize()
        db.start_refresh()
        print("[main] Lakebase connection initialized")
    except Exception as e:
        print(f"[main] WARNING: Lakebase init failed (simulations will still work, "
              f"but save/load requires DB): {e}")
        traceback.print_exc()
    yield
    await db.close()


app = FastAPI(
    title="Underwriting Simulation Portal",
    description="What-if analysis for underwriters and actuaries at Red Bricks Insurance",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(api)

# Serve React frontend from pre-built static directory
static_dir = Path(__file__).parent / "static"
if static_dir.exists():
    app.mount("/", StaticFiles(directory=str(static_dir), html=True), name="frontend")
