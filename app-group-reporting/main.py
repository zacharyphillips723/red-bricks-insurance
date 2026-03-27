"""Group Reporting Portal — FastAPI application entry point."""

from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from backend.router import api


app = FastAPI(
    title="Group Reporting Portal",
    description="Employer group analytics and sales enablement for Red Bricks Insurance",
    version="1.0.0",
)

app.include_router(api)

# Serve React frontend from pre-built static directory
static_dir = Path(__file__).parent / "static"
if static_dir.exists() and any(static_dir.iterdir()):
    app.mount("/", StaticFiles(directory=str(static_dir), html=True), name="frontend")
