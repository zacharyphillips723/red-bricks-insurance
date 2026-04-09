"""Lakebase Autoscaling database connection manager with OAuth token refresh.

Reuses the same pattern as the Command Center app — SQLAlchemy async engine
with automatic Databricks OAuth token injection.
"""

import asyncio
import os
import time
import traceback
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional

from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine


class LakebaseConnection:
    """Manages Lakebase Autoscaling connections with automatic token refresh."""

    def __init__(self) -> None:
        self._engine = None
        self._session_maker = None
        self._current_token: Optional[str] = None
        self._refresh_task: Optional[asyncio.Task] = None
        self._initialized = False

    def _build_endpoint_path(self) -> str:
        project_id = os.environ.get("LAKEBASE_PROJECT_ID", "red-bricks-insurance")
        branch = os.environ.get("LAKEBASE_BRANCH", "production")
        return f"projects/{project_id}/branches/{branch}/endpoints/primary"

    def _generate_token(self, endpoint_path: str) -> str:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        cred = w.postgres.generate_database_credential(endpoint=endpoint_path)
        return cred.token

    def _get_host(self, endpoint_path: str) -> str:
        """Resolve the Autoscaling endpoint host, retrying for scale-to-zero wake-up."""
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        max_attempts = 10
        for attempt in range(1, max_attempts + 1):
            ep = w.postgres.get_endpoint(name=endpoint_path)
            if ep.status and ep.status.hosts and ep.status.hosts.host:
                return ep.status.hosts.host
            if attempt < max_attempts:
                wait = min(5 * attempt, 30)
                print(f"[DB] Endpoint not ready (attempt {attempt}/{max_attempts}), retrying in {wait}s...")
                time.sleep(wait)
        raise RuntimeError(f"Endpoint {endpoint_path} did not become ready after {max_attempts} attempts")

    async def _refresh_loop(self, endpoint_path: str) -> None:
        while True:
            await asyncio.sleep(50 * 60)
            try:
                self._current_token = await asyncio.to_thread(
                    self._generate_token, endpoint_path
                )
                print("Lakebase token refreshed successfully")
            except Exception as e:
                print(f"Token refresh failed: {e}")

    def initialize(self) -> None:
        """Initialize the database engine."""
        pg_url = os.environ.get("LAKEBASE_PG_URL")

        print(f"[DB] LAKEBASE_PG_URL set: {bool(pg_url)}")
        print(f"[DB] LAKEBASE_PROJECT_ID: {os.environ.get('LAKEBASE_PROJECT_ID', 'not set')}")
        print(f"[DB] LAKEBASE_BRANCH: {os.environ.get('LAKEBASE_BRANCH', 'not set')}")
        print(f"[DB] LAKEBASE_DATABASE_NAME: {os.environ.get('LAKEBASE_DATABASE_NAME', 'not set')}")

        if pg_url:
            if pg_url.startswith("postgresql://"):
                pg_url = pg_url.replace("postgresql://", "postgresql+psycopg://", 1)
            self._engine = create_async_engine(
                pg_url,
                pool_size=5,
                max_overflow=10,
                pool_recycle=3600,
                pool_pre_ping=True,
                connect_args={"sslmode": "require"},
            )
        else:
            from databricks.sdk import WorkspaceClient

            endpoint_path = self._build_endpoint_path()
            database_name = os.environ.get("LAKEBASE_DATABASE_NAME", "fwa_cases")

            print(f"[DB] Using Databricks OAuth mode for endpoint '{endpoint_path}'")

            w = WorkspaceClient()
            host = self._get_host(endpoint_path)
            username = os.environ.get("LAKEBASE_USERNAME", w.current_user.me().user_name)

            self._current_token = self._generate_token(endpoint_path)

            url = (
                f"postgresql+psycopg://{username}@"
                f"{host}:5432/{database_name}"
            )
            self._engine = create_async_engine(
                url,
                pool_size=5,
                max_overflow=10,
                pool_recycle=3600,
                pool_pre_ping=True,
                connect_args={"sslmode": "require"},
            )

            @event.listens_for(self._engine.sync_engine, "do_connect")
            def inject_token(dialect, conn_rec, cargs, cparams):
                cparams["password"] = self._current_token

        self._session_maker = async_sessionmaker(
            self._engine, class_=AsyncSession, expire_on_commit=False
        )
        self._initialized = True
        print("[DB] FWA database engine initialized successfully")

    def start_refresh(self) -> None:
        endpoint_path = self._build_endpoint_path()
        if not os.environ.get("LAKEBASE_PG_URL") and not self._refresh_task:
            self._refresh_task = asyncio.create_task(
                self._refresh_loop(endpoint_path)
            )

    async def close(self) -> None:
        if self._refresh_task:
            self._refresh_task.cancel()
            try:
                await self._refresh_task
            except asyncio.CancelledError:
                pass
        if self._engine:
            await self._engine.dispose()

    @asynccontextmanager
    async def session(self) -> AsyncGenerator[AsyncSession, None]:
        if not self._initialized or not self._session_maker:
            raise RuntimeError("Database not initialized")
        async with self._session_maker() as session:
            yield session


db = LakebaseConnection()
