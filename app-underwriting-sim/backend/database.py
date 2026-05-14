"""Lakebase Autoscaling database connection manager with OAuth token refresh.

Canonical shared implementation — synced to each app's backend/ directory by
sync_shared_backend.sh. Edit THIS file, then run the sync script.

Each app's DAB resource config sets LAKEBASE_DATABASE_NAME via env vars,
so the hardcoded default here is just a safety fallback.
"""

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional

from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

logger = logging.getLogger("lakebase")


class LakebaseConnection:
    """Manages Lakebase Autoscaling connections with automatic token refresh."""

    def __init__(self) -> None:
        self._engine = None
        self._session_maker = None
        self._current_token: Optional[str] = None
        self._refresh_task: Optional[asyncio.Task] = None
        self._initialized = False
        self._consecutive_refresh_failures = 0

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
                logger.warning("Endpoint not ready (attempt %d/%d), retrying in %ds...", attempt, max_attempts, wait)
                time.sleep(wait)
        raise RuntimeError(f"Endpoint {endpoint_path} did not become ready after {max_attempts} attempts")

    async def _refresh_loop(self, endpoint_path: str) -> None:
        while True:
            await asyncio.sleep(50 * 60)  # Refresh every 50 min (tokens expire at 60)
            try:
                self._current_token = await asyncio.to_thread(
                    self._generate_token, endpoint_path
                )
                self._consecutive_refresh_failures = 0
                logger.info("Lakebase token refreshed successfully")
            except Exception:
                self._consecutive_refresh_failures += 1
                logger.exception(
                    "Token refresh failed (consecutive failures: %d)",
                    self._consecutive_refresh_failures,
                )
                if self._consecutive_refresh_failures >= 3:
                    logger.error(
                        "Token refresh failed %d consecutive times — "
                        "database queries will fail when the current token expires. "
                        "Attempting re-initialization...",
                        self._consecutive_refresh_failures,
                    )
                    try:
                        self._current_token = await asyncio.to_thread(
                            self._generate_token, endpoint_path
                        )
                        self._consecutive_refresh_failures = 0
                        logger.info("Re-initialization succeeded")
                    except Exception:
                        logger.exception("Re-initialization also failed")

    def initialize(self) -> None:
        """Initialize the database engine."""
        pg_url = os.environ.get("LAKEBASE_PG_URL")

        logger.info("LAKEBASE_PG_URL set: %s", bool(pg_url))
        logger.info("LAKEBASE_PROJECT_ID: %s", os.environ.get("LAKEBASE_PROJECT_ID", "not set"))
        logger.info("LAKEBASE_BRANCH: %s", os.environ.get("LAKEBASE_BRANCH", "not set"))
        logger.info("LAKEBASE_DATABASE_NAME: %s", os.environ.get("LAKEBASE_DATABASE_NAME", "not set"))

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
            database_name = os.environ.get("LAKEBASE_DATABASE_NAME", "red_bricks_alerts")

            logger.info("Using Databricks OAuth mode for endpoint '%s', database '%s'", endpoint_path, database_name)

            w = WorkspaceClient()
            host = self._get_host(endpoint_path)
            username = os.environ.get(
                "LAKEBASE_USERNAME", w.current_user.me().user_name
            )
            logger.info("Connecting as '%s' to '%s'", username, host)

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
        logger.info("Database engine initialized successfully")

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

    @property
    def is_healthy(self) -> bool:
        """Returns False if token refresh has failed repeatedly."""
        return self._consecutive_refresh_failures < 3

    @asynccontextmanager
    async def session(self) -> AsyncGenerator[AsyncSession, None]:
        if not self._initialized or not self._session_maker:
            raise RuntimeError("Database not initialized")
        async with self._session_maker() as session:
            yield session


db = LakebaseConnection()
