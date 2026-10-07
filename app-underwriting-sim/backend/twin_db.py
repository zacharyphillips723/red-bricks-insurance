"""Lakebase (Autoscaling Postgres) connection for the Underwriting Digital Twin.

Phase 4 of the underwriting vision roadmap. This is the ONE place the app talks to
Lakebase — the twin's hot memory layer (decision_memory + pgvector embeddings).
Everything else (simulations, approvals, funding quotes, factor versions) stays on
the Lakehouse Delta store (`backend/database.py`), so Delta remains system-of-record
and Lakebase does only what it's good at: millisecond point lookups, frequent small
writes, and pgvector semantic recall.

Connection + OAuth token-refresh follow the proven pattern the apps used before the
Delta conversion (async SQLAlchemy + psycopg, 50-minute token refresh via the
`do_connect` event). Points at the `uw_sim` database, whose schema (incl. the twin
tables + pgvector) is provisioned by `setup_lakebase.py` and whose security label is
granted to the app SP by `bootstrap_workspace.py`.
"""

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional

from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

logger = logging.getLogger("twin_lakebase")

# Re-exported so callers can `from .twin_db import twin_db, text`.
__all__ = ["twin_db", "text"]


class TwinLakebase:
    """Manages the Lakebase Autoscaling connection for the digital twin."""

    def __init__(self) -> None:
        self._engine = None
        self._session_maker = None
        self._current_token: Optional[str] = None
        self._refresh_task: Optional[asyncio.Task] = None
        self._initialized = False
        self._consecutive_refresh_failures = 0
        self._pgvector: Optional[bool] = None  # lazily probed

    # --- endpoint / token helpers (identical to the shared Lakebase pattern) ---

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
        for attempt in range(1, 11):
            ep = w.postgres.get_endpoint(name=endpoint_path)
            if ep.status and ep.status.hosts and ep.status.hosts.host:
                return ep.status.hosts.host
            if attempt < 10:
                time.sleep(min(5 * attempt, 30))
        raise RuntimeError(f"Endpoint {endpoint_path} did not become ready")

    async def _refresh_loop(self, endpoint_path: str) -> None:
        while True:
            await asyncio.sleep(50 * 60)  # tokens expire at 60 min
            try:
                self._current_token = await asyncio.to_thread(self._generate_token, endpoint_path)
                self._consecutive_refresh_failures = 0
                logger.info("Twin Lakebase token refreshed")
            except Exception:
                self._consecutive_refresh_failures += 1
                logger.exception("Twin token refresh failed (%d)", self._consecutive_refresh_failures)

    def initialize(self) -> None:
        from databricks.sdk import WorkspaceClient

        endpoint_path = self._build_endpoint_path()
        database_name = os.environ.get("LAKEBASE_DATABASE_NAME", "uw_sim")
        w = WorkspaceClient()
        host = self._get_host(endpoint_path)
        username = os.environ.get("LAKEBASE_USERNAME", w.current_user.me().user_name)
        self._current_token = self._generate_token(endpoint_path)

        url = f"postgresql+psycopg://{username}@{host}:5432/{database_name}"
        self._engine = create_async_engine(
            url,
            pool_size=5,
            max_overflow=10,
            pool_recycle=3600,
            pool_pre_ping=True,
            connect_args={"sslmode": "require"},
        )

        @event.listens_for(self._engine.sync_engine, "do_connect")
        def _inject_token(dialect, conn_rec, cargs, cparams):
            cparams["password"] = self._current_token

        self._session_maker = async_sessionmaker(
            self._engine, class_=AsyncSession, expire_on_commit=False
        )
        self._initialized = True
        logger.info("Twin Lakebase engine initialized (db=%s host=%s)", database_name, host)

    def start_refresh(self) -> None:
        endpoint_path = self._build_endpoint_path()
        if not self._refresh_task:
            self._refresh_task = asyncio.create_task(self._refresh_loop(endpoint_path))

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
        return self._initialized and self._consecutive_refresh_failures < 3

    async def pgvector_available(self) -> bool:
        """True if the decision_embeddings (pgvector) table exists. Cached after first probe."""
        if self._pgvector is not None:
            return self._pgvector
        try:
            async with self.session() as s:
                r = await s.execute(text("SELECT to_regclass('public.decision_embeddings') AS t"))
                self._pgvector = r.scalar() is not None
        except Exception as e:
            logger.warning("pgvector probe failed, assuming unavailable: %s", e)
            self._pgvector = False
        return self._pgvector

    @asynccontextmanager
    async def session(self) -> AsyncGenerator[AsyncSession, None]:
        if not self._initialized or not self._session_maker:
            raise RuntimeError("Twin Lakebase not initialized")
        async with self._session_maker() as session:
            yield session


twin_db = TwinLakebase()
