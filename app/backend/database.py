"""Lakebase database connection manager with OAuth token refresh."""

import asyncio
import os
import uuid
import traceback
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional

from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine


class LakebaseConnection:
    """Manages Lakebase Provisioned connections with automatic token refresh."""

    def __init__(self) -> None:
        self._engine = None
        self._session_maker = None
        self._current_token: Optional[str] = None
        self._refresh_task: Optional[asyncio.Task] = None
        self._initialized = False

    def _generate_token(self, instance_name: str) -> str:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        cred = w.database.generate_database_credential(
            request_id=str(uuid.uuid4()),
            instance_names=[instance_name],
        )
        return cred.token

    async def _refresh_loop(self, instance_name: str) -> None:
        while True:
            await asyncio.sleep(50 * 60)  # 50 minutes
            try:
                self._current_token = await asyncio.to_thread(
                    self._generate_token, instance_name
                )
                print("Lakebase token refreshed successfully")
            except Exception as e:
                print(f"Token refresh failed: {e}")

    def initialize(self) -> None:
        """Initialize the database engine."""
        pg_url = os.environ.get("LAKEBASE_PG_URL")

        print(f"[DB] LAKEBASE_PG_URL set: {bool(pg_url)}")
        print(f"[DB] LAKEBASE_INSTANCE_NAME: {os.environ.get('LAKEBASE_INSTANCE_NAME', 'not set')}")
        print(f"[DB] LAKEBASE_DATABASE_NAME: {os.environ.get('LAKEBASE_DATABASE_NAME', 'not set')}")

        if pg_url:
            print(f"[DB] Using static PG URL (host: {pg_url.split('@')[1].split('/')[0] if '@' in pg_url else '?'})")
            if pg_url.startswith("postgresql://"):
                pg_url = pg_url.replace("postgresql://", "postgresql+psycopg://", 1)
            self._engine = create_async_engine(
                pg_url,
                pool_size=5,
                max_overflow=10,
                pool_recycle=3600,
                connect_args={"sslmode": "require"},
            )
        else:
            from databricks.sdk import WorkspaceClient

            instance_name = os.environ.get(
                "LAKEBASE_INSTANCE_NAME", "pop-health-command-center"
            )
            database_name = os.environ.get("LAKEBASE_DATABASE_NAME", "pop_health")

            print(f"[DB] Using Databricks OAuth mode for instance '{instance_name}'")

            w = WorkspaceClient()
            instance = w.database.get_database_instance(name=instance_name)
            username = os.environ.get(
                "LAKEBASE_USERNAME", w.current_user.me().user_name
            )
            print(f"[DB] Connecting as '{username}' to '{instance.read_write_dns}'")

            self._current_token = self._generate_token(instance_name)

            url = (
                f"postgresql+psycopg://{username}@"
                f"{instance.read_write_dns}:5432/{database_name}"
            )
            self._engine = create_async_engine(
                url,
                pool_size=5,
                max_overflow=10,
                pool_recycle=3600,
                connect_args={"sslmode": "require"},
            )

            @event.listens_for(self._engine.sync_engine, "do_connect")
            def inject_token(dialect, conn_rec, cargs, cparams):
                cparams["password"] = self._current_token

        self._session_maker = async_sessionmaker(
            self._engine, class_=AsyncSession, expire_on_commit=False
        )
        self._initialized = True
        print("[DB] Database engine initialized successfully")

    def start_refresh(self) -> None:
        instance_name = os.environ.get(
            "LAKEBASE_INSTANCE_NAME", "pop-health-command-center"
        )
        if not os.environ.get("LAKEBASE_PG_URL") and not self._refresh_task:
            self._refresh_task = asyncio.create_task(
                self._refresh_loop(instance_name)
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
