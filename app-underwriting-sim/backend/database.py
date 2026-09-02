"""Lakehouse (Unity Catalog Delta) writeback layer — drop-in replacement for the
former Lakebase (Postgres) connection manager.

This module keeps the SAME public surface the apps already use:

    from .database import db, text
    async with db.session() as session:
        result = await session.execute(text("SELECT ... :param"), {"param": v})
        rows = result.mappings().all()
        await session.commit()

...but instead of talking to Lakebase over Postgres/SQLAlchemy, every statement is
executed against a Databricks **serverless SQL warehouse** via the SDK Statement
Execution API, writing to Delta tables in Unity Catalog. App state that used to live
in a Lakebase database now lives in `{UC_CATALOG}.{APP_STATE_SCHEMA}`.

Canonical shared implementation — synced to each app's backend/ directory by
sync_shared_backend.sh. Edit THIS file, then run the sync script.

Design notes / limitations (see PORTABILITY_CONVERSION_GUIDE):
  * Each `session.execute(...)` is its own auto-committed statement — there are no
    multi-statement transactions. `session.commit()` / `rollback()` are no-ops.
  * Unqualified table names resolve against the app-state schema because we set the
    statement's default `catalog`/`schema`. The Lakebase-origin queries only ever
    touched their own tables, so no table names need qualifying.
  * Postgres dialect is translated on the fly (now(), ::text, ::jsonb / ::uuid /
    enum casts, gen_random_uuid()). `RETURNING`, `to_jsonb(row)` and `ON CONFLICT`
    are handled at the call sites, not here.
  * JSONB and array columns are stored as STRING (JSON text).
"""

import asyncio
import json
import logging
import os
import re
import uuid
from contextlib import asynccontextmanager
from datetime import date, datetime
from decimal import Decimal
from typing import Any, AsyncGenerator, Optional

logger = logging.getLogger("lakehouse")


# ---------------------------------------------------------------------------
# text() — minimal stand-in for sqlalchemy.text so imports stay unchanged.
# ---------------------------------------------------------------------------

class _SQL:
    __slots__ = ("text",)

    def __init__(self, statement: str) -> None:
        self.text = statement


def text(statement: str) -> _SQL:
    """Drop-in for sqlalchemy.text(): wraps a SQL string."""
    return _SQL(statement)


# ---------------------------------------------------------------------------
# Postgres -> Databricks SQL dialect translation
# ---------------------------------------------------------------------------

# Custom Postgres types (enums, jsonb, uuid) that become STRING in Delta.
# Casts to these types are stripped: `expr::type` -> `expr`, `CAST(expr AS type)` -> `expr`.
_STRIP_CAST_TYPES = [
    "jsonb", "json", "uuid",
    # enums across all app schemas
    "simulation_status", "simulation_type",
    "scrub_request_type", "scrub_decision", "scrub_reason_layer",
    "investigation_status", "fraud_severity", "investigation_source", "investigation_type",
    "pa_review_status", "pa_urgency", "pa_determination_tier", "reviewer_role",
    "appeal_type", "appeal_status", "peer_review_status", "notice_type",
    "delivery_channel", "delivery_status", "rule_action", "rule_status",
    "qa_status", "escalation_status",
    "risk_tier", "care_cycle_status", "alert_source",
]


_UNIT_KW = {
    "day": "DAY", "days": "DAY", "hour": "HOUR", "hours": "HOUR",
    "minute": "MINUTE", "minutes": "MINUTE", "second": "SECOND", "seconds": "SECOND",
    "week": "WEEK", "weeks": "WEEK", "month": "MONTH", "months": "MONTH",
    "year": "YEAR", "years": "YEAR",
}


def _match_paren(s: str, open_idx: int) -> int:
    depth = 0
    for i in range(open_idx, len(s)):
        if s[i] == "(":
            depth += 1
        elif s[i] == ")":
            depth -= 1
            if depth == 0:
                return i
    return -1


def _split_top_minus(expr: str):
    depth = 0
    for i, ch in enumerate(expr):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "-" and depth == 0:
            return expr[:i].strip(), expr[i + 1:].strip()
    return expr.strip(), None


def _fix_time_arith(sql: str) -> str:
    """Postgres time math -> Databricks:
       EXTRACT(EPOCH FROM (A - B))     -> timestampdiff(SECOND, B, A)
       A - B <op> INTERVAL 'N unit'    -> timestampdiff(UNIT, B, A) <op> N
       INTERVAL 'N unit' (additive)    -> INTERVAL N UNIT
    """
    while True:
        m = re.search(r"EXTRACT\s*\(\s*EPOCH\s+FROM\s+", sql, flags=re.IGNORECASE)
        if not m:
            break
        open_idx = sql.index("(", m.start())
        close_idx = _match_paren(sql, open_idx)
        if close_idx < 0:
            break
        inner = sql[sql.index("FROM", m.start()) + 4:close_idx].strip()
        if inner.startswith("(") and _match_paren(inner, 0) == len(inner) - 1:
            inner = inner[1:-1].strip()
        a, b = _split_top_minus(inner)
        repl = f"timestampdiff(SECOND, {b}, {a})" if b else f"unix_timestamp({a})"
        sql = sql[:m.start()] + repl + sql[close_idx + 1:]

    def _interval_cmp(mm):
        a, b, op, n, unit = mm.groups()
        return f"timestampdiff({_UNIT_KW.get(unit.lower(), 'SECOND')}, {b.strip()}, {a.strip()}) {op} {n}"

    sql = re.sub(
        r"(current_timestamp\(\)|[\w\.]+)\s*-\s*(current_timestamp\(\)|COALESCE\([^()]*\)|[\w\.]+)"
        r"\s*(<=|>=|<|>|=)\s*INTERVAL\s*'(\d+)\s*(\w+)'",
        _interval_cmp, sql, flags=re.IGNORECASE,
    )
    # Remaining additive interval literals: INTERVAL '1 day' -> INTERVAL 1 DAY
    sql = re.sub(
        r"INTERVAL\s*'(\d+)\s*(\w+)'",
        lambda mm: f"INTERVAL {mm.group(1)} {_UNIT_KW.get(mm.group(2).lower(), mm.group(2).upper())}",
        sql, flags=re.IGNORECASE,
    )
    return sql


def _translate(sql: str) -> str:
    """Translate the Postgres-flavored SQL the apps emit into Databricks SQL."""
    # now() -> current_timestamp()
    sql = re.sub(r"(?i)\bnow\s*\(\s*\)", "current_timestamp()", sql)
    # gen_random_uuid() -> uuid()
    sql = re.sub(r"(?i)\bgen_random_uuid\s*\(\s*\)", "uuid()", sql)
    # timestamptz -> timestamp (type name and cast)
    sql = re.sub(r"(?i)\btimestamptz\b", "timestamp", sql)
    # Postgres time arithmetic (EXTRACT EPOCH / INTERVAL literals)
    sql = _fix_time_arith(sql)
    # Postgres JSON extraction ->> 'k' on a STRING(JSON) column
    sql = re.sub(r"([\w\.]+)\s*->>\s*'([^']+)'", r"get_json_object(\1, '$.\2')", sql)

    # Strip casts to custom/JSON/uuid/enum types.
    for t in _STRIP_CAST_TYPES:
        # expr::type   (expr is a bind param, quoted literal, or bare identifier — no parens)
        sql = re.sub(r"::\s*" + t + r"\b", "", sql, flags=re.IGNORECASE)
        # CAST(expr AS type)
        sql = re.sub(
            r"(?i)CAST\(\s*([^()]+?)\s+AS\s+" + t + r"\s*\)",
            r"\1",
            sql,
        )

    # text -> string for the remaining (legitimate) casts.
    sql = re.sub(r"::\s*text\b", "::string", sql, flags=re.IGNORECASE)
    sql = re.sub(r"(?i)CAST\(\s*([^()]+?)\s+AS\s+text\s*\)", r"CAST(\1 AS string)", sql)
    return sql


# ---------------------------------------------------------------------------
# Parameter binding — infer a Databricks SQL type from the Python value.
# ---------------------------------------------------------------------------

def _to_params(params: Optional[dict]) -> list:
    from databricks.sdk.service.sql import StatementParameterListItem

    if not params:
        return []
    items = []
    for name, value in params.items():
        if value is None:
            items.append(StatementParameterListItem(name=name, value=None))
            continue
        if isinstance(value, bool):
            items.append(StatementParameterListItem(name=name, value=str(value).lower(), type="BOOLEAN"))
        elif isinstance(value, int):
            items.append(StatementParameterListItem(name=name, value=str(value), type="BIGINT"))
        elif isinstance(value, float):
            items.append(StatementParameterListItem(name=name, value=repr(value), type="DOUBLE"))
        elif isinstance(value, Decimal):
            items.append(StatementParameterListItem(name=name, value=str(value), type="DECIMAL(38,6)"))
        elif isinstance(value, (datetime, date)):
            items.append(StatementParameterListItem(name=name, value=value.isoformat(), type="STRING"))
        elif isinstance(value, (dict, list)):
            # JSONB / array column payloads -> JSON text into a STRING column.
            items.append(StatementParameterListItem(name=name, value=json.dumps(value), type="STRING"))
        else:
            items.append(StatementParameterListItem(name=name, value=str(value), type="STRING"))
    return items


# ---------------------------------------------------------------------------
# Result wrappers — mimic the slice of the SQLAlchemy Result API the apps use.
# ---------------------------------------------------------------------------

class _Row(dict):
    """A result row that supports mapping access (row['x']), attribute access
    (row.x), and positional access (row[0])."""

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def __getitem__(self, key):
        if isinstance(key, int):
            return list(self.values())[key]
        return super().__getitem__(key)


class _Result:
    """Return value of session.execute(). Supports both core and .mappings() styles."""

    def __init__(self, rows: list) -> None:
        self._rows = rows

    # .mappings() is a no-op view here — rows are already dict-like.
    def mappings(self) -> "_Result":
        return self

    def all(self) -> list:
        return self._rows

    def fetchall(self) -> list:
        return self._rows

    def first(self) -> Optional[_Row]:
        return self._rows[0] if self._rows else None

    def fetchone(self) -> Optional[_Row]:
        return self._rows[0] if self._rows else None

    def one(self) -> _Row:
        if len(self._rows) != 1:
            raise ValueError(f"one() expected exactly one row, got {len(self._rows)}")
        return self._rows[0]

    def one_or_none(self) -> Optional[_Row]:
        if not self._rows:
            return None
        if len(self._rows) > 1:
            raise ValueError(f"one_or_none() expected at most one row, got {len(self._rows)}")
        return self._rows[0]

    def scalar(self) -> Any:
        if not self._rows:
            return None
        return self._rows[0][0]

    def scalar_one_or_none(self) -> Any:
        if not self._rows:
            return None
        return self._rows[0][0]

    def keys(self):
        return list(self._rows[0].keys()) if self._rows else []

    def __iter__(self):
        return iter(self._rows)


# ---------------------------------------------------------------------------
# Session + connection
# ---------------------------------------------------------------------------

class _Session:
    """A no-transaction session backed by the Statement Execution API."""

    def __init__(self, store: "LakehouseStore") -> None:
        self._store = store

    async def execute(self, statement, params: Optional[dict] = None) -> _Result:
        sql = statement.text if hasattr(statement, "text") else str(statement)
        sql = _translate(sql)
        rows = await asyncio.to_thread(self._store._run, sql, params)
        return _Result(rows)

    async def commit(self) -> None:  # statements auto-commit; nothing to do
        return None

    async def rollback(self) -> None:  # cannot roll back auto-committed statements
        return None


class LakehouseStore:
    """Runs SQL against a serverless SQL warehouse, writing to UC Delta tables.

    Public surface matches the former LakebaseConnection so app code and lifespans
    need no changes: initialize(), start_refresh(), close(), is_healthy, session().
    """

    def __init__(self) -> None:
        self._initialized = False
        self._catalog: Optional[str] = None
        self._schema: Optional[str] = None
        self._warehouse_id: Optional[str] = None
        # table (lowercase) -> surrogate id column, learned from the DDL. Used to
        # auto-fill ids that Postgres previously supplied via gen_random_uuid().
        self._id_cols: dict = {}

    def initialize(self) -> None:
        # Resolve config from the app's env_config (already auto-detected) with env fallback.
        try:
            from .env_config import UC_CATALOG as _cat, SQL_WAREHOUSE_ID as _wh
        except Exception:  # pragma: no cover - defensive
            _cat = os.environ.get("UC_CATALOG", "")
            _wh = os.environ.get("SQL_WAREHOUSE_ID", "")
        self._catalog = os.environ.get("UC_CATALOG_RESOLVED") or _cat or os.environ.get("UC_CATALOG", "")
        self._warehouse_id = _wh or os.environ.get("SQL_WAREHOUSE_ID", "")
        self._schema = os.environ.get("APP_STATE_SCHEMA", "app_state")
        logger.info(
            "Lakehouse store: catalog=%s schema=%s warehouse=%s",
            self._catalog, self._schema, self._warehouse_id,
        )
        self._initialized = True

    def start_refresh(self) -> None:  # no OAuth token loop needed
        return None

    async def close(self) -> None:
        return None

    @property
    def is_healthy(self) -> bool:
        return self._initialized

    def ensure_tables(self, ddl: str) -> None:
        """Create the app-state schema and its Delta tables/views (idempotent).

        `ddl` is the Delta DDL for this app with `{catalog}` and `{schema}`
        placeholders; statements are separated by `;`.
        """
        if not self._initialized:
            self.initialize()
        # Create the schema first (fully qualified), then create objects inside it.
        self._run(f"CREATE SCHEMA IF NOT EXISTS `{self._catalog}`.`{self._schema}`", None)
        body = ddl.format(catalog=f"`{self._catalog}`", schema=f"`{self._schema}`")
        self._learn_id_cols(body)
        for stmt in _split_statements(body):
            if stmt.strip():
                self._run(stmt, None)
        logger.info("Lakehouse app-state tables ensured in %s.%s", self._catalog, self._schema)

    def _learn_id_cols(self, ddl_body: str) -> None:
        """Record each table's surrogate id column (first `*_id STRING` column)."""
        for m in re.finditer(
            r"CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+([`\w\.]+)\s*\(\s*([`\w]+)\s+(\w+)",
            ddl_body, flags=re.IGNORECASE,
        ):
            table = m.group(1).replace("`", "").split(".")[-1].lower()
            col = m.group(2).replace("`", "")
            col_type = m.group(3).upper()
            if col.lower().endswith("_id") and col_type == "STRING":
                self._id_cols[table] = col

    def _inject_autoid(self, sql: str) -> str:
        """Auto-fill a surrogate id for `INSERT INTO t (...) VALUES (...)` when the
        insert omits the table's id column (formerly a gen_random_uuid() default)."""
        if not self._id_cols:
            return sql
        m = re.match(
            r"(\s*INSERT\s+INTO\s+)([`\w\.]+)(\s*\()([^)]*)(\)\s*VALUES\s*\()",
            sql, flags=re.IGNORECASE,
        )
        if not m:
            return sql
        table = m.group(2).replace("`", "").split(".")[-1].lower()
        id_col = self._id_cols.get(table)
        if not id_col:
            return sql
        cols = [c.strip().strip("`").lower() for c in m.group(4).split(",")]
        if id_col.lower() in cols:
            return sql
        rebuilt = (m.group(1) + m.group(2) + m.group(3) + id_col + ", "
                   + m.group(4) + m.group(5) + "uuid(), ")
        return rebuilt + sql[m.end():]

    def _run(self, sql: str, params: Optional[dict]) -> list:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.sql import StatementState

        if not self._initialized:
            self.initialize()

        sql = self._inject_autoid(sql)
        w = WorkspaceClient()
        kwargs: dict = {
            "warehouse_id": self._warehouse_id,
            "statement": sql,
            "wait_timeout": "30s",
        }
        # Default catalog/schema so unqualified names resolve to the app-state schema.
        if self._catalog:
            kwargs["catalog"] = self._catalog
        if self._schema:
            kwargs["schema"] = self._schema
        param_items = _to_params(params)
        if param_items:
            kwargs["parameters"] = param_items

        stmt = w.statement_execution.execute_statement(**kwargs)

        # Poll if the warehouse didn't finish within wait_timeout.
        statement_id = stmt.statement_id
        while stmt.status and stmt.status.state in (
            StatementState.PENDING,
            StatementState.RUNNING,
        ):
            stmt = w.statement_execution.get_statement(statement_id)

        if stmt.status and stmt.status.state != StatementState.SUCCEEDED:
            err = stmt.status.error
            msg = err.message if err else str(stmt.status.state)
            raise RuntimeError(f"Statement failed: {msg}\nSQL: {sql[:500]}")

        if not stmt.result or not stmt.result.data_array:
            return []
        cols = []
        if stmt.manifest and stmt.manifest.schema and stmt.manifest.schema.columns:
            cols = stmt.manifest.schema.columns
        if not cols:
            return []
        names = [c.name for c in cols]
        types = [getattr(c.type_name, "value", str(c.type_name)) for c in cols]
        out = []
        for row in stmt.result.data_array:
            out.append(_Row((names[i], _coerce_value(types[i], row[i])) for i in range(len(names))))
        return out

    @asynccontextmanager
    async def session(self) -> AsyncGenerator[_Session, None]:
        if not self._initialized:
            self.initialize()
        yield _Session(self)


def _coerce_value(type_name: str, raw: Any) -> Any:
    """Coerce a Statement Execution string cell into a native Python type.

    The Statement Execution API returns every value as a string (INLINE
    JSON_ARRAY). SQLAlchemy/asyncpg returned native types, so we restore them
    here using the result manifest's column types.
    """
    if raw is None:
        return None
    t = (type_name or "").upper()
    try:
        if t in ("INT", "INTEGER", "SHORT", "BYTE", "LONG", "BIGINT", "SMALLINT", "TINYINT"):
            return int(raw)
        if t in ("FLOAT", "DOUBLE", "REAL"):
            return float(raw)
        if t in ("DECIMAL", "NUMERIC"):
            return Decimal(raw)
        if t in ("BOOLEAN", "BOOL"):
            return str(raw).lower() in ("true", "t", "1")
        if t == "DATE":
            return date.fromisoformat(raw)
        if t in ("TIMESTAMP", "TIMESTAMP_NTZ"):
            return datetime.fromisoformat(raw.replace(" ", "T", 1))
    except Exception:
        return raw
    return raw


def _split_statements(sql: str) -> list:
    """Split a DDL script on semicolons (DDL has no semicolons inside literals)."""
    return [s for s in sql.split(";") if s.strip()]


db = LakehouseStore()
