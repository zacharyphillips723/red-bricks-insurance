#!/usr/bin/env python3
"""Convert a Lakebase (Postgres) schema .sql file into Delta (Unity Catalog) DDL.

Used by the Lakebase -> Lakehouse migration. Emits CREATE TABLE / CREATE VIEW
statements with `{catalog}` and `{schema}` placeholders that each app's
`db.ensure_tables(...)` fills in at startup.

Transformations:
  * drop `DO $$ ... END $$;` enum blocks and standalone `CREATE TYPE ... ;`
  * drop `CREATE [UNIQUE] INDEX ...;`
  * map Postgres types -> Delta (TEXT/VARCHAR/UUID/jsonb/enum -> STRING,
    TIMESTAMPTZ -> TIMESTAMP, SERIAL -> BIGINT, DOUBLE PRECISION -> DOUBLE,
    NUMERIC -> DECIMAL, TEXT[]/UUID[] -> STRING)
  * strip inline FK (`REFERENCES ...`, `ON DELETE ...`), PRIMARY KEY, UNIQUE,
    and table-level CONSTRAINT lines (UC does not enforce them)
  * `DEFAULT gen_random_uuid()` -> dropped (app generates the id);
    `DEFAULT now()` -> `DEFAULT current_timestamp()`; other literal defaults kept
  * qualify table/view names with {catalog}.{schema}
  * add TBLPROPERTIES to enable column defaults where any DEFAULT remains

This is a pragmatic converter for these specific schemas — not a general Postgres
parser. Review the output.

Usage:  python3 pg_to_delta_ddl.py <input.sql> > <output.sql>
"""

import re
import sys

_TYPE_MAP = [
    (r"\bTIMESTAMP\s+WITH\s+TIME\s+ZONE\b", "TIMESTAMP"),
    (r"\bTIMESTAMPTZ\b", "TIMESTAMP"),
    (r"\bTEXT\s*\[\s*\]", "STRING"),
    (r"\bUUID\s*\[\s*\]", "STRING"),
    (r"\bJSONB\b", "STRING"),
    (r"\bJSON\b", "STRING"),
    (r"\bUUID\b", "STRING"),
    (r"\bBIGSERIAL\b", "BIGINT"),
    (r"\bSERIAL\b", "BIGINT"),
    (r"\bDOUBLE\s+PRECISION\b", "DOUBLE"),
    (r"\bVARCHAR\s*\(\s*\d+\s*\)", "STRING"),
    (r"\bVARCHAR\b", "STRING"),
    (r"\bTEXT\b", "STRING"),
    (r"\bBOOL\b", "BOOLEAN"),
    (r"\bINTEGER\b", "INT"),
    (r"\bSMALLINT\b", "INT"),
    (r"\bNUMERIC\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)", r"DECIMAL(\1,\2)"),
    (r"\bNUMERIC\b", "DECIMAL(38,6)"),
    (r"\bREAL\b", "FLOAT"),
]

# Enum type names in these schemas (any cast/column of these -> STRING).
_ENUMS = [
    "simulation_status", "simulation_type",
    "investigation_status", "fraud_severity", "investigation_source", "investigation_type",
    "pa_review_status", "pa_urgency", "pa_determination_tier", "reviewer_role",
    "appeal_type", "appeal_status", "peer_review_status", "notice_type",
    "delivery_channel", "delivery_status", "rule_action", "rule_status",
    "qa_status", "escalation_status",
    "risk_tier", "care_cycle_status", "alert_source",
]


def _strip_enum_defs(sql: str) -> str:
    sql = re.sub(r"DO\s*\$\$.*?END\s*\$\$\s*;", "", sql, flags=re.DOTALL | re.IGNORECASE)
    sql = re.sub(r"CREATE\s+TYPE\s+\w+\s+AS\s+ENUM\s*\([^)]*\)\s*;", "", sql, flags=re.IGNORECASE)
    return sql


def _strip_indexes(sql: str) -> str:
    return re.sub(r"CREATE\s+(UNIQUE\s+)?INDEX[^;]*;", "", sql, flags=re.IGNORECASE)


def _split_top_level(sql: str) -> list:
    """Split on semicolons (safe: no semicolons inside these DDL literals)."""
    return [s.strip() for s in sql.split(";") if s.strip()]


_CHECK_RE = re.compile(r"\s*CHECK\s*\((?:[^()]+|\([^()]*\))*\)", re.IGNORECASE)


def _translate_type_portion(rest: str) -> str:
    """Apply type/enum/default/constraint transforms to everything after the
    column NAME (so a column named after an enum type isn't clobbered)."""
    # enum type -> STRING
    for enum in _ENUMS:
        rest = re.sub(r"\b" + enum + r"\b", "STRING", rest)
    # type map
    for pat, repl in _TYPE_MAP:
        rest = re.sub(pat, repl, rest, flags=re.IGNORECASE)
    # defaults
    rest = re.sub(r"DEFAULT\s+gen_random_uuid\(\)", "", rest, flags=re.IGNORECASE)
    rest = re.sub(r"(?i)DEFAULT\s+now\(\)", "DEFAULT current_timestamp()", rest)
    # strip inline CHECK (...) — Delta rejects inline CHECK in CREATE TABLE
    rest = _CHECK_RE.sub("", rest)
    # strip inline FK and delete/update actions
    rest = re.sub(r"\s+REFERENCES\s+[\w\.]+\s*\([^)]*\)", "", rest, flags=re.IGNORECASE)
    rest = re.sub(r"\s+REFERENCES\s+[\w\.]+", "", rest, flags=re.IGNORECASE)
    rest = re.sub(r"\s+ON\s+DELETE\s+(CASCADE|SET\s+NULL|RESTRICT|NO\s+ACTION)", "", rest, flags=re.IGNORECASE)
    rest = re.sub(r"\s+ON\s+UPDATE\s+(CASCADE|SET\s+NULL|RESTRICT|NO\s+ACTION)", "", rest, flags=re.IGNORECASE)
    # strip PRIMARY KEY / UNIQUE column qualifiers (UC doesn't enforce)
    rest = re.sub(r"\s+PRIMARY\s+KEY", "", rest, flags=re.IGNORECASE)
    rest = re.sub(r"\s+UNIQUE\b", "", rest, flags=re.IGNORECASE)
    return rest


def _convert_column_line(line: str) -> str:
    line = line.strip()
    parts = line.split(None, 1)
    if len(parts) == 2:
        name, rest = parts
        line = name + " " + _translate_type_portion(rest)
    else:
        line = _translate_type_portion(line)
    return re.sub(r"\s+", " ", line).strip().rstrip(",")


def _convert_create_table(stmt: str) -> str:
    m = re.match(r"(CREATE\s+TABLE(\s+IF\s+NOT\s+EXISTS)?)\s+([\w\.]+)\s*\((.*)\)\s*$",
                 stmt, flags=re.IGNORECASE | re.DOTALL)
    if not m:
        return ""  # not a plain create table (skip / handled elsewhere)
    head, _ifne, name, body = m.group(1), m.group(2), m.group(3), m.group(4)
    name = name.split(".")[-1]  # unqualify, we re-qualify below

    cols = []
    for raw in _split_columns(body):
        raw = raw.strip()
        if not raw:
            continue
        low = raw.lower()
        # drop table-level constraints
        if low.startswith(("primary key", "unique", "constraint", "foreign key", "check")):
            continue
        conv = _convert_column_line(raw)
        if conv.strip():
            cols.append("    " + conv.strip())

    has_default = any("default" in c.lower() for c in cols)
    out = f"{head} {{catalog}}.{{schema}}.{name} (\n" + ",\n".join(cols) + "\n)"
    if has_default:
        out += "\nTBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')"
    return out


def _split_columns(body: str) -> list:
    """Split a CREATE TABLE body on top-level commas (ignoring commas in parens)."""
    parts, depth, cur = [], 0, []
    for ch in body:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    if cur:
        parts.append("".join(cur))
    return parts


def _convert_view(stmt: str) -> str:
    # Qualify the view name; leave the SELECT body (unqualified table refs resolve
    # against the default schema set on the statement).
    stmt = re.sub(
        r"(CREATE\s+(OR\s+REPLACE\s+)?VIEW)\s+([\w\.]+)",
        lambda mm: f"{mm.group(1)} {{catalog}}.{{schema}}." + mm.group(3).split(".")[-1],
        stmt, count=1, flags=re.IGNORECASE,
    )
    # translate enum casts / types that may appear in the view SELECT
    for enum in _ENUMS:
        stmt = re.sub(r"::\s*" + enum + r"\b", "", stmt, flags=re.IGNORECASE)
    stmt = re.sub(r"::\s*text\b", "::string", stmt, flags=re.IGNORECASE)
    stmt = re.sub(r"(?i)\bnow\(\)", "current_timestamp()", stmt)
    # Postgres JSON extraction ->> 'k' / -> 'k' on a STRING(JSON) column
    stmt = re.sub(r"([\w\.]+)\s*->>\s*'([^']+)'", r"get_json_object(\1, '$.\2')", stmt)
    stmt = re.sub(r"([\w\.]+)\s*->\s*'([^']+)'", r"get_json_object(\1, '$.\2')", stmt)
    stmt = fix_time_arith(stmt)
    return stmt


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


_UNIT = {"hour": "HOUR", "hours": "HOUR", "day": "DAY", "days": "DAY",
         "minute": "MINUTE", "minutes": "MINUTE", "second": "SECOND", "seconds": "SECOND"}


def fix_time_arith(sql: str) -> str:
    """Rewrite Postgres time math into Databricks:
       EXTRACT(EPOCH FROM (A - B))        -> timestampdiff(SECOND, B, A)
       A - B  <op>  INTERVAL 'N unit'     -> timestampdiff(UNIT, B, A) <op> N
    """
    # EXTRACT(EPOCH FROM ( ... ))  — balanced-paren aware
    out = sql
    while True:
        m = re.search(r"EXTRACT\s*\(\s*EPOCH\s+FROM\s+", out, flags=re.IGNORECASE)
        if not m:
            break
        # opening paren of EXTRACT(
        open_idx = out.index("(", m.start())
        close_idx = _match_paren(out, open_idx)
        inner = out[out.index("FROM", m.start()) + 4:close_idx].strip()
        if inner.startswith("(") and _match_paren(inner, 0) == len(inner) - 1:
            inner = inner[1:-1].strip()
        a, b = _split_top_minus(inner)
        repl = f"timestampdiff(SECOND, {b}, {a})" if b else f"unix_timestamp({a})"
        out = out[:m.start()] + repl + out[close_idx + 1:]

    # A - B <op> INTERVAL 'N unit'
    def _interval(mm):
        a, b, op, n, unit = mm.group(1), mm.group(2), mm.group(3), mm.group(4), mm.group(5).lower()
        return f"timestampdiff({_UNIT.get(unit, 'SECOND')}, {b.strip()}, {a.strip()}) {op} {n}"

    out = re.sub(
        r"(current_timestamp\(\)|[\w\.]+)\s*-\s*(current_timestamp\(\)|COALESCE\([^()]*\)|[\w\.]+)"
        r"\s*(<=|>=|<|>|=)\s*INTERVAL\s*'(\d+)\s*(\w+)'",
        _interval, out, flags=re.IGNORECASE,
    )
    return out


def _strip_comments(sql: str) -> str:
    # Remove full-line and trailing `-- ...` comments (no `--` appears inside these DDL literals).
    return re.sub(r"--[^\n]*", "", sql)


def convert(sql: str) -> str:
    sql = _strip_comments(sql)
    sql = _strip_enum_defs(sql)
    sql = _strip_indexes(sql)
    # drop ALTER TABLE ... ADD COLUMN handled separately below; convert simple ones
    out = []
    for stmt in _split_top_level(sql):
        low = stmt.lower().lstrip()
        if low.startswith("create table"):
            conv = _convert_create_table(stmt)
            if conv:
                out.append(conv + ";")
        elif low.startswith("create ") and "view" in low.split("(")[0]:
            out.append(_convert_view(stmt) + ";")
        elif low.startswith("alter table") and "add column" in low:
            # ALTER TABLE t ADD COLUMN IF NOT EXISTS c TYPE ... -> keep, convert type
            conv = _convert_column_line(stmt)
            conv = re.sub(r"([\w]+)$", r"\1", conv)
            # qualify table name
            conv = re.sub(r"(ALTER\s+TABLE(\s+IF\s+EXISTS)?)\s+([\w\.]+)",
                          lambda mm: f"{mm.group(1)} {{catalog}}.{{schema}}." + mm.group(3).split(".")[-1],
                          conv, count=1, flags=re.IGNORECASE)
            out.append(conv + ";")
        # else: comments / grants / enum leftovers -> dropped
    return "\n\n".join(out) + "\n"


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(__doc__)
        sys.exit(1)
    with open(sys.argv[1]) as f:
        print(convert(f.read()))
