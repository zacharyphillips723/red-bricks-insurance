"""Shared pytest fixtures and import-time environment setup.

The backend package imports `backend.env_config`, which constructs a
`WorkspaceClient()` at module load and, when SQL_WAREHOUSE_ID / UC_CATALOG /
GENIE_SPACE_ID are unset or "auto", calls the workspace to auto-detect them.
Setting those three to concrete values here means importing the backend for a
unit test never touches the network. (SDK client construction itself is lazy —
it only authenticates on the first API call.)

This runs before test collection because pytest imports conftest first.
"""

import os

# Freeze config so `backend.env_config` skips all `_auto_detect_*` network calls.
os.environ.setdefault("UC_CATALOG", "test_catalog")
os.environ.setdefault("UC_SCHEMA", "prior_auth")
os.environ.setdefault("SQL_WAREHOUSE_ID", "test_warehouse")
os.environ.setdefault("GENIE_SPACE_ID", "none")
# Harmless placeholders so nothing tries a real auth handshake if it ever gets that far.
os.environ.setdefault("DATABRICKS_HOST", "https://example.invalid")
os.environ.setdefault("DATABRICKS_TOKEN", "not-a-real-token")

import pytest


# The policy "database" the fake _execute_sql serves. Mirrors the shape of
# silver_medical_policy_rules rows the real _match_policy / _known_procedure_codes
# queries return: policy_id, policy_name, service_category, procedure_codes,
# diagnosis_codes. Codes are pipe-delimited strings, exactly as stored in UC.
DEFAULT_POLICY_RULES = [
    {
        "policy_id": "MP-KNEE-001",
        "policy_name": "Total Knee Arthroplasty",
        "service_category": "orthopedic_surgery",
        # Note the deliberate near-collision: 27447 is covered, 274470 is not.
        "procedure_codes": "27447|27446",
        "diagnosis_codes": "M17.11|M17.12|M17.0",
    },
    {
        "policy_id": "MP-CGM-002",
        "policy_name": "Continuous Glucose Monitoring",
        "service_category": "dme",
        "procedure_codes": "95249|E0784",
        "diagnosis_codes": "E11.65|E10.65",
    },
    {
        "policy_id": "MP-MRI-003",
        "policy_name": "Advanced Imaging — Lumbar MRI",
        "service_category": "imaging",
        "procedure_codes": "72148",
        # No diagnosis restriction on this policy (covers-any indication case).
        "diagnosis_codes": None,
    },
]


class FakeSQL:
    """A stand-in for backend.agent._execute_sql.

    Returns canned policy-rule rows regardless of the SQL text, which is all the
    Tier-1 adjudication helpers need (`_match_policy` and `_known_procedure_codes`
    only ever SELECT from silver_medical_policy_rules). Records every call so
    tests can assert on query behavior if needed.
    """

    def __init__(self, rows):
        self._rows = rows
        self.calls: list[str] = []

    def __call__(self, sql, params=None, poll_timeout_s=120):
        self.calls.append(sql)
        return list(self._rows)


@pytest.fixture
def policy_rules():
    """The default synthetic policy rule set (a fresh copy per test)."""
    return [dict(r) for r in DEFAULT_POLICY_RULES]


@pytest.fixture
def patch_sql(monkeypatch, policy_rules):
    """Patch `_execute_sql` in the documents module to serve fake policy rows.

    documents.py imports `_execute_sql` by name (`from .agent import _execute_sql`),
    so we patch the reference on the documents module, not on agent.
    Yields the FakeSQL instance so tests can inspect/override it.
    """
    from backend import documents

    fake = FakeSQL(policy_rules)
    monkeypatch.setattr(documents, "_execute_sql", fake)
    return fake
