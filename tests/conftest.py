"""
conftest.py — Shared fixtures for MemPalace tests (Databricks-native).

Provides:
- pyspark / databricks SDK mocks so modules that import them at top-level
  don't crash in a bare pytest environment.
- DatabricksConfig fixture pointed at a test catalog/schema.
- Mock ``_sql`` / ``_vs_search`` helpers for MCP server unit tests.
- Seeded row data matching the old ChromaDB fixtures.
"""

import os
import shutil
import sys
import tempfile
from unittest.mock import MagicMock

import pytest

# ── Mock heavy optional dependencies before any mempalace imports ─────────────
# pyspark is only available inside a Databricks runtime.  Modules like
# palace.py and knowledge_graph.py do ``from pyspark.sql import ...`` at the
# top level.  Injecting a MagicMock into sys.modules lets pytest import
# those modules without Spark installed.

_MOCK_MODULES = [
    "pyspark",
    "pyspark.sql",
    "databricks",
    "databricks.sdk",
    "databricks.sdk.service",
    "databricks.sdk.service.sql",
    "databricks.sdk.service.vectorsearch",
    "databricks.vector_search",
    "databricks.vector_search.client",
]

for _mod in _MOCK_MODULES:
    if _mod not in sys.modules:
        sys.modules[_mod] = MagicMock()

# Now safe to import mempalace modules.
from mempalace.config import DatabricksConfig  # noqa: E402


# ── Session-level temp HOME isolation ─────────────────────────────────────────

_original_env = {}
_session_tmp = tempfile.mkdtemp(prefix="mempalace_session_")

for _var in ("HOME", "USERPROFILE"):
    _original_env[_var] = os.environ.get(_var)

os.environ["HOME"] = _session_tmp
os.environ["USERPROFILE"] = _session_tmp


@pytest.fixture(scope="session", autouse=True)
def _isolate_home():
    """Ensure HOME points to a temp dir for the entire test session."""
    yield
    for var, orig in _original_env.items():
        if orig is None:
            os.environ.pop(var, None)
        else:
            os.environ[var] = orig
    shutil.rmtree(_session_tmp, ignore_errors=True)


# ── Basic fixtures ────────────────────────────────────────────────────────────


@pytest.fixture
def tmp_dir():
    """Create and auto-cleanup a temporary directory."""
    d = tempfile.mkdtemp(prefix="mempalace_test_")
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def db_config():
    """A DatabricksConfig pointed at a safe test namespace."""
    return DatabricksConfig(catalog="test_catalog", schema="test_schema")


# ── Seeded data (mirrors the old ChromaDB fixtures) ───────────────────────────

SEED_DRAWERS = [
    {
        "id": "drawer_proj_backend_aaa",
        "text": (
            "The authentication module uses JWT tokens for session management. "
            "Tokens expire after 24 hours. Refresh tokens are stored in HttpOnly cookies."
        ),
        "wing": "project",
        "room": "backend",
        "source_file": "auth.py",
        "chunk_index": "0",
        "importance": "3.0",
        "agent": "miner",
        "filed_at": "2026-01-01T00:00:00",
        "date": None,
        "hall": None,
        "source_mtime": None,
    },
    {
        "id": "drawer_proj_backend_bbb",
        "text": (
            "Database migrations are handled by Alembic. We use PostgreSQL 15 "
            "with connection pooling via pgbouncer."
        ),
        "wing": "project",
        "room": "backend",
        "source_file": "db.py",
        "chunk_index": "0",
        "importance": "3.0",
        "agent": "miner",
        "filed_at": "2026-01-02T00:00:00",
        "date": None,
        "hall": None,
        "source_mtime": None,
    },
    {
        "id": "drawer_proj_frontend_ccc",
        "text": (
            "The React frontend uses TanStack Query for server state management. "
            "All API calls go through a centralized fetch wrapper."
        ),
        "wing": "project",
        "room": "frontend",
        "source_file": "App.tsx",
        "chunk_index": "0",
        "importance": "3.0",
        "agent": "miner",
        "filed_at": "2026-01-03T00:00:00",
        "date": None,
        "hall": None,
        "source_mtime": None,
    },
    {
        "id": "drawer_notes_planning_ddd",
        "text": (
            "Sprint planning: migrate auth to passkeys by Q3. "
            "Evaluate ChromaDB alternatives for vector search."
        ),
        "wing": "notes",
        "room": "planning",
        "source_file": "sprint.md",
        "chunk_index": "0",
        "importance": "3.0",
        "agent": "miner",
        "filed_at": "2026-01-04T00:00:00",
        "date": None,
        "hall": None,
        "source_mtime": None,
    },
]


# ── MCP server mock helpers ───────────────────────────────────────────────────


def _make_sql_responder(rows_by_pattern: dict):
    """Build a mock ``_sql`` that matches SQL fragments to canned responses.

    Args:
        rows_by_pattern: Dict mapping a substring (e.g. ``"GROUP BY wing"``
            or ``"mempalace_entities"``) to a list of row-dicts to return.
            Patterns are checked in insertion order; first match wins.
            A ``"*"`` key serves as the default fallback.
    """
    def _mock_sql(query: str):
        for pattern, rows in rows_by_pattern.items():
            if pattern != "*" and pattern in query:
                return rows
        return rows_by_pattern.get("*", [])
    return _mock_sql


def _make_vs_responder(hits: list):
    """Build a mock ``_vs_search`` that returns *hits* for every query."""
    def _mock_vs(query_text, columns, num_results=5, filters=None):
        return hits[:num_results]
    return _mock_vs
