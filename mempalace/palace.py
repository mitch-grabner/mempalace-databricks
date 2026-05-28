"""palace.py — Shared palace operations (Databricks-native)."""

from __future__ import annotations

import hashlib
from typing import Any, Dict, List, Optional

from .config import DatabricksConfig
from .databricks_backend import DatabricksBackend

# ── Directory skip-list (backend-agnostic) ────────────────────────────────────
# Used by miners when walking a project tree.  Unchanged from upstream.

SKIP_DIRS = {
    ".git",
    "node_modules",
    "__pycache__",
    ".venv",
    "venv",
    "env",
    "dist",
    "build",
    ".next",
    "coverage",
    ".mempalace",
    ".ruff_cache",
    ".mypy_cache",
    ".pytest_cache",
    ".cache",
    ".tox",
    ".nox",
    ".idea",
    ".vscode",
    ".ipynb_checkpoints",
    ".eggs",
    "htmlcov",
    "target",
}


# ── Backend helpers ───────────────────────────────────────────────────────────


def get_backend(config: Optional[DatabricksConfig] = None) -> DatabricksBackend:
    """Return a new Databricks backend for the provided config."""
    return DatabricksBackend(config or DatabricksConfig())


def _get_spark():
    """Compatibility stub for old imports.

    The default backend no longer uses Spark. Code paths that still call this
    need to be rewired to ``DatabricksBackend``.
    """
    raise RuntimeError("Spark is not used by local MemPalace Databricks storage.")


# ── Drawer ID generation ─────────────────────────────────────────────────────


def make_drawer_id(
    wing: str,
    room: str,
    source_file: str,
    chunk_index: int,
) -> str:
    """Create a deterministic drawer ID from its composite key.

    Args:
        wing: Wing (person / project) name.
        room: Room (topic) slug.
        source_file: Original file path that was mined.
        chunk_index: Position of the chunk within the source file.

    Returns:
        A hex-encoded SHA-256 hash string.
    """
    key = f"{wing}|{room}|{source_file}|{chunk_index}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


# ── Palace table operations ───────────────────────────────────────────────────


def get_drawers_df(config: Optional[DatabricksConfig] = None) -> List[Dict[str, Any]]:
    """Return recent drawer rows as dictionaries."""
    config = config or DatabricksConfig()
    return get_backend(config).sql(f"SELECT * FROM {config.drawers_table} LIMIT 1000")


def file_already_mined(
    config: DatabricksConfig,
    source_file: str,
    check_mtime: bool = False,
) -> bool:
    """Check whether a file has already been ingested into the palace.

    When ``check_mtime=True`` (used by the project miner), returns ``False``
    if the file has been modified on disk since it was last mined, triggering
    a re-mine.  When ``check_mtime=False`` (used by the convo miner), just
    checks for existence of any row with the same ``source_file``.

    Args:
        config: Databricks configuration.
        source_file: Absolute path of the file to check.
        check_mtime: If True, also compare stored mtime against current.

    Returns:
        True if the file is already present (and up-to-date when
        ``check_mtime`` is set), False otherwise.
    """
    return get_backend(config).file_already_mined(source_file, check_mtime=check_mtime)


def add_drawers(
    config: DatabricksConfig,
    drawers: List[Dict[str, Any]],
) -> int:
    """Insert a batch of drawers into the palace Delta table.

    Uses ``MERGE INTO`` keyed on ``id`` so re-running a mine on the same
    source is idempotent (upsert semantics).

    Args:
        config: Databricks configuration.
        drawers: List of drawer dicts, each containing at minimum the keys
            ``id``, ``text``, ``wing``, ``room``.  Additional metadata
            (``hall``, ``source_file``, ``source_mtime``, ``chunk_index``,
            ``date``, ``importance``, ``agent``) is optional and filled
            with defaults.

    Returns:
        Number of drawers written.
    """
    if not drawers:
        return 0

    return get_backend(config).add_drawers(drawers)


def delete_drawer(config: DatabricksConfig, drawer_id: str) -> bool:
    """Delete a single drawer by ID.

    Args:
        config: Databricks configuration.
        drawer_id: The deterministic hash ID of the drawer.

    Returns:
        True if a row was deleted, False if the ID was not found.
    """
    get_backend(config).delete_drawer(drawer_id)
    return True
