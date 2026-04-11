"""
palace.py — Shared palace operations (Databricks-native).

Consolidates Delta table access patterns used by both miners and the MCP server.
Replaces the original ChromaDB-backed implementation.
"""

from __future__ import annotations

import hashlib
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, Row, SparkSession

from .config import DatabricksConfig

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


# ── Spark helpers ─────────────────────────────────────────────────────────────


def _get_spark() -> SparkSession:
    """Return the active SparkSession.

    Raises:
        RuntimeError: If no active SparkSession is found (i.e. running
            outside a Databricks notebook / job context).
    """
    session = SparkSession.getActiveSession()
    if session is None:
        raise RuntimeError(
            "No active SparkSession. MemPalace-Databricks requires a "
            "notebook or job execution context."
        )
    return session


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


def get_drawers_df(config: Optional[DatabricksConfig] = None) -> DataFrame:
    """Return the drawers Delta table as a Spark DataFrame.

    Args:
        config: Databricks configuration.  Uses defaults if not provided.

    Returns:
        A Spark DataFrame pointing at the ``mempalace_drawers`` table.

    Raises:
        RuntimeError: If no active SparkSession exists.
        AnalysisException: If the table does not exist (run setup notebook first).
    """
    config = config or DatabricksConfig()
    spark = _get_spark()
    return spark.table(config.drawers_table)


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
    spark = _get_spark()
    try:
        # Parameterised via f-string with escaped single quotes — source_file
        # comes from os.walk, not user input, but we still escape defensively.
        safe_source = source_file.replace("'", "\\'")
        rows = spark.sql(
            f"SELECT source_mtime FROM {config.drawers_table} "
            f"WHERE source_file = '{safe_source}' LIMIT 1"
        ).collect()

        if not rows:
            return False

        if check_mtime:
            stored_mtime = rows[0]["source_mtime"]
            if stored_mtime is None:
                return False
            current_mtime = os.path.getmtime(source_file)
            return float(stored_mtime) == current_mtime

        return True
    except Exception:  # table may not exist yet during first run
        return False


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

    spark = _get_spark()
    now = datetime.utcnow().isoformat()

    rows = []
    for d in drawers:
        rows.append(Row(
            id=d["id"],
            text=d["text"],
            wing=d["wing"],
            room=d["room"],
            hall=d.get("hall"),
            source_file=d.get("source_file"),
            source_mtime=d.get("source_mtime"),
            chunk_index=d.get("chunk_index"),
            date=d.get("date"),
            importance=float(d.get("importance", 3.0)),
            agent=d.get("agent", "mempalace"),
            filed_at=now,
        ))

    df = spark.createDataFrame(rows)
    df.createOrReplaceTempView("_mempalace_new_drawers")

    spark.sql(f"""
        MERGE INTO {config.drawers_table} AS target
        USING _mempalace_new_drawers AS source
        ON target.id = source.id
        WHEN NOT MATCHED THEN INSERT *
    """)

    return len(rows)


def delete_drawer(config: DatabricksConfig, drawer_id: str) -> bool:
    """Delete a single drawer by ID.

    Args:
        config: Databricks configuration.
        drawer_id: The deterministic hash ID of the drawer.

    Returns:
        True if a row was deleted, False if the ID was not found.
    """
    spark = _get_spark()
    safe_id = drawer_id.replace("'", "\\'")
    result = spark.sql(
        f"DELETE FROM {config.drawers_table} WHERE id = '{safe_id}'"
    )
    # Delta DELETE returns a DataFrame with 'num_affected_rows'
    affected = result.collect()
    if affected and hasattr(affected[0], "num_affected_rows"):
        return affected[0]["num_affected_rows"] > 0
    return True  # assume success if we can't determine count
