"""
databricks_backend.py — shared Databricks storage client layer.

This module is the only default storage backend for the Databricks branch.
It uses Databricks SDK auto-auth/profile auth from a local Python process or
Databricks App identity when deployed as an app.
"""

from __future__ import annotations

import json
import logging
import sys
import threading
import time
from contextlib import redirect_stdout
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence

from .config import DatabricksConfig

logger = logging.getLogger("mempalace_mcp")

VS_TOKEN_TTL_SECONDS = 45 * 60
VS_SYNC_DEBOUNCE_SECONDS = 60.0
INSERT_BATCH_SIZE = 100


def sql_escape(value: Any) -> str:
    """Escape a Python value for SQL string literal interpolation."""
    if value is None:
        return ""
    return str(value).replace("'", "\\'")


def sql_literal(value: Any) -> str:
    """Convert a Python value into a Databricks SQL literal."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return f"'{sql_escape(value)}'"


def chunked(items: Sequence[Dict[str, Any]], size: int) -> Iterable[Sequence[Dict[str, Any]]]:
    """Yield fixed-size slices from a sequence."""
    for i in range(0, len(items), size):
        yield items[i : i + size]


class DatabricksBackend:
    """Databricks SQL + Vector Search backend for MemPalace."""

    def __init__(self, config: Optional[DatabricksConfig] = None) -> None:
        self.config = config or DatabricksConfig()
        self._ws_client = None
        self._vs_client = None
        self._vs_client_created_at = 0.0
        self._vs_sync_lock = threading.Lock()
        self._vs_last_sync = 0.0

    # ── SDK clients ──────────────────────────────────────────────────────

    def workspace_client(self):
        """Return a cached WorkspaceClient using profile/env/app auth."""
        if self._ws_client is None:
            from databricks.sdk import WorkspaceClient

            with redirect_stdout(sys.stderr):
                if self.config.databricks_profile:
                    self._ws_client = WorkspaceClient(profile=self.config.databricks_profile)
                else:
                    self._ws_client = WorkspaceClient()
        return self._ws_client

    def _build_vector_search_client(self):
        """Create a fresh VectorSearchClient with a current bearer token."""
        from databricks.vector_search.client import VectorSearchClient

        with redirect_stdout(sys.stderr):
            ws = self.workspace_client()
            header_factory = ws.config.authenticate()
            headers = header_factory() if callable(header_factory) else header_factory
            token = (headers.get("Authorization") or "").removeprefix("Bearer ")
            return VectorSearchClient(
                workspace_url=ws.config.host,
                personal_access_token=token,
                disable_notice=True,
            )

    def invalidate_vector_search_client(self) -> None:
        """Force the next Vector Search call to refresh auth."""
        self._vs_client = None
        self._vs_client_created_at = 0.0
        logger.info("VS client invalidated — will refresh on next call.")

    def vector_search_client(self):
        """Return a cached Vector Search client with token refresh."""
        now = time.monotonic()
        if (
            self._vs_client is None
            or (now - self._vs_client_created_at) > VS_TOKEN_TTL_SECONDS
        ):
            self._vs_client = self._build_vector_search_client()
            self._vs_client_created_at = now
            logger.info("VS client (re)created with fresh token.")
        return self._vs_client

    # ── SQL ──────────────────────────────────────────────────────────────

    def warehouse_id(self) -> str:
        """Return configured warehouse ID or raise a setup error."""
        if not self.config.warehouse_id:
            raise RuntimeError(
                "MEMPALACE_WAREHOUSE_ID env var is required for local SQL execution. "
                "Set it to a SQL warehouse ID, or configure app.yaml valueFrom for Databricks Apps."
            )
        return self.config.warehouse_id

    def sql(self, query: str) -> List[Dict[str, Optional[str]]]:
        """Execute a Databricks SQL statement and return row dictionaries."""
        from databricks.sdk.service.sql import StatementState

        t0 = time.perf_counter()
        with redirect_stdout(sys.stderr):
            resp = self.workspace_client().statement_execution.execute_statement(
                warehouse_id=self.warehouse_id(),
                statement=query,
                wait_timeout="30s",
            )
        elapsed = (time.perf_counter() - t0) * 1000
        logger.info("SQL %.0fms: %s", elapsed, query[:80].replace(chr(10), " "))

        if resp.status and resp.status.state == StatementState.FAILED:
            msg = resp.status.error.message if resp.status.error else "Unknown SQL error"
            raise RuntimeError(f"SQL failed: {msg}")

        if not resp.manifest or not resp.result or not resp.result.data_array:
            return []

        columns = [c.name for c in resp.manifest.schema.columns]
        return [dict(zip(columns, row)) for row in resp.result.data_array]

    # ── Vector Search ────────────────────────────────────────────────────

    def vector_search(
        self,
        query_text: str,
        columns: List[str],
        num_results: int = 5,
        filters: Optional[Dict[str, str]] = None,
        query_type: str = "hybrid",
    ) -> List[Dict[str, Any]]:
        """Run Vector Search and return row dictionaries with score when present."""
        t0 = time.perf_counter()
        for attempt in range(2):
            try:
                with redirect_stdout(sys.stderr):
                    index = self.vector_search_client().get_index(
                        endpoint_name=self.config.vs_endpoint,
                        index_name=self.config.vs_index_name,
                    )
                    resp = index.similarity_search(
                        query_text=query_text,
                        columns=columns,
                        num_results=num_results,
                        filters=filters,
                        query_type=query_type,
                    )
                col_names = [
                    c["name"] for c in resp.get("manifest", {}).get("columns", [])
                ]
                if not col_names:
                    col_names = resp.get("result", {}).get("column_names", [])
                rows = [
                    dict(zip(col_names, row))
                    for row in resp.get("result", {}).get("data_array", [])
                ]
                elapsed = (time.perf_counter() - t0) * 1000
                logger.info(
                    "VS search %.0fms: %d hits for '%s'",
                    elapsed,
                    len(rows),
                    query_text[:50],
                )
                return rows
            except Exception as exc:
                err_str = str(exc).lower()
                if attempt == 0 and (
                    "token" in err_str or "401" in err_str or "auth" in err_str
                ):
                    logger.warning("VS search token error — refreshing client: %s", exc)
                    self.invalidate_vector_search_client()
                    continue
                raise
        return []

    def trigger_vector_sync(self) -> None:
        """Trigger Delta Sync index refresh in a daemon thread."""
        with self._vs_sync_lock:
            now = time.monotonic()
            if (now - self._vs_last_sync) < VS_SYNC_DEBOUNCE_SECONDS:
                logger.debug("VS sync debounced (last sync %.0fs ago).", now - self._vs_last_sync)
                return
            self._vs_last_sync = now

        def _sync() -> None:
            try:
                with redirect_stdout(sys.stderr):
                    self.workspace_client().vector_search_indexes.sync_index(
                        index_name=self.config.vs_index_name,
                    )
                logger.info("VS index sync triggered for %s.", self.config.vs_index_name)
            except Exception as exc:
                logger.warning("VS index sync failed (non-fatal): %s", exc)

        threading.Thread(target=_sync, daemon=True).start()

    # ── Shared table operations ──────────────────────────────────────────

    def wal_log(self, operation: str, params: dict, result: Optional[dict] = None) -> None:
        """Append an audit entry to the WAL table in a daemon thread."""
        def _write() -> None:
            now = datetime.now(timezone.utc).isoformat()
            try:
                self.sql(f"""
                    INSERT INTO {self.config.wal_table}
                    (timestamp, operation, params, result, caller)
                    VALUES ('{now}', '{sql_escape(operation)}',
                            '{sql_escape(json.dumps(params, default=str))}',
                            '{sql_escape(json.dumps(result, default=str) if result else "")}',
                            current_user())
                """)
            except Exception as exc:
                logger.error("WAL write failed: %s", exc)

        threading.Thread(target=_write, daemon=True).start()

    def file_already_mined(self, source_file: str, check_mtime: bool = False) -> bool:
        """Return whether a source file has already been filed."""
        try:
            rows = self.sql(
                f"SELECT source_mtime FROM {self.config.drawers_table} "
                f"WHERE source_file = '{sql_escape(source_file)}' LIMIT 1"
            )
            if not rows:
                return False
            if check_mtime:
                import os

                stored_mtime = rows[0].get("source_mtime")
                if stored_mtime is None:
                    return False
                return float(stored_mtime) == os.path.getmtime(source_file)
            return True
        except Exception:
            return False

    def add_drawers(self, drawers: List[Dict[str, Any]]) -> int:
        """Insert drawers into the Delta table with idempotent MERGE semantics."""
        if not drawers:
            return 0

        now = datetime.now(timezone.utc).isoformat()
        columns = [
            "id",
            "text",
            "wing",
            "room",
            "hall",
            "source_file",
            "source_mtime",
            "chunk_index",
            "date",
            "importance",
            "agent",
            "filed_at",
        ]

        for batch in chunked(drawers, INSERT_BATCH_SIZE):
            values = []
            for d in batch:
                row = {
                    "id": d["id"],
                    "text": d["text"],
                    "wing": d["wing"],
                    "room": d["room"],
                    "hall": d.get("hall"),
                    "source_file": d.get("source_file"),
                    "source_mtime": d.get("source_mtime"),
                    "chunk_index": d.get("chunk_index"),
                    "date": d.get("date"),
                    "importance": float(d.get("importance", 3.0)),
                    "agent": d.get("agent", "mempalace"),
                    "filed_at": now,
                }
                values.append("(" + ", ".join(sql_literal(row[c]) for c in columns) + ")")

            source_cols = ", ".join(columns)
            insert_cols = ", ".join(columns)
            insert_values = ", ".join(f"source.{c}" for c in columns)
            self.sql(f"""
                MERGE INTO {self.config.drawers_table} AS target
                USING (
                    SELECT * FROM VALUES
                    {", ".join(values)}
                    AS rows({source_cols})
                ) AS source
                ON target.id = source.id
                WHEN NOT MATCHED THEN INSERT ({insert_cols})
                VALUES ({insert_values})
            """)

        self.trigger_vector_sync()
        return len(drawers)

    def delete_drawers_for_source(self, source_file: str) -> None:
        """Delete stale drawers for a source file."""
        self.sql(
            f"DELETE FROM {self.config.drawers_table} "
            f"WHERE source_file = '{sql_escape(source_file)}'"
        )

    def delete_drawer(self, drawer_id: str) -> None:
        """Delete one drawer by ID."""
        self.sql(
            f"DELETE FROM {self.config.drawers_table} WHERE id = '{sql_escape(drawer_id)}'"
        )
