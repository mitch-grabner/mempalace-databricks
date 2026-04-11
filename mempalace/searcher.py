#!/usr/bin/env python3
"""
searcher.py — Find anything. Exact words. (Databricks-native)

Semantic search against the palace via Mosaic AI Vector Search.
Returns verbatim text — the actual words, never summaries.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from databricks.vector_search.client import VectorSearchClient

from .config import DatabricksConfig

logger = logging.getLogger("mempalace_mcp")

# ── Columns returned from every VS query ──────────────────────────────────────

_VS_COLUMNS = ["id", "text", "wing", "room", "source_file", "date"]


class SearchError(Exception):
    """Raised when search cannot proceed (e.g. no index found)."""


# ── Filter helpers ────────────────────────────────────────────────────────────


def _build_vs_filters(
    wing: Optional[str] = None,
    room: Optional[str] = None,
) -> Optional[Dict[str, str]]:
    """Build a Vector Search filter dict from wing/room parameters.

    Args:
        wing: Optional wing (person / project) filter.
        room: Optional room (topic) filter.

    Returns:
        A filter dict for ``index.similarity_search(filters=...)``,
        or None if no filters are specified.
    """
    filters: Dict[str, str] = {}
    if wing:
        filters["wing"] = wing
    if room:
        filters["room"] = room
    return filters or None


# ── Internal query helper ─────────────────────────────────────────────────────


def _query_vs_index(
    query: str,
    config: DatabricksConfig,
    wing: Optional[str] = None,
    room: Optional[str] = None,
    n_results: int = 5,
    query_type: str = "hybrid",
) -> List[Dict[str, Any]]:
    """Execute a similarity search against the Vector Search index.

    Args:
        query: Natural-language search string.
        config: Databricks configuration.
        wing: Optional wing filter.
        room: Optional room filter.
        n_results: Maximum number of results to return.
        query_type: ``"hybrid"`` (keyword + semantic) or ``"ann"``.

    Returns:
        List of hit dicts with keys: ``text``, ``wing``, ``room``,
        ``source_file``, ``date``, ``similarity``.

    Raises:
        SearchError: If the index cannot be reached or the query fails.
    """
    try:
        vsc = VectorSearchClient()
        index = vsc.get_index(
            endpoint_name=config.vs_endpoint,
            index_name=config.vs_index_name,
        )
    except Exception as e:
        raise SearchError(f"Cannot reach VS index {config.vs_index_name}: {e}") from e

    filters = _build_vs_filters(wing, room)

    try:
        results = index.similarity_search(
            query_text=query,
            columns=_VS_COLUMNS,
            num_results=n_results,
            filters=filters,
            query_type=query_type,
        )
    except Exception as e:
        raise SearchError(f"Vector Search query failed: {e}") from e

    column_names = results.get("result", {}).get("column_names", [])
    data_array = results.get("result", {}).get("data_array", [])

    hits: List[Dict[str, Any]] = []
    for row in data_array:
        row_dict = dict(zip(column_names, row))
        hits.append({
            "text": row_dict.get("text", ""),
            "wing": row_dict.get("wing", "unknown"),
            "room": row_dict.get("room", "unknown"),
            "source_file": Path(row_dict.get("source_file") or "?").name,
            "date": row_dict.get("date"),
            "similarity": round(float(row_dict.get("score", 0.0)), 3),
        })

    return hits


# ── Public API: CLI search (prints to stdout) ────────────────────────────────


def search(
    query: str,
    config: Optional[DatabricksConfig] = None,
    wing: Optional[str] = None,
    room: Optional[str] = None,
    n_results: int = 5,
) -> None:
    """Search the palace and print verbatim drawer content.

    Args:
        query: Natural-language search string.
        config: Databricks configuration.  Uses defaults if not provided.
        wing: Optional wing (person / project) filter.
        room: Optional room (topic) filter.
        n_results: Maximum number of results to return.
    """
    config = config or DatabricksConfig()

    try:
        hits = _query_vs_index(query, config, wing, room, n_results)
    except SearchError as e:
        print(f"\n  Search error: {e}")
        raise

    if not hits:
        print(f'\n  No results found for: "{query}"')
        return

    print(f"\n{'=' * 60}")
    print(f'  Results for: "{query}"')
    if wing:
        print(f"  Wing: {wing}")
    if room:
        print(f"  Room: {room}")
    print(f"{'=' * 60}\n")

    for i, hit in enumerate(hits, 1):
        print(f"  [{i}] {hit['wing']} / {hit['room']}")
        print(f"      Source: {hit['source_file']}")
        print(f"      Match:  {hit['similarity']}")
        print()
        for line in hit["text"].strip().split("\n"):
            print(f"      {line}")
        print()
        print(f"  {'─' * 56}")

    print()


# ── Public API: Programmatic search (returns dict) ───────────────────────────


def search_memories(
    query: str,
    config: Optional[DatabricksConfig] = None,
    wing: Optional[str] = None,
    room: Optional[str] = None,
    n_results: int = 5,
) -> Dict[str, Any]:
    """Programmatic search — returns a dict instead of printing.

    Used by the MCP server and other callers that need structured data.

    Args:
        query: Natural-language search string.
        config: Databricks configuration.  Uses defaults if not provided.
        wing: Optional wing filter.
        room: Optional room filter.
        n_results: Maximum number of results to return.

    Returns:
        Dict with keys ``query``, ``filters``, ``results`` (list of hit dicts).
        On error, returns dict with ``error`` and optionally ``hint`` keys.
    """
    config = config or DatabricksConfig()

    try:
        hits = _query_vs_index(query, config, wing, room, n_results)
    except SearchError as e:
        logger.error("Search failed: %s", e)
        return {
            "error": str(e),
            "hint": "Ensure the VS index exists. Run the setup notebook.",
        }

    return {
        "query": query,
        "filters": {"wing": wing, "room": room},
        "results": hits,
    }
