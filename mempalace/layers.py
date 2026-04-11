#!/usr/bin/env python3
"""
layers.py — 4-Layer Memory Stack for MemPalace (Databricks-native)
===================================================================

Load only what you need, when you need it.

    Layer 0: Identity       (~100 tokens)   — Always loaded. "Who am I?"
    Layer 1: Essential Story (~500-800)      — Always loaded. Top moments from the palace.
    Layer 2: On-Demand      (~200-500 each)  — Loaded when a topic/wing comes up.
    Layer 3: Deep Search    (unlimited)      — Full Vector Search semantic search.

Wake-up cost: ~600-900 tokens (L0+L1). Leaves 95%+ of context free.

Reads from:
  - Config Volume identity.txt  (L0)
  - Delta table mempalace_drawers  (L1, L2)
  - Vector Search index  (L3)
"""

from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

from .config import DatabricksConfig
from .palace import _get_spark
from .searcher import _query_vs_index, SearchError


# ── Layer 0 — Identity ───────────────────────────────────────────────────────


class Layer0:
    """~100 tokens. Always loaded.

    Reads from the config Volume ``identity.txt``.

    Example identity.txt::

        I am Atlas, a personal AI assistant for Alice.
        Traits: warm, direct, remembers everything.
        People: Alice (creator), Bob (Alice's partner).
        Project: A journaling app that helps people process emotions.
    """

    def __init__(self, config: Optional[DatabricksConfig] = None) -> None:
        self._config = config or DatabricksConfig()
        self._text: Optional[str] = None

    def render(self) -> str:
        """Return the identity text, or a sensible default."""
        if self._text is not None:
            return self._text
        self._text = self._config.load_identity()
        return self._text

    def token_estimate(self) -> int:
        """Rough token count (~4 chars per token)."""
        return len(self.render()) // 4


# ── Layer 1 — Essential Story (auto-generated from palace) ───────────────────


class Layer1:
    """~500-800 tokens. Always loaded.

    Auto-generated from the highest-importance drawers in the Delta table.
    Groups by room, picks the top N moments, compresses to compact text.
    """

    MAX_DRAWERS = 15   # at most 15 moments in wake-up
    MAX_CHARS = 3200   # hard cap on total L1 text (~800 tokens)

    def __init__(
        self,
        config: Optional[DatabricksConfig] = None,
        wing: Optional[str] = None,
    ) -> None:
        self._config = config or DatabricksConfig()
        self.wing = wing

    def generate(self) -> str:
        """Pull top drawers from Delta and format as compact L1 text.

        Returns:
            A multi-line string suitable for injection into a system prompt.
        """
        try:
            spark = _get_spark()
        except RuntimeError:
            return "## L1 — No Spark session. Run from a notebook or job."

        table = self._config.drawers_table

        # Build query — importance DESC, recent first as tiebreaker
        where = f"WHERE wing = '{self.wing}'" if self.wing else ""
        query = f"""
            SELECT text, wing, room, source_file, importance
            FROM {table}
            {where}
            ORDER BY COALESCE(importance, 3.0) DESC, filed_at DESC
            LIMIT {self.MAX_DRAWERS}
        """

        try:
            rows = spark.sql(query).collect()
        except Exception:
            return "## L1 — No palace found. Run the setup notebook, then mine data."

        if not rows:
            return "## L1 — No memories yet."

        # Group by room for readability
        by_room: Dict[str, list] = defaultdict(list)
        for row in rows:
            room = row["room"] or "general"
            by_room[room].append(row)

        # Build compact text
        lines = ["## L1 — ESSENTIAL STORY"]
        total_len = 0

        for room, entries in sorted(by_room.items()):
            room_line = f"\n[{room}]"
            lines.append(room_line)
            total_len += len(room_line)

            for row in entries:
                source = Path(row["source_file"] or "").name if row["source_file"] else ""

                # Truncate doc to keep L1 compact
                snippet = (row["text"] or "").strip().replace("\n", " ")
                if len(snippet) > 200:
                    snippet = snippet[:197] + "..."

                entry_line = f"  - {snippet}"
                if source:
                    entry_line += f"  ({source})"

                if total_len + len(entry_line) > self.MAX_CHARS:
                    lines.append("  ... (more in L3 search)")
                    return "\n".join(lines)

                lines.append(entry_line)
                total_len += len(entry_line)

        return "\n".join(lines)


# ── Layer 2 — On-Demand (wing/room filtered retrieval) ───────────────────────


class Layer2:
    """~200-500 tokens per retrieval.

    Loaded when a specific topic or wing comes up in conversation.
    Queries the Delta drawers table with a wing/room filter.
    """

    def __init__(self, config: Optional[DatabricksConfig] = None) -> None:
        self._config = config or DatabricksConfig()

    def retrieve(
        self,
        wing: Optional[str] = None,
        room: Optional[str] = None,
        n_results: int = 10,
    ) -> str:
        """Retrieve drawers filtered by wing and/or room.

        Args:
            wing: Optional wing filter.
            room: Optional room filter.
            n_results: Maximum rows to return.

        Returns:
            Formatted multi-line string of drawer snippets.
        """
        try:
            spark = _get_spark()
        except RuntimeError:
            return "No Spark session available."

        table = self._config.drawers_table

        # Build WHERE clause
        conditions: List[str] = []
        if wing:
            conditions.append(f"wing = '{wing}'")
        if room:
            conditions.append(f"room = '{room}'")
        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        query = f"""
            SELECT text, room, source_file
            FROM {table}
            {where}
            ORDER BY filed_at DESC
            LIMIT {n_results}
        """

        try:
            rows = spark.sql(query).collect()
        except Exception as e:
            return f"Retrieval error: {e}"

        if not rows:
            label = f"wing={wing}" if wing else ""
            if room:
                label += f" room={room}" if label else f"room={room}"
            return f"No drawers found for {label}."

        lines = [f"## L2 — ON-DEMAND ({len(rows)} drawers)"]
        for row in rows:
            room_name = row["room"] or "?"
            source = Path(row["source_file"] or "").name if row["source_file"] else ""
            snippet = (row["text"] or "").strip().replace("\n", " ")
            if len(snippet) > 300:
                snippet = snippet[:297] + "..."
            entry = f"  [{room_name}] {snippet}"
            if source:
                entry += f"  ({source})"
            lines.append(entry)

        return "\n".join(lines)


# ── Layer 3 — Deep Search (Vector Search semantic search) ────────────────────


class Layer3:
    """Unlimited depth. Semantic search against the full palace.

    Delegates to ``searcher._query_vs_index`` for the actual VS call.
    """

    def __init__(self, config: Optional[DatabricksConfig] = None) -> None:
        self._config = config or DatabricksConfig()

    def search(
        self,
        query: str,
        wing: Optional[str] = None,
        room: Optional[str] = None,
        n_results: int = 5,
    ) -> str:
        """Semantic search, returns compact result text.

        Args:
            query: Natural-language search string.
            wing: Optional wing filter.
            room: Optional room filter.
            n_results: Maximum number of results.

        Returns:
            Formatted multi-line string of search results.
        """
        try:
            hits = _query_vs_index(query, self._config, wing, room, n_results)
        except SearchError as e:
            return f"Search error: {e}"

        if not hits:
            return "No results found."

        lines = [f'## L3 — SEARCH RESULTS for "{query}"']
        for i, hit in enumerate(hits, 1):
            lines.append(
                f"  [{i}] {hit['wing']}/{hit['room']} (sim={hit['similarity']})"
            )
            snippet = hit["text"].strip().replace("\n", " ")
            if len(snippet) > 300:
                snippet = snippet[:297] + "..."
            lines.append(f"      {snippet}")
            if hit["source_file"] and hit["source_file"] != "?":
                lines.append(f"      src: {hit['source_file']}")

        return "\n".join(lines)

    def search_raw(
        self,
        query: str,
        wing: Optional[str] = None,
        room: Optional[str] = None,
        n_results: int = 5,
    ) -> List[Dict[str, Any]]:
        """Return raw dicts instead of formatted text.

        Args:
            query: Natural-language search string.
            wing: Optional wing filter.
            room: Optional room filter.
            n_results: Maximum number of results.

        Returns:
            List of hit dicts (empty list on error).
        """
        try:
            return _query_vs_index(query, self._config, wing, room, n_results)
        except SearchError:
            return []


# ── MemoryStack — unified interface ──────────────────────────────────────────


class MemoryStack:
    """The full 4-layer stack. One class, one config, everything works.

    Example::

        stack = MemoryStack()
        print(stack.wake_up())                # L0 + L1 (~600-900 tokens)
        print(stack.recall(wing="my_app"))    # L2 on-demand
        print(stack.search("pricing change")) # L3 deep search
    """

    def __init__(self, config: Optional[DatabricksConfig] = None) -> None:
        self._config = config or DatabricksConfig()
        self.l0 = Layer0(self._config)
        self.l1 = Layer1(self._config)
        self.l2 = Layer2(self._config)
        self.l3 = Layer3(self._config)

    def wake_up(self, wing: Optional[str] = None) -> str:
        """Generate wake-up text: L0 (identity) + L1 (essential story).

        Typically ~600-900 tokens. Inject into system prompt or first message.

        Args:
            wing: Optional wing filter for L1 (project-specific wake-up).
        """
        parts = []

        # L0: Identity
        parts.append(self.l0.render())
        parts.append("")

        # L1: Essential Story
        if wing:
            self.l1.wing = wing
        parts.append(self.l1.generate())

        return "\n".join(parts)

    def recall(
        self,
        wing: Optional[str] = None,
        room: Optional[str] = None,
        n_results: int = 10,
    ) -> str:
        """On-demand L2 retrieval filtered by wing/room."""
        return self.l2.retrieve(wing=wing, room=room, n_results=n_results)

    def search(
        self,
        query: str,
        wing: Optional[str] = None,
        room: Optional[str] = None,
        n_results: int = 5,
    ) -> str:
        """Deep L3 semantic search."""
        return self.l3.search(query, wing=wing, room=room, n_results=n_results)

    def status(self) -> Dict[str, Any]:
        """Status of all layers."""
        config = self._config
        identity_path = config._volume_path("identity.txt")

        result: Dict[str, Any] = {
            "catalog": config.catalog,
            "schema": config.schema,
            "drawers_table": config.drawers_table,
            "vs_index": config.vs_index_name,
            "L0_identity": {
                "path": str(identity_path),
                "exists": identity_path.exists(),
                "tokens": self.l0.token_estimate(),
            },
            "L1_essential": {
                "description": "Auto-generated from top palace drawers (Delta)",
            },
            "L2_on_demand": {
                "description": "Wing/room filtered retrieval (Delta)",
            },
            "L3_deep_search": {
                "description": "Full semantic search via Vector Search",
            },
        }

        # Count drawers
        try:
            spark = _get_spark()
            count_row = spark.sql(
                f"SELECT COUNT(*) AS cnt FROM {config.drawers_table}"
            ).collect()[0]
            result["total_drawers"] = count_row["cnt"]
        except Exception:
            result["total_drawers"] = 0

        return result


# ── CLI (standalone) ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    import json

    def usage() -> None:
        """Print usage and exit."""
        print("layers.py — 4-Layer Memory Stack (Databricks-native)")
        print()
        print("Usage:")
        print("  python layers.py wake-up              Show L0 + L1")
        print("  python layers.py wake-up --wing=NAME  Wake-up for a specific project")
        print("  python layers.py recall --wing=NAME   On-demand L2 retrieval")
        print("  python layers.py search <query>       Deep L3 search")
        print("  python layers.py status               Show layer status")
        sys.exit(0)

    if len(sys.argv) < 2:
        usage()

    cmd = sys.argv[1]

    # Parse flags
    flags: Dict[str, str] = {}
    positional: List[str] = []
    for arg in sys.argv[2:]:
        if arg.startswith("--") and "=" in arg:
            key, val = arg.split("=", 1)
            flags[key.lstrip("-")] = val
        elif not arg.startswith("--"):
            positional.append(arg)

    stack = MemoryStack()

    if cmd in ("wake-up", "wakeup"):
        wing = flags.get("wing")
        text = stack.wake_up(wing=wing)
        tokens = len(text) // 4
        print(f"Wake-up text (~{tokens} tokens):")
        print("=" * 50)
        print(text)

    elif cmd == "recall":
        wing = flags.get("wing")
        room = flags.get("room")
        text = stack.recall(wing=wing, room=room)
        print(text)

    elif cmd == "search":
        query = " ".join(positional) if positional else ""
        if not query:
            print("Usage: python layers.py search <query>")
            sys.exit(1)
        wing = flags.get("wing")
        room = flags.get("room")
        text = stack.search(query, wing=wing, room=room)
        print(text)

    elif cmd == "status":
        s = stack.status()
        print(json.dumps(s, indent=2))

    else:
        usage()
