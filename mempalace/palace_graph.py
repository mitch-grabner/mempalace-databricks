"""
palace_graph.py — Graph traversal layer for MemPalace (Databricks-native)
==========================================================================

Builds a navigable graph from the palace structure:
  - Nodes = rooms (named ideas)
  - Edges = shared rooms across wings (tunnels)
  - Edge types = halls (the corridors)

Enables queries like:
  "Start at chromadb-setup in wing_code, walk to wing_myproject"
  "Find all rooms connected to riley-college-apps"
  "What topics bridge wing_hardware and wing_myproject?"

No external graph DB needed — built from Delta table metadata via Spark SQL.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Dict, List, Optional, Tuple, Union

from .config import DatabricksConfig
from .palace import _get_spark


# ── Graph construction ────────────────────────────────────────────────────────


def build_graph(
    config: Optional[DatabricksConfig] = None,
) -> Tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]]]:
    """Build the palace graph from Delta table metadata.

    Replaces the original ChromaDB batch-iteration loop with a single
    Spark SQL aggregation query — significantly faster at scale.

    Args:
        config: Databricks configuration. Uses defaults if not provided.

    Returns:
        Tuple of (nodes, edges) where:
        - nodes: ``{room: {wings: list, halls: list, count: int, dates: list}}``
        - edges: list of ``{room, wing_a, wing_b, hall, count}`` — one per tunnel crossing
    """
    config = config or DatabricksConfig()

    try:
        spark = _get_spark()
    except RuntimeError:
        return {}, []

    # Single aggregation query replaces the Python batch loop
    try:
        rows = spark.sql(f"""
            SELECT room, wing, hall, date, COUNT(*) AS cnt
            FROM {config.drawers_table}
            WHERE room IS NOT NULL
              AND room != 'general'
              AND wing IS NOT NULL
            GROUP BY room, wing, hall, date
        """).collect()
    except Exception:
        return {}, []

    if not rows:
        return {}, []

    # Aggregate into room_data (mirrors the original defaultdict structure)
    room_data: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {"wings": set(), "halls": set(), "count": 0, "dates": set()}
    )
    for row in rows:
        room = row["room"]
        rd = room_data[room]
        rd["wings"].add(row["wing"])
        if row["hall"]:
            rd["halls"].add(row["hall"])
        if row["date"]:
            rd["dates"].add(row["date"])
        rd["count"] += row["cnt"]

    # Build edges from rooms that span multiple wings
    edges: List[Dict[str, Any]] = []
    for room, data in room_data.items():
        wings = sorted(data["wings"])
        if len(wings) >= 2:
            for i, wa in enumerate(wings):
                for wb in wings[i + 1:]:
                    for hall in data["halls"]:
                        edges.append({
                            "room": room,
                            "wing_a": wa,
                            "wing_b": wb,
                            "hall": hall,
                            "count": data["count"],
                        })

    # Convert sets to sorted lists for JSON serialization
    nodes: Dict[str, Dict[str, Any]] = {}
    for room, data in room_data.items():
        nodes[room] = {
            "wings": sorted(data["wings"]),
            "halls": sorted(data["halls"]),
            "count": data["count"],
            "dates": sorted(data["dates"])[-5:] if data["dates"] else [],
        }

    return nodes, edges


# ── Traversal ─────────────────────────────────────────────────────────────────


def traverse(
    start_room: str,
    config: Optional[DatabricksConfig] = None,
    max_hops: int = 2,
) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
    """Walk the graph from a starting room via shared wings (BFS).

    Args:
        start_room: Room slug to start from.
        config: Databricks configuration.
        max_hops: Maximum BFS depth (default 2).

    Returns:
        List of path dicts ``{room, wings, halls, count, hop, connected_via}``,
        sorted by (hop, -count). On error, returns a dict with ``error`` and
        ``suggestions`` keys.
    """
    nodes, edges = build_graph(config)

    if start_room not in nodes:
        return {
            "error": f"Room '{start_room}' not found",
            "suggestions": _fuzzy_match(start_room, nodes),
        }

    start = nodes[start_room]
    visited = {start_room}
    results: List[Dict[str, Any]] = [
        {
            "room": start_room,
            "wings": start["wings"],
            "halls": start["halls"],
            "count": start["count"],
            "hop": 0,
        }
    ]

    # BFS traversal
    frontier: List[Tuple[str, int]] = [(start_room, 0)]
    while frontier:
        current_room, depth = frontier.pop(0)
        if depth >= max_hops:
            continue

        current = nodes.get(current_room, {})
        current_wings = set(current.get("wings", []))

        # Find all rooms that share a wing with current room
        for room, data in nodes.items():
            if room in visited:
                continue
            shared_wings = current_wings & set(data["wings"])
            if shared_wings:
                visited.add(room)
                results.append({
                    "room": room,
                    "wings": data["wings"],
                    "halls": data["halls"],
                    "count": data["count"],
                    "hop": depth + 1,
                    "connected_via": sorted(shared_wings),
                })
                if depth + 1 < max_hops:
                    frontier.append((room, depth + 1))

    # Sort by relevance (hop distance, then count descending)
    results.sort(key=lambda x: (x["hop"], -x["count"]))
    return results[:50]


# ── Tunnel discovery ──────────────────────────────────────────────────────────


def find_tunnels(
    wing_a: Optional[str] = None,
    wing_b: Optional[str] = None,
    config: Optional[DatabricksConfig] = None,
) -> List[Dict[str, Any]]:
    """Find rooms that connect two wings (or all tunnel rooms).

    Tunnels are rooms with the same slug appearing in multiple wings — the
    cross-domain bridges of the palace.

    Args:
        wing_a: Optional first wing filter.
        wing_b: Optional second wing filter.
        config: Databricks configuration.

    Returns:
        List of tunnel dicts ``{room, wings, halls, count, recent}``,
        sorted by count descending. Capped at 50 results.
    """
    nodes, _edges = build_graph(config)

    tunnels: List[Dict[str, Any]] = []
    for room, data in nodes.items():
        wings = data["wings"]
        if len(wings) < 2:
            continue
        if wing_a and wing_a not in wings:
            continue
        if wing_b and wing_b not in wings:
            continue

        tunnels.append({
            "room": room,
            "wings": wings,
            "halls": data["halls"],
            "count": data["count"],
            "recent": data["dates"][-1] if data["dates"] else "",
        })

    tunnels.sort(key=lambda x: -x["count"])
    return tunnels[:50]


# ── Statistics ────────────────────────────────────────────────────────────────


def graph_stats(
    config: Optional[DatabricksConfig] = None,
) -> Dict[str, Any]:
    """Summary statistics about the palace graph.

    Args:
        config: Databricks configuration.

    Returns:
        Dict with total_rooms, tunnel_rooms, total_edges,
        rooms_per_wing, and top_tunnels.
    """
    nodes, edges = build_graph(config)

    tunnel_rooms = sum(1 for n in nodes.values() if len(n["wings"]) >= 2)
    wing_counts: Counter = Counter()
    for data in nodes.values():
        for w in data["wings"]:
            wing_counts[w] += 1

    return {
        "total_rooms": len(nodes),
        "tunnel_rooms": tunnel_rooms,
        "total_edges": len(edges),
        "rooms_per_wing": dict(wing_counts.most_common()),
        "top_tunnels": [
            {"room": r, "wings": d["wings"], "count": d["count"]}
            for r, d in sorted(nodes.items(), key=lambda x: -len(x[1]["wings"]))[:10]
            if len(d["wings"]) >= 2
        ],
    }


# ── Fuzzy matching (backend-agnostic) ─────────────────────────────────────────


def _fuzzy_match(query: str, nodes: Dict[str, Any], n: int = 5) -> List[str]:
    """Find rooms that approximately match a query string.

    Args:
        query: Room slug or partial name to search for.
        nodes: The nodes dict from ``build_graph()``.
        n: Maximum number of suggestions to return.

    Returns:
        List of room slugs ranked by match quality.
    """
    query_lower = query.lower()
    scored: List[Tuple[str, float]] = []
    for room in nodes:
        if query_lower in room:
            scored.append((room, 1.0))
        elif any(word in room for word in query_lower.split("-")):
            scored.append((room, 0.5))
    scored.sort(key=lambda x: -x[1])
    return [r for r, _ in scored[:n]]
