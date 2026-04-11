#!/usr/bin/env python3
"""
MemPalace MCP Server — read/write palace access (Databricks-native).
====================================================================

Databricks App mode:
    Deployed as a Databricks App. SQL via Statement Execution API,
    semantic search via Mosaic AI Vector Search. No Spark needed.

CLI mode (legacy):
    python -m mempalace.mcp_server
    Requires MEMPALACE_WAREHOUSE_ID env var for SQL execution.

Tools (read):
  mempalace_status          — total drawers, wing/room breakdown
  mempalace_list_wings      — all wings with drawer counts
  mempalace_list_rooms      — rooms within a wing
  mempalace_get_taxonomy    — full wing → room → count tree
  mempalace_get_aaak_spec   — AAAK compressed-memory dialect spec
  mempalace_search          — hybrid semantic + keyword search
  mempalace_check_duplicate — check if content already exists

Tools (write):
  mempalace_add_drawer      — file verbatim content into a wing/room
  mempalace_delete_drawer   — remove a drawer by ID

Tools (knowledge graph):
  mempalace_kg_query        — query entity relationships
  mempalace_kg_add          — add a fact (subject → predicate → object)
  mempalace_kg_invalidate   — mark a fact as no longer true
  mempalace_kg_timeline     — chronological timeline of facts
  mempalace_kg_stats        — KG overview

Tools (graph navigation):
  mempalace_traverse        — BFS walk through palace rooms
  mempalace_find_tunnels    — rooms bridging two wings
  mempalace_graph_stats     — palace graph overview

Tools (agent diary):
  mempalace_diary_write     — write a diary entry
  mempalace_diary_read      — read recent diary entries
"""

import hashlib
import json
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .config import DatabricksConfig, sanitize_content, sanitize_name
from .query_sanitizer import sanitize_query
from .version import __version__

logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stderr)
logger = logging.getLogger("mempalace_mcp")


# ── Configuration ─────────────────────────────────────────────────────────────

_config = DatabricksConfig()


# ── Lazy SDK clients ──────────────────────────────────────────────────────────

_ws_client = None
_vs_client = None


def _get_ws() -> "WorkspaceClient":
    """Return a cached ``WorkspaceClient`` (auto-auth from env / app identity)."""
    global _ws_client
    if _ws_client is None:
        from databricks.sdk import WorkspaceClient
        _ws_client = WorkspaceClient()
    return _ws_client


def _get_vsc() -> "VectorSearchClient":
    """Return a cached ``VectorSearchClient``."""
    global _vs_client
    if _vs_client is None:
        from databricks.vector_search.client import VectorSearchClient
        _vs_client = VectorSearchClient()
    return _vs_client


def _warehouse_id() -> str:
    """Resolve the SQL warehouse ID from environment."""
    wid = os.environ.get("MEMPALACE_WAREHOUSE_ID", "")
    if not wid:
        raise RuntimeError(
            "MEMPALACE_WAREHOUSE_ID env var is required. "
            "Set it in app.yaml or export it before running."
        )
    return wid


# ── SQL & VS helpers ──────────────────────────────────────────────────────────


def _sql(query: str) -> List[Dict[str, Optional[str]]]:
    """Execute SQL via the Statement Execution API.

    Returns:
        List of row dicts.  All values are strings (or None) because the
        Statement API serialises every column as text.
    """
    from databricks.sdk.service.sql import StatementState

    resp = _get_ws().statement_execution.execute_statement(
        warehouse_id=_warehouse_id(),
        statement=query,
        wait_timeout="30s",
    )
    if resp.status and resp.status.state == StatementState.FAILED:
        msg = resp.status.error.message if resp.status.error else "Unknown SQL error"
        raise RuntimeError(f"SQL failed: {msg}")

    if not resp.manifest or not resp.result or not resp.result.data_array:
        return []

    columns = [c.name for c in resp.manifest.schema.columns]
    return [dict(zip(columns, row)) for row in resp.result.data_array]


def _esc(value: str) -> str:
    """Escape single quotes for SQL string interpolation."""
    if value is None:
        return ""
    return str(value).replace("'", "\\'")


def _entity_id(name: str) -> str:
    """Derive a stable entity key from a display name."""
    return name.strip().lower().replace(" ", "_")


def _vs_search(
    query_text: str,
    columns: List[str],
    num_results: int = 5,
    filters: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """Run a hybrid similarity search and return row-dicts with a ``score`` key."""
    index = _get_vsc().get_index(
        endpoint_name=_config.vs_endpoint,
        index_name=_config.vs_index_name,
    )
    resp = index.similarity_search(
        query_text=query_text,
        columns=columns,
        num_results=num_results,
        filters=filters,
        query_type="hybrid",
    )
    col_names = [c["name"] for c in resp.get("manifest", {}).get("columns", [])]
    rows: List[Dict[str, Any]] = []
    for row in resp.get("result", {}).get("data_array", []):
        rows.append(dict(zip(col_names, row)))
    return rows


# ── Write-Ahead Log ───────────────────────────────────────────────────────────


def _wal_log(operation: str, params: dict, result: Optional[dict] = None) -> None:
    """Append an audit entry to the WAL Delta table."""
    now = datetime.now(timezone.utc).isoformat()
    try:
        _sql(f"""
            INSERT INTO {_config.wal_table}
            (timestamp, operation, params, result, caller)
            VALUES ('{now}', '{_esc(operation)}',
                    '{_esc(json.dumps(params, default=str))}',
                    '{_esc(json.dumps(result, default=str) if result else "")}',
                    current_user())
        """)
    except Exception as exc:
        logger.error("WAL write failed: %s", exc)


def _no_palace() -> dict:
    """Standard error when tables are not reachable."""
    return {
        "error": "Palace tables not found",
        "hint": "Run the setup_mempalace notebook first.",
    }


# ── AAAK Dialect Spec ─────────────────────────────────────────────────────────

PALACE_PROTOCOL = """IMPORTANT — MemPalace Memory Protocol:
1. ON WAKE-UP: Call mempalace_status to load palace overview + AAAK spec.
2. BEFORE RESPONDING about any person, project, or past event: call mempalace_kg_query or mempalace_search FIRST. Never guess — verify.
3. IF UNSURE about a fact (name, gender, age, relationship): say "let me check" and query the palace. Wrong is worse than slow.
4. AFTER EACH SESSION: call mempalace_diary_write to record what happened, what you learned, what matters.
5. WHEN FACTS CHANGE: call mempalace_kg_invalidate on the old fact, mempalace_kg_add for the new one.

This protocol ensures the AI KNOWS before it speaks. Storage is not memory — but storage + this protocol = memory."""

AAAK_SPEC = """AAAK is a compressed memory dialect that MemPalace uses for efficient storage.
It is designed to be readable by both humans and LLMs without decoding.

FORMAT:
  ENTITIES: 3-letter uppercase codes. ALC=Alice, JOR=Jordan, RIL=Riley, MAX=Max, BEN=Ben.
  EMOTIONS: *action markers* before/during text. *warm*=joy, *fierce*=determined, *raw*=vulnerable, *bloom*=tenderness.
  STRUCTURE: Pipe-separated fields. FAM: family | PROJ: projects | ⚠: warnings/reminders.
  DATES: ISO format (2026-03-31). COUNTS: Nx = N mentions (e.g., 570x).
  IMPORTANCE: ★ to ★★★★★ (1-5 scale).
  HALLS: hall_facts, hall_events, hall_discoveries, hall_preferences, hall_advice.
  WINGS: wing_user, wing_agent, wing_team, wing_code, wing_myproject, wing_hardware, wing_ue5, wing_ai_research.
  ROOMS: Hyphenated slugs representing named ideas (e.g., chromadb-setup, gpu-pricing).

EXAMPLE:
  FAM: ALC→♡JOR | 2D(kids): RIL(18,sports) MAX(11,chess+swimming) | BEN(contributor)

Read AAAK naturally — expand codes mentally, treat *markers* as emotional context.
When WRITING AAAK: use entity codes, mark emotions, keep structure tight."""


# ── Read tools ────────────────────────────────────────────────────────────────


def tool_status() -> dict:
    """Palace overview — total drawers, wing and room counts."""
    try:
        rows = _sql(f"""
            SELECT wing, room, COUNT(*) AS cnt
            FROM {_config.drawers_table}
            GROUP BY wing, room
        """)
    except Exception:
        return _no_palace()

    total = sum(int(r["cnt"]) for r in rows)
    wings: Dict[str, int] = {}
    rooms: Dict[str, int] = {}
    for r in rows:
        w, rm, c = r["wing"], r["room"], int(r["cnt"])
        wings[w] = wings.get(w, 0) + c
        rooms[rm] = rooms.get(rm, 0) + c

    return {
        "total_drawers": total,
        "wings": wings,
        "rooms": rooms,
        "target": _config.drawers_table,
        "protocol": PALACE_PROTOCOL,
        "aaak_dialect": AAAK_SPEC,
    }


def tool_list_wings() -> dict:
    """List all wings with drawer counts."""
    try:
        rows = _sql(
            f"SELECT wing, COUNT(*) AS cnt FROM {_config.drawers_table} GROUP BY wing"
        )
    except Exception as exc:
        return {"wings": {}, "error": str(exc)}
    return {"wings": {r["wing"]: int(r["cnt"]) for r in rows}}


def tool_list_rooms(wing: Optional[str] = None) -> dict:
    """List rooms within a wing (or all rooms if no wing given)."""
    where = f" WHERE wing = '{_esc(wing)}'" if wing else ""
    try:
        rows = _sql(
            f"SELECT room, COUNT(*) AS cnt FROM {_config.drawers_table}{where} GROUP BY room"
        )
    except Exception as exc:
        return {"wing": wing or "all", "rooms": {}, "error": str(exc)}
    return {"wing": wing or "all", "rooms": {r["room"]: int(r["cnt"]) for r in rows}}


def tool_get_taxonomy() -> dict:
    """Full taxonomy: wing → room → drawer count."""
    try:
        rows = _sql(f"""
            SELECT wing, room, COUNT(*) AS cnt
            FROM {_config.drawers_table}
            GROUP BY wing, room
        """)
    except Exception as exc:
        return {"taxonomy": {}, "error": str(exc)}

    taxonomy: Dict[str, Dict[str, int]] = {}
    for r in rows:
        w = r["wing"]
        if w not in taxonomy:
            taxonomy[w] = {}
        taxonomy[w][r["room"]] = int(r["cnt"])
    return {"taxonomy": taxonomy}


def tool_get_aaak_spec() -> dict:
    """Return the AAAK dialect specification."""
    return {"aaak_spec": AAAK_SPEC}


def tool_check_duplicate(content: str, threshold: float = 0.9) -> dict:
    """Check if content already exists in the palace before filing."""
    try:
        hits = _vs_search(
            query_text=content,
            columns=["id", "text", "wing", "room"],
            num_results=5,
        )
    except Exception as exc:
        return {"error": str(exc)}

    duplicates = []
    for h in hits:
        score = float(h.get("score", 0))
        if score >= threshold:
            text = h.get("text", "")
            duplicates.append({
                "id": h.get("id", ""),
                "wing": h.get("wing", ""),
                "room": h.get("room", ""),
                "similarity": round(score, 3),
                "content": text[:200] + "..." if len(text) > 200 else text,
            })
    return {"is_duplicate": len(duplicates) > 0, "matches": duplicates}


# ── Search ────────────────────────────────────────────────────────────────────


def tool_search(
    query: str,
    limit: int = 5,
    wing: Optional[str] = None,
    room: Optional[str] = None,
    context: Optional[str] = None,
) -> dict:
    """Hybrid semantic + keyword search across palace drawers."""
    sanitized = sanitize_query(query)
    q = sanitized["clean_query"]

    filters: Dict[str, str] = {}
    if wing:
        filters["wing"] = wing
    if room:
        filters["room"] = room

    try:
        hits = _vs_search(
            query_text=q,
            columns=["id", "text", "wing", "room", "source_file", "date", "importance"],
            num_results=limit,
            filters=filters or None,
        )
    except Exception as exc:
        return {"error": str(exc), "query": q}

    results = []
    for h in hits:
        results.append({
            "text": h.get("text", ""),
            "wing": h.get("wing", ""),
            "room": h.get("room", ""),
            "source_file": h.get("source_file", ""),
            "date": h.get("date", ""),
            "score": h.get("score", 0),
        })

    out: dict = {"query": q, "filters": {"wing": wing, "room": room}, "results": results}
    if sanitized["was_sanitized"]:
        out["query_sanitized"] = True
        out["sanitizer"] = {
            "method": sanitized["method"],
            "original_length": sanitized["original_length"],
            "clean_length": sanitized["clean_length"],
            "clean_query": sanitized["clean_query"],
        }
    if context:
        out["context_received"] = True
    return out


# ── Write tools ───────────────────────────────────────────────────────────────


def tool_add_drawer(
    wing: str,
    room: str,
    content: str,
    source_file: Optional[str] = None,
    added_by: str = "mcp",
) -> dict:
    """File verbatim content into a wing/room. Checks for duplicates first."""
    try:
        wing = sanitize_name(wing, "wing")
        room = sanitize_name(room, "room")
        content = sanitize_content(content)
    except ValueError as exc:
        return {"success": False, "error": str(exc)}

    drawer_id = hashlib.sha256(
        f"{wing}|{room}|{content[:100]}|0".encode()
    ).hexdigest()

    _wal_log("add_drawer", {
        "drawer_id": drawer_id,
        "wing": wing,
        "room": room,
        "added_by": added_by,
        "content_length": len(content),
        "content_preview": content[:200],
    })

    # Idempotency check
    try:
        existing = _sql(
            f"SELECT id FROM {_config.drawers_table} "
            f"WHERE id = '{drawer_id}' LIMIT 1"
        )
        if existing:
            return {"success": True, "reason": "already_exists", "drawer_id": drawer_id}
    except Exception:
        pass

    now = datetime.now(timezone.utc).isoformat()
    try:
        _sql(f"""
            INSERT INTO {_config.drawers_table}
            (id, text, wing, room, source_file, chunk_index, importance, agent, filed_at)
            VALUES ('{drawer_id}', '{_esc(content)}', '{_esc(wing)}', '{_esc(room)}',
                    '{_esc(source_file or "")}', 0, 3.0, '{_esc(added_by)}', '{now}')
        """)
        logger.info("Filed drawer: %s → %s/%s", drawer_id, wing, room)
        return {"success": True, "drawer_id": drawer_id, "wing": wing, "room": room}
    except Exception as exc:
        return {"success": False, "error": str(exc)}


def tool_delete_drawer(drawer_id: str) -> dict:
    """Delete a single drawer by ID."""
    try:
        existing = _sql(
            f"SELECT id, text, wing, room FROM {_config.drawers_table} "
            f"WHERE id = '{_esc(drawer_id)}' LIMIT 1"
        )
    except Exception:
        return _no_palace()

    if not existing:
        return {"success": False, "error": f"Drawer not found: {drawer_id}"}

    _wal_log("delete_drawer", {
        "drawer_id": drawer_id,
        "deleted_meta": existing[0],
        "content_preview": (existing[0].get("text") or "")[:200],
    })

    try:
        _sql(f"DELETE FROM {_config.drawers_table} WHERE id = '{_esc(drawer_id)}'")
        logger.info("Deleted drawer: %s", drawer_id)
        return {"success": True, "drawer_id": drawer_id}
    except Exception as exc:
        return {"success": False, "error": str(exc)}


# ── Knowledge Graph tools ─────────────────────────────────────────────────────


def tool_kg_query(
    entity: str,
    as_of: Optional[str] = None,
    direction: str = "both",
) -> dict:
    """Query the knowledge graph for an entity's relationships."""
    eid = _entity_id(entity)
    conds: List[str] = []
    if direction in ("outgoing", "both"):
        conds.append(f"t.subject = '{_esc(eid)}'")
    if direction in ("incoming", "both"):
        conds.append(f"t.object = '{_esc(eid)}'")
    where = " OR ".join(conds)

    time_filter = ""
    if as_of:
        time_filter = (
            f" AND (t.valid_from IS NULL OR t.valid_from <= '{_esc(as_of)}')"
            f" AND (t.valid_to IS NULL OR t.valid_to >= '{_esc(as_of)}')"
        )

    rows = _sql(f"""
        SELECT t.subject, t.predicate, t.object, t.valid_from, t.valid_to,
               t.confidence, t.source_closet, t.source_file,
               se.name AS subject_name, oe.name AS object_name
        FROM {_config.triples_table} t
        LEFT JOIN {_config.entities_table} se ON t.subject = se.id
        LEFT JOIN {_config.entities_table} oe ON t.object = oe.id
        WHERE ({where}){time_filter}
        ORDER BY t.valid_from NULLS LAST
    """)

    facts = []
    for r in rows:
        facts.append({
            "subject": r.get("subject_name") or r["subject"],
            "predicate": r["predicate"],
            "object": r.get("object_name") or r["object"],
            "valid_from": r.get("valid_from"),
            "valid_to": r.get("valid_to"),
            "confidence": r.get("confidence"),
            "source": r.get("source_closet") or r.get("source_file"),
        })
    return {"entity": entity, "as_of": as_of, "facts": facts, "count": len(facts)}


def tool_kg_add(
    subject: str,
    predicate: str,
    object: str,
    valid_from: Optional[str] = None,
    source_closet: Optional[str] = None,
) -> dict:
    """Add a relationship to the knowledge graph."""
    try:
        subject = sanitize_name(subject, "subject")
        predicate = sanitize_name(predicate, "predicate")
        object = sanitize_name(object, "object")
    except ValueError as exc:
        return {"success": False, "error": str(exc)}

    _wal_log("kg_add", {
        "subject": subject, "predicate": predicate, "object": object,
        "valid_from": valid_from, "source_closet": source_closet,
    })

    sub_id = _entity_id(subject)
    obj_id = _entity_id(object)
    now = datetime.now(timezone.utc).isoformat()

    # Ensure both entities exist (INSERT-if-absent)
    for eid, name in [(sub_id, subject), (obj_id, object)]:
        _sql(f"""
            MERGE INTO {_config.entities_table} AS tgt
            USING (SELECT '{_esc(eid)}' AS id, '{_esc(name)}' AS name) AS src
            ON tgt.id = src.id
            WHEN NOT MATCHED THEN INSERT (id, name, type, properties, created_at)
                VALUES (src.id, src.name, 'unknown', '{{}}'  , '{now}')
        """)

    # Duplicate check — same SPO with open validity
    existing = _sql(f"""
        SELECT id FROM {_config.triples_table}
        WHERE subject = '{_esc(sub_id)}' AND predicate = '{_esc(predicate)}'
              AND object = '{_esc(obj_id)}' AND valid_to IS NULL
        LIMIT 1
    """)
    if existing:
        return {
            "success": True,
            "triple_id": existing[0]["id"],
            "fact": f"{subject} → {predicate} → {object}",
            "note": "already_exists",
        }

    triple_id = hashlib.sha256(
        f"{sub_id}|{predicate}|{obj_id}|{valid_from or ''}".encode()
    ).hexdigest()

    vf = f"'{_esc(valid_from)}'" if valid_from else "NULL"
    sc = f"'{_esc(source_closet)}'" if source_closet else "NULL"

    _sql(f"""
        INSERT INTO {_config.triples_table}
        (id, subject, predicate, object, valid_from, valid_to,
         confidence, source_closet, source_file, extracted_at)
        VALUES ('{triple_id}', '{_esc(sub_id)}', '{_esc(predicate)}', '{_esc(obj_id)}',
                {vf}, NULL, 1.0, {sc}, NULL, '{now}')
    """)
    return {
        "success": True,
        "triple_id": triple_id,
        "fact": f"{subject} → {predicate} → {object}",
    }


def tool_kg_invalidate(
    subject: str,
    predicate: str,
    object: str,
    ended: Optional[str] = None,
) -> dict:
    """Mark a fact as no longer true (set end date)."""
    _wal_log("kg_invalidate", {
        "subject": subject, "predicate": predicate,
        "object": object, "ended": ended,
    })

    sub_id = _entity_id(subject)
    obj_id = _entity_id(object)
    end_date = ended or datetime.now(timezone.utc).strftime("%Y-%m-%d")

    _sql(f"""
        UPDATE {_config.triples_table}
        SET valid_to = '{_esc(end_date)}'
        WHERE subject = '{_esc(sub_id)}'
          AND predicate = '{_esc(predicate)}'
          AND object = '{_esc(obj_id)}'
          AND valid_to IS NULL
    """)
    return {
        "success": True,
        "fact": f"{subject} → {predicate} → {object}",
        "ended": end_date,
    }


def tool_kg_timeline(entity: Optional[str] = None) -> dict:
    """Chronological timeline of facts, optionally for one entity."""
    where = ""
    if entity:
        eid = _entity_id(entity)
        where = f" WHERE t.subject = '{_esc(eid)}' OR t.object = '{_esc(eid)}'"

    rows = _sql(f"""
        SELECT t.subject, t.predicate, t.object, t.valid_from, t.valid_to,
               se.name AS subject_name, oe.name AS object_name
        FROM {_config.triples_table} t
        LEFT JOIN {_config.entities_table} se ON t.subject = se.id
        LEFT JOIN {_config.entities_table} oe ON t.object = oe.id
        {where}
        ORDER BY t.valid_from NULLS LAST
    """)

    timeline = []
    for r in rows:
        timeline.append({
            "subject": r.get("subject_name") or r["subject"],
            "predicate": r["predicate"],
            "object": r.get("object_name") or r["object"],
            "valid_from": r.get("valid_from"),
            "valid_to": r.get("valid_to"),
            "current": r.get("valid_to") is None,
        })
    return {"entity": entity or "all", "timeline": timeline, "count": len(timeline)}


def tool_kg_stats() -> dict:
    """Knowledge graph overview: entities, triples, current vs expired."""
    ent = _sql(f"SELECT COUNT(*) AS cnt FROM {_config.entities_table}")
    tri = _sql(f"""
        SELECT COUNT(*) AS total,
               SUM(CASE WHEN valid_to IS NULL THEN 1 ELSE 0 END) AS current_cnt
        FROM {_config.triples_table}
    """)

    entities = int(ent[0]["cnt"]) if ent else 0
    total = int(tri[0]["total"]) if tri else 0
    current = int(tri[0]["current_cnt"] or 0) if tri else 0

    return {
        "entities": entities,
        "triples": total,
        "current": current,
        "expired": total - current,
    }


# ── Graph navigation tools ───────────────────────────────────────────────────


def _build_graph() -> tuple:
    """Build palace graph from Delta aggregation (Statement API version).

    Returns:
        Tuple of (nodes, edges).
        nodes: ``{room: {"wings": [str], "count": int}}``
        edges:  list of ``{"from": str, "to": str, "wing": str}``
    """
    rows = _sql(f"""
        SELECT room, wing, COUNT(*) AS cnt
        FROM {_config.drawers_table}
        WHERE room IS NOT NULL AND wing IS NOT NULL
        GROUP BY room, wing
    """)

    nodes: Dict[str, Dict[str, Any]] = {}
    wing_rooms: Dict[str, set] = defaultdict(set)
    for r in rows:
        rm, w, c = r["room"], r["wing"], int(r["cnt"])
        if rm not in nodes:
            nodes[rm] = {"wings": set(), "count": 0}
        nodes[rm]["wings"].add(w)
        nodes[rm]["count"] += c
        wing_rooms[w].add(rm)

    # Build edges: rooms connected through a shared wing
    edges: list = []
    for w, rm_set in wing_rooms.items():
        rm_list = sorted(rm_set)
        for i in range(len(rm_list)):
            for j in range(i + 1, len(rm_list)):
                edges.append({"from": rm_list[i], "to": rm_list[j], "wing": w})

    # Serialise sets → sorted lists
    for rm in nodes:
        nodes[rm]["wings"] = sorted(nodes[rm]["wings"])

    return nodes, edges


def tool_traverse_graph(start_room: str, max_hops: int = 2) -> dict:
    """BFS walk through the palace graph from a starting room."""
    try:
        nodes, edges = _build_graph()
    except Exception:
        return _no_palace()

    if start_room not in nodes:
        suggestions = [rm for rm in nodes if start_room.lower() in rm.lower()][:5]
        return {"error": f"Room '{start_room}' not found", "suggestions": suggestions}

    # BFS
    visited = {start_room}
    queue = [(start_room, 0)]
    path: list = []

    while queue:
        current, depth = queue.pop(0)
        if depth > max_hops:
            break
        nd = nodes.get(current, {})
        path.append({
            "room": current,
            "depth": depth,
            "wings": nd.get("wings", []),
            "count": nd.get("count", 0),
        })
        if depth < max_hops:
            for edge in edges:
                nbr = None
                if edge["from"] == current:
                    nbr = edge["to"]
                elif edge["to"] == current:
                    nbr = edge["from"]
                if nbr and nbr not in visited:
                    visited.add(nbr)
                    queue.append((nbr, depth + 1))

    return {"start": start_room, "max_hops": max_hops, "path": path, "rooms_visited": len(path)}


def tool_find_tunnels(
    wing_a: Optional[str] = None,
    wing_b: Optional[str] = None,
) -> dict:
    """Find rooms that bridge two wings."""
    try:
        nodes, _ = _build_graph()
    except Exception:
        return _no_palace()

    tunnels = []
    for rm, data in nodes.items():
        if len(data["wings"]) < 2:
            continue
        if wing_a and wing_a not in data["wings"]:
            continue
        if wing_b and wing_b not in data["wings"]:
            continue
        tunnels.append({
            "room": rm,
            "wings": data["wings"],
            "count": data["count"],
        })

    tunnels.sort(key=lambda x: len(x["wings"]), reverse=True)
    return {"tunnels": tunnels, "count": len(tunnels)}


def tool_graph_stats() -> dict:
    """Palace graph overview: total rooms, tunnels, edges, connectivity."""
    try:
        nodes, edges = _build_graph()
    except Exception:
        return _no_palace()

    tunnel_rooms = [rm for rm, d in nodes.items() if len(d["wings"]) >= 2]
    wing_counts: Dict[str, int] = {}
    for data in nodes.values():
        for w in data["wings"]:
            wing_counts[w] = wing_counts.get(w, 0) + 1

    return {
        "total_rooms": len(nodes),
        "tunnel_rooms": len(tunnel_rooms),
        "total_edges": len(edges),
        "rooms_per_wing": wing_counts,
        "top_tunnels": sorted(
            [{"room": rm, "wings": nodes[rm]["wings"], "count": nodes[rm]["count"]}
             for rm in tunnel_rooms],
            key=lambda x: len(x["wings"]),
            reverse=True,
        )[:10],
    }


# ── Agent diary tools ─────────────────────────────────────────────────────────


def tool_diary_write(
    agent_name: str,
    entry: str,
    topic: str = "general",
) -> dict:
    """Write a diary entry for this agent."""
    try:
        agent_name = sanitize_name(agent_name, "agent_name")
        entry = sanitize_content(entry)
    except ValueError as exc:
        return {"success": False, "error": str(exc)}

    now = datetime.now(timezone.utc)
    entry_id = (
        f"diary_{agent_name}_{now.strftime('%Y%m%d_%H%M%S')}_"
        f"{hashlib.sha256(entry[:50].encode()).hexdigest()[:12]}"
    )

    _wal_log("diary_write", {
        "agent_name": agent_name,
        "topic": topic,
        "entry_id": entry_id,
        "entry_preview": entry[:200],
    })

    try:
        _sql(f"""
            INSERT INTO {_config.diaries_table}
            (id, agent_name, entry, written_at)
            VALUES ('{entry_id}', '{_esc(agent_name)}', '{_esc(entry)}', '{now.isoformat()}')
        """)
        logger.info("Diary entry: %s → %s/%s", entry_id, agent_name, topic)
        return {
            "success": True,
            "entry_id": entry_id,
            "agent": agent_name,
            "topic": topic,
            "timestamp": now.isoformat(),
        }
    except Exception as exc:
        return {"success": False, "error": str(exc)}


def tool_diary_read(agent_name: str, last_n: int = 10) -> dict:
    """Read an agent's recent diary entries."""
    try:
        rows = _sql(f"""
            SELECT id, agent_name, entry, written_at
            FROM {_config.diaries_table}
            WHERE agent_name = '{_esc(agent_name)}'
            ORDER BY written_at DESC
            LIMIT {int(last_n)}
        """)
    except Exception as exc:
        return {"error": str(exc)}

    if not rows:
        return {"agent": agent_name, "entries": [], "message": "No diary entries yet."}

    entries = [{"timestamp": r["written_at"], "content": r["entry"]} for r in rows]

    total_rows = _sql(
        f"SELECT COUNT(*) AS cnt FROM {_config.diaries_table} "
        f"WHERE agent_name = '{_esc(agent_name)}'"
    )
    total = int(total_rows[0]["cnt"]) if total_rows else 0

    return {
        "agent": agent_name,
        "entries": entries,
        "total": total,
        "showing": len(entries),
    }


# ── MCP Protocol ──────────────────────────────────────────────────────────────

TOOLS = {
    "mempalace_status": {
        "description": "Palace overview — total drawers, wing and room counts",
        "input_schema": {"type": "object", "properties": {}},
        "handler": tool_status,
    },
    "mempalace_list_wings": {
        "description": "List all wings with drawer counts",
        "input_schema": {"type": "object", "properties": {}},
        "handler": tool_list_wings,
    },
    "mempalace_list_rooms": {
        "description": "List rooms within a wing (or all rooms if no wing given)",
        "input_schema": {
            "type": "object",
            "properties": {
                "wing": {"type": "string", "description": "Wing to list rooms for (optional)"},
            },
        },
        "handler": tool_list_rooms,
    },
    "mempalace_get_taxonomy": {
        "description": "Full taxonomy: wing → room → drawer count",
        "input_schema": {"type": "object", "properties": {}},
        "handler": tool_get_taxonomy,
    },
    "mempalace_get_aaak_spec": {
        "description": "Get the AAAK dialect specification — the compressed memory format MemPalace uses. Call this if you need to read or write AAAK-compressed memories.",
        "input_schema": {"type": "object", "properties": {}},
        "handler": tool_get_aaak_spec,
    },
    "mempalace_kg_query": {
        "description": "Query the knowledge graph for an entity's relationships. Returns typed facts with temporal validity.",
        "input_schema": {
            "type": "object",
            "properties": {
                "entity": {"type": "string", "description": "Entity to query"},
                "as_of": {"type": "string", "description": "Date filter (YYYY-MM-DD, optional)"},
                "direction": {"type": "string", "description": "outgoing, incoming, or both (default: both)"},
            },
            "required": ["entity"],
        },
        "handler": tool_kg_query,
    },
    "mempalace_kg_add": {
        "description": "Add a fact to the knowledge graph. Subject → predicate → object with optional time window.",
        "input_schema": {
            "type": "object",
            "properties": {
                "subject": {"type": "string", "description": "The entity doing/being something"},
                "predicate": {"type": "string", "description": "The relationship type"},
                "object": {"type": "string", "description": "The entity being connected to"},
                "valid_from": {"type": "string", "description": "When this became true (YYYY-MM-DD, optional)"},
                "source_closet": {"type": "string", "description": "Closet ID where this fact appears (optional)"},
            },
            "required": ["subject", "predicate", "object"],
        },
        "handler": tool_kg_add,
    },
    "mempalace_kg_invalidate": {
        "description": "Mark a fact as no longer true. E.g. ankle injury resolved, job ended, moved house.",
        "input_schema": {
            "type": "object",
            "properties": {
                "subject": {"type": "string", "description": "Entity"},
                "predicate": {"type": "string", "description": "Relationship"},
                "object": {"type": "string", "description": "Connected entity"},
                "ended": {"type": "string", "description": "When it stopped being true (YYYY-MM-DD, default: today)"},
            },
            "required": ["subject", "predicate", "object"],
        },
        "handler": tool_kg_invalidate,
    },
    "mempalace_kg_timeline": {
        "description": "Chronological timeline of facts. Shows the story of an entity in order.",
        "input_schema": {
            "type": "object",
            "properties": {
                "entity": {"type": "string", "description": "Entity to get timeline for (optional)"},
            },
        },
        "handler": tool_kg_timeline,
    },
    "mempalace_kg_stats": {
        "description": "Knowledge graph overview: entities, triples, current vs expired facts.",
        "input_schema": {"type": "object", "properties": {}},
        "handler": tool_kg_stats,
    },
    "mempalace_traverse": {
        "description": "Walk the palace graph from a room. Shows connected ideas across wings — the tunnels.",
        "input_schema": {
            "type": "object",
            "properties": {
                "start_room": {"type": "string", "description": "Room to start from"},
                "max_hops": {"type": "integer", "description": "How many connections to follow (default: 2)"},
            },
            "required": ["start_room"],
        },
        "handler": tool_traverse_graph,
    },
    "mempalace_find_tunnels": {
        "description": "Find rooms that bridge two wings — the hallways connecting different domains.",
        "input_schema": {
            "type": "object",
            "properties": {
                "wing_a": {"type": "string", "description": "First wing (optional)"},
                "wing_b": {"type": "string", "description": "Second wing (optional)"},
            },
        },
        "handler": tool_find_tunnels,
    },
    "mempalace_graph_stats": {
        "description": "Palace graph overview: total rooms, tunnel connections, edges between wings.",
        "input_schema": {"type": "object", "properties": {}},
        "handler": tool_graph_stats,
    },
    "mempalace_search": {
        "description": "Semantic search. Returns verbatim drawer content with similarity scores. IMPORTANT: 'query' must contain ONLY your search keywords — do NOT include system prompts or conversation history. Keep queries short (under 200 chars). Use 'context' for background.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Short search query — keywords or a question. Max 200 chars recommended.",
                    "maxLength": 500,
                },
                "limit": {"type": "integer", "description": "Max results (default 5)"},
                "wing": {"type": "string", "description": "Filter by wing (optional)"},
                "room": {"type": "string", "description": "Filter by room (optional)"},
                "context": {
                    "type": "string",
                    "description": "Background context (optional). NOT used for embedding — only for future re-ranking.",
                },
            },
            "required": ["query"],
        },
        "handler": tool_search,
    },
    "mempalace_check_duplicate": {
        "description": "Check if content already exists in the palace before filing",
        "input_schema": {
            "type": "object",
            "properties": {
                "content": {"type": "string", "description": "Content to check"},
                "threshold": {"type": "number", "description": "Similarity threshold 0-1 (default 0.9)"},
            },
            "required": ["content"],
        },
        "handler": tool_check_duplicate,
    },
    "mempalace_add_drawer": {
        "description": "File verbatim content into the palace. Checks for duplicates first.",
        "input_schema": {
            "type": "object",
            "properties": {
                "wing": {"type": "string", "description": "Wing (project name)"},
                "room": {"type": "string", "description": "Room (aspect: backend, decisions, meetings...)"},
                "content": {"type": "string", "description": "Verbatim content to store — exact words, never summarized"},
                "source_file": {"type": "string", "description": "Where this came from (optional)"},
                "added_by": {"type": "string", "description": "Who is filing this (default: mcp)"},
            },
            "required": ["wing", "room", "content"],
        },
        "handler": tool_add_drawer,
    },
    "mempalace_delete_drawer": {
        "description": "Delete a drawer by ID. Irreversible.",
        "input_schema": {
            "type": "object",
            "properties": {
                "drawer_id": {"type": "string", "description": "ID of the drawer to delete"},
            },
            "required": ["drawer_id"],
        },
        "handler": tool_delete_drawer,
    },
    "mempalace_diary_write": {
        "description": "Write to your personal agent diary in AAAK format. Your observations, thoughts, what you worked on, what matters. Each agent has their own diary. Write in AAAK for compression.",
        "input_schema": {
            "type": "object",
            "properties": {
                "agent_name": {"type": "string", "description": "Your name — each agent gets their own diary"},
                "entry": {"type": "string", "description": "Your diary entry in AAAK format"},
                "topic": {"type": "string", "description": "Topic tag (optional, default: general)"},
            },
            "required": ["agent_name", "entry"],
        },
        "handler": tool_diary_write,
    },
    "mempalace_diary_read": {
        "description": "Read your recent diary entries (in AAAK). See what past versions of yourself recorded.",
        "input_schema": {
            "type": "object",
            "properties": {
                "agent_name": {"type": "string", "description": "Your name"},
                "last_n": {"type": "integer", "description": "Number of recent entries (default: 10)"},
            },
            "required": ["agent_name"],
        },
        "handler": tool_diary_read,
    },
}


SUPPORTED_PROTOCOL_VERSIONS = [
    "2025-11-25",
    "2025-06-18",
    "2025-03-26",
    "2024-11-05",
]


def handle_request(request: dict) -> Optional[dict]:
    """Handle a JSON-RPC MCP request (used by legacy CLI mode)."""
    method = request.get("method", "")
    params = request.get("params", {})
    req_id = request.get("id")

    if method == "initialize":
        client_version = params.get("protocolVersion", SUPPORTED_PROTOCOL_VERSIONS[-1])
        negotiated = (
            client_version
            if client_version in SUPPORTED_PROTOCOL_VERSIONS
            else SUPPORTED_PROTOCOL_VERSIONS[0]
        )
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "protocolVersion": negotiated,
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "mempalace", "version": __version__},
            },
        }

    if method == "notifications/initialized":
        return None

    if method == "tools/list":
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "tools": [
                    {"name": n, "description": t["description"], "inputSchema": t["input_schema"]}
                    for n, t in TOOLS.items()
                ]
            },
        }

    if method == "tools/call":
        tool_name = params.get("name")
        tool_args = params.get("arguments") or {}
        if tool_name not in TOOLS:
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {"code": -32601, "message": f"Unknown tool: {tool_name}"},
            }
        # Coerce argument types from JSON transport
        schema_props = TOOLS[tool_name]["input_schema"].get("properties", {})
        for key, value in list(tool_args.items()):
            declared = schema_props.get(key, {}).get("type")
            if declared == "integer" and not isinstance(value, int):
                tool_args[key] = int(value)
            elif declared == "number" and not isinstance(value, (int, float)):
                tool_args[key] = float(value)
        try:
            result = TOOLS[tool_name]["handler"](**tool_args)
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "content": [{"type": "text", "text": json.dumps(result, indent=2)}]
                },
            }
        except Exception:
            logger.exception("Tool error in %s", tool_name)
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {"code": -32000, "message": "Internal tool error"},
            }

    return {
        "jsonrpc": "2.0",
        "id": req_id,
        "error": {"code": -32601, "message": f"Unknown method: {method}"},
    }


def main() -> None:
    """Run the MCP server in legacy CLI mode (JSON-RPC over stdin/stdout)."""
    logger.info("MemPalace MCP Server starting (CLI mode)...")
    while True:
        try:
            line = sys.stdin.readline()
            if not line:
                break
            line = line.strip()
            if not line:
                continue
            request = json.loads(line)
            response = handle_request(request)
            if response is not None:
                sys.stdout.write(json.dumps(response) + "\n")
                sys.stdout.flush()
        except KeyboardInterrupt:
            break
        except Exception as exc:
            logger.error("Server error: %s", exc)


if __name__ == "__main__":
    main()
