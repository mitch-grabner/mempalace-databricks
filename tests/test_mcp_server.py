"""
test_mcp_server.py — Tests for the MCP server tool handlers and dispatch.

Uses monkeypatched ``_sql`` and ``_vs_search`` helpers so no SQL warehouse,
Vector Search endpoint, or Spark session is needed.
"""

import json
from collections import defaultdict

import pytest

from conftest import SEED_DRAWERS, _make_sql_responder, _make_vs_responder


# ── Helpers to build canned SQL responses from SEED_DRAWERS ───────────────────


def _wing_room_counts(drawers=SEED_DRAWERS):
    """GROUP BY wing, room → [{wing, room, cnt}, ...]"""
    counts = defaultdict(int)
    for d in drawers:
        counts[(d["wing"], d["room"])] += 1
    return [{"wing": w, "room": r, "cnt": str(c)} for (w, r), c in counts.items()]


def _wing_counts(drawers=SEED_DRAWERS):
    """GROUP BY wing → [{wing, cnt}, ...]"""
    counts = defaultdict(int)
    for d in drawers:
        counts[d["wing"]] += 1
    return [{"wing": w, "cnt": str(c)} for w, c in counts.items()]


def _room_counts(drawers=SEED_DRAWERS, wing=None):
    """GROUP BY room → [{room, cnt}, ...]"""
    counts = defaultdict(int)
    for d in drawers:
        if wing and d["wing"] != wing:
            continue
        counts[d["room"]] += 1
    return [{"room": r, "cnt": str(c)} for r, c in counts.items()]


def _seed_vs_hits(n=5):
    """VS-style row dicts with a score key."""
    return [
        {**d, "score": round(0.95 - i * 0.05, 2)}
        for i, d in enumerate(SEED_DRAWERS[:n])
    ]


# ── Patch helper ──────────────────────────────────────────────────────────────


def _patch_mcp(monkeypatch, sql_patterns=None, vs_hits=None, db_config=None):
    """Monkeypatch the MCP server's I/O layer with canned data."""
    from mempalace import mcp_server
    from mempalace.config import DatabricksConfig

    if db_config is None:
        db_config = DatabricksConfig(catalog="test", schema="test")
    monkeypatch.setattr(mcp_server, "_config", db_config)

    if sql_patterns is not None:
        monkeypatch.setattr(mcp_server, "_sql", _make_sql_responder(sql_patterns))

    if vs_hits is not None:
        monkeypatch.setattr(mcp_server, "_vs_search", _make_vs_responder(vs_hits))

    # Suppress WAL writes
    monkeypatch.setattr(mcp_server, "_wal_log", lambda *a, **kw: None)


# ══════════════════════════════════════════════════════════════════════════════
# Protocol Layer
# ══════════════════════════════════════════════════════════════════════════════


class TestHandleRequest:
    def test_initialize(self):
        from mempalace.mcp_server import handle_request

        resp = handle_request({"method": "initialize", "id": 1, "params": {}})
        assert resp["result"]["serverInfo"]["name"] == "mempalace"
        assert resp["id"] == 1

    def test_initialize_negotiates_client_version(self):
        from mempalace.mcp_server import handle_request

        resp = handle_request(
            {"method": "initialize", "id": 1, "params": {"protocolVersion": "2025-11-25"}}
        )
        assert resp["result"]["protocolVersion"] == "2025-11-25"

    def test_initialize_unknown_version_falls_back_to_latest(self):
        from mempalace.mcp_server import SUPPORTED_PROTOCOL_VERSIONS, handle_request

        resp = handle_request(
            {"method": "initialize", "id": 1, "params": {"protocolVersion": "9999-12-31"}}
        )
        assert resp["result"]["protocolVersion"] == SUPPORTED_PROTOCOL_VERSIONS[0]

    def test_notifications_initialized_returns_none(self):
        from mempalace.mcp_server import handle_request

        resp = handle_request({"method": "notifications/initialized", "id": None, "params": {}})
        assert resp is None

    def test_tools_list(self):
        from mempalace.mcp_server import handle_request

        resp = handle_request({"method": "tools/list", "id": 2, "params": {}})
        names = {t["name"] for t in resp["result"]["tools"]}
        assert "mempalace_status" in names
        assert "mempalace_search" in names
        assert "mempalace_add_drawer" in names
        assert "mempalace_kg_add" in names
        assert "mempalace_diary_write" in names
        assert len(names) == 19

    def test_unknown_tool(self):
        from mempalace.mcp_server import handle_request

        resp = handle_request(
            {"method": "tools/call", "id": 3, "params": {"name": "nonexistent", "arguments": {}}}
        )
        assert resp["error"]["code"] == -32601

    def test_unknown_method(self):
        from mempalace.mcp_server import handle_request

        resp = handle_request({"method": "unknown/method", "id": 4, "params": {}})
        assert resp["error"]["code"] == -32601

    def test_tools_call_dispatches(self, monkeypatch):
        _patch_mcp(monkeypatch, sql_patterns={"GROUP BY": _wing_room_counts()})
        from mempalace.mcp_server import handle_request

        resp = handle_request(
            {"method": "tools/call", "id": 5,
             "params": {"name": "mempalace_status", "arguments": {}}}
        )
        content = json.loads(resp["result"]["content"][0]["text"])
        assert "total_drawers" in content

    def test_null_arguments(self, monkeypatch):
        _patch_mcp(monkeypatch, sql_patterns={"GROUP BY": _wing_room_counts()})
        from mempalace.mcp_server import handle_request

        resp = handle_request(
            {"method": "tools/call", "id": 10,
             "params": {"name": "mempalace_status", "arguments": None}}
        )
        assert "error" not in resp


# ══════════════════════════════════════════════════════════════════════════════
# Read Tools
# ══════════════════════════════════════════════════════════════════════════════


class TestReadTools:
    def test_status_with_data(self, monkeypatch):
        _patch_mcp(monkeypatch, sql_patterns={"GROUP BY": _wing_room_counts()})
        from mempalace.mcp_server import tool_status

        result = tool_status()
        assert result["total_drawers"] == 4
        assert "project" in result["wings"]
        assert "notes" in result["wings"]
        assert "protocol" in result

    def test_status_empty(self, monkeypatch):
        _patch_mcp(monkeypatch, sql_patterns={"*": []})
        from mempalace.mcp_server import tool_status

        result = tool_status()
        assert result["total_drawers"] == 0

    def test_list_wings(self, monkeypatch):
        _patch_mcp(monkeypatch, sql_patterns={"GROUP BY wing": _wing_counts()})
        from mempalace.mcp_server import tool_list_wings

        result = tool_list_wings()
        assert result["wings"]["project"] == 3
        assert result["wings"]["notes"] == 1

    def test_list_rooms_all(self, monkeypatch):
        _patch_mcp(monkeypatch, sql_patterns={"GROUP BY room": _room_counts()})
        from mempalace.mcp_server import tool_list_rooms

        result = tool_list_rooms()
        assert "backend" in result["rooms"]
        assert "frontend" in result["rooms"]

    def test_list_rooms_filtered(self, monkeypatch):
        _patch_mcp(monkeypatch, sql_patterns={"GROUP BY room": _room_counts(wing="project")})
        from mempalace.mcp_server import tool_list_rooms

        result = tool_list_rooms(wing="project")
        assert "backend" in result["rooms"]
        assert "planning" not in result["rooms"]

    def test_get_taxonomy(self, monkeypatch):
        _patch_mcp(monkeypatch, sql_patterns={"GROUP BY": _wing_room_counts()})
        from mempalace.mcp_server import tool_get_taxonomy

        result = tool_get_taxonomy()
        assert result["taxonomy"]["project"]["backend"] == 2

    def test_get_aaak_spec(self):
        from mempalace.mcp_server import tool_get_aaak_spec

        result = tool_get_aaak_spec()
        assert "AAAK" in result["aaak_spec"]


# ══════════════════════════════════════════════════════════════════════════════
# Search Tools
# ══════════════════════════════════════════════════════════════════════════════


class TestSearchTools:
    def test_search_basic(self, monkeypatch):
        _patch_mcp(monkeypatch, sql_patterns={"*": []}, vs_hits=_seed_vs_hits())
        from mempalace.mcp_server import tool_search

        result = tool_search(query="JWT auth tokens")
        assert len(result["results"]) > 0
        assert "text" in result["results"][0]
        assert result["query"] == "JWT auth tokens"

    def test_search_with_wing_filter(self, monkeypatch):
        _patch_mcp(monkeypatch, sql_patterns={"*": []}, vs_hits=_seed_vs_hits())
        from mempalace.mcp_server import tool_search

        result = tool_search(query="auth", wing="project")
        assert result["filters"]["wing"] == "project"

    def test_check_duplicate_found(self, monkeypatch):
        _patch_mcp(monkeypatch, sql_patterns={"*": []}, vs_hits=_seed_vs_hits())
        from mempalace.mcp_server import tool_check_duplicate

        result = tool_check_duplicate(content="JWT tokens session", threshold=0.8)
        assert result["is_duplicate"] is True
        assert len(result["matches"]) > 0

    def test_check_duplicate_not_found(self, monkeypatch):
        _patch_mcp(monkeypatch, sql_patterns={"*": []}, vs_hits=[])
        from mempalace.mcp_server import tool_check_duplicate

        result = tool_check_duplicate(content="completely unique content")
        assert result["is_duplicate"] is False


# ══════════════════════════════════════════════════════════════════════════════
# Write Tools
# ══════════════════════════════════════════════════════════════════════════════


class TestWriteTools:
    def test_add_drawer(self, monkeypatch):
        _patch_mcp(monkeypatch, sql_patterns={"SELECT id": [], "INSERT INTO": [], "*": []})
        from mempalace.mcp_server import tool_add_drawer

        result = tool_add_drawer(wing="test", room="general", content="hello world")
        assert result["success"] is True
        assert "drawer_id" in result

    def test_add_drawer_invalid_wing(self, monkeypatch):
        _patch_mcp(monkeypatch, sql_patterns={"*": []})
        from mempalace.mcp_server import tool_add_drawer

        result = tool_add_drawer(wing="../bad", room="room", content="data")
        assert result["success"] is False
        assert "error" in result

    def test_delete_drawer_not_found(self, monkeypatch):
        _patch_mcp(monkeypatch, sql_patterns={"SELECT id": [], "*": []})
        from mempalace.mcp_server import tool_delete_drawer

        result = tool_delete_drawer(drawer_id="nonexistent")
        assert result["success"] is False

    def test_delete_drawer_found(self, monkeypatch):
        _patch_mcp(monkeypatch, sql_patterns={
            "SELECT id": [SEED_DRAWERS[0]],
            "DELETE FROM": [],
            "*": [],
        })
        from mempalace.mcp_server import tool_delete_drawer

        result = tool_delete_drawer(drawer_id=SEED_DRAWERS[0]["id"])
        assert result["success"] is True


# ══════════════════════════════════════════════════════════════════════════════
# Knowledge Graph Tools
# ══════════════════════════════════════════════════════════════════════════════


class TestKGTools:
    def test_kg_query(self, monkeypatch):
        rows = [
            {"subject": "alice", "predicate": "parent_of", "object": "max",
             "valid_from": "2015-04-01", "valid_to": None,
             "confidence": "1.0", "source_closet": None, "source_file": None,
             "subject_name": "Alice", "object_name": "Max"},
        ]
        _patch_mcp(monkeypatch, sql_patterns={"SELECT t.subject": rows, "*": []})
        from mempalace.mcp_server import tool_kg_query

        result = tool_kg_query(entity="Alice")
        assert result["count"] == 1
        assert result["facts"][0]["predicate"] == "parent_of"

    def test_kg_add(self, monkeypatch):
        _patch_mcp(monkeypatch, sql_patterns={
            "MERGE INTO": [],
            "SELECT id FROM": [],
            "INSERT INTO": [],
            "*": [],
        })
        from mempalace.mcp_server import tool_kg_add

        result = tool_kg_add(subject="Alice", predicate="parent_of", object="Max")
        assert result["success"] is True

    def test_kg_invalidate(self, monkeypatch):
        _patch_mcp(monkeypatch, sql_patterns={"UPDATE": [], "*": []})
        from mempalace.mcp_server import tool_kg_invalidate

        result = tool_kg_invalidate(subject="Alice", predicate="works_at", object="Acme")
        assert result["success"] is True
        assert "ended" in result

    def test_kg_timeline(self, monkeypatch):
        _patch_mcp(monkeypatch, sql_patterns={"ORDER BY t.valid_from": [], "*": []})
        from mempalace.mcp_server import tool_kg_timeline

        result = tool_kg_timeline()
        assert result["entity"] == "all"
        assert isinstance(result["timeline"], list)

    def test_kg_stats(self, monkeypatch):
        _patch_mcp(monkeypatch, sql_patterns={
            "mempalace_entities": [{"cnt": "5"}],
            "mempalace_triples": [{"total": "10", "current_cnt": "7"}],
            "*": [],
        })
        from mempalace.mcp_server import tool_kg_stats

        result = tool_kg_stats()
        assert result["entities"] == 5
        assert result["triples"] == 10
        assert result["current"] == 7
        assert result["expired"] == 3


# ══════════════════════════════════════════════════════════════════════════════
# Graph Navigation Tools
# ══════════════════════════════════════════════════════════════════════════════


class TestGraphTools:
    def _graph_rows(self):
        return _wing_room_counts()

    def test_traverse(self, monkeypatch):
        _patch_mcp(monkeypatch, sql_patterns={"GROUP BY room, wing": self._graph_rows(), "*": []})
        from mempalace.mcp_server import tool_traverse_graph

        result = tool_traverse_graph(start_room="backend")
        assert result["start"] == "backend"
        assert result["rooms_visited"] >= 1

    def test_traverse_not_found(self, monkeypatch):
        _patch_mcp(monkeypatch, sql_patterns={"GROUP BY room, wing": self._graph_rows(), "*": []})
        from mempalace.mcp_server import tool_traverse_graph

        result = tool_traverse_graph(start_room="nonexistent_room_xyz")
        assert "error" in result

    def test_find_tunnels(self, monkeypatch):
        _patch_mcp(monkeypatch, sql_patterns={"GROUP BY room, wing": self._graph_rows(), "*": []})
        from mempalace.mcp_server import tool_find_tunnels

        result = tool_find_tunnels()
        assert isinstance(result["tunnels"], list)

    def test_graph_stats(self, monkeypatch):
        _patch_mcp(monkeypatch, sql_patterns={"GROUP BY room, wing": self._graph_rows(), "*": []})
        from mempalace.mcp_server import tool_graph_stats

        result = tool_graph_stats()
        assert "total_rooms" in result
        assert "tunnel_rooms" in result


# ══════════════════════════════════════════════════════════════════════════════
# Diary Tools
# ══════════════════════════════════════════════════════════════════════════════


class TestDiaryTools:
    def test_diary_write(self, monkeypatch):
        _patch_mcp(monkeypatch, sql_patterns={"INSERT INTO": [], "*": []})
        from mempalace.mcp_server import tool_diary_write

        result = tool_diary_write(agent_name="test_agent", entry="Did some work today")
        assert result["success"] is True
        assert result["agent"] == "test_agent"

    def test_diary_read_empty(self, monkeypatch):
        _patch_mcp(monkeypatch, sql_patterns={"*": []})
        from mempalace.mcp_server import tool_diary_read

        result = tool_diary_read(agent_name="nobody")
        assert result["entries"] == []

    def test_diary_read_with_entries(self, monkeypatch):
        rows = [
            {"id": "d1", "agent_name": "bot", "entry": "Hello", "written_at": "2026-01-01T00:00:00"},
            {"id": "d2", "agent_name": "bot", "entry": "World", "written_at": "2026-01-02T00:00:00"},
        ]
        _patch_mcp(monkeypatch, sql_patterns={
            "ORDER BY written_at": rows,
            "COUNT": [{"cnt": "2"}],
            "*": [],
        })
        from mempalace.mcp_server import tool_diary_read

        result = tool_diary_read(agent_name="bot", last_n=10)
        assert result["showing"] == 2
        assert result["total"] == 2
