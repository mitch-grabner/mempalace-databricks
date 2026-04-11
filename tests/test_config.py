"""
test_config.py — Tests for DatabricksConfig, sanitize_name, sanitize_content.

All pure-Python tests — no Spark, no network calls.
"""

import os

import pytest

from mempalace.config import (
    DEFAULT_CATALOG,
    DEFAULT_SCHEMA,
    DEFAULT_VS_ENDPOINT,
    MAX_NAME_LENGTH,
    DatabricksConfig,
    sanitize_content,
    sanitize_name,
)


# ── DatabricksConfig defaults & properties ────────────────────────────────────


class TestDatabricksConfig:
    def test_defaults(self):
        cfg = DatabricksConfig()
        assert cfg.catalog == DEFAULT_CATALOG
        assert cfg.schema == DEFAULT_SCHEMA
        assert cfg.vs_endpoint == DEFAULT_VS_ENDPOINT

    def test_explicit_values(self):
        cfg = DatabricksConfig(catalog="prod", schema="ml", vs_endpoint="ep1")
        assert cfg.catalog == "prod"
        assert cfg.schema == "ml"
        assert cfg.vs_endpoint == "ep1"

    def test_env_var_override(self, monkeypatch):
        monkeypatch.setenv("MEMPALACE_CATALOG", "env_cat")
        monkeypatch.setenv("MEMPALACE_SCHEMA", "env_sch")
        monkeypatch.setenv("MEMPALACE_VS_ENDPOINT", "env_ep")
        cfg = DatabricksConfig()
        assert cfg.catalog == "env_cat"
        assert cfg.schema == "env_sch"
        assert cfg.vs_endpoint == "env_ep"

    def test_explicit_beats_env(self, monkeypatch):
        monkeypatch.setenv("MEMPALACE_CATALOG", "env_cat")
        cfg = DatabricksConfig(catalog="explicit")
        assert cfg.catalog == "explicit"

    def test_drawers_table(self):
        cfg = DatabricksConfig(catalog="c", schema="s")
        assert cfg.drawers_table == "c.s.mempalace_drawers"

    def test_vs_index_name(self):
        cfg = DatabricksConfig(catalog="c", schema="s")
        assert cfg.vs_index_name == "c.s.mempalace_drawers_index"

    def test_entities_table(self):
        cfg = DatabricksConfig(catalog="c", schema="s")
        assert cfg.entities_table == "c.s.mempalace_entities"

    def test_triples_table(self):
        cfg = DatabricksConfig(catalog="c", schema="s")
        assert cfg.triples_table == "c.s.mempalace_triples"

    def test_diaries_table(self):
        cfg = DatabricksConfig(catalog="c", schema="s")
        assert cfg.diaries_table == "c.s.mempalace_diaries"

    def test_wal_table(self):
        cfg = DatabricksConfig(catalog="c", schema="s")
        assert cfg.wal_table == "c.s.mempalace_wal"

    def test_config_volume(self):
        cfg = DatabricksConfig(catalog="c", schema="s")
        assert cfg.config_volume == "/Volumes/c/s/mempalace_config"

    def test_frozen(self):
        cfg = DatabricksConfig()
        with pytest.raises(AttributeError):
            cfg.catalog = "nope"


# ── sanitize_name ─────────────────────────────────────────────────────────────


class TestSanitizeName:
    def test_valid(self):
        assert sanitize_name("my_wing", "wing") == "my_wing"

    def test_strips_whitespace(self):
        assert sanitize_name("  hello  ", "x") == "hello"

    def test_empty_raises(self):
        with pytest.raises(ValueError, match="non-empty"):
            sanitize_name("", "wing")

    def test_none_raises(self):
        with pytest.raises(ValueError, match="non-empty"):
            sanitize_name(None, "wing")

    def test_too_long_raises(self):
        with pytest.raises(ValueError, match="maximum length"):
            sanitize_name("a" * (MAX_NAME_LENGTH + 1), "wing")

    def test_path_traversal_raises(self):
        with pytest.raises(ValueError, match="path"):
            sanitize_name("../etc/passwd", "wing")

    def test_slash_raises(self):
        with pytest.raises(ValueError, match="path"):
            sanitize_name("wing/room", "wing")

    def test_null_byte_raises(self):
        with pytest.raises(ValueError, match="null"):
            sanitize_name("wing\x00bad", "wing")


# ── sanitize_content ──────────────────────────────────────────────────────────


class TestSanitizeContent:
    def test_valid(self):
        assert sanitize_content("hello world") == "hello world"

    def test_empty_raises(self):
        with pytest.raises(ValueError, match="non-empty"):
            sanitize_content("")

    def test_too_long_raises(self):
        with pytest.raises(ValueError, match="maximum length"):
            sanitize_content("x" * 100_001)

    def test_null_byte_raises(self):
        with pytest.raises(ValueError, match="null"):
            sanitize_content("hello\x00world")

    def test_custom_max_length(self):
        assert sanitize_content("short", max_length=10) == "short"
        with pytest.raises(ValueError):
            sanitize_content("way too long", max_length=5)
