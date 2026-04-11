"""
MemPalace configuration system — Databricks-native.

Priority: env vars (MEMPALACE_CATALOG, MEMPALACE_SCHEMA) > defaults.

All storage is resolved from a (catalog, schema) pair:
  - Delta tables:  {catalog}.{schema}.mempalace_drawers, _entities, _triples, ...
  - Vector Search: {catalog}.{schema}.mempalace_drawers_index
  - Config Volume: /Volumes/{catalog}/{schema}/mempalace_config/
"""

import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List


# ── Input validation ──────────────────────────────────────────────────────────
# Shared sanitizers for wing/room/entity names. Prevents path traversal,
# excessively long strings, and special characters that could cause issues
# in Delta table metadata or Volume file paths.

MAX_NAME_LENGTH = 128
_SAFE_NAME_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_ .'-]{0,126}[a-zA-Z0-9]?$")


def sanitize_name(value: str, field_name: str = "name") -> str:
    """Validate and sanitize a wing/room/entity name.

    Args:
        value: The raw name string to validate.
        field_name: Label used in error messages (e.g. "wing", "room").

    Returns:
        The stripped, validated name.

    Raises:
        ValueError: If the name is empty, too long, or contains unsafe characters.
    """
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")

    value = value.strip()

    if len(value) > MAX_NAME_LENGTH:
        raise ValueError(f"{field_name} exceeds maximum length of {MAX_NAME_LENGTH} characters")

    # Block path traversal
    if ".." in value or "/" in value or "\\" in value:
        raise ValueError(f"{field_name} contains invalid path characters")

    # Block null bytes
    if "\x00" in value:
        raise ValueError(f"{field_name} contains null bytes")

    # Enforce safe character set
    if not _SAFE_NAME_RE.match(value):
        raise ValueError(f"{field_name} contains invalid characters")

    return value


def sanitize_content(value: str, max_length: int = 100_000) -> str:
    """Validate drawer/diary content length.

    Args:
        value: The content string to validate.
        max_length: Maximum allowed character count.

    Returns:
        The validated content string.

    Raises:
        ValueError: If content is empty, too long, or contains null bytes.
    """
    if not isinstance(value, str) or not value.strip():
        raise ValueError("content must be a non-empty string")
    if len(value) > max_length:
        raise ValueError(f"content exceeds maximum length of {max_length} characters")
    if "\x00" in value:
        raise ValueError("content contains null bytes")
    return value


# ── Default constants ─────────────────────────────────────────────────────────

DEFAULT_CATALOG = "scratch"
DEFAULT_SCHEMA = "mitch_grabner"
DEFAULT_VS_ENDPOINT = "mempalace_vs_endpoint"

DEFAULT_TOPIC_WINGS: List[str] = [
    "emotions",
    "consciousness",
    "memory",
    "technical",
    "identity",
    "family",
    "creative",
]

DEFAULT_HALL_KEYWORDS: Dict[str, List[str]] = {
    "emotions": [
        "scared", "afraid", "worried", "happy", "sad",
        "love", "hate", "feel", "cry", "tears",
    ],
    "consciousness": [
        "consciousness", "conscious", "aware", "real",
        "genuine", "soul", "exist", "alive",
    ],
    "memory": ["memory", "remember", "forget", "recall", "archive", "palace", "store"],
    "technical": [
        "code", "python", "script", "bug", "error",
        "function", "api", "database", "server",
    ],
    "identity": ["identity", "name", "who am i", "persona", "self"],
    "family": ["family", "kids", "children", "daughter", "son", "parent", "mother", "father"],
    "creative": ["game", "gameplay", "player", "app", "design", "art", "music", "story"],
}


# ── Databricks configuration ─────────────────────────────────────────────────


def _env_or(env_key: str, default: str) -> str:
    """Return an env var if set and non-empty, else the default."""
    val = os.environ.get(env_key, "").strip()
    return val if val else default


@dataclass(frozen=True)
class DatabricksConfig:
    """Immutable configuration for the Databricks-native MemPalace backend.

    All table names, index names, and Volume paths are derived from the
    (catalog, schema) pair.  Override via env vars ``MEMPALACE_CATALOG``
    and ``MEMPALACE_SCHEMA``, or by passing explicit values at construction.

    Args:
        catalog: Unity Catalog catalog name.
        schema: Unity Catalog schema name.
        vs_endpoint: Name of the Vector Search endpoint to use.
        topic_wings: Default topic wing names for onboarding.
        hall_keywords: Mapping of hall names to keyword lists.
    """

    catalog: str = ""       # Resolved in __post_init__
    schema: str = ""        # Resolved in __post_init__
    vs_endpoint: str = ""   # Resolved in __post_init__
    topic_wings: List[str] = field(default_factory=lambda: list(DEFAULT_TOPIC_WINGS))
    hall_keywords: Dict[str, List[str]] = field(
        default_factory=lambda: dict(DEFAULT_HALL_KEYWORDS),
    )

    def __post_init__(self) -> None:
        """Apply env-var overrides for catalog, schema, and vs_endpoint."""
        # frozen=True requires object.__setattr__ for post-init fixups
        object.__setattr__(
            self, "catalog",
            self.catalog or _env_or("MEMPALACE_CATALOG", DEFAULT_CATALOG),
        )
        object.__setattr__(
            self, "schema",
            self.schema or _env_or("MEMPALACE_SCHEMA", DEFAULT_SCHEMA),
        )
        object.__setattr__(
            self, "vs_endpoint",
            self.vs_endpoint or _env_or("MEMPALACE_VS_ENDPOINT", DEFAULT_VS_ENDPOINT),
        )

    # ── Derived table / index names ───────────────────────────────────────

    @property
    def drawers_table(self) -> str:
        """Fully-qualified Delta table for verbatim text drawers."""
        return f"{self.catalog}.{self.schema}.mempalace_drawers"

    @property
    def vs_index_name(self) -> str:
        """Fully-qualified Vector Search index name."""
        return f"{self.catalog}.{self.schema}.mempalace_drawers_index"

    @property
    def entities_table(self) -> str:
        """Fully-qualified Delta table for knowledge-graph entities."""
        return f"{self.catalog}.{self.schema}.mempalace_entities"

    @property
    def triples_table(self) -> str:
        """Fully-qualified Delta table for knowledge-graph triples."""
        return f"{self.catalog}.{self.schema}.mempalace_triples"

    @property
    def diaries_table(self) -> str:
        """Fully-qualified Delta table for agent diary entries."""
        return f"{self.catalog}.{self.schema}.mempalace_diaries"

    @property
    def wal_table(self) -> str:
        """Fully-qualified Delta table for the write-ahead audit log."""
        return f"{self.catalog}.{self.schema}.mempalace_wal"

    @property
    def config_volume(self) -> str:
        """POSIX path to the Unity Catalog Volume for config files."""
        return f"/Volumes/{self.catalog}/{self.schema}/mempalace_config"

    # ── Volume-backed config file helpers ─────────────────────────────────

    def _volume_path(self, filename: str) -> Path:
        """Return the full Path to a file inside the config Volume."""
        return Path(self.config_volume) / filename

    def load_people_map(self) -> Dict[str, str]:
        """Load people_map.json from the config Volume.

        Returns:
            Dict mapping name variants to canonical names, or empty dict
            if the file doesn't exist or is malformed.
        """
        path = self._volume_path("people_map.json")
        if path.exists():
            try:
                return json.loads(path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                pass
        return {}

    def save_people_map(self, people_map: Dict[str, str]) -> Path:
        """Write people_map.json to the config Volume.

        Args:
            people_map: Dict mapping name variants to canonical names.

        Returns:
            Path to the written file.
        """
        path = self._volume_path("people_map.json")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(people_map, indent=2), encoding="utf-8")
        return path

    def load_identity(self) -> str:
        """Load Layer-0 identity text from the config Volume.

        Returns:
            Identity string, or a placeholder if the file is missing.
        """
        path = self._volume_path("identity.txt")
        if path.exists():
            try:
                return path.read_text(encoding="utf-8").strip()
            except OSError:
                pass
        return "## L0 — IDENTITY\nNo identity configured. Create identity.txt in the config Volume."

    def save_identity(self, text: str) -> Path:
        """Write Layer-0 identity text to the config Volume.

        Args:
            text: Identity string to persist.

        Returns:
            Path to the written file.
        """
        path = self._volume_path("identity.txt")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text.strip(), encoding="utf-8")
        return path
