"""
knowledge_graph.py — Temporal Entity-Relationship Graph (Databricks-native)
============================================================================

Real knowledge graph with:
  - Entity nodes (people, projects, tools, concepts)
  - Typed relationship edges (daughter_of, does, loves, works_on, etc.)
  - Temporal validity (valid_from → valid_to — knows WHEN facts are true)
  - Closet references (links back to the verbatim memory)

Storage: Delta tables in Unity Catalog (scratch.mitch_grabner.mempalace_entities / _triples)
Query: entity-first traversal with time filtering via Spark SQL

Usage::

    from mempalace.knowledge_graph import KnowledgeGraph

    kg = KnowledgeGraph()
    kg.add_triple("Max", "child_of", "Alice", valid_from="2015-04-01")
    kg.add_triple("Max", "does", "swimming", valid_from="2025-01-01")
    kg.query_entity("Max")
    kg.query_entity("Max", as_of="2026-01-15")
    kg.invalidate("Max", "has_issue", "sports_injury", ended="2026-02-15")
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

from .config import DatabricksConfig
from .palace import _get_spark


class KnowledgeGraph:
    """Temporal entity-relationship graph backed by Delta tables.

    Args:
        config: Databricks configuration. Uses defaults if not provided.
    """

    def __init__(self, config: Optional[DatabricksConfig] = None) -> None:
        self._config = config or DatabricksConfig()
        self._spark = _get_spark()

    # ── Helpers ────────────────────────────────────────────────────────────

    @staticmethod
    def _entity_id(name: str) -> str:
        """Derive a stable entity ID from a display name."""
        return name.lower().replace(" ", "_").replace("'", "")

    def _sql(self, query: str) -> List[Dict[str, Any]]:
        """Execute SQL and return a list of row dicts."""
        return [row.asDict() for row in self._spark.sql(query).collect()]

    @staticmethod
    def _esc(value: str) -> str:
        """Escape single quotes for safe SQL interpolation."""
        return value.replace("'", "\\'") if value else ""

    def close(self) -> None:
        """No-op — retained for API compatibility with the SQLite version."""

    # ── Write operations ──────────────────────────────────────────────────

    def add_entity(
        self,
        name: str,
        entity_type: str = "unknown",
        properties: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Add or update an entity node.

        Args:
            name: Display name (e.g. "Max", "Orion").
            entity_type: Category — person, project, tool, concept, animal.
            properties: Arbitrary JSON-serialisable metadata.

        Returns:
            The entity ID string.
        """
        eid = self._entity_id(name)
        props = self._esc(json.dumps(properties or {}))
        etype = self._esc(entity_type)
        ename = self._esc(name)
        table = self._config.entities_table

        self._spark.sql(f"""
            MERGE INTO {table} AS target
            USING (SELECT
                '{eid}' AS id,
                '{ename}' AS name,
                '{etype}' AS type,
                '{props}' AS properties,
                current_timestamp() AS created_at
            ) AS source
            ON target.id = source.id
            WHEN MATCHED THEN UPDATE SET
                target.name = source.name,
                target.type = source.type,
                target.properties = source.properties
            WHEN NOT MATCHED THEN INSERT *
        """)
        return eid

    def add_triple(
        self,
        subject: str,
        predicate: str,
        obj: str,
        valid_from: Optional[str] = None,
        valid_to: Optional[str] = None,
        confidence: float = 1.0,
        source_closet: Optional[str] = None,
        source_file: Optional[str] = None,
    ) -> str:
        """Add a relationship triple: subject → predicate → object.

        Auto-creates entity nodes for subject and object if they don't exist.
        Skips insertion if an identical active triple already exists.

        Args:
            subject: Source entity name.
            predicate: Relationship type (e.g. "works_on", "child_of").
            obj: Target entity name.
            valid_from: ISO date when the fact became true.
            valid_to: ISO date when the fact stopped being true (None = current).
            confidence: 0.0–1.0 confidence score.
            source_closet: Optional closet reference.
            source_file: Optional originating file path.

        Returns:
            The triple ID string (existing ID if duplicate).
        """
        sub_id = self._entity_id(subject)
        obj_id = self._entity_id(obj)
        pred = predicate.lower().replace(" ", "_")
        entities_table = self._config.entities_table
        triples_table = self._config.triples_table

        # Auto-create entities if missing
        for eid, ename in [(sub_id, subject), (obj_id, obj)]:
            safe_name = self._esc(ename)
            self._spark.sql(f"""
                MERGE INTO {entities_table} AS target
                USING (SELECT '{eid}' AS id, '{safe_name}' AS name) AS source
                ON target.id = source.id
                WHEN NOT MATCHED THEN INSERT (id, name, type, properties, created_at)
                VALUES (source.id, source.name, 'unknown', '{{}}', current_timestamp())
            """)

        # Check for existing active triple
        existing = self._sql(f"""
            SELECT id FROM {triples_table}
            WHERE subject = '{sub_id}'
              AND predicate = '{pred}'
              AND object = '{obj_id}'
              AND valid_to IS NULL
            LIMIT 1
        """)
        if existing:
            return existing[0]["id"]

        # Generate triple ID
        ts = datetime.now(timezone.utc).isoformat()
        hash_suffix = hashlib.sha256(f"{valid_from}{ts}".encode()).hexdigest()[:12]
        triple_id = f"t_{sub_id}_{pred}_{obj_id}_{hash_suffix}"

        # Build SQL-safe values
        vf = f"'{valid_from}'" if valid_from else "NULL"
        vt = f"'{valid_to}'" if valid_to else "NULL"
        sc = f"'{self._esc(source_closet)}'" if source_closet else "NULL"
        sf = f"'{self._esc(source_file)}'" if source_file else "NULL"

        self._spark.sql(f"""
            INSERT INTO {triples_table}
            (id, subject, predicate, object, valid_from, valid_to,
             confidence, source_closet, source_file, extracted_at)
            VALUES (
                '{triple_id}', '{sub_id}', '{pred}', '{obj_id}',
                {vf}, {vt}, {confidence}, {sc}, {sf}, current_timestamp()
            )
        """)
        return triple_id

    def invalidate(
        self,
        subject: str,
        predicate: str,
        obj: str,
        ended: Optional[str] = None,
    ) -> None:
        """Mark a relationship as no longer valid (set valid_to date).

        Args:
            subject: Source entity name.
            predicate: Relationship type.
            obj: Target entity name.
            ended: ISO date string. Defaults to today.
        """
        sub_id = self._entity_id(subject)
        obj_id = self._entity_id(obj)
        pred = predicate.lower().replace(" ", "_")
        ended = ended or date.today().isoformat()
        table = self._config.triples_table

        self._spark.sql(f"""
            UPDATE {table}
            SET valid_to = '{ended}'
            WHERE subject = '{sub_id}'
              AND predicate = '{pred}'
              AND object = '{obj_id}'
              AND valid_to IS NULL
        """)

    # ── Query operations ──────────────────────────────────────────────────

    def query_entity(
        self,
        name: str,
        as_of: Optional[str] = None,
        direction: str = "outgoing",
    ) -> List[Dict[str, Any]]:
        """Get all relationships for an entity.

        Args:
            name: Entity display name.
            as_of: ISO date — only return facts valid at that time.
            direction: ``"outgoing"`` (entity → ?), ``"incoming"``
                (? → entity), or ``"both"``.

        Returns:
            List of relationship dicts.
        """
        eid = self._entity_id(name)
        et = self._config.entities_table
        tt = self._config.triples_table
        results: List[Dict[str, Any]] = []

        time_filter = ""
        if as_of:
            time_filter = (
                f" AND (t.valid_from IS NULL OR t.valid_from <= '{as_of}')"
                f" AND (t.valid_to IS NULL OR t.valid_to >= '{as_of}')"
            )

        if direction in ("outgoing", "both"):
            rows = self._sql(f"""
                SELECT t.*, e.name AS obj_name
                FROM {tt} t JOIN {et} e ON t.object = e.id
                WHERE t.subject = '{eid}'{time_filter}
            """)
            for r in rows:
                results.append({
                    "direction": "outgoing",
                    "subject": name,
                    "predicate": r["predicate"],
                    "object": r["obj_name"],
                    "valid_from": r["valid_from"],
                    "valid_to": r["valid_to"],
                    "confidence": r["confidence"],
                    "source_closet": r["source_closet"],
                    "current": r["valid_to"] is None,
                })

        if direction in ("incoming", "both"):
            rows = self._sql(f"""
                SELECT t.*, e.name AS sub_name
                FROM {tt} t JOIN {et} e ON t.subject = e.id
                WHERE t.object = '{eid}'{time_filter}
            """)
            for r in rows:
                results.append({
                    "direction": "incoming",
                    "subject": r["sub_name"],
                    "predicate": r["predicate"],
                    "object": name,
                    "valid_from": r["valid_from"],
                    "valid_to": r["valid_to"],
                    "confidence": r["confidence"],
                    "source_closet": r["source_closet"],
                    "current": r["valid_to"] is None,
                })

        return results

    def query_relationship(
        self,
        predicate: str,
        as_of: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Get all triples with a given relationship type.

        Args:
            predicate: Relationship type (e.g. "works_on").
            as_of: Optional temporal filter.

        Returns:
            List of relationship dicts.
        """
        pred = predicate.lower().replace(" ", "_")
        et = self._config.entities_table
        tt = self._config.triples_table

        time_filter = ""
        if as_of:
            time_filter = (
                f" AND (t.valid_from IS NULL OR t.valid_from <= '{as_of}')"
                f" AND (t.valid_to IS NULL OR t.valid_to >= '{as_of}')"
            )

        rows = self._sql(f"""
            SELECT t.*, s.name AS sub_name, o.name AS obj_name
            FROM {tt} t
            JOIN {et} s ON t.subject = s.id
            JOIN {et} o ON t.object = o.id
            WHERE t.predicate = '{pred}'{time_filter}
        """)

        return [
            {
                "subject": r["sub_name"],
                "predicate": pred,
                "object": r["obj_name"],
                "valid_from": r["valid_from"],
                "valid_to": r["valid_to"],
                "current": r["valid_to"] is None,
            }
            for r in rows
        ]

    def timeline(
        self,
        entity_name: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Get all facts in chronological order, optionally filtered by entity.

        Args:
            entity_name: If provided, only show facts involving this entity.

        Returns:
            List of relationship dicts ordered by valid_from ascending.
        """
        et = self._config.entities_table
        tt = self._config.triples_table

        if entity_name:
            eid = self._entity_id(entity_name)
            where = f"WHERE (t.subject = '{eid}' OR t.object = '{eid}')"
        else:
            where = ""

        rows = self._sql(f"""
            SELECT t.*, s.name AS sub_name, o.name AS obj_name
            FROM {tt} t
            JOIN {et} s ON t.subject = s.id
            JOIN {et} o ON t.object = o.id
            {where}
            ORDER BY t.valid_from ASC NULLS LAST
            LIMIT 100
        """)

        return [
            {
                "subject": r["sub_name"],
                "predicate": r["predicate"],
                "object": r["obj_name"],
                "valid_from": r["valid_from"],
                "valid_to": r["valid_to"],
                "current": r["valid_to"] is None,
            }
            for r in rows
        ]

    # ── Stats ─────────────────────────────────────────────────────────────

    def stats(self) -> Dict[str, Any]:
        """Summary statistics about the knowledge graph.

        Returns:
            Dict with entity/triple counts, current/expired breakdown,
            and distinct relationship types.
        """
        et = self._config.entities_table
        tt = self._config.triples_table

        entity_count = self._sql(f"SELECT COUNT(*) AS cnt FROM {et}")[0]["cnt"]
        triple_count = self._sql(f"SELECT COUNT(*) AS cnt FROM {tt}")[0]["cnt"]
        current_count = self._sql(
            f"SELECT COUNT(*) AS cnt FROM {tt} WHERE valid_to IS NULL"
        )[0]["cnt"]
        predicates = [
            r["predicate"]
            for r in self._sql(
                f"SELECT DISTINCT predicate FROM {tt} ORDER BY predicate"
            )
        ]

        return {
            "entities": entity_count,
            "triples": triple_count,
            "current_facts": current_count,
            "expired_facts": triple_count - current_count,
            "relationship_types": predicates,
        }

    # ── Seed from known facts ─────────────────────────────────────────────

    def seed_from_entity_facts(self, entity_facts: Dict[str, Any]) -> None:
        """Seed the knowledge graph from fact_checker.py ENTITY_FACTS.

        Bootstraps the graph with known ground truth. Idempotent — safe
        to call repeatedly.

        Args:
            entity_facts: Dict keyed by entity short name, with values
                containing ``full_name``, ``type``, ``gender``, ``birthday``,
                ``parent``, ``partner``, ``relationship``, ``interests``, etc.
        """
        for key, facts in entity_facts.items():
            name = facts.get("full_name", key.capitalize())
            etype = facts.get("type", "person")
            self.add_entity(
                name,
                etype,
                {
                    "gender": facts.get("gender", ""),
                    "birthday": facts.get("birthday", ""),
                },
            )

            # Relationships
            parent = facts.get("parent")
            if parent:
                self.add_triple(
                    name, "child_of", parent.capitalize(),
                    valid_from=facts.get("birthday"),
                )

            partner = facts.get("partner")
            if partner:
                self.add_triple(name, "married_to", partner.capitalize())

            relationship = facts.get("relationship", "")
            if relationship == "daughter":
                self.add_triple(
                    name, "is_child_of",
                    facts.get("parent", "").capitalize() or name,
                    valid_from=facts.get("birthday"),
                )
            elif relationship == "husband":
                self.add_triple(
                    name, "is_partner_of",
                    facts.get("partner", name).capitalize(),
                )
            elif relationship == "brother":
                self.add_triple(
                    name, "is_sibling_of",
                    facts.get("sibling", name).capitalize(),
                )
            elif relationship == "dog":
                self.add_triple(
                    name, "is_pet_of",
                    facts.get("owner", name).capitalize(),
                )
                self.add_entity(name, "animal")

            # Interests
            for interest in facts.get("interests", []):
                self.add_triple(
                    name, "loves", interest.capitalize(),
                    valid_from="2025-01-01",
                )
