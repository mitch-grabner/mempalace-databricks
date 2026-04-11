# MemPalace → Databricks: Conversion Plan

> **Goal**: Replace ChromaDB (vector store) and SQLite (knowledge graph) with
> Databricks-native services — Delta tables in Unity Catalog for storage,
> Mosaic AI Vector Search for semantic retrieval — while preserving every
> public API surface (MCP tools, CLI, Python imports).

---

## 1 — Architecture Mapping

```
BEFORE (local)                          AFTER (Databricks)
─────────────────                       ──────────────────
ChromaDB PersistentClient        →      Delta table  +  Vector Search Index
  └─ mempalace_drawers collection       └─ <catalog>.<schema>.mempalace_drawers
                                            + VS index: <catalog>.<schema>.mempalace_drawers_index

SQLite knowledge_graph.sqlite3   →      Delta tables
  ├─ entities table                     ├─ <catalog>.<schema>.mempalace_entities
  └─ triples table                      └─ <catalog>.<schema>.mempalace_triples

~/.mempalace/config.json         →      Volume JSON  /Volumes/<catalog>/<schema>/mempalace_config/config.json
~/.mempalace/identity.txt        →      Volume file  /Volumes/<catalog>/<schema>/mempalace_config/identity.txt
~/.mempalace/wing_config.json    →      Volume JSON  /Volumes/<catalog>/<schema>/mempalace_config/wing_config.json
~/.mempalace/people_map.json     →      Volume JSON  /Volumes/<catalog>/<schema>/mempalace_config/people_map.json

~/.mempalace/agents/*.json       →      Delta table  <catalog>.<schema>.mempalace_agents
~/.mempalace/wal/write_log.jsonl →      Delta table  <catalog>.<schema>.mempalace_wal  (append-only)
```

### Why These Choices

| Decision | Rationale |
| --- | --- |
| Delta table for drawers | Source-of-truth for Vector Search Delta Sync Index. ACID, time-travel, MERGE for dedup. |
| Vector Search (Databricks-managed embeddings) | `databricks-gte-large-en` computes embeddings automatically — no local model needed. Delta Sync keeps index fresh. |
| Delta tables for KG | Temporal validity queries map cleanly to SQL WHERE clauses. No Neo4j/SQLite dependency. |
| Volumes for config files | JSON/text config files are tiny and human-editable. Volumes give a POSIX-like path without a full table. |
| Delta table for WAL | Append-only audit log is a natural fit for Delta. Queryable, time-travel-enabled. |

---

## 2 — Table Schemas (DDL)

### 2.1 — Drawers (verbatim text chunks)

```sql
CREATE TABLE IF NOT EXISTS <catalog>.<schema>.mempalace_drawers (
    id              STRING        NOT NULL,   -- deterministic hash: sha256(wing + room + source_file + chunk_index)
    text            STRING        NOT NULL,   -- verbatim content (the "drawer")
    wing            STRING        NOT NULL,   -- person or project
    room            STRING        NOT NULL,   -- topic slug (e.g. "auth-migration")
    hall            STRING,                   -- memory type: hall_facts, hall_events, etc.
    source_file     STRING,                   -- original file path
    source_mtime    DOUBLE,                   -- file mtime at ingest (for re-mine detection)
    chunk_index     INT,                      -- position within source file
    date            STRING,                   -- ISO date from content, if detected
    importance      DOUBLE        DEFAULT 3.0,-- 1-5 importance score
    agent           STRING        DEFAULT 'mempalace',
    filed_at        TIMESTAMP     DEFAULT current_timestamp(),
    CONSTRAINT pk_drawers PRIMARY KEY (id)
)
USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'     -- required for Vector Search Delta Sync
);
```

### 2.2 — Vector Search Index

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.vectorsearch import (
    DeltaSyncVectorIndexSpecRequest,
    EmbeddingSourceColumn,
    VectorIndexType,
)

w = WorkspaceClient()

# One-time: create a Vector Search endpoint (or reuse existing)
w.vector_search_endpoints.create(name="mempalace_vs_endpoint")

# Create Delta Sync index with Databricks-managed embeddings
w.vector_search_indexes.create(
    name="<catalog>.<schema>.mempalace_drawers_index",
    endpoint_name="mempalace_vs_endpoint",
    primary_key="id",
    index_type=VectorIndexType.DELTA_SYNC,
    delta_sync_vector_index_spec=DeltaSyncVectorIndexSpecRequest(
        source_table="<catalog>.<schema>.mempalace_drawers",
        embedding_source_columns=[
            EmbeddingSourceColumn(
                name="text",
                embedding_model_endpoint_name="databricks-gte-large-en",
            )
        ],
        pipeline_type="TRIGGERED",           # or CONTINUOUS for real-time
        columns_to_sync=["id", "text", "wing", "room", "hall",
                         "source_file", "date", "importance", "filed_at"],
    ),
)
```

### 2.3 — Knowledge Graph: Entities

```sql
CREATE TABLE IF NOT EXISTS <catalog>.<schema>.mempalace_entities (
    id              STRING        NOT NULL,   -- lowercased, underscored name
    name            STRING        NOT NULL,   -- display name
    type            STRING        DEFAULT 'unknown',  -- person, project, tool, concept
    properties      STRING        DEFAULT '{}',       -- JSON blob
    created_at      TIMESTAMP     DEFAULT current_timestamp(),
    CONSTRAINT pk_entities PRIMARY KEY (id)
)
USING DELTA;
```

### 2.4 — Knowledge Graph: Triples

```sql
CREATE TABLE IF NOT EXISTS <catalog>.<schema>.mempalace_triples (
    id              STRING        NOT NULL,   -- deterministic hash
    subject         STRING        NOT NULL,   -- FK → entities.id
    predicate       STRING        NOT NULL,   -- relationship type
    object          STRING        NOT NULL,   -- FK → entities.id
    valid_from      STRING,                   -- ISO date
    valid_to        STRING,                   -- NULL = still current
    confidence      DOUBLE        DEFAULT 1.0,
    source_closet   STRING,
    source_file     STRING,
    extracted_at    TIMESTAMP     DEFAULT current_timestamp(),
    CONSTRAINT pk_triples PRIMARY KEY (id)
)
USING DELTA;
```

### 2.5 — Agent Diaries

```sql
CREATE TABLE IF NOT EXISTS <catalog>.<schema>.mempalace_diaries (
    id              STRING        NOT NULL,
    agent_name      STRING        NOT NULL,
    entry           STRING        NOT NULL,   -- AAAK-encoded diary line
    written_at      TIMESTAMP     DEFAULT current_timestamp(),
    CONSTRAINT pk_diaries PRIMARY KEY (id)
)
USING DELTA;
```

### 2.6 — Write-Ahead Log (Audit)

```sql
CREATE TABLE IF NOT EXISTS <catalog>.<schema>.mempalace_wal (
    timestamp       TIMESTAMP     NOT NULL,
    operation       STRING        NOT NULL,   -- add_drawer, delete_drawer, kg_add, etc.
    params          STRING        NOT NULL,   -- JSON of call params
    result          STRING,                   -- JSON of result
    caller          STRING        DEFAULT current_user()
)
USING DELTA;
```

### 2.7 — Volume for Config Files

```sql
CREATE VOLUME IF NOT EXISTS <catalog>.<schema>.mempalace_config;
```

Files stored inside:
- `config.json` — global settings (catalog, schema, people_map reference)
- `identity.txt` — Layer 0 identity text
- `wing_config.json` — wing definitions and keywords
- `people_map.json` — name variant → canonical name mapping

---

## 3 — File-by-File Changes

### 3.1 — `config.py` → Databricks-aware config

**Current**: Reads `~/.mempalace/config.json` from local filesystem.

**New**: `DatabricksConfig` class that resolves catalog/schema/volume paths.

```python
@dataclass(frozen=True)
class DatabricksConfig:
    catalog: str                      # e.g. "scratch"
    schema: str                       # e.g. "mempalace"
    vs_endpoint: str                  # e.g. "mempalace_vs_endpoint"
    vs_index_name: str                # derived: "<catalog>.<schema>.mempalace_drawers_index"
    drawers_table: str                # derived: "<catalog>.<schema>.mempalace_drawers"
    entities_table: str               # derived
    triples_table: str                # derived
    diaries_table: str                # derived
    wal_table: str                    # derived
    config_volume: str                # "/Volumes/<catalog>/<schema>/mempalace_config"
```

Priority: env vars (`MEMPALACE_CATALOG`, `MEMPALACE_SCHEMA`) > Volume config > defaults.

Sanitizers (`sanitize_name`, `sanitize_content`) are backend-agnostic — **keep as-is**.

### 3.2 — `palace.py` → Delta table operations

**Current**: `get_collection()` returns a ChromaDB collection. `file_already_mined()` queries ChromaDB metadata.

**New**: Replace with Spark SQL / `databricks-sql-connector` operations.

| Old function | New implementation |
| --- | --- |
| `get_collection(palace_path)` | `spark.table(config.drawers_table)` — or validate table exists |
| `file_already_mined(col, source_file)` | `SELECT 1 FROM drawers WHERE source_file = ? LIMIT 1` |
| `file_already_mined(..., check_mtime=True)` | `SELECT source_mtime FROM drawers WHERE source_file = ? LIMIT 1` then compare |

`SKIP_DIRS` constant is backend-agnostic — **keep as-is**.

### 3.3 — `searcher.py` → Vector Search client

**Current**: `chromadb.PersistentClient` → `col.query(query_texts=[...], where={...})`.

**New**: `VectorSearchClient` → `index.similarity_search(query_text=..., filters=..., columns=[...])`.

```python
from databricks.vector_search.client import VectorSearchClient

def search_memories(query: str, config: DatabricksConfig, wing: str = None,
                    room: str = None, n_results: int = 5) -> dict:
    vsc = VectorSearchClient()
    index = vsc.get_index(
        endpoint_name=config.vs_endpoint,
        index_name=config.vs_index_name,
    )

    # Build filter dict
    filters = {}
    if wing:
        filters["wing"] = wing
    if room:
        filters["room"] = room

    results = index.similarity_search(
        query_text=query,
        columns=["id", "text", "wing", "room", "source_file", "date"],
        num_results=n_results,
        filters=filters if filters else None,
    )

    # Map to existing return format
    hits = []
    for row in results.get("result", {}).get("data_array", []):
        hits.append({
            "text": row[1],
            "wing": row[2],
            "room": row[3],
            "source_file": row[4],
            "similarity": row[-1],   # score column
        })
    return {"query": query, "filters": {"wing": wing, "room": room}, "results": hits}
```

Key differences from ChromaDB:
- `query_texts` → `query_text` (single string, not list)
- `distances` (lower = better) → `score` (higher = better) — invert similarity display
- `where` dict syntax → `filters` dict (simpler key-value for standard endpoints)
- No `include` param — specify `columns` instead

### 3.4 — `knowledge_graph.py` → Delta tables via Spark SQL

**Current**: SQLite with `sqlite3.connect()`, raw SQL, `Row` factory.

**New**: Spark SQL via `spark.sql()` or `databricks-sql-connector` for external use.

| SQLite pattern | Delta equivalent |
| --- | --- |
| `CREATE TABLE IF NOT EXISTS` | DDL in setup notebook (idempotent) |
| `INSERT OR REPLACE` | `MERGE INTO ... USING ... WHEN MATCHED THEN UPDATE WHEN NOT MATCHED THEN INSERT` |
| `INSERT OR IGNORE` | `MERGE INTO ... WHEN NOT MATCHED THEN INSERT` |
| `UPDATE ... SET valid_to=?` | `UPDATE <table> SET valid_to = ? WHERE ...` |
| `SELECT ... WHERE subject=?` | `spark.sql("SELECT ... WHERE subject = '{eid}'")` or parameterized |
| `PRAGMA journal_mode=WAL` | N/A — Delta handles this natively |
| `conn.row_factory = sqlite3.Row` | Spark returns `Row` objects natively |

The `KnowledgeGraph` class keeps its public API (`add_entity`, `add_triple`, `invalidate`,
`query_entity`, `timeline`, `stats`). Only the internal `_conn()` / `_init_db()` / raw SQL changes.

### 3.5 — `palace_graph.py` → Delta aggregation queries

**Current**: Iterates ChromaDB metadata in batches of 1000 via `col.get(limit=1000, offset=...)`.

**New**: Single Spark SQL aggregation query:

```sql
SELECT room, wing, hall, date,
       COUNT(*) as count
FROM   <catalog>.<schema>.mempalace_drawers
WHERE  room != 'general' AND wing IS NOT NULL
GROUP BY room, wing, hall, date
```

This replaces the entire `build_graph()` Python loop. Tunnels, traversal, and stats
become SQL-derived instead of Python-iterated — significantly faster at scale.

### 3.6 — `layers.py` → Mixed Delta + Vector Search

| Layer | Current backend | New backend |
| --- | --- | --- |
| L0 (Identity) | `~/.mempalace/identity.txt` | Volume: `/Volumes/.../mempalace_config/identity.txt` |
| L1 (Essential Story) | ChromaDB `col.get()` sorted by importance | `SELECT * FROM drawers ORDER BY importance DESC LIMIT 15` |
| L2 (On-Demand) | ChromaDB `col.get(where=...)` | `SELECT * FROM drawers WHERE wing=? AND room=? LIMIT 10` |
| L3 (Deep Search) | ChromaDB `col.query(query_texts=...)` | Vector Search `index.similarity_search(query_text=...)` |

L0/L1/L2 are metadata lookups — plain Delta reads. Only L3 needs Vector Search.

### 3.7 — `miner.py` + `convo_miner.py` → Delta writes

**Current**: `collection.add(ids=[...], documents=[...], metadatas=[...])`.

**New**: Append rows to Delta table, then trigger VS index sync.

```python
from pyspark.sql import Row

rows = [Row(id=doc_id, text=content, wing=wing, room=room, hall=hall,
            source_file=str(filepath), source_mtime=mtime,
            chunk_index=idx, date=date_str, importance=3.0,
            agent=agent)]
df = spark.createDataFrame(rows)
df.write.mode("append").saveAsTable(config.drawers_table)

# After batch ingest, trigger VS index sync
vsc.get_index(...).sync()
```

Deduplication: use `MERGE INTO` keyed on `id` (deterministic hash) to avoid re-inserting
already-mined chunks. This replaces the `file_already_mined()` check-then-insert pattern.

### 3.8 — `mcp_server.py` → New backends, same tool signatures

All 19 MCP tools keep their exact names and parameter signatures. Only the internal
implementation changes. Organized by backend swap:

**ChromaDB → Delta reads (metadata queries):**
- `mempalace_status` — `SELECT COUNT(*), wing, room FROM drawers GROUP BY ...`
- `mempalace_list_wings` — `SELECT wing, COUNT(*) FROM drawers GROUP BY wing`
- `mempalace_list_rooms` — `SELECT room, COUNT(*) FROM drawers WHERE wing=? GROUP BY room`
- `mempalace_get_taxonomy` — `SELECT wing, room, COUNT(*) FROM drawers GROUP BY wing, room`
- `mempalace_check_duplicate` — `SELECT id FROM drawers WHERE id = ?`

**ChromaDB → Vector Search (semantic queries):**
- `mempalace_search` — `index.similarity_search(query_text=...)`

**ChromaDB → Delta writes:**
- `mempalace_add_drawer` — `INSERT INTO drawers VALUES (...)`
- `mempalace_delete_drawer` — `DELETE FROM drawers WHERE id = ?`

**SQLite → Delta (knowledge graph):**
- `mempalace_kg_query` — `SELECT ... FROM triples JOIN entities ...`
- `mempalace_kg_add` — `MERGE INTO entities ...; MERGE INTO triples ...`
- `mempalace_kg_invalidate` — `UPDATE triples SET valid_to=? WHERE ...`
- `mempalace_kg_timeline` — `SELECT ... FROM triples WHERE ... ORDER BY valid_from`
- `mempalace_kg_stats` — `SELECT COUNT(*) FROM entities; SELECT COUNT(*) FROM triples`

**ChromaDB metadata → Delta aggregation (graph nav):**
- `mempalace_traverse` — SQL aggregation + Python BFS (same logic, Delta source)
- `mempalace_find_tunnels` — SQL: `HAVING COUNT(DISTINCT wing) >= 2`
- `mempalace_graph_stats` — SQL aggregation

**Filesystem → Volume / Delta (diaries):**
- `mempalace_diary_write` — `INSERT INTO diaries VALUES (...)`
- `mempalace_diary_read` — `SELECT ... FROM diaries WHERE agent_name=? ORDER BY written_at DESC LIMIT ?`

**WAL:** `_wal_log()` → `INSERT INTO mempalace_wal VALUES (...)`.

### 3.9 — `pyproject.toml` → Dependency swap

```toml
# REMOVE
dependencies = [
    "chromadb>=0.5.0,<0.7",
]

# ADD
dependencies = [
    "databricks-sdk>=0.40.0",
    "databricks-vectorsearch>=0.40",
    "pyyaml>=6.0,<7",
]
```

`pyspark` is not listed as a dependency — it's provided by the Databricks runtime.
For external MCP server use, add `databricks-sql-connector>=3.0` as an optional dep.

---

## 4 — Execution Context Strategy

The codebase must work in **two contexts**:

| Context | Spark available? | Auth method | Primary use |
| --- | --- | --- | --- |
| **Notebook / Job** | Yes (`spark` global) | Automatic (workspace identity) | Mining, bulk ingest, setup |
| **MCP Server** (external process) | No | PAT / OAuth M2M via `databricks-sdk` | Real-time tool calls from Claude/Cursor |

### Dual-mode pattern

```python
def _get_spark():
    """Return SparkSession if available (notebook), else None (MCP external)."""
    try:
        from pyspark.sql import SparkSession
        return SparkSession.getActiveSession()
    except ImportError:
        return None

def _execute_sql(query: str, config: DatabricksConfig):
    """Run SQL via Spark if available, else via databricks-sql-connector."""
    spark = _get_spark()
    if spark:
        return spark.sql(query)
    else:
        from databricks import sql as dbsql
        with dbsql.connect(
            server_hostname=config.server_hostname,
            http_path=config.warehouse_http_path,
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchall()
```

Vector Search calls always use `databricks-sdk` (works in both contexts).

---

## 5 — Migration Path (Existing Local Data)

For users with an existing local ChromaDB palace:

```python
# migrate_to_databricks.py  (one-time script)

import chromadb
from pyspark.sql import Row

def migrate_drawers(local_palace_path: str, config: DatabricksConfig):
    """Export all ChromaDB drawers to Delta table."""
    client = chromadb.PersistentClient(path=local_palace_path)
    col = client.get_collection("mempalace_drawers")

    offset, batch_size = 0, 1000
    all_rows = []
    while True:
        batch = col.get(limit=batch_size, offset=offset,
                        include=["documents", "metadatas"])
        if not batch["ids"]:
            break
        for doc_id, doc, meta in zip(batch["ids"], batch["documents"], batch["metadatas"]):
            all_rows.append(Row(
                id=doc_id, text=doc,
                wing=meta.get("wing", "unknown"),
                room=meta.get("room", "unknown"),
                hall=meta.get("hall"),
                source_file=meta.get("source_file"),
                source_mtime=meta.get("source_mtime"),
                chunk_index=meta.get("chunk_index"),
                date=meta.get("date"),
                importance=meta.get("importance", 3.0),
                agent=meta.get("agent", "mempalace"),
            ))
        offset += len(batch["ids"])

    df = spark.createDataFrame(all_rows)
    df.write.mode("overwrite").saveAsTable(config.drawers_table)
    print(f"Migrated {len(all_rows)} drawers to {config.drawers_table}")

def migrate_kg(local_kg_path: str, config: DatabricksConfig):
    """Export SQLite KG to Delta tables."""
    import sqlite3
    conn = sqlite3.connect(local_kg_path)
    conn.row_factory = sqlite3.Row

    entities = [dict(r) for r in conn.execute("SELECT * FROM entities").fetchall()]
    triples = [dict(r) for r in conn.execute("SELECT * FROM triples").fetchall()]
    conn.close()

    spark.createDataFrame(entities).write.mode("overwrite").saveAsTable(config.entities_table)
    spark.createDataFrame(triples).write.mode("overwrite").saveAsTable(config.triples_table)
    print(f"Migrated {len(entities)} entities, {len(triples)} triples")
```

---

## 6 — Setup Notebook (Equivalent of `mempalace init`)

A single notebook that:

1. Creates the catalog + schema (if permitted)
2. Creates all Delta tables (DDL from Section 2)
3. Creates the Volume for config files
4. Creates or reuses a Vector Search endpoint
5. Creates the Delta Sync Vector Search index
6. Writes default config files to the Volume
7. Validates the setup with a test write + search round-trip

---

## 7 — Files Unchanged (Backend-Agnostic)

These files need **no changes** — they operate on strings/dicts, not storage:

| File | Why unchanged |
| --- | --- |
| `dialect.py` | Pure text transformation (AAAK compression) |
| `normalize.py` | Transcript format detection (5 formats → standard) |
| `entity_detector.py` | Regex/heuristic entity extraction from text |
| `entity_registry.py` | In-memory entity code registry |
| `general_extractor.py` | Content classification (decisions, preferences, etc.) |
| `split_mega_files.py` | File splitting — reads/writes local files before mining |
| `query_sanitizer.py` | Input sanitization for search queries |
| `dedup.py` | Deduplication logic (content hashing) |
| `spellcheck.py` | Optional spell checking |
| `version.py` | Version string |

---

## 8 — Open Questions

| # | Question | Options | Recommendation |
| --- | --- | --- | --- |
| 1 | **Catalog + schema** | `scratch.mempalace`, dedicated catalog, or configurable? | Configurable via env vars with `scratch.mempalace` as default |
| 2 | **Vector Search endpoint** | Create new or reuse existing? | Create `mempalace_vs_endpoint` — dedicated, avoids contention |
| 3 | **Sync mode** | `TRIGGERED` (manual sync after ingest) or `CONTINUOUS`? | `TRIGGERED` — cheaper, adequate for batch mining workflows |
| 4 | **MCP server execution** | External Python process (needs `databricks-sql-connector`) or Databricks serving endpoint? | Start with external process + SDK auth; serving endpoint is a future enhancement |
| 5 | **CLI** | Keep `mempalace` CLI or convert to notebook-only? | Keep CLI for MCP server compatibility; add notebook alternatives |
| 6 | **Embedding model** | `databricks-gte-large-en` or custom? | `databricks-gte-large-en` — production-grade, no setup |
| 7 | **Hybrid search** | Use `query_type="hybrid"` for keyword + semantic? | Yes — mirrors ChromaDB's default behavior more closely |

---

## 9 — Implementation Order

| Phase | Files | Estimated effort |
| --- | --- | --- |
| **Phase 1: Foundation** | `config.py`, `palace.py`, setup notebook | Core plumbing — everything depends on this |
| **Phase 2: Search** | `searcher.py`, `layers.py` | Highest-value feature — search must work first |
| **Phase 3: Knowledge Graph** | `knowledge_graph.py` | Self-contained module, clean swap |
| **Phase 4: Ingest** | `miner.py`, `convo_miner.py` | Writes to Delta instead of ChromaDB |
| **Phase 5: Graph Nav** | `palace_graph.py` | Derives from Delta metadata — fast once Phase 1 is done |
| **Phase 6: MCP Server** | `mcp_server.py` | Wires everything together — do last |
| **Phase 7: Migration** | `migrate_to_databricks.py` | One-time script for existing users |
| **Phase 8: Tests** | `tests/test_*.py` | Adapt fixtures to use Delta/VS mocks |

---

## 10 — Risk & Mitigations

| Risk | Impact | Mitigation |
| --- | --- | --- |
| Vector Search latency > ChromaDB local | Search feels slower | Use `query_type="hybrid"` + tune `num_results`. ChromaDB was in-process; VS is a network call but returns in <200ms for small indexes. |
| Delta write latency for single-row inserts | `add_drawer` MCP tool feels slow | Batch writes where possible. For single-row MCP writes, use `INSERT INTO` via SQL warehouse (sub-second). |
| Embedding model drift | Results differ from ChromaDB's default model | Accept: `databricks-gte-large-en` is higher quality than ChromaDB's default `all-MiniLM-L6-v2`. Re-benchmark on LongMemEval after migration. |
| External MCP server auth | Needs PAT or OAuth setup | Document in setup guide. `databricks-sdk` handles token refresh automatically. |
| Concurrent writes from multiple MCP sessions | Delta handles this natively (ACID) | Non-issue — Delta's optimistic concurrency handles it. ChromaDB had worse semantics here. |
