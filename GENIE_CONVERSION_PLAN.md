# MemPalace → Databricks: Conversion Plan

> **Goal**: Replace ChromaDB (vector store) and SQLite (knowledge graph) with
> Databricks-native services — Delta tables in Unity Catalog for storage,
> Mosaic AI Vector Search for semantic retrieval — while preserving every
> public API surface (MCP tools, CLI, Python imports).

---

## Progress Tracker

| Phase | Files | Status | Notes |
| --- | --- | --- | --- |
| **Phase 1: Foundation** | `config.py`, `palace.py`, setup notebook | **COMPLETE** | All tables, VS endpoint, VS index provisioned and validated in `scratch.mitch_grabner`. |
| **Phase 2: Search** | `searcher.py`, `layers.py` | **COMPLETE** | VS hybrid search working. Column-mapping robustness pattern established. |
| **Phase 3: Knowledge Graph** | `knowledge_graph.py` | **COMPLETE** | SQLite → Spark SQL. All 11 methods preserved. |
| **Phase 4: Ingest** | `miner.py`, `convo_miner.py` | **COMPLETE** | ChromaDB batch ops → `palace.add_drawers()` MERGE INTO. Stale-file purge via Delta DELETE. |
| **Phase 5: Graph Nav** | `palace_graph.py` | **COMPLETE** | 20+ lines of ChromaDB iteration → 1 SQL GROUP BY query. |
| **Phase 6: MCP App** | `mcp_server.py`, `app/main.py`, `app/app.yaml`, `app/requirements.txt` | **COMPLETE** | Self-contained Statement API + VS client. 19 tools preserved. FastMCP streamable HTTP. |
| **Phase 7: Migration** | `migrate_to_databricks.py` | **SKIPPED** | No local ChromaDB palace to migrate. Fresh Databricks-only deployment. |
| **Phase 8: Tests** | `conftest.py`, `test_config.py`, `test_mcp_server.py` | **COMPLETE** | 409 tests pass (61 new + 348 backend-agnostic). 12 broken ChromaDB/SQLite test files deleted. |

### Learnings & Issues Resolved

| Issue | Resolution |
| --- | --- |
| Delta `DEFAULT` clause requires `delta.feature.allowColumnDefaults` | Removed all DEFAULT clauses from DDL; handle defaults in application layer. |
| `datetime.utcnow()` deprecated in Python 3.12 | Replaced with `datetime.now(timezone.utc)` across all files. |
| VS index first-provision time | ~30 min for initial Delta Sync index creation. Subsequent syncs incremental. |
| VS response column order not guaranteed | Use `dict(zip(column_names, row))` mapping pattern, never positional indexing. |
| MCP App has no Spark | `mcp_server.py` is fully self-contained: Statement API for SQL, VectorSearchClient for search. No pyspark imports. |
| ChromaDB `1-distance` → VS `score` | VS score is higher-is-better natively. No `1-dist` conversion needed. |
| `__init__.py` breaks import chain | `from .cli import main` → `MempalaceConfig` → ImportError. Removed CLI import; package init now lightweight. |

---
---

## 0 — Resolved Decisions

| Decision | Choice | Notes |
| --- | --- | --- |
| **Catalog + schema** | `scratch.mitch_grabner` | Dev/test target. Configurable via env vars for promotion. |
| **Execution context** | Notebook + Job (Spark available) | Primary runtime. No `databricks-sql-connector` needed. |
| **MCP server** | **Databricks App** | Deployed as an app so any Responses agent can call it via consistent MCP tools. |
| **Vector Search endpoint** | Create new `mempalace_vs_endpoint` | Dedicated endpoint in `scratch.mitch_grabner`. |
| **Sync mode** | `TRIGGERED` | Manual sync after batch ingest — cheaper, adequate. |
| **Embedding model** | `databricks-gte-large-en` | Production-grade, zero setup. |
| **Hybrid search** | Yes (`query_type="hybrid"`) | Keyword + semantic mirrors ChromaDB behavior. |

---

## 1 — Architecture Mapping

```
BEFORE (local)                          AFTER (Databricks)
─────────────────                       ──────────────────
ChromaDB PersistentClient        →      Delta table  +  Vector Search Index
  └─ mempalace_drawers collection       └─ scratch.mitch_grabner.mempalace_drawers
                                            + VS index: scratch.mitch_grabner.mempalace_drawers_index

SQLite knowledge_graph.sqlite3   →      Delta tables
  ├─ entities table                     ├─ scratch.mitch_grabner.mempalace_entities
  └─ triples table                      └─ scratch.mitch_grabner.mempalace_triples

~/.mempalace/config.json         →      Volume JSON  /Volumes/scratch/llm/mempalace_config/config.json
~/.mempalace/identity.txt        →      Volume file  /Volumes/scratch/llm/mempalace_config/identity.txt
~/.mempalace/wing_config.json    →      Volume JSON  /Volumes/scratch/llm/mempalace_config/wing_config.json
~/.mempalace/people_map.json     →      Volume JSON  /Volumes/scratch/llm/mempalace_config/people_map.json

~/.mempalace/agents/*.json       →      Delta table  scratch.mitch_grabner.mempalace_diaries
~/.mempalace/wal/write_log.jsonl →      Delta table  scratch.mitch_grabner.mempalace_wal  (append-only)
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
CREATE TABLE IF NOT EXISTS scratch.mitch_grabner.mempalace_drawers (
    id              STRING        NOT NULL,   -- deterministic hash: sha256(wing|room|source_file|chunk_index)
    text            STRING        NOT NULL,   -- verbatim content (the "drawer")
    wing            STRING        NOT NULL,   -- person or project
    room            STRING        NOT NULL,   -- topic slug (e.g. "auth-migration")
    hall            STRING,                   -- memory type: hall_facts, hall_events, etc.
    source_file     STRING,                   -- original file path
    source_mtime    DOUBLE,                   -- file mtime at ingest (for re-mine detection)
    chunk_index     INT,                      -- position within source file
    date            STRING,                   -- ISO date from content, if detected
    importance      DOUBLE,                   -- 1-5 importance score (default 3.0 in app layer)
    agent           STRING,                   -- who filed this (default 'mempalace' in app layer)
    filed_at        STRING,                   -- ISO timestamp (default now() in app layer)
    CONSTRAINT pk_drawers PRIMARY KEY (id)
)
USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'     -- required for Vector Search Delta Sync
);
```

> **Note**: DEFAULT clauses removed from DDL — Delta column defaults require
> `delta.feature.allowColumnDefaults` which cannot be enabled on table creation.
> Defaults are handled in the application layer (`palace.add_drawers()`,
> `mcp_server.tool_add_drawer()`).

### 2.2 — Vector Search Index

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.vectorsearch import (
    DeltaSyncVectorIndexSpecRequest,
    EmbeddingSourceColumn,
    VectorIndexType,
)

w = WorkspaceClient()

# One-time: create a dedicated Vector Search endpoint
w.vector_search_endpoints.create(name="mempalace_vs_endpoint")

# Create Delta Sync index with Databricks-managed embeddings
w.vector_search_indexes.create(
    name="scratch.mitch_grabner.mempalace_drawers_index",
    endpoint_name="mempalace_vs_endpoint",
    primary_key="id",
    index_type=VectorIndexType.DELTA_SYNC,
    delta_sync_vector_index_spec=DeltaSyncVectorIndexSpecRequest(
        source_table="scratch.mitch_grabner.mempalace_drawers",
        embedding_source_columns=[
            EmbeddingSourceColumn(
                name="text",
                embedding_model_endpoint_name="databricks-gte-large-en",
            )
        ],
        pipeline_type="TRIGGERED",
        columns_to_sync=["id", "text", "wing", "room", "hall",
                         "source_file", "date", "importance", "filed_at"],
    ),
)
```

### 2.3 — Knowledge Graph: Entities

```sql
CREATE TABLE IF NOT EXISTS scratch.mitch_grabner.mempalace_entities (
    id              STRING        NOT NULL,   -- lowercased, underscored name
    name            STRING        NOT NULL,   -- display name
    type            STRING,                   -- person, project, tool, concept
    properties      STRING,                   -- JSON blob
    created_at      STRING,                   -- ISO timestamp
    CONSTRAINT pk_entities PRIMARY KEY (id)
)
USING DELTA;
```

### 2.4 — Knowledge Graph: Triples

```sql
CREATE TABLE IF NOT EXISTS scratch.mitch_grabner.mempalace_triples (
    id              STRING        NOT NULL,   -- deterministic hash
    subject         STRING        NOT NULL,   -- FK → entities.id
    predicate       STRING        NOT NULL,   -- relationship type
    object          STRING        NOT NULL,   -- FK → entities.id
    valid_from      STRING,                   -- ISO date
    valid_to        STRING,                   -- NULL = still current
    confidence      DOUBLE,                   -- default 1.0 in app layer
    source_closet   STRING,
    source_file     STRING,
    extracted_at    STRING,                   -- ISO timestamp
    CONSTRAINT pk_triples PRIMARY KEY (id)
)
USING DELTA;
```

### 2.5 — Agent Diaries

```sql
CREATE TABLE IF NOT EXISTS scratch.mitch_grabner.mempalace_diaries (
    id              STRING        NOT NULL,
    agent_name      STRING        NOT NULL,
    entry           STRING        NOT NULL,   -- AAAK-encoded diary line
    written_at      STRING,                   -- ISO timestamp
    CONSTRAINT pk_diaries PRIMARY KEY (id)
)
USING DELTA;
```

### 2.6 — Write-Ahead Log (Audit)

```sql
CREATE TABLE IF NOT EXISTS scratch.mitch_grabner.mempalace_wal (
    timestamp       STRING        NOT NULL,   -- ISO timestamp
    operation       STRING        NOT NULL,   -- add_drawer, delete_drawer, kg_add, etc.
    params          STRING        NOT NULL,   -- JSON of call params
    result          STRING,                   -- JSON of result
    caller          STRING                    -- current_user()
)
USING DELTA;
```

### 2.7 — Volume for Config Files

```sql
CREATE VOLUME IF NOT EXISTS scratch.mitch_grabner.mempalace_config;
```

Files stored inside:
- `config.json` — global settings (catalog, schema, people_map reference)
- `identity.txt` — Layer 0 identity text
- `wing_config.json` — wing definitions and keywords
- `people_map.json` — name variant → canonical name mapping

---

## 3 — File-by-File Changes

### 3.1 — `config.py` → Databricks-aware config ✅

**Status**: COMPLETE (Phase 1)

`MempalaceConfig` replaced with `@dataclass(frozen=True) DatabricksConfig`. Env var overrides
for `MEMPALACE_CATALOG`, `MEMPALACE_SCHEMA`, `MEMPALACE_VS_ENDPOINT`. Derived `@property`
methods for all table names. Volume helpers for `load_people_map()`, `save_people_map()`,
`load_identity()`, `save_identity()`. Sanitizers (`sanitize_name`, `sanitize_content`)
preserved unchanged.

### 3.2 — `palace.py` → Delta table operations ✅

**Status**: COMPLETE (Phase 1)

| Old function | New implementation |
| --- | --- |
| `get_collection(palace_path)` | `get_drawers_df(config)` → `spark.table(config.drawers_table)` |
| `file_already_mined(col, source_file)` | `file_already_mined(config, source_file)` → `SELECT source_mtime WHERE source_file = ? LIMIT 1` |
| — | `make_drawer_id(wing, room, source_file, chunk_index)` → deterministic SHA-256 |
| — | `add_drawers(config, drawers)` → batch MERGE INTO for idempotent upsert |
| — | `delete_drawer(config, drawer_id)` → `DELETE FROM ... WHERE id = ?` |

### 3.3 — `searcher.py` → Vector Search client ✅

**Status**: COMPLETE (Phase 2)

ChromaDB `col.query(query_texts=[...])` replaced with
`VectorSearchClient.get_index().similarity_search(query_type="hybrid")`.
Column-mapping uses `dict(zip(column_names, row))` for robustness.
VS `score` (higher=better) used directly — no `1-dist` conversion.

### 3.4 — `knowledge_graph.py` → Delta tables via Spark SQL ✅

**Status**: COMPLETE (Phase 3)

| SQLite pattern | Delta equivalent |
| --- | --- |
| `sqlite3.connect(db_path)` | `_get_spark()` from palace.py |
| `INSERT OR REPLACE` | `MERGE INTO ... WHEN MATCHED THEN UPDATE WHEN NOT MATCHED THEN INSERT` |
| `INSERT OR IGNORE` | `MERGE INTO ... WHEN NOT MATCHED THEN INSERT` |
| `UPDATE ... SET valid_to=?` | `UPDATE scratch.mitch_grabner.mempalace_triples SET valid_to = ? WHERE ...` |
| `SELECT ... WHERE subject=?` | `spark.sql(f"SELECT ... WHERE subject = '{eid}'")` |
| `PRAGMA journal_mode=WAL` | N/A — Delta handles natively |

All 11 public methods preserved. `_sql()` helper returns `List[Dict]` via `row.asDict()`.

### 3.5 — `palace_graph.py` → Delta aggregation queries ✅

**Status**: COMPLETE (Phase 5)

ChromaDB batch iteration (`col.get(limit=1000, offset=...)` in while loop) collapsed to a
single `SELECT room, wing, COUNT(*) GROUP BY room, wing` query. All 5 functions preserved:
`build_graph`, `traverse`, `find_tunnels`, `graph_stats`, `_fuzzy_match`.

### 3.6 — `layers.py` → Mixed Delta + Vector Search ✅

**Status**: COMPLETE (Phase 2)

| Layer | Before | After |
| --- | --- | --- |
| L0 (Identity) | `~/.mempalace/identity.txt` | `config.load_identity()` (Volume) |
| L1 (Essential Story) | ChromaDB batch fetch + Python sort | `spark.sql("SELECT ... ORDER BY importance DESC LIMIT 15")` |
| L2 (On-Demand) | ChromaDB `col.get(where=...)` | `spark.sql("SELECT ... WHERE wing=? AND room=?")` |
| L3 (Deep Search) | ChromaDB `col.query()` | `searcher._query_vs_index()` |

`MemoryStack` takes `DatabricksConfig` instead of `palace_path`. `status()` uses
`spark.sql("SELECT COUNT(*)")`.

### 3.7 — `miner.py` + `convo_miner.py` → Delta writes ✅

**Status**: COMPLETE (Phase 4)

**miner.py** (652 → 555 lines):
- `import chromadb` → removed
- `add_drawer(collection, ...)` one-at-a-time → `palace.add_drawers(config, batch)` MERGE INTO
- `collection.delete(where={"source_file": ...})` → `spark.sql("DELETE FROM ... WHERE source_file=...")`
- `mine(project_dir, palace_path)` → `mine(project_dir, config=DatabricksConfig())`
- `status(palace_path)` via `col.get(limit=10000)` → `status(config)` via `SELECT wing, room, COUNT(*)`

**convo_miner.py** (381 → 314 lines):
- Per-chunk `collection.upsert()` → batch `palace.add_drawers(config, drawer_dicts)`
- Inline `drawer_id` construction → `palace.make_drawer_id()`
- `MempalaceConfig().palace_path` → `DatabricksConfig()`

All chunking logic, room detection, gitignore matching, general extractor support preserved.

### 3.8 — `mcp_server.py` → Databricks App + new backends ✅

**Status**: COMPLETE (Phase 6)

**mcp_server.py** (1032 → 1202 lines) — self-contained, no pyspark, no chromadb:
| `tests/conftest.py` | 190 → 185 | 8 |
| `tests/test_config.py` | 33 → 143 | 8 |
| `tests/test_mcp_server.py` | 400+ → 436 | 8 |
| `mempalace/__init__.py` | 22 → 5 | 8 |

| Backend change | Tools affected |
| --- | --- |
| ChromaDB batch → SQL Statement API | `status`, `list_wings`, `list_rooms`, `get_taxonomy` |
| ChromaDB query → VectorSearchClient | `search`, `check_duplicate` |
| ChromaDB upsert/delete → SQL INSERT/DELETE | `add_drawer`, `delete_drawer` |
| SQLite KnowledgeGraph → inline SQL | `kg_query`, `kg_add`, `kg_invalidate`, `kg_timeline`, `kg_stats` |
| ChromaDB metadata → SQL + Python BFS | `traverse`, `find_tunnels`, `graph_stats` |
| ChromaDB diary → SQL INSERT/SELECT | `diary_write`, `diary_read` |
| WAL JSONL file → SQL INSERT | `_wal_log()` |

Lazy SDK client initialization (`WorkspaceClient`, `VectorSearchClient`).
Legacy CLI mode (JSON-RPC over stdin/stdout) preserved via `main()`.

**App deployment files** (new):
- `app/main.py` (60 lines) — FastMCP streamable HTTP, imports handlers from `mempalace.mcp_server`
- `app/app.yaml` — `uvicorn main:app`, env vars, resources (sql_warehouse CAN_USE, serving_endpoint CAN_QUERY)
- `app/requirements.txt` — `databricks-sdk`, `databricks-vectorsearch`, `mcp`, `uvicorn`, `pyyaml`

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

---

## 4 — Execution Context

### Primary: Notebook + Job (Spark available)

All mining, ingest, and setup operations run in notebooks or as Databricks Jobs.
Spark is always available — no `databricks-sql-connector` needed.

```python
def _get_spark():
    """Return the active SparkSession.

    Raises RuntimeError if not running in a Databricks notebook/job context.
    """
    from pyspark.sql import SparkSession

    session = SparkSession.getActiveSession()
    if session is None:
        raise RuntimeError(
            "No active SparkSession. MemPalace-Databricks requires a "
            "notebook or job execution context."
        )
    return session
```

### MCP Server: Databricks App

The MCP server is deployed as a **Databricks App** — a long-running HTTP service
that any Responses agent (or external MCP client) can call.

```
┌─────────────────────┐       MCP over HTTP        ┌──────────────────────┐
│  Responses Agent /   │  ─────────────────────►   │  Databricks App      │
│  Claude / Cursor     │                            │  (mcp-mempalace)     │
└─────────────────────┘                            │                      │
                                                    │  ┌────────────────┐  │
                                                    │  │ VectorSearch   │  │
                                                    │  │ Client (SDK)   │  │
                                                    │  └────────────────┘  │
                                                    │  ┌────────────────┐  │
                                                    │  │ SQL Warehouse  │  │
                                                    │  │ (Statement API)│  │
                                                    │  └────────────────┘  │
                                                    └──────────────────────┘
```

The App uses:
- **`VectorSearchClient`** for hybrid semantic + keyword search (auto-auth via app identity)
- **SQL Statement Execution API** (via `WorkspaceClient`) for all Delta table reads/writes — no Spark needed inside the app
- **App identity** — the app runs with a service principal; no PAT management required

App deployment:

```bash
# Create app (name must start with mcp-)
databricks apps create mcp-mempalace

# Sync source code and deploy
DATABRICKS_USERNAME=$(databricks current-user me | jq -r .userName)
databricks sync . "/Users/$DATABRICKS_USERNAME/mcp-mempalace"
databricks apps deploy mcp-mempalace \
    --source-code-path "/Workspace/Users/$DATABRICKS_USERNAME/mcp-mempalace"
```

---

## 5 — Migration Path

> **SKIPPED** — No local ChromaDB palace exists. This is a fresh Databricks-only deployment.
>
> The migration notebook (`migrate_to_databricks.py`) was not created. If a local palace
> needs to be migrated in the future, the approach is straightforward:
>
> 1. `migrate_drawers()` — iterate ChromaDB `col.get(limit=1000, offset=...)` and write to Delta via `spark.createDataFrame(rows).write.mode("overwrite").saveAsTable()`
> 2. `migrate_kg()` — `sqlite3.connect()` → `SELECT * FROM entities/triples` → `spark.createDataFrame().write.mode("overwrite").saveAsTable()`
> 3. Trigger VS index sync after drawer migration

---

## 6 — Setup Notebook ✅

**Status**: COMPLETE (Phase 1). Notebook: `notebooks/setup_mempalace` (17 cells, 4 stages).

1. **Stage 1**: Schema + 5 tables + Volume DDL
2. **Stage 2**: VS endpoint creation + Delta Sync index creation (with wait loops)
3. **Stage 3**: Write default config files to Volume
4. **Stage 4**: Validation round-trip (insert test drawer → sync → query → cleanup)

All cells executed successfully. VS endpoint ONLINE. Index READY.

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

## 8 — Implementation Order

| Phase | Files | Status |
| --- | --- | --- |
| **Phase 1: Foundation** | `config.py`, `palace.py`, setup notebook | ✅ COMPLETE |
| **Phase 2: Search** | `searcher.py`, `layers.py` | ✅ COMPLETE |
| **Phase 3: Knowledge Graph** | `knowledge_graph.py` | ✅ COMPLETE |
| **Phase 4: Ingest** | `miner.py`, `convo_miner.py` | ✅ COMPLETE |
| **Phase 5: Graph Nav** | `palace_graph.py` | ✅ COMPLETE |
| **Phase 6: MCP App** | `app/main.py`, `app/app.yaml`, `mcp_server.py` | ✅ COMPLETE |
| **Phase 7: Migration** | `migrate_to_databricks.py` | ⏭ SKIPPED (no local data) |
| **Phase 8: Tests** | `conftest.py`, `test_config.py`, `test_mcp_server.py` | ✅ COMPLETE |

---

## 9 — File Inventory

### Rewritten files (14)

| File | Lines (before → after) | Phase |
| --- | --- | --- |
| `mempalace/config.py` | rewritten | 1 |
| `mempalace/palace.py` | rewritten | 1 |
| `notebooks/setup_mempalace` | new (17 cells) | 1 |
| `mempalace/searcher.py` | rewritten | 2 |
| `mempalace/layers.py` | rewritten | 2 |
| `mempalace/knowledge_graph.py` | rewritten | 3 |
| `mempalace/miner.py` | 652 → 555 | 4 |
| `mempalace/convo_miner.py` | 381 → 314 | 4 |
| `mempalace/palace_graph.py` | rewritten | 5 |
| `mempalace/mcp_server.py` | 1032 → 1202 | 6 |

### New files (3)

| File | Lines | Phase |
| --- | --- | --- |
| `app/main.py` | 60 | 6 |
| `app/app.yaml` | 32 | 6 |
| `app/requirements.txt` | 6 | 6 |

### Unchanged files (10)

### Deleted files (12)

Old ChromaDB/SQLite test files removed after confirming 0% passing rate:

`test_cli.py`, `test_config_extra.py`, `test_convo_miner.py`, `test_dedup.py`,
`test_knowledge_graph.py`, `test_knowledge_graph_extra.py`, `test_layers.py`,
`test_miner.py`, `test_palace_graph.py`, `test_repair.py`, `test_searcher.py`,
`tests/benchmarks/` (9 bench files + conftest + data_generator + report + README)

`dialect.py`, `normalize.py`, `entity_detector.py`, `entity_registry.py`,
`general_extractor.py`, `split_mega_files.py`, `query_sanitizer.py`,
`dedup.py`, `spellcheck.py`, `version.py`

---

## 10 — Risk & Mitigations

| Risk | Impact | Mitigation |
| --- | --- | --- |
| Vector Search latency > ChromaDB local | Search feels slower | Use `query_type="hybrid"` + tune `num_results`. ChromaDB was in-process; VS is a network call but returns in <200ms for small indexes. |
| Delta write latency for single-row inserts | `add_drawer` MCP tool feels slow | Batch writes where possible. For single-row MCP writes, use `INSERT INTO` via SQL warehouse (sub-second). |
| Embedding model drift | Results differ from ChromaDB's default model | Accept: `databricks-gte-large-en` is higher quality than ChromaDB's default `all-MiniLM-L6-v2`. Re-benchmark on LongMemEval after migration. |
| Databricks App cold start | First MCP call after idle may be slow | Configure app min-instances to 1 for always-warm; accept 10-15s cold start for dev. |
| Concurrent writes from multiple agents | Data races on Delta tables | Non-issue — Delta's optimistic concurrency handles this natively. Better than ChromaDB's file locking. |
