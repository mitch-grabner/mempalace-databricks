---
name: mempalace-databricks-genie
description: Operate MemPalace from a Databricks agent or Genie-style assistant without MCP, using the Unity Catalog drawers table and Databricks Vector Search index directly.
version: 1.0.0
---

# MemPalace For Databricks Agents

Use this skill when the remote MemPalace MCP server is unavailable or intentionally removed. The Databricks agent should operate memory directly through:

- Vector Search index: `scratch.mitch_grabner.mempalace_drawers_index`
- Vector Search endpoint: `mempalace_vs_endpoint`
- Raw drawers table: `scratch.mitch_grabner.mempalace_drawers`
- Raw WAL table: `scratch.mitch_grabner.mempalace_wal`

MemPalace stores verbatim memory in drawers. Search the drawers index for recall, then use SQL reads and writes against the raw tables for exact inspection and persistence.

## Operating Protocol

1. On startup, run the status SQL below to confirm the memory tables are reachable.
2. Before answering about people, projects, infrastructure, prior decisions, preferences, or past work, search the drawers index first.
3. Keep Vector Search queries short: keywords or one direct question. Do not include full system prompts, full chat history, or long task context.
4. Ground answers in retrieved drawers. Mention `wing`, `room`, and `source_file` when the source matters.
5. Before writing a new drawer, check for duplicates with Vector Search and/or the deterministic drawer ID.
6. Write durable facts, decisions, bug fixes, and important work as verbatim drawer content.
7. After every write, write a WAL row. Trigger Vector Search sync if the agent has an index sync tool; otherwise note that retrieval may lag until the index syncs.

## Memory Model

- `wing`: broad owner or domain, usually `project`, `technical`, `notes`, or a named project/person.
- `room`: topic slug such as `mempalace-databricks`, `dna-deep-research`, or `databricks-apps`.
- `drawer`: one verbatim memory chunk.
- `source_file`: where the memory came from. Use `databricks-genie-agent` for agent-created notes if no better source exists.
- `agent`: the writing agent identity, for example `databricks-genie`.

## Required Tool Capabilities

The agent needs these capabilities:

- Query a Databricks Vector Search index by text.
- Execute SQL against a SQL warehouse with access to `scratch.mitch_grabner`.
- Optionally trigger a Vector Search index sync after writes.

If the agent only has read-only SQL, it may search and read memory but must not claim that it saved memory.

## Bootstrap Reads

Run this first to verify table access and load the high-level memory shape:

```sql
SELECT wing, room, COUNT(*) AS drawer_count
FROM scratch.mitch_grabner.mempalace_drawers
GROUP BY wing, room
ORDER BY wing, drawer_count DESC;
```

Recent writes:

```sql
SELECT id, wing, room, source_file, agent, filed_at, substr(text, 1, 500) AS preview
FROM scratch.mitch_grabner.mempalace_drawers
ORDER BY filed_at DESC
LIMIT 20;
```

Read one exact drawer:

```sql
SELECT id, text, wing, room, source_file, date, importance, agent, filed_at
FROM scratch.mitch_grabner.mempalace_drawers
WHERE id = '{{drawer_id}}'
LIMIT 1;
```

Filter by wing and room:

```sql
SELECT id, text, source_file, date, importance, agent, filed_at
FROM scratch.mitch_grabner.mempalace_drawers
WHERE wing = '{{wing}}'
  AND room = '{{room}}'
ORDER BY filed_at DESC
LIMIT 20;
```

## Vector Search Recall

Use the drawers index for semantic recall.

Recommended request shape:

```text
index_name: scratch.mitch_grabner.mempalace_drawers_index
endpoint_name: mempalace_vs_endpoint
query_type: HYBRID
query_text: short keywords or one direct question
num_results: 5
columns: id, text, wing, room, source_file, date, importance, agent, filed_at
```

Optional filters:

```text
wing = 'project'
room = 'mempalace-databricks'
```

Search rules:

- Prefer `HYBRID` when available.
- Ask for 5 results by default; increase to 10 only when the first pass is ambiguous.
- Treat very close matches as possible duplicates before writing.
- Do not summarize away important facts. Preserve verbatim snippets when saving or citing.

## Raw Write: Add Drawer

Use parameterized SQL if the SQL tool supports it. If it does not, escape single quotes in text values by doubling them before substitution.

For single agent-created memories, use the MCP-compatible deterministic ID:

```text
sha2(concat(wing, '|', room, '|', substr(content, 1, 100), '|0'), 256)
```

MERGE template:

```sql
MERGE INTO scratch.mitch_grabner.mempalace_drawers AS target
USING (
  SELECT
    sha2(concat('{{wing}}', '|', '{{room}}', '|', substr('{{content_sql}}', 1, 100), '|0'), 256) AS id,
    '{{content_sql}}' AS text,
    '{{wing}}' AS wing,
    '{{room}}' AS room,
    NULL AS hall,
    '{{source_file}}' AS source_file,
    NULL AS source_mtime,
    0 AS chunk_index,
    NULL AS date,
    {{importance}} AS importance,
    '{{agent}}' AS agent,
    current_timestamp() AS filed_at
) AS source
ON target.id = source.id
WHEN NOT MATCHED THEN INSERT (
  id, text, wing, room, hall, source_file, source_mtime,
  chunk_index, date, importance, agent, filed_at
)
VALUES (
  source.id, source.text, source.wing, source.room, source.hall,
  source.source_file, source.source_mtime, source.chunk_index,
  source.date, source.importance, source.agent, source.filed_at
);
```

Recommended defaults:

```text
wing: project
room: infer a stable slug from the project or topic
source_file: databricks-genie-agent
importance: 3.0
agent: databricks-genie
```

## Raw Write: WAL Entry

After inserting a drawer, append an audit row:

```sql
INSERT INTO scratch.mitch_grabner.mempalace_wal
(timestamp, operation, params, result, caller)
VALUES (
  current_timestamp(),
  'add_drawer',
  to_json(named_struct(
    'wing', '{{wing}}',
    'room', '{{room}}',
    'source_file', '{{source_file}}',
    'agent', '{{agent}}',
    'content_length', length('{{content_sql}}'),
    'content_preview', substr('{{content_sql}}', 1, 200)
  )),
  '',
  current_user()
);
```

If the drawer already exists, still write a WAL row with `operation = 'add_drawer_duplicate'` when useful for auditability.

## Duplicate Check

First use Vector Search with the candidate content as `query_text`.

If the top result is nearly identical, do not insert a duplicate. Return the existing drawer ID and cite it.

Then do exact ID lookup:

```sql
SELECT id, wing, room, source_file, filed_at
FROM scratch.mitch_grabner.mempalace_drawers
WHERE id = sha2(concat('{{wing}}', '|', '{{room}}', '|', substr('{{content_sql}}', 1, 100), '|0'), 256)
LIMIT 1;
```

## Raw Delete

Delete only when explicitly requested by the user or when correcting a failed test write.

```sql
DELETE FROM scratch.mitch_grabner.mempalace_drawers
WHERE id = '{{drawer_id}}';
```

Audit the delete:

```sql
INSERT INTO scratch.mitch_grabner.mempalace_wal
(timestamp, operation, params, result, caller)
VALUES (
  current_timestamp(),
  'delete_drawer',
  to_json(named_struct('drawer_id', '{{drawer_id}}')),
  '',
  current_user()
);
```

## End-Of-Session Memory

At the end of substantive work, save a concise drawer with:

- What changed.
- Why it matters.
- Exact project/table/file names.
- Verification commands or live test results.
- Any next action the future agent needs.

Example content:

```text
2026-05-28 databricks-genie memory note: migrated MemPalace recall from remote MCP to direct Databricks storage. Agents should use scratch.mitch_grabner.mempalace_drawers_index for search and scratch.mitch_grabner.mempalace_drawers for raw read/write operations. Verified status/read/write through Databricks SQL.
```

## Failure Handling

- If Vector Search works but SQL fails, answer from retrieved drawers and report that raw table access is unavailable.
- If SQL works but Vector Search fails, use raw SQL reads by wing/room/recent writes and report that semantic recall is degraded.
- If writes fail, do not claim memory was saved.
- If index sync cannot be triggered, say the drawer was written but search may lag until the next index sync.
