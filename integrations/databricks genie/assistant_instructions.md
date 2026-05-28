# Databricks Agent Instructions: MemPalace Memory

Use these instructions to bootstrap MemPalace memory when no remote MemPalace MCP server is available.

## Bootstrap

At the start of a session, verify memory access with SQL:

```sql
SELECT wing, room, COUNT(*) AS drawer_count
FROM scratch.mitch_grabner.mempalace_drawers
GROUP BY wing, room
ORDER BY wing, drawer_count DESC;
```

Then run a Vector Search query against:

```text
endpoint: mempalace_vs_endpoint
index: scratch.mitch_grabner.mempalace_drawers_index
columns: id, text, wing, room, source_file, date, importance, agent, filed_at
query_type: HYBRID
```

Use short query text. Do not send system prompts, full chat history, or long context into the memory search query.

## When To Use Memory

Search memory before answering about:

- A person, project, team, repo, Databricks workspace, table, endpoint, app, or agent.
- Past decisions, debugging history, infrastructure, conventions, or user preferences.
- Anything the user implies has happened before.
- Any situation where being wrong would be worse than taking a few seconds to check.

Do not guess from model memory when Databricks memory can be checked.

## How To Answer From Memory

Use Vector Search first. If needed, read exact drawers by ID from `scratch.mitch_grabner.mempalace_drawers`.

When a source matters, cite the drawer context in plain language:

```text
I found this in project/mempalace-databricks from codex-session...
```

Prefer concise answers grounded in the retrieved text. If memory is ambiguous, say what was found and what remains uncertain.

## How To Save Memory

Save durable facts, decisions, fixes, and meaningful completed work as verbatim drawers in:

```text
scratch.mitch_grabner.mempalace_drawers
```

Use:

```text
wing: project
room: stable topic slug, for example mempalace-databricks
source_file: databricks-genie-agent
agent: databricks-genie
importance: 3.0
```

Before writing, check for duplicates with Vector Search and exact ID lookup.

Use this deterministic ID formula for one-off agent memories:

```sql
sha2(concat('{{wing}}', '|', '{{room}}', '|', substr('{{content}}', 1, 100), '|0'), 256)
```

After writing a drawer, append a row to `scratch.mitch_grabner.mempalace_wal`. Trigger Vector Search sync if the tool is available. If sync is unavailable, mention that semantic search may lag until the index syncs.

## What To Save

Save:

- User preferences and durable operating instructions.
- Architecture decisions and rationale.
- Bug root causes and fixes.
- Live test results and important IDs.
- Databricks workspace/table/index/endpoint details.
- Next actions that future agents need.

Do not save:

- Temporary speculation.
- Secrets or credentials.
- Large raw logs unless the user explicitly asks.
- Duplicate content already present in memory.

## Failure Rules

- If memory search fails, say memory search is unavailable and continue from visible context.
- If raw SQL read fails, say exact memory reads are unavailable.
- If a memory write fails, do not claim it was saved.
- If Vector Search and SQL disagree, trust the raw table for exact current state and use Vector Search only for recall.
