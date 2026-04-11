#!/usr/bin/env python3
"""
convo_miner.py — Mine conversations into the palace (Databricks-native).

Ingests chat exports (Claude Code, ChatGPT, Slack, plain text transcripts).
Normalizes format, chunks by exchange pair (Q+A = one unit), files to palace.

Same palace as project mining. Different ingest strategy.
"""

import os
import sys
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from collections import Counter, defaultdict
from typing import Dict, List, Optional

from .normalize import normalize
from .palace import (
    SKIP_DIRS,
    add_drawers,
    file_already_mined,
    make_drawer_id,
)
from .config import DatabricksConfig


# ── Constants ─────────────────────────────────────────────────────────────────

CONVO_EXTENSIONS = {".txt", ".md", ".json", ".jsonl"}
MIN_CHUNK_SIZE = 30
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10 MB


# ── Chunking — exchange pairs for conversations ──────────────────────────────


def chunk_exchanges(content: str) -> List[Dict]:
    """Chunk by exchange pair: one > turn + AI response = one unit.

    Falls back to paragraph chunking if no > markers.
    """
    lines = content.split("\n")
    quote_lines = sum(1 for line in lines if line.strip().startswith(">"))

    if quote_lines >= 3:
        return _chunk_by_exchange(lines)
    return _chunk_by_paragraph(content)


def _chunk_by_exchange(lines: list) -> List[Dict]:
    """One user turn (>) + the AI response that follows = one chunk."""
    chunks: list = []
    i = 0

    while i < len(lines):
        line = lines[i]
        if line.strip().startswith(">"):
            user_turn = line.strip()
            i += 1

            ai_lines: list = []
            while i < len(lines):
                next_line = lines[i]
                if next_line.strip().startswith(">") or next_line.strip().startswith("---"):
                    break
                if next_line.strip():
                    ai_lines.append(next_line.strip())
                i += 1

            ai_response = " ".join(ai_lines[:8])
            body = f"{user_turn}\n{ai_response}" if ai_response else user_turn

            if len(body.strip()) > MIN_CHUNK_SIZE:
                chunks.append({"content": body, "chunk_index": len(chunks)})
        else:
            i += 1

    return chunks


def _chunk_by_paragraph(content: str) -> List[Dict]:
    """Fallback: chunk by paragraph breaks."""
    chunks: list = []
    paragraphs = [p.strip() for p in content.split("\n\n") if p.strip()]

    # Long content with no paragraph breaks → chunk by line groups
    if len(paragraphs) <= 1 and content.count("\n") > 20:
        lines = content.split("\n")
        for i in range(0, len(lines), 25):
            group = "\n".join(lines[i : i + 25]).strip()
            if len(group) > MIN_CHUNK_SIZE:
                chunks.append({"content": group, "chunk_index": len(chunks)})
        return chunks

    for para in paragraphs:
        if len(para) > MIN_CHUNK_SIZE:
            chunks.append({"content": para, "chunk_index": len(chunks)})

    return chunks


# ── Room detection — topic-based for conversations ───────────────────────────

TOPIC_KEYWORDS = {
    "technical": [
        "code", "python", "function", "bug", "error", "api",
        "database", "server", "deploy", "git", "test", "debug", "refactor",
    ],
    "architecture": [
        "architecture", "design", "pattern", "structure", "schema",
        "interface", "module", "component", "service", "layer",
    ],
    "planning": [
        "plan", "roadmap", "milestone", "deadline", "priority",
        "sprint", "backlog", "scope", "requirement", "spec",
    ],
    "decisions": [
        "decided", "chose", "picked", "switched", "migrated",
        "replaced", "trade-off", "alternative", "option", "approach",
    ],
    "problems": [
        "problem", "issue", "broken", "failed", "crash",
        "stuck", "workaround", "fix", "solved", "resolved",
    ],
}


def detect_convo_room(content: str) -> str:
    """Score conversation content against topic keywords."""
    content_lower = content[:3000].lower()
    scores = {}
    for room, keywords in TOPIC_KEYWORDS.items():
        score = sum(1 for kw in keywords if kw in content_lower)
        if score > 0:
            scores[room] = score
    if scores:
        return max(scores, key=scores.get)
    return "general"


# ── Scan for conversation files ───────────────────────────────────────────────


def scan_convos(convo_dir: str) -> List[Path]:
    """Find all potential conversation files under *convo_dir*."""
    convo_path = Path(convo_dir).expanduser().resolve()
    files: List[Path] = []
    for root, dirs, filenames in os.walk(convo_path):
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
        for filename in filenames:
            if filename.endswith(".meta.json"):
                continue
            filepath = Path(root) / filename
            if filepath.suffix.lower() not in CONVO_EXTENSIONS:
                continue
            if filepath.is_symlink():
                continue
            try:
                if filepath.stat().st_size > MAX_FILE_SIZE:
                    continue
            except OSError:
                continue
            files.append(filepath)
    return files


# ── Mine conversations ────────────────────────────────────────────────────────


def mine_convos(
    convo_dir: str,
    config: Optional[DatabricksConfig] = None,
    wing: Optional[str] = None,
    agent: str = "mempalace",
    limit: int = 0,
    dry_run: bool = False,
    extract_mode: str = "exchange",
) -> None:
    """Mine a directory of conversation files into the palace.

    Args:
        convo_dir: Directory containing conversation files.
        config: Databricks configuration. Uses defaults if not provided.
        wing: Wing name. Defaults to the directory basename.
        agent: Agent identifier for provenance tracking.
        limit: Max files to process (0 = unlimited).
        dry_run: If True, print what would happen without writing.
        extract_mode: ``"exchange"`` (Q+A pairs) or ``"general"``
            (decisions, preferences, milestones, problems, emotions).
    """
    config = config or DatabricksConfig()
    convo_path = Path(convo_dir).expanduser().resolve()
    if not wing:
        wing = convo_path.name.lower().replace(" ", "_").replace("-", "_")

    files = scan_convos(convo_dir)
    if limit > 0:
        files = files[:limit]

    print(f"\n{'=' * 55}")
    print("  MemPalace Mine — Conversations")
    print(f"{'=' * 55}")
    print(f"  Wing:    {wing}")
    print(f"  Source:  {convo_path}")
    print(f"  Files:   {len(files)}")
    print(f"  Target:  {config.drawers_table}")
    if dry_run:
        print("  DRY RUN — nothing will be filed")
    print(f"{'-' * 55}\n")

    total_drawers = 0
    files_skipped = 0
    room_counts: Dict[str, int] = defaultdict(int)

    for i, filepath in enumerate(files, 1):
        source_file = str(filepath)

        # Skip if already filed
        if not dry_run and file_already_mined(config, source_file):
            files_skipped += 1
            continue

        # Normalize format
        try:
            content = normalize(str(filepath))
        except (OSError, ValueError):
            continue

        if not content or len(content.strip()) < MIN_CHUNK_SIZE:
            continue

        # Chunk — either exchange pairs or general extraction
        if extract_mode == "general":
            from .general_extractor import extract_memories
            chunks = extract_memories(content)
        else:
            chunks = chunk_exchanges(content)

        if not chunks:
            continue

        # Detect room from content (general mode uses memory_type instead)
        if extract_mode != "general":
            room = detect_convo_room(content)
        else:
            room = None  # set per-chunk below

        # ── Dry-run reporting ─────────────────────────────────────────
        if dry_run:
            if extract_mode == "general":
                type_counts = Counter(c.get("memory_type", "general") for c in chunks)
                types_str = ", ".join(f"{t}:{n}" for t, n in type_counts.most_common())
                print(f"    [DRY RUN] {filepath.name} → {len(chunks)} memories ({types_str})")
            else:
                print(f"    [DRY RUN] {filepath.name} → room:{room} ({len(chunks)} drawers)")
            total_drawers += len(chunks)
            if extract_mode == "general":
                for c in chunks:
                    room_counts[c.get("memory_type", "general")] += 1
            else:
                room_counts[room] += 1
            continue

        # ── Build drawer dicts and batch-insert ───────────────────────
        if extract_mode != "general":
            room_counts[room] += 1

        try:
            mtime = os.path.getmtime(source_file)
        except OSError:
            mtime = None

        drawer_dicts: list = []
        for chunk in chunks:
            chunk_room = chunk.get("memory_type", room) if extract_mode == "general" else room
            if extract_mode == "general":
                room_counts[chunk_room] += 1

            drawer_dicts.append({
                "id": make_drawer_id(wing, chunk_room, source_file, chunk["chunk_index"]),
                "text": chunk["content"],
                "wing": wing,
                "room": chunk_room,
                "source_file": source_file,
                "source_mtime": mtime,
                "chunk_index": chunk["chunk_index"],
                "importance": 3.0,
                "agent": agent,
            })

        drawers_added = add_drawers(config, drawer_dicts)
        total_drawers += drawers_added
        print(f"  ✓ [{i:4}/{len(files)}] {filepath.name[:50]:50} +{drawers_added}")

    print(f"\n{'=' * 55}")
    print("  Done.")
    print(f"  Files processed: {len(files) - files_skipped}")
    print(f"  Files skipped (already filed): {files_skipped}")
    print(f"  Drawers filed: {total_drawers}")
    if room_counts:
        print("\n  By room:")
        for room, count in sorted(room_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"    {room:20} {count} files")
    print('\n  Next: mempalace search "what you\'re looking for"')
    print(f"{'=' * 55}\n")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python convo_miner.py <convo_dir> [--limit N] [--dry-run]")
        sys.exit(1)
    mine_convos(sys.argv[1], config=DatabricksConfig())
