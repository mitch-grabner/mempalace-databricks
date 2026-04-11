#!/usr/bin/env python3
"""
miner.py — Files everything into the palace (Databricks-native).

Reads mempalace.yaml from the project directory to know the wing + rooms.
Routes each file to the right room based on content.
Stores verbatim chunks as drawers. No summaries. Ever.
"""

import os
import sys
import fnmatch
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Optional, Set, Tuple

from .palace import (
    SKIP_DIRS,
    _get_spark,
    add_drawers,
    file_already_mined,
    make_drawer_id,
)
from .config import DatabricksConfig


# ── Constants ─────────────────────────────────────────────────────────────────

READABLE_EXTENSIONS = {
    ".txt", ".md", ".py", ".js", ".ts", ".jsx", ".tsx", ".json",
    ".yaml", ".yml", ".html", ".css", ".java", ".go", ".rs", ".rb",
    ".sh", ".csv", ".sql", ".toml",
}

SKIP_FILENAMES = {
    "mempalace.yaml", "mempalace.yml", "mempal.yaml",
    "mempal.yml", ".gitignore", "package-lock.json",
}

CHUNK_SIZE = 800        # chars per drawer
CHUNK_OVERLAP = 100     # overlap between chunks
MIN_CHUNK_SIZE = 50     # skip tiny chunks
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10 MB


# ── Ignore matching ───────────────────────────────────────────────────────────


class GitignoreMatcher:
    """Lightweight matcher for one directory's .gitignore patterns."""

    def __init__(self, base_dir: Path, rules: list) -> None:
        self.base_dir = base_dir
        self.rules = rules

    @classmethod
    def from_dir(cls, dir_path: Path) -> Optional["GitignoreMatcher"]:
        """Parse .gitignore in *dir_path*; return None if absent."""
        gitignore_path = dir_path / ".gitignore"
        if not gitignore_path.is_file():
            return None
        try:
            lines = gitignore_path.read_text(encoding="utf-8", errors="replace").splitlines()
        except Exception:
            return None

        rules: list = []
        for raw_line in lines:
            line = raw_line.strip()
            if not line:
                continue
            if line.startswith("\\#") or line.startswith("\\!"):
                line = line[1:]
            elif line.startswith("#"):
                continue

            negated = line.startswith("!")
            if negated:
                line = line[1:]
            anchored = line.startswith("/")
            if anchored:
                line = line.lstrip("/")
            dir_only = line.endswith("/")
            if dir_only:
                line = line.rstrip("/")
            if not line:
                continue

            rules.append({
                "pattern": line,
                "anchored": anchored,
                "dir_only": dir_only,
                "negated": negated,
            })

        return cls(dir_path, rules) if rules else None

    def matches(self, path: Path, is_dir: bool = None) -> Optional[bool]:
        """Return True/False/None for ignored/not-ignored/no-opinion."""
        try:
            relative = path.relative_to(self.base_dir).as_posix().strip("/")
        except ValueError:
            return None
        if not relative:
            return None
        if is_dir is None:
            is_dir = path.is_dir()

        ignored = None
        for rule in self.rules:
            if self._rule_matches(rule, relative, is_dir):
                ignored = not rule["negated"]
        return ignored

    def _rule_matches(self, rule: dict, relative: str, is_dir: bool) -> bool:
        pattern = rule["pattern"]
        parts = relative.split("/")
        pattern_parts = pattern.split("/")

        if rule["dir_only"]:
            target_parts = parts if is_dir else parts[:-1]
            if not target_parts:
                return False
            if rule["anchored"] or len(pattern_parts) > 1:
                return self._match_from_root(target_parts, pattern_parts)
            return any(fnmatch.fnmatch(part, pattern) for part in target_parts)

        if rule["anchored"] or len(pattern_parts) > 1:
            return self._match_from_root(parts, pattern_parts)
        return any(fnmatch.fnmatch(part, pattern) for part in parts)

    def _match_from_root(self, target_parts: list, pattern_parts: list) -> bool:
        """Recursive glob matching from the repo root."""
        def matches(pi: int, ppi: int) -> bool:
            if ppi == len(pattern_parts):
                return True
            if pi == len(target_parts):
                return all(p == "**" for p in pattern_parts[ppi:])
            pp = pattern_parts[ppi]
            if pp == "**":
                return matches(pi, ppi + 1) or matches(pi + 1, ppi)
            if not fnmatch.fnmatch(target_parts[pi], pp):
                return False
            return matches(pi + 1, ppi + 1)
        return matches(0, 0)


def load_gitignore_matcher(dir_path: Path, cache: dict) -> Optional[GitignoreMatcher]:
    """Load and cache one directory's .gitignore matcher."""
    if dir_path not in cache:
        cache[dir_path] = GitignoreMatcher.from_dir(dir_path)
    return cache[dir_path]


def is_gitignored(path: Path, matchers: list, is_dir: bool = False) -> bool:
    """Apply active .gitignore matchers in ancestor order; last match wins."""
    ignored = False
    for matcher in matchers:
        decision = matcher.matches(path, is_dir=is_dir)
        if decision is not None:
            ignored = decision
    return ignored


def should_skip_dir(dirname: str) -> bool:
    """Skip known generated/cache directories before gitignore matching."""
    return dirname in SKIP_DIRS or dirname.endswith(".egg-info")


def normalize_include_paths(include_ignored: Optional[list]) -> Set[str]:
    """Normalize comma-parsed include paths into project-relative POSIX strings."""
    normalized: Set[str] = set()
    for raw_path in include_ignored or []:
        candidate = str(raw_path).strip().strip("/")
        if candidate:
            normalized.add(Path(candidate).as_posix())
    return normalized


def is_exact_force_include(path: Path, project_path: Path, include_paths: Set[str]) -> bool:
    """Return True when *path* exactly matches an explicit include override."""
    if not include_paths:
        return False
    try:
        relative = path.relative_to(project_path).as_posix().strip("/")
    except ValueError:
        return False
    return relative in include_paths


def is_force_included(path: Path, project_path: Path, include_paths: Set[str]) -> bool:
    """Return True when *path* or an ancestor/descendant was explicitly included."""
    if not include_paths:
        return False
    try:
        relative = path.relative_to(project_path).as_posix().strip("/")
    except ValueError:
        return False
    if not relative:
        return False
    for ip in include_paths:
        if relative == ip or relative.startswith(f"{ip}/") or ip.startswith(f"{relative}/"):
            return True
    return False


# ── Config loading ────────────────────────────────────────────────────────────


def load_config(project_dir: str) -> dict:
    """Load mempalace.yaml from project directory (falls back to mempal.yaml)."""
    import yaml

    config_path = Path(project_dir).expanduser().resolve() / "mempalace.yaml"
    if not config_path.exists():
        legacy_path = Path(project_dir).expanduser().resolve() / "mempal.yaml"
        if legacy_path.exists():
            config_path = legacy_path
        else:
            print(f"ERROR: No mempalace.yaml found in {project_dir}")
            print(f"Run: mempalace init {project_dir}")
            sys.exit(1)
    with open(config_path) as f:
        return yaml.safe_load(f)


# ── File routing ──────────────────────────────────────────────────────────────


def detect_room(filepath: Path, content: str, rooms: list, project_path: Path) -> str:
    """Route a file to the right room based on path, name, and content keywords."""
    relative = str(filepath.relative_to(project_path)).lower()
    filename = filepath.stem.lower()
    content_lower = content[:2000].lower()

    # Priority 1: folder path matches room name or keywords
    path_parts = relative.replace("\\", "/").split("/")
    for part in path_parts[:-1]:
        for room in rooms:
            candidates = [room["name"].lower()] + [k.lower() for k in room.get("keywords", [])]
            if any(part == c or c in part or part in c for c in candidates):
                return room["name"]

    # Priority 2: filename matches room name
    for room in rooms:
        if room["name"].lower() in filename or filename in room["name"].lower():
            return room["name"]

    # Priority 3: keyword scoring
    scores: Dict[str, int] = defaultdict(int)
    for room in rooms:
        keywords = room.get("keywords", []) + [room["name"]]
        for kw in keywords:
            scores[room["name"]] += content_lower.count(kw.lower())

    if scores:
        best = max(scores, key=scores.get)
        if scores[best] > 0:
            return best

    return "general"


# ── Chunking ──────────────────────────────────────────────────────────────────


def chunk_text(content: str, source_file: str) -> List[Dict]:
    """Split content into drawer-sized chunks on paragraph/line boundaries."""
    content = content.strip()
    if not content:
        return []

    chunks: list = []
    start = 0
    chunk_index = 0

    while start < len(content):
        end = min(start + CHUNK_SIZE, len(content))

        if end < len(content):
            newline_pos = content.rfind("\n\n", start, end)
            if newline_pos > start + CHUNK_SIZE // 2:
                end = newline_pos
            else:
                newline_pos = content.rfind("\n", start, end)
                if newline_pos > start + CHUNK_SIZE // 2:
                    end = newline_pos

        chunk = content[start:end].strip()
        if len(chunk) >= MIN_CHUNK_SIZE:
            chunks.append({"content": chunk, "chunk_index": chunk_index})
            chunk_index += 1

        start = end - CHUNK_OVERLAP if end < len(content) else end

    return chunks


# ── Process one file ──────────────────────────────────────────────────────────


def process_file(
    filepath: Path,
    project_path: Path,
    config: DatabricksConfig,
    wing: str,
    rooms: list,
    agent: str,
    dry_run: bool,
) -> Tuple[int, Optional[str]]:
    """Read, chunk, route, and file one file.

    Returns:
        Tuple of (drawer_count, room_name). Returns (0, None) if skipped.
    """
    source_file = str(filepath)

    # Skip if already filed (and not modified since last mine)
    if not dry_run and file_already_mined(config, source_file, check_mtime=True):
        return 0, None

    try:
        content = filepath.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return 0, None

    content = content.strip()
    if len(content) < MIN_CHUNK_SIZE:
        return 0, None

    room = detect_room(filepath, content, rooms, project_path)
    chunks = chunk_text(content, source_file)

    if dry_run:
        print(f"    [DRY RUN] {filepath.name} → room:{room} ({len(chunks)} drawers)")
        return len(chunks), room

    # Purge stale drawers for this file before re-inserting fresh chunks.
    # With Delta MERGE this is cleaner than the ChromaDB upsert workaround.
    try:
        spark = _get_spark()
        safe_source = source_file.replace("'", "\\'")
        spark.sql(
            f"DELETE FROM {config.drawers_table} WHERE source_file = '{safe_source}'"
        )
    except Exception:
        pass  # table may not exist during first run

    # Build drawer dicts and batch-insert
    try:
        mtime = os.path.getmtime(source_file)
    except OSError:
        mtime = None

    drawer_dicts = [
        {
            "id": make_drawer_id(wing, room, source_file, chunk["chunk_index"]),
            "text": chunk["content"],
            "wing": wing,
            "room": room,
            "source_file": source_file,
            "source_mtime": mtime,
            "chunk_index": chunk["chunk_index"],
            "importance": 3.0,
            "agent": agent,
        }
        for chunk in chunks
    ]

    added = add_drawers(config, drawer_dicts)
    return added, room


# ── Scan project ──────────────────────────────────────────────────────────────


def scan_project(
    project_dir: str,
    respect_gitignore: bool = True,
    include_ignored: Optional[list] = None,
) -> List[Path]:
    """Return list of all readable file paths."""
    project_path = Path(project_dir).expanduser().resolve()
    files: List[Path] = []
    active_matchers: list = []
    matcher_cache: dict = {}
    include_paths = normalize_include_paths(include_ignored)

    for root, dirs, filenames in os.walk(project_path):
        root_path = Path(root)

        if respect_gitignore:
            active_matchers = [
                m for m in active_matchers
                if root_path == m.base_dir or m.base_dir in root_path.parents
            ]
            current_matcher = load_gitignore_matcher(root_path, matcher_cache)
            if current_matcher is not None:
                active_matchers.append(current_matcher)

        dirs[:] = [
            d for d in dirs
            if is_force_included(root_path / d, project_path, include_paths)
            or not should_skip_dir(d)
        ]
        if respect_gitignore and active_matchers:
            dirs[:] = [
                d for d in dirs
                if is_force_included(root_path / d, project_path, include_paths)
                or not is_gitignored(root_path / d, active_matchers, is_dir=True)
            ]

        for filename in filenames:
            filepath = root_path / filename
            force_include = is_force_included(filepath, project_path, include_paths)
            exact_force = is_exact_force_include(filepath, project_path, include_paths)

            if not force_include and filename in SKIP_FILENAMES:
                continue
            if filepath.suffix.lower() not in READABLE_EXTENSIONS and not exact_force:
                continue
            if respect_gitignore and active_matchers and not force_include:
                if is_gitignored(filepath, active_matchers, is_dir=False):
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


# ── Main: mine ────────────────────────────────────────────────────────────────


def mine(
    project_dir: str,
    config: Optional[DatabricksConfig] = None,
    wing_override: Optional[str] = None,
    agent: str = "mempalace",
    limit: int = 0,
    dry_run: bool = False,
    respect_gitignore: bool = True,
    include_ignored: Optional[list] = None,
) -> None:
    """Mine a project directory into the palace.

    Args:
        project_dir: Path to the project root containing mempalace.yaml.
        config: Databricks configuration. Uses defaults if not provided.
        wing_override: Explicit wing name (overrides mempalace.yaml).
        agent: Agent identifier for provenance tracking.
        limit: Max files to process (0 = unlimited).
        dry_run: If True, print what would happen without writing.
        respect_gitignore: Honour .gitignore rules.
        include_ignored: Paths to include even if gitignored.
    """
    config = config or DatabricksConfig()
    project_path = Path(project_dir).expanduser().resolve()
    yaml_config = load_config(project_dir)

    wing = wing_override or yaml_config["wing"]
    rooms = yaml_config.get("rooms", [{"name": "general", "description": "All project files"}])

    files = scan_project(project_dir, respect_gitignore=respect_gitignore,
                         include_ignored=include_ignored)
    if limit > 0:
        files = files[:limit]

    print(f"\n{'=' * 55}")
    print("  MemPalace Mine")
    print(f"{'=' * 55}")
    print(f"  Wing:    {wing}")
    print(f"  Rooms:   {', '.join(r['name'] for r in rooms)}")
    print(f"  Files:   {len(files)}")
    print(f"  Target:  {config.drawers_table}")
    if dry_run:
        print("  DRY RUN — nothing will be filed")
    if not respect_gitignore:
        print("  .gitignore: DISABLED")
    if include_ignored:
        print(f"  Include: {', '.join(sorted(normalize_include_paths(include_ignored)))}")
    print(f"{'─' * 55}\n")

    total_drawers = 0
    files_skipped = 0
    room_counts: Dict[str, int] = defaultdict(int)

    for i, filepath in enumerate(files, 1):
        drawers, room = process_file(
            filepath=filepath,
            project_path=project_path,
            config=config,
            wing=wing,
            rooms=rooms,
            agent=agent,
            dry_run=dry_run,
        )
        if drawers == 0 and not dry_run:
            files_skipped += 1
        else:
            total_drawers += drawers
            room_counts[room] += 1
            if not dry_run:
                print(f"  ✓ [{i:4}/{len(files)}] {filepath.name[:50]:50} +{drawers}")

    print(f"\n{'=' * 55}")
    print("  Done.")
    print(f"  Files processed: {len(files) - files_skipped}")
    print(f"  Files skipped (already filed): {files_skipped}")
    print(f"  Drawers filed: {total_drawers}")
    print("\n  By room:")
    for room, count in sorted(room_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"    {room:20} {count} files")
    print('\n  Next: mempalace search "what you\'re looking for"')
    print(f"{'=' * 55}\n")


# ── Status ────────────────────────────────────────────────────────────────────


def status(config: Optional[DatabricksConfig] = None) -> None:
    """Show what's been filed in the palace via Delta table query."""
    config = config or DatabricksConfig()
    try:
        spark = _get_spark()
        rows = spark.sql(f"""
            SELECT wing, room, COUNT(*) AS cnt
            FROM {config.drawers_table}
            GROUP BY wing, room
            ORDER BY wing, cnt DESC
        """).collect()
    except Exception:
        print(f"\n  No palace found at {config.drawers_table}")
        print("  Run the setup notebook, then mine data.")
        return

    total = sum(r["cnt"] for r in rows)
    wing_rooms: Dict[str, Dict[str, int]] = defaultdict(dict)
    for r in rows:
        wing_rooms[r["wing"]][r["room"]] = r["cnt"]

    print(f"\n{'=' * 55}")
    print(f"  MemPalace Status — {total} drawers")
    print(f"{'=' * 55}\n")
    for wing, rooms_map in sorted(wing_rooms.items()):
        print(f"  WING: {wing}")
        for room, count in sorted(rooms_map.items(), key=lambda x: x[1], reverse=True):
            print(f"    ROOM: {room:20} {count:5} drawers")
        print()
    print(f"{'=' * 55}\n")
