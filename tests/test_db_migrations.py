"""The forward-only schema-migration hook in db.py — the mechanism (with a
stand-in migration), migration 2 (`written_at`) and migration 3 (`videos` →
`items`) for real.

Migration 2 matters more than most: it backfills `written_at`, the ordering
key `GET /videos` pages on. A value that moves after a client has paged past
it puts the row behind that client's cursor, where it is never seen again.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from video_digest import db as db_module
from video_digest.db import SCHEMA_VERSION, connect


def test_fresh_db_is_stamped_at_the_current_version(tmp_path: Path) -> None:
    conn = connect(tmp_path / "s.sqlite")
    row = conn.execute("SELECT value FROM meta WHERE key = 'schema_version'").fetchone()
    assert row["value"] == str(SCHEMA_VERSION)


def test_apply_migrations_runs_steps_newer_than_the_stored_version(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = connect(tmp_path / "s.sqlite")
    conn.execute("UPDATE meta SET value = '0' WHERE key = 'schema_version'")
    conn.commit()

    monkeypatch.setattr(
        db_module,
        "_MIGRATIONS",
        {1: "CREATE TABLE IF NOT EXISTS _probe (x INTEGER);"},
    )
    db_module._apply_migrations(conn)

    assert (
        conn.execute("SELECT value FROM meta WHERE key = 'schema_version'").fetchone()["value"]
        == str(SCHEMA_VERSION)
    )
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
    assert "_probe" in tables


def test_apply_migrations_is_a_noop_when_already_current(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = connect(tmp_path / "s.sqlite")
    ran: list[int] = []
    monkeypatch.setattr(db_module, "_MIGRATIONS", {1: "CREATE TABLE _never (x);"})
    # Already stamped at SCHEMA_VERSION by connect(), so the range is empty.
    db_module._apply_migrations(conn)
    assert ran == []
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
    assert "_never" not in tables


#: The `videos` table as it stood at schema version 1 — before `written_at`.
#: A frozen copy on purpose: a migration test that builds its "old" database
#: from the *current* SCHEMA tests nothing, because the column it is meant to
#: add is already there. Only the columns migration 2 touches, plus the
#: NOT NULLs that make an INSERT possible.
_V1_VIDEOS = """
CREATE TABLE videos (
  id TEXT PRIMARY KEY,
  video_id TEXT NOT NULL,
  url TEXT NOT NULL,
  canonical_url TEXT NOT NULL,
  stage_write TEXT NOT NULL DEFAULT 'pending',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
"""

_ROWS = [
    ("youtube:done", "done", "done", "2026-01-01T00:00:00+00:00", "2026-06-06T00:00:00+00:00"),
    ("youtube:pending", "pending", "pending", "2026-02-02T00:00:00+00:00", "2026-06-06T00:00:00+00:00"),
]


def _raw_v1_db(tmp_path: Path) -> sqlite3.Connection:
    """A bare version-1 database — no `connect()`, so no current SCHEMA."""
    conn = sqlite3.connect(tmp_path / "s.sqlite")
    conn.row_factory = sqlite3.Row
    conn.executescript(_V1_VIDEOS)
    conn.executemany(
        "INSERT INTO videos (id, video_id, url, canonical_url, stage_write, "
        "created_at, updated_at) VALUES (?, ?, 'u', 'u', ?, ?, ?)",
        _ROWS,
    )
    conn.commit()
    return conn


def test_migration_2_adds_written_at_and_backfills_from_created_at(tmp_path: Path) -> None:
    conn = _raw_v1_db(tmp_path)
    assert not db_module._has_column(conn, "videos", "written_at")

    db_module._migrate_2_written_at(conn)

    assert db_module._has_column(conn, "videos", "written_at")
    rows = {
        r["id"]: r["written_at"]
        for r in conn.execute("SELECT id, written_at FROM videos")
    }
    # Backfilled from created_at (never mutated), NOT updated_at.
    assert rows["youtube:done"] == "2026-01-01T00:00:00+00:00"
    # Never written -> no ordering key, and the export filters it out.
    assert rows["youtube:pending"] is None


def test_migration_2_is_idempotent(tmp_path: Path) -> None:
    """A crash between the ALTER and the version stamp must leave a database
    that can still be opened. Re-running must not raise, and must not move a
    timestamp a reader may already have seen."""
    conn = _raw_v1_db(tmp_path)
    db_module._migrate_2_written_at(conn)
    conn.execute("UPDATE videos SET updated_at = '2099-01-01T00:00:00+00:00'")
    conn.commit()

    db_module._migrate_2_written_at(conn)  # must not raise "duplicate column name"

    written = conn.execute(
        "SELECT written_at FROM videos WHERE id = 'youtube:done'"
    ).fetchone()["written_at"]
    assert written == "2026-01-01T00:00:00+00:00"


def test_fresh_db_has_written_at_without_running_the_migration(tmp_path: Path) -> None:
    """SCHEMA already carries the column, so a new deployment runs no steps."""
    conn = connect(tmp_path / "fresh.sqlite")
    assert db_module._has_column(conn, "items", "written_at")


#: A version-2 database as the NAS holds it today: the `videos` layout with
#: every table that points at it. Frozen for the same reason as `_V1_VIDEOS`.
#: Only the columns migration 3 renames or that carry data under test.
_V2_LAYOUT = """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
INSERT INTO meta VALUES ('schema_version', '2');
CREATE TABLE videos (
  id TEXT PRIMARY KEY,
  video_id TEXT NOT NULL,
  adapter TEXT NOT NULL DEFAULT 'youtube',
  origin TEXT NOT NULL DEFAULT 'manual',
  url TEXT NOT NULL,
  canonical_url TEXT NOT NULL,
  metadata TEXT,
  stage_resolve TEXT NOT NULL DEFAULT 'pending',
  stage_metadata TEXT NOT NULL DEFAULT 'pending',
  stage_transcript TEXT NOT NULL DEFAULT 'pending',
  stage_normalize TEXT NOT NULL DEFAULT 'pending',
  stage_summarize TEXT NOT NULL DEFAULT 'pending',
  stage_write TEXT NOT NULL DEFAULT 'pending',
  stage_errors TEXT,
  transcript_tier TEXT,
  transcript_tier_degraded INTEGER NOT NULL DEFAULT 0,
  transcript_json TEXT,
  asr_model TEXT,
  transcript_path TEXT,
  note_path TEXT,
  written_at TEXT,
  digest TEXT,
  attempts TEXT NOT NULL DEFAULT '{}',
  last_error TEXT,
  asr_queued_at TEXT,
  force_asr INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE UNIQUE INDEX idx_videos_video_id ON videos(video_id, adapter);
CREATE INDEX idx_videos_asr_queued ON videos(asr_queued_at) WHERE asr_queued_at IS NOT NULL;
CREATE INDEX idx_videos_written ON videos(written_at, id) WHERE written_at IS NOT NULL;
CREATE TABLE note_names (
  video_id TEXT PRIMARY KEY REFERENCES videos(id) ON DELETE CASCADE,
  filename TEXT NOT NULL UNIQUE
);
CREATE TABLE topic_note_names (topic_key TEXT PRIMARY KEY, filename TEXT NOT NULL UNIQUE);
CREATE TABLE jobs (
  id TEXT PRIMARY KEY,
  video_id TEXT NOT NULL REFERENCES videos(id),
  force INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'queued',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX idx_jobs_video ON jobs(video_id);
CREATE TABLE metric_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  kind TEXT NOT NULL,
  video_id TEXT,
  value REAL NOT NULL DEFAULT 1,
  meta TEXT,
  created_at TEXT NOT NULL
);
"""


def _v2_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(_V2_LAYOUT)
    conn.executemany(
        "INSERT INTO videos (id, video_id, url, canonical_url, stage_transcript, "
        "transcript_json, stage_write, note_path, written_at, created_at, updated_at) "
        "VALUES (?, ?, 'u', 'u', 'done', '[{\"start_s\": 0, \"text\": \"hi\"}]', ?, ?, ?, ?, ?)",
        [
            ("youtube:a", "a", "done", "13 video-summaries/a", "2026-01-03T00:00:00+00:00",
             "2026-01-01T00:00:00+00:00", "2026-06-06T00:00:00+00:00"),
            ("youtube:b", "b", "done", "13 video-summaries/b", "2026-01-03T00:00:00+00:00",
             "2026-01-02T00:00:00+00:00", "2026-06-06T00:00:00+00:00"),
            ("wired:c", "c", "pending", None, None,
             "2026-02-02T00:00:00+00:00", "2026-06-06T00:00:00+00:00"),
        ],
    )
    conn.execute("UPDATE videos SET adapter = 'wired' WHERE id = 'wired:c'")
    conn.execute("INSERT INTO note_names VALUES ('youtube:a', 'a-note')")
    conn.execute(
        "INSERT INTO jobs VALUES ('j1', 'youtube:a', 0, 'done', "
        "'2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00')"
    )
    conn.execute(
        "INSERT INTO metric_events (kind, video_id, created_at) "
        "VALUES ('llm_tokens', 'youtube:a', '2026-01-01T00:00:00+00:00')"
    )
    conn.commit()
    conn.close()


def test_migration_3_moves_a_v2_database_to_items_keeping_every_row(tmp_path: Path) -> None:
    path = tmp_path / "v2.sqlite"
    _v2_db(path)

    conn = connect(path)

    assert not db_module._table_exists(conn, "videos")
    assert conn.execute("SELECT value FROM meta WHERE key = 'schema_version'").fetchone()[
        "value"
    ] == str(SCHEMA_VERSION)
    rows = {r["id"]: r for r in conn.execute("SELECT * FROM items")}
    assert set(rows) == {"youtube:a", "youtube:b", "wired:c"}
    # Renamed columns carry their data; existing rows are kinded as videos.
    assert rows["youtube:a"]["item_key"] == "a"
    assert rows["youtube:a"]["kind"] == "video"
    assert rows["youtube:a"]["stage_acquire"] == "done"
    assert rows["youtube:a"]["document_json"] == '[{"start_s": 0, "text": "hi"}]'
    # The export cursor is untouched: a moved written_at strands a reader.
    assert rows["youtube:a"]["written_at"] == "2026-01-03T00:00:00+00:00"
    assert rows["wired:c"]["written_at"] is None
    # Child tables follow, with their foreign keys intact.
    assert conn.execute("SELECT item_id, filename FROM note_names").fetchone()["item_id"] == (
        "youtube:a"
    )
    assert conn.execute("SELECT item_id FROM jobs").fetchone()["item_id"] == "youtube:a"
    assert conn.execute("SELECT item_id FROM metric_events").fetchone()["item_id"] == "youtube:a"
    assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    # The cascade still works through the renamed parent (jobs has no cascade).
    conn.execute("DELETE FROM jobs")
    conn.execute("DELETE FROM items WHERE id = 'youtube:a'")
    assert conn.execute("SELECT COUNT(*) FROM note_names").fetchone()[0] == 0


def test_migration_3_leaves_no_duplicate_indexes(tmp_path: Path) -> None:
    path = tmp_path / "v2.sqlite"
    _v2_db(path)

    conn = connect(path)

    names = {r["name"] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='index'")}
    assert {"idx_items_key", "idx_items_asr_queued", "idx_items_written", "idx_jobs_item"} <= names
    assert not {n for n in names if n.startswith("idx_videos") or n == "idx_jobs_video"}


def test_migration_3_is_idempotent_and_reopen_safe(tmp_path: Path) -> None:
    path = tmp_path / "v2.sqlite"
    _v2_db(path)
    connect(path).close()

    conn = connect(path)  # reopening a migrated database must not raise or re-run
    db_module._migrate_3_items(conn)  # a stamp that never landed re-runs the step

    assert conn.execute("SELECT COUNT(*) FROM items").fetchone()[0] == 3


def test_migration_3_rolls_back_on_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A failure part-way must leave the version-2 layout, not half a rename."""
    path = tmp_path / "v2.sqlite"
    _v2_db(path)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    monkeypatch.setattr(db_module, "_V3_DROPPED_INDEXES", ("idx_videos_video_id", "no-such;bad"))

    with pytest.raises(sqlite3.Error):
        db_module._migrate_3_items(conn)

    assert db_module._table_exists(conn, "videos")
    assert not db_module._table_exists(conn, "items")
