"""The kind seam: the queue and export are generic, a kind is one registry entry.

No second kind exists yet, so these use a stand-in row with `kind = 'pdf'` to
prove that nothing in the queue, the runner's per-kind pass or the exports
assumes every item is a video.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from video_digest.config import Settings
from video_digest.db import connect
from video_digest.main import create_app
from video_digest.pipeline.kinds import get_kind, kind_names
from video_digest.pipeline.runner import run_due_jobs, run_job

_MIN_LLM = {
    "tiers": {
        "map": {"primary": {"provider": "ollama", "model": "x"}},
        "reduce": {"primary": {"provider": "ollama", "model": "x"}},
    }
}
KEY = {"X-API-Key": "test-admin-key"}


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        llm=_MIN_LLM,
        transcription={"remote_url": "http://127.0.0.1:9"},
        output={"db_path": tmp_path / "state.sqlite", "work_dir": tmp_path / "work"},
        admin_api_key="test-admin-key",
    )


def _seed(db: sqlite3.Connection, key: str, kind: str, *, written: bool = False) -> str:
    row_id = f"{kind}:{key}"
    db.execute(
        "INSERT INTO items (id, kind, item_key, adapter, url, canonical_url, metadata, digest, "
        "stage_write, written_at, note_path, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, 'u', 'u', '{\"title\": \"T\"}', ?, ?, ?, ?, 'now', 'now')",
        (
            row_id,
            kind,
            key,
            kind,
            json.dumps({"tldr": "x"}) if written else None,
            "done" if written else "pending",
            "2026-08-01T10:00:00+00:00" if written else None,
            f"{kind}/{key}.md" if written else None,
        ),
    )
    db.execute(
        "INSERT INTO jobs (id, item_id, force, status, created_at, updated_at) "
        "VALUES (?, ?, 0, 'queued', ?, 'now')",
        (f"job-{row_id}", row_id, f"2026-01-01T00:00:0{len(key)}+00:00"),
    )
    db.commit()
    return row_id


def test_video_is_registered_and_unknown_kinds_are_an_error() -> None:
    assert "video" in kind_names()
    assert get_kind("video").name == "video"
    with pytest.raises(KeyError, match="unknown item kind"):
        get_kind("nope")


@pytest.mark.asyncio
async def test_a_pass_can_be_restricted_to_one_kind(tmp_path: Path) -> None:
    db = connect(tmp_path / "s.sqlite")
    _seed(db, "a", "video")
    _seed(db, "bb", "pdf")
    seen: list[str] = []

    async def fake(_db: Any, _settings: Any, row: sqlite3.Row, **_kw: Any) -> str:
        seen.append(row["id"])
        return "done"

    await run_due_jobs(
        db, _settings(tmp_path), llm=object(), vault=object(), kind="pdf", _run_job=fake  # type: ignore[arg-type]
    )
    assert seen == ["pdf:bb"]

    # The video job was not touched by the pdf pass, so it is still due.
    await run_due_jobs(
        db, _settings(tmp_path), llm=object(), vault=object(), kind="video", _run_job=fake  # type: ignore[arg-type]
    )
    assert seen == ["pdf:bb", "video:a"]


@pytest.mark.asyncio
async def test_no_kind_filter_takes_every_kind(tmp_path: Path) -> None:
    db = connect(tmp_path / "s.sqlite")
    _seed(db, "a", "video")
    _seed(db, "bb", "pdf")
    seen: list[str] = []

    async def fake(_db: Any, _settings: Any, row: sqlite3.Row, **_kw: Any) -> str:
        seen.append(row["id"])
        return "done"

    await run_due_jobs(
        db, _settings(tmp_path), llm=object(), vault=object(), _run_job=fake  # type: ignore[arg-type]
    )
    assert sorted(seen) == ["pdf:bb", "video:a"]


@pytest.mark.asyncio
async def test_run_job_with_an_unregistered_kind_fails_loudly(tmp_path: Path) -> None:
    db = connect(tmp_path / "s.sqlite")
    row_id = _seed(db, "a", "pdf")  # no pdf kind is registered yet
    row = db.execute("SELECT * FROM items WHERE id = ?", (row_id,)).fetchone()

    with pytest.raises(KeyError, match="unknown item kind 'pdf'"):
        await run_job(db, _settings(tmp_path), row, llm=object(), vault=object())  # type: ignore[arg-type]


class TestExports:
    def test_items_lists_every_kind_and_filters_by_kind(self, tmp_path: Path) -> None:
        app = create_app(_settings(tmp_path))
        _seed(app.state.db, "v1", "video", written=True)
        _seed(app.state.db, "p1", "pdf", written=True)
        _seed(app.state.db, "unwritten", "pdf")
        client = TestClient(app)

        everything = client.get("/items", headers=KEY).json()
        assert {i["id"] for i in everything["items"]} == {"video:v1", "pdf:p1"}

        only_pdf = client.get("/items?kind=pdf", headers=KEY).json()
        assert [i["kind"] for i in only_pdf["items"]] == ["pdf"]
        assert only_pdf["items"][0]["item_key"] == "p1"
        assert only_pdf["items"][0]["digest"] == {"tldr": "x"}

    def test_items_requires_the_admin_key(self, tmp_path: Path) -> None:
        assert TestClient(create_app(_settings(tmp_path))).get("/items").status_code == 401

    def test_videos_export_never_includes_another_kind(self, tmp_path: Path) -> None:
        """podcast-digest mirrors `/videos`; a PDF summary must not appear there."""
        app = create_app(_settings(tmp_path))
        _seed(app.state.db, "v1", "video", written=True)
        _seed(app.state.db, "p1", "pdf", written=True)

        body = TestClient(app).get("/videos", headers=KEY).json()

        assert [v["video_id"] for v in body["videos"]] == ["v1"]

    def test_items_cursor_pages_without_skipping(self, tmp_path: Path) -> None:
        app = create_app(_settings(tmp_path))
        for key in ("a", "b", "c"):  # same written_at: the cursor must tie-break on id
            _seed(app.state.db, key, "pdf", written=True)
        client = TestClient(app)

        first = client.get("/items?limit=2", headers=KEY).json()
        assert first["count"] == 2 and first["next"] is not None
        second = client.get(
            f"/items?limit=2&since={first['next']['since']}&since_id={first['next']['since_id']}",
            headers=KEY,
        ).json()

        ids = [i["id"] for i in first["items"]] + [i["id"] for i in second["items"]]
        assert sorted(ids) == ["pdf:a", "pdf:b", "pdf:c"]
