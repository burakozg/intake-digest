"""The `pdf` kind end to end, with the LLM and the vault faked."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

import pytest
from pdfs import LONG, make_pdf

from intake_digest.config import Settings
from intake_digest.db import connect
from intake_digest.llm.base import LLMUnavailable
from intake_digest.llm.models import ChunkSummary, PdfDigest
from intake_digest.pipeline.runner import run_job
from intake_digest.vault.livesync import VaultUnavailable

_LLM = {
    "tiers": {
        "map": {"primary": {"provider": "ollama", "model": "m"}},
        "reduce": {"primary": {"provider": "ollama", "model": "r"}},
    }
}


def _settings(root: Path, **pdf: Any) -> Settings:
    return Settings(
        llm=_LLM,
        transcription={"remote_url": "http://mac.local:8000"},
        pdf={"enabled": True, "input_dir": root, **pdf},
    )


DIGEST = PdfDigest(
    title="Q3 Security Report",
    tldr="Things happened.",
    summary_md="A **detailed** summary.",
    key_points=["Point one", "Point two"],
    sections=[{"heading": "Intro", "summary": "Opens."}],
    entities=["WorkOS"],
    topics=["CIAM"],
    relevance="high",
)


class FakeLLM:
    def __init__(self) -> None:
        self.tiers: list[str] = []

    async def complete_structured(self, tier: str, system: str, user: str, model: type, **kw: Any):
        self.tiers.append(tier)
        result = ChunkSummary(covered="stuff") if model is ChunkSummary else DIGEST
        return result, None


class FakeVault:
    def __init__(self, *, down: bool = False) -> None:
        self.down = down
        self.written: dict[str, str] = {}

    async def project(self, path: str, markdown: str, *, mtime_ms: int, merge: bool) -> bool:
        if self.down:
            raise VaultUnavailable("couch is down")
        assert merge is False  # whole-file: no read-before-write
        changed = self.written.get(path) != markdown
        self.written[path] = markdown
        return changed

    async def list_prefix(self, prefix: str) -> list[Any]:
        return []


class Notifier:
    def __init__(self) -> None:
        self.failures: list[tuple[str, str, str]] = []

    async def job_failed(self, item: str, stage: str, error: str) -> None:
        self.failures.append((item, stage, error))


def _seed(db: sqlite3.Connection, root: Path, pages: list[str | None], rel: str = "r/q3.pdf"):
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(make_pdf(pages))
    meta = {"title": "q3", "filename": "q3.pdf", "source_path": rel, "size": 1, "mtime_ns": 1}
    db.execute(
        "INSERT INTO items (id, kind, item_key, adapter, origin, url, canonical_url, metadata, "
        "stage_resolve, stage_metadata, created_at, updated_at) "
        "VALUES ('pdf:abc', 'pdf', 'abc', 'nas', 'inbox', ?, ?, ?, 'done', 'done', "
        "'2026-10-01T09:00:00+00:00', 'now')",
        (rel, rel, json.dumps(meta)),
    )
    db.commit()


def _row(db: sqlite3.Connection) -> sqlite3.Row:
    return db.execute("SELECT * FROM items WHERE id = 'pdf:abc'").fetchone()


async def _run(db, settings, vault, llm=None, notifier=None, **kw) -> str:
    return await run_job(
        db, settings, _row(db), llm=llm or FakeLLM(), vault=vault, notifier=notifier, **kw
    )


@pytest.mark.asyncio
async def test_a_pdf_becomes_a_note_in_the_pdf_folder(tmp_path: Path) -> None:
    db = connect(tmp_path / "s.sqlite")
    root = tmp_path / "in"
    _seed(db, root, [LONG + "One.", LONG + "Two."])
    vault, llm = FakeVault(), FakeLLM()

    assert await _run(db, _settings(root), vault, llm) == "done"

    (path, md), = vault.written.items()
    assert path == "15 PDFs/2026-10-01-q3-security-report.md"
    assert md.startswith("---\ntype: pdf\ntitle: ")
    for needle in ("created: 2026-10-01", "source: ", "r/q3.pdf", "pdf_sha256: abc",
                   "pdf_pages: 2", "pdf_extraction: text", "## Summary", "A **detailed** summary.",
                   "- Point one", "**Intro**", "WorkOS"):
        assert needle in md
    assert llm.tiers == ["map", "reduce"]  # two pages fit one slice
    row = _row(db)
    assert row["note_path"] == path and row["written_at"] and row["stage_write"] == "done"


@pytest.mark.asyncio
async def test_the_note_is_deterministic_so_a_rerun_writes_nothing(tmp_path: Path) -> None:
    db = connect(tmp_path / "s.sqlite")
    root = tmp_path / "in"
    _seed(db, root, [LONG])
    vault = FakeVault()
    await _run(db, _settings(root), vault)
    first = dict(vault.written)

    await _run(db, _settings(root), vault, force=True)

    assert vault.written == first
    import re

    # The only date in the note is the item's own creation date.
    assert re.findall(r"\d{4}-\d{2}-\d{2}", first[next(iter(first))]).count("2026-10-01") >= 1
    assert not re.search(r"\d{2}:\d{2}:\d{2}", first[next(iter(first))])


@pytest.mark.asyncio
async def test_the_filename_is_pinned_even_if_the_title_changes(tmp_path: Path) -> None:
    db = connect(tmp_path / "s.sqlite")
    root = tmp_path / "in"
    _seed(db, root, [LONG])
    vault = FakeVault()
    await _run(db, _settings(root), vault)

    renamed = DIGEST.model_copy(update={"title": "A Completely Different Title"})
    db.execute("UPDATE items SET digest = ?, stage_write = 'pending'", (renamed.model_dump_json(),))
    db.commit()
    await _run(db, _settings(root), vault)

    assert list(vault.written) == ["15 PDFs/2026-10-01-q3-security-report.md"]


@pytest.mark.asyncio
async def test_llm_outage_defers_and_resumes_without_rereading(tmp_path: Path) -> None:
    db = connect(tmp_path / "s.sqlite")
    root = tmp_path / "in"
    _seed(db, root, [LONG])

    class Down(FakeLLM):
        async def complete_structured(self, *a: Any, **kw: Any):
            raise LLMUnavailable("tier down")

    assert await _run(db, _settings(root), FakeVault(), Down()) == "deferred"
    row = _row(db)
    assert row["stage_acquire"] == "done" and row["stage_summarize"] == "pending"

    (root / "r/q3.pdf").unlink()  # acquired text is kept; the file is not needed again
    assert await _run(db, _settings(root), FakeVault()) == "done"


@pytest.mark.asyncio
async def test_vault_outage_defers_after_summarising(tmp_path: Path) -> None:
    db = connect(tmp_path / "s.sqlite")
    root = tmp_path / "in"
    _seed(db, root, [LONG])
    llm = FakeLLM()

    assert await _run(db, _settings(root), FakeVault(down=True), llm) == "deferred"
    assert _row(db)["stage_summarize"] == "done" and _row(db)["stage_write"] == "pending"

    llm.tiers.clear()
    assert await _run(db, _settings(root), FakeVault(), llm) == "done"
    assert llm.tiers == []  # the summary was kept; no second LLM spend


@pytest.mark.asyncio
async def test_an_unreadable_pdf_fails_with_a_reason_and_tells_the_operator(tmp_path: Path) -> None:
    db = connect(tmp_path / "s.sqlite")
    root = tmp_path / "in"
    _seed(db, root, [LONG])
    (root / "r/q3.pdf").write_bytes(b"not a pdf")
    notifier = Notifier()

    assert await _run(db, _settings(root), FakeVault(), notifier=notifier) == "failed"

    assert _row(db)["stage_acquire"] == "failed"
    assert notifier.failures and "q3.pdf" in notifier.failures[0][2]


@pytest.mark.asyncio
async def test_scans_without_a_vision_tier_fail_clearly(tmp_path: Path) -> None:
    db = connect(tmp_path / "s.sqlite")
    root = tmp_path / "in"
    _seed(db, root, [None])
    notifier = Notifier()

    assert await _run(db, _settings(root), FakeVault(), notifier=notifier) == "failed"
    assert "llm.tiers.vision" in notifier.failures[0][2]


@pytest.mark.asyncio
async def test_missing_original_rewrites_only_the_note(tmp_path: Path) -> None:
    db = connect(tmp_path / "s.sqlite")
    root = tmp_path / "in"
    _seed(db, root, [LONG])
    vault, llm = FakeVault(), FakeLLM()
    await _run(db, _settings(root), vault, llm)
    llm.tiers.clear()

    meta = json.loads(_row(db)["metadata"])
    meta["missing_since"] = "2026-10-18"
    db.execute("UPDATE items SET metadata = ?, stage_write = 'pending'", (json.dumps(meta),))
    db.commit()
    assert await _run(db, _settings(root), vault, llm) == "done"

    (md,) = vault.written.values()
    assert "pdf_missing_since: 2026-10-18" in md and "no longer in the folder" in md
    assert llm.tiers == []


@pytest.mark.asyncio
async def test_long_pdfs_say_when_pages_were_left_out(tmp_path: Path) -> None:
    db = connect(tmp_path / "s.sqlite")
    root = tmp_path / "in"
    _seed(db, root, [LONG] * 4)
    vault = FakeVault()

    await _run(db, _settings(root, max_pages=2), vault)

    (md,) = vault.written.values()
    assert "pdf_truncated: true" in md and "pdf_pages_read: 2" in md and "pdf_pages: 4" in md
    assert "Only the first 2 of 4 pages were read" in md
