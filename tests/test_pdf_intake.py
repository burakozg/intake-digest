"""PDFs arriving by link or upload: kept, deduplicated by content, and a link
that can never work is remembered rather than fetched on every poll.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

import httpx
import pytest
from fastapi.testclient import TestClient
from pdfs import LONG, make_pdf

from intake_digest.config import AcquisitionConfig, PdfConfig, Settings, VaultConfig
from intake_digest.db import connect
from intake_digest.main import create_app
from intake_digest.pipeline import pdf_intake
from intake_digest.pipeline.inbox import QUEUE_PLACEHOLDER, poll_inbox
from intake_digest.pipeline.pdf_intake import (
    LinkRejected,
    LinkUnavailable,
    NotAPdf,
    TooLarge,
    fetch_pdf,
    ingest_link,
    ingest_pdf,
)
from intake_digest.vault.livesync import VaultUnavailable

PDF = make_pdf([LONG + "Hello."])
URL = "https://example.org/papers/Quarterly_Report.pdf"
KEY = {"X-API-Key": "test-admin-key"}


@pytest.fixture
def db(tmp_path: Path) -> sqlite3.Connection:
    return connect(tmp_path / "s.sqlite")


@pytest.fixture
def cfg(tmp_path: Path) -> PdfConfig:
    return PdfConfig(store_dir=tmp_path / "store")


def _serve(monkeypatch: pytest.MonkeyPatch, handler: Any) -> list[httpx.Request]:
    """Route the module's HTTP client at `handler`, recording what it was asked."""
    seen: list[httpx.Request] = []
    real = httpx.AsyncClient

    def recording(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return handler(request)

    monkeypatch.setattr(
        pdf_intake.httpx,
        "AsyncClient",
        lambda **kw: real(transport=httpx.MockTransport(recording), **kw),
    )
    return seen


def _ok(_: httpx.Request) -> httpx.Response:
    return httpx.Response(200, content=PDF, headers={"content-type": "application/pdf"})


class TestIngest:
    def test_keeps_the_file_and_queues_it(self, db, cfg: PdfConfig) -> None:
        got = ingest_pdf(db, cfg, PDF, name="My Report.pdf", via="upload", origin="upload")

        assert got.status == "created" and got.job_id
        row = db.execute("SELECT * FROM items WHERE id = ?", (got.item_id,)).fetchone()
        meta = json.loads(row["metadata"])
        assert (row["kind"], row["origin"], meta["via"], meta["title"]) == (
            "pdf", "upload", "upload", "My Report",
        )
        assert (cfg.store_dir / meta["stored_path"]).read_bytes() == PDF

    def test_the_same_bytes_are_one_item_however_they_arrive(self, db, cfg: PdfConfig) -> None:
        first = ingest_pdf(db, cfg, PDF, name="a.pdf", via="upload", origin="upload")
        again = ingest_pdf(db, cfg, PDF, name="renamed.pdf", via="upload", origin="upload")

        assert again.status == "existing" and again.item_id == first.item_id
        assert db.execute("SELECT COUNT(*) FROM items").fetchone()[0] == 1
        assert db.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 1

    def test_something_that_is_not_a_pdf_is_refused_and_not_kept(self, db, cfg) -> None:
        with pytest.raises(NotAPdf):
            ingest_pdf(db, cfg, b"<html>nope</html>", name="x.pdf", via="upload", origin="upload")
        assert not cfg.store_dir.exists() or not list(cfg.store_dir.iterdir())

    def test_oversize_is_refused(self, db, tmp_path: Path) -> None:
        small = PdfConfig(store_dir=tmp_path / "s", max_upload_mb=1)
        with pytest.raises(TooLarge):
            ingest_pdf(db, small, PDF + b"0" * (2 * 1024 * 1024), name="x.pdf",
                       via="upload", origin="upload")

    def test_hostile_filenames_cannot_escape(self, db, cfg) -> None:
        got = ingest_pdf(db, cfg, PDF, name="../../etc/passwd.pdf", via="upload", origin="upload")
        meta = json.loads(db.execute("SELECT metadata FROM items").fetchone()[0])
        assert meta["filename"] == "passwd.pdf" and got.status == "created"
        assert all(p.parent == cfg.store_dir for p in cfg.store_dir.iterdir())


class TestLinks:
    @pytest.mark.asyncio
    async def test_a_link_is_fetched_once_then_recognised(self, db, cfg, monkeypatch) -> None:
        seen = _serve(monkeypatch, _ok)

        first = await ingest_link(db, cfg, URL, origin="inbox")
        second = await ingest_link(db, cfg, URL, origin="inbox")

        assert first.status == "created" and second.status == "existing"
        assert len(seen) == 1  # no second download
        meta = json.loads(db.execute("SELECT metadata FROM items").fetchone()[0])
        assert meta["via"] == "link" and meta["source_urls"] == [URL]
        assert meta["title"] == "Quarterly Report"

    @pytest.mark.asyncio
    async def test_a_second_url_for_the_same_document_is_remembered(
        self, db, cfg, monkeypatch
    ) -> None:
        seen = _serve(monkeypatch, _ok)
        await ingest_link(db, cfg, URL, origin="inbox")
        other = "https://mirror.example.net/q.pdf"
        await ingest_link(db, cfg, other, origin="inbox")
        await ingest_link(db, cfg, other, origin="inbox")

        assert len(seen) == 2 and db.execute("SELECT COUNT(*) FROM items").fetchone()[0] == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", [404, 403, 410])
    async def test_a_dead_link_is_recorded_and_never_refetched(
        self, db, cfg, monkeypatch, status: int
    ) -> None:
        seen = _serve(monkeypatch, lambda _: httpx.Response(status))

        with pytest.raises(LinkRejected):
            await ingest_link(db, cfg, URL, origin="inbox")
        again = await ingest_link(db, cfg, URL, origin="inbox")

        assert len(seen) == 1 and again.note_path is None
        row = db.execute("SELECT * FROM items").fetchone()
        assert f"HTTP {status}" in row["last_error"] and row["stage_metadata"] == "failed"
        assert db.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 0

    @pytest.mark.asyncio
    async def test_a_link_that_is_not_a_pdf_is_rejected(self, db, cfg, monkeypatch) -> None:
        _serve(monkeypatch, lambda _: httpx.Response(200, content=b"<html>login</html>"))
        with pytest.raises(LinkRejected, match="did not return a PDF"):
            await ingest_link(db, cfg, URL, origin="inbox")

    @pytest.mark.asyncio
    async def test_a_server_error_is_retried_not_recorded(self, db, cfg, monkeypatch) -> None:
        seen = _serve(monkeypatch, lambda _: httpx.Response(503))

        with pytest.raises(LinkUnavailable):
            await ingest_link(db, cfg, URL, origin="inbox")
        with pytest.raises(LinkUnavailable):
            await ingest_link(db, cfg, URL, origin="inbox")

        assert len(seen) == 2 and db.execute("SELECT COUNT(*) FROM items").fetchone()[0] == 0

    @pytest.mark.asyncio
    async def test_a_network_failure_is_retried_not_recorded(self, db, cfg, monkeypatch) -> None:
        def boom(_: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("unreachable")

        _serve(monkeypatch, boom)
        with pytest.raises(LinkUnavailable):
            await ingest_link(db, cfg, URL, origin="inbox")
        assert db.execute("SELECT COUNT(*) FROM items").fetchone()[0] == 0

    @pytest.mark.asyncio
    async def test_oversize_downloads_are_cut_off(self, db, tmp_path: Path, monkeypatch) -> None:
        _serve(monkeypatch, lambda _: httpx.Response(200, content=PDF + b"0" * (2 << 20)))
        small = PdfConfig(store_dir=tmp_path / "s", max_upload_mb=1)
        with pytest.raises(LinkRejected, match="larger than 1 MB"):
            await fetch_pdf(URL, small)

    @pytest.mark.asyncio
    async def test_only_http_links_are_fetched(self, cfg) -> None:
        with pytest.raises(LinkRejected):
            await fetch_pdf("file:///etc/passwd.pdf", cfg)


class FakeVault:
    def __init__(self, notes: dict[str, str | None]) -> None:
        self.notes = dict(notes)

    async def read_note(self, path: str) -> str | None:
        return self.notes.get(path)

    async def project(self, path: str, markdown: str, *, mtime_ms: int, merge: bool,
                      revive: bool = False) -> bool:
        self.notes[path] = markdown
        return True


QUEUE = "00 intake/_intake_queue.md"
LEGACY = "13 video-summaries/_video-queue.md"
DONE = "00 intake/_intake_completed.md"


async def _poll(db, vault, cfg: PdfConfig, vault_cfg: VaultConfig | None = None) -> int:
    return await poll_inbox(db, vault, AcquisitionConfig(), vault_cfg or VaultConfig(), cfg)


class TestQueueNote:
    @pytest.mark.asyncio
    async def test_the_queue_note_is_created_where_you_paste(self, db, cfg) -> None:
        vault = FakeVault({})
        await _poll(db, vault, cfg)
        assert vault.notes[QUEUE] == QUEUE_PLACEHOLDER + "\n"
        assert LEGACY not in vault.notes  # the old note is never recreated

    @pytest.mark.asyncio
    async def test_a_pasted_pdf_link_flows_through_to_the_completed_note(
        self, db, cfg, monkeypatch
    ) -> None:
        seen = _serve(monkeypatch, _ok)
        queue = f"{QUEUE_PLACEHOLDER}\n- {URL}\nsome prose of mine\n"
        vault = FakeVault({QUEUE: queue})

        assert await _poll(db, vault, cfg) == 0  # fetched and queued; no note yet
        assert vault.notes[QUEUE] == queue and len(seen) == 1
        assert await _poll(db, vault, cfg) == 0 and len(seen) == 1  # not refetched

        # The runner finishes it.
        db.execute("UPDATE items SET note_path = '15 PDFs/2026-10-18-quarterly-report.md', "
                   "stage_write = 'done'")
        db.commit()
        assert await _poll(db, vault, cfg) == 1

        assert URL not in vault.notes[QUEUE] and "some prose of mine" in vault.notes[QUEUE]
        assert "[[15 PDFs/2026-10-18-quarterly-report.md|Quarterly Report]]" in vault.notes[DONE]
        assert vault.notes[DONE].lstrip().startswith("## 20")  # grouped by request day

    @pytest.mark.asyncio
    async def test_a_dead_pdf_link_stays_in_the_queue_and_is_not_hammered(
        self, db, cfg, monkeypatch
    ) -> None:
        seen = _serve(monkeypatch, lambda _: httpx.Response(404))
        vault = FakeVault({QUEUE: f"- {URL}\n"})

        for _ in range(3):
            await _poll(db, vault, cfg)

        assert vault.notes[QUEUE] == f"- {URL}\n" and len(seen) == 1

    @pytest.mark.asyncio
    async def test_the_old_queue_note_is_still_polled(self, db, cfg, monkeypatch) -> None:
        _serve(monkeypatch, _ok)
        ingest_pdf(db, cfg, PDF, name="q.pdf", via="link", origin="inbox", source_url=URL)
        db.execute("UPDATE items SET note_path = '15 PDFs/q.md', stage_write = 'done'")
        db.commit()
        vault = FakeVault({QUEUE: QUEUE_PLACEHOLDER + "\n", LEGACY: f"- {URL}\n"})

        assert await _poll(db, vault, cfg) == 1

        assert "[[15 PDFs/q.md" in vault.notes[DONE]
        assert vault.notes[LEGACY] == QUEUE_PLACEHOLDER + "\n"

    @pytest.mark.asyncio
    async def test_a_pdf_link_is_left_alone_when_the_kind_is_off(self, db, tmp_path) -> None:
        off = PdfConfig(enabled=False, store_dir=tmp_path / "s")
        vault = FakeVault({QUEUE: f"- {URL}\n"})
        await _poll(db, vault, off)
        assert vault.notes[QUEUE] == f"- {URL}\n"
        assert db.execute("SELECT COUNT(*) FROM items").fetchone()[0] == 0

    @pytest.mark.asyncio
    async def test_a_vault_outage_is_a_noop(self, db, cfg) -> None:
        class Down(FakeVault):
            async def read_note(self, path: str) -> str | None:
                raise VaultUnavailable("couch down")

        assert await _poll(db, Down({}), cfg) == 0


def _app(tmp_path: Path, **pdf: Any):
    settings = Settings(
        llm={"tiers": {"map": {"primary": {"provider": "ollama", "model": "x"}},
                       "reduce": {"primary": {"provider": "ollama", "model": "x"}}}},
        transcription={"remote_url": "http://127.0.0.1:9"},
        output={"db_path": tmp_path / "state.sqlite", "work_dir": tmp_path / "work"},
        pdf={"store_dir": tmp_path / "store", **pdf},
        admin_api_key="test-admin-key",
    )
    return create_app(settings)


class TestApi:
    def test_the_upload_page_loads_without_a_key(self, tmp_path: Path) -> None:
        resp = TestClient(_app(tmp_path)).get("/upload")
        assert resp.status_code == 200 and "Upload PDFs" in resp.text

    def test_uploading_needs_the_key(self, tmp_path: Path) -> None:
        resp = TestClient(_app(tmp_path)).post("/pdfs", content=PDF)
        assert resp.status_code == 401

    def test_an_upload_is_queued_then_recognised(self, tmp_path: Path) -> None:
        client = TestClient(_app(tmp_path))

        first = client.post("/pdfs?name=Report.pdf", content=PDF, headers=KEY)
        again = client.post("/pdfs?name=Other.pdf", content=PDF, headers=KEY)

        assert first.status_code == 202 and first.json()["status"] == "created"
        assert again.json()["status"] == "existing"
        assert again.json()["item_id"] == first.json()["item_id"]
        # The job the page polls is readable through the existing endpoint.
        job = client.get(f"/jobs/{first.json()['job_id']}", headers=KEY).json()
        assert job["status"] == "queued"
        # ...and the same status under /pdfs, which the upload page polls
        # (Traefik gates /pdfs* with the login and leaves /jobs to machines).
        via_pdfs = client.get(f"/pdfs/{first.json()['job_id']}", headers=KEY)
        assert via_pdfs.json() == job
        assert client.get(f"/pdfs/{first.json()['job_id']}").status_code == 401
        assert client.get("/pdfs/nope", headers=KEY).status_code == 404

    def test_non_pdfs_and_oversize_are_refused(self, tmp_path: Path) -> None:
        client = TestClient(_app(tmp_path, max_upload_mb=1))
        assert client.post("/pdfs", content=b"hello", headers=KEY).status_code == 415
        big = PDF + b"0" * (2 << 20)
        assert client.post("/pdfs", content=big, headers=KEY).status_code == 413

    def test_a_pdf_link_posted_to_jobs_is_fetched_and_queued(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _serve(monkeypatch, _ok)
        client = TestClient(_app(tmp_path))

        body = client.post("/jobs", json={"url": URL}, headers=KEY).json()

        assert body["status"] == "created" and body["job_id"]
        assert client.post("/jobs", json={"url": URL}, headers=KEY).json()["status"] == "existing"

    def test_a_dead_pdf_link_posted_to_jobs_is_rejected_not_an_error(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _serve(monkeypatch, lambda _: httpx.Response(404))
        resp = TestClient(_app(tmp_path)).post("/jobs", json={"url": URL}, headers=KEY)
        assert resp.status_code == 202 and resp.json()["status"] == "rejected"
