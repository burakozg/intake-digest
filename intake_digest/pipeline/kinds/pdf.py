"""The `pdf` kind: acquire pages, summarise, write a note into `15 PDFs/`.

Same shape as the video kind and the same non-terminal outcomes: an
unreachable LLM tier or vault leaves the job queued (``"deferred"``), anything
else is recorded on the row and reported (``"failed"``). The stage checkpoints
resume the run, which is also how a note is re-rendered when its PDF goes
missing: only the write stage is reset, so no LLM call is repeated.

The summary is whole-file and owned outright; topics are linked later by
`clippings-topics`, which reads these notes. Nothing here touches `99 topics/`.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from ...config import Settings
from ...llm.base import LLMUnavailable, StructuredLLM
from ...llm.models import PdfDigest
from ...logging_setup import get_logger
from ...notify import Notifier
from ...utils import epoch_ms, iso_now
from ...vault.livesync import LiveSyncVault, VaultUnavailable
from ...vault.names import resolve_note_filename
from ...vault.render_pdf import render_pdf_note
from ..pdf_extract import Acquired, Page, PdfUnreadable, VisionUnavailable, acquire_pdf
from ..pdf_summarize import summarize_pdf
from ..stages import msg, reload_row, set_stage
from ..write import fetch_known_topics
from . import RewriteUnavailable

log = get_logger(__name__)


def _pages_from_json(raw: str | None) -> list[Page]:
    return [Page(number=int(p["page"]), text=str(p["text"])) for p in json.loads(raw or "[]")]


def _pages_to_json(pages: list[Page]) -> str:
    return json.dumps([{"page": p.number, "text": p.text} for p in pages])


def _file_for(settings: Settings, meta: dict[str, Any]) -> Path:
    """Where the original is: our own copy for a link or an upload, the watched
    folder (read in place) otherwise."""
    if meta.get("stored_path"):
        return Path(settings.pdf.store_dir) / str(meta["stored_path"])
    if settings.pdf.input_dir is None:
        raise FileNotFoundError(f"{meta.get('source_path')}: no watched folder is configured")
    return Path(settings.pdf.input_dir) / str(meta.get("source_path", ""))


def _source_label(meta: dict[str, Any]) -> str:
    """What the note's `source:` says the original is."""
    via = meta.get("via", "folder")
    if via == "link":
        urls = meta.get("source_urls") or []
        return str(urls[0]) if urls else str(meta.get("filename", ""))
    if via == "upload":
        return f"upload: {meta.get('filename', '')}"
    return str(meta.get("source_path", ""))


def _summary_model(settings: Settings) -> str:
    tier = settings.llm.tiers.get("reduce")
    return tier.primary.model if tier else ""


def _stored(row: sqlite3.Row) -> dict[str, Any]:
    """The item's metadata. Also where the acquire stage records what it
    found (page counts, extraction method), so the write stage can run alone."""
    stored: dict[str, Any] = json.loads(row["metadata"] or "{}")
    return stored


async def _write_note(
    db: sqlite3.Connection,
    vault: LiveSyncVault,
    settings: Settings,
    row: sqlite3.Row,
) -> str:
    meta = _stored(row)
    digest = PdfDigest.model_validate_json(row["digest"])
    created = str(row["created_at"])[:10]
    name = resolve_note_filename(db, str(row["id"]), published=created, title=digest.title)
    path = f"{settings.pdf.notes_dir}/{name}.md"
    note = render_pdf_note(
        path=path,
        digest=digest,
        created=created,
        source=_source_label(meta),
        via=str(meta.get("via", "folder")),
        sha256=str(row["item_key"]),
        pages_read=int(meta.get("pages_read", meta.get("total_pages", 0))),
        total_pages=int(meta.get("total_pages", 0)),
        extraction=str(meta.get("extraction", "text")),
        summary_model=_summary_model(settings),
        missing_since=meta.get("missing_since"),
    )
    await vault.project(note.path, note.markdown, mtime_ms=epoch_ms(), merge=False)
    now = iso_now()
    db.execute(
        # written_at is COALESCEd so the export cursor can never move (db.py).
        "UPDATE items SET note_path = ?, written_at = COALESCE(written_at, ?), "
        "updated_at = ? WHERE id = ?",
        (path, now, now, row["id"]),
    )
    db.commit()
    return path


def _reset_for_force(db: sqlite3.Connection, row_id: str) -> None:
    db.execute(
        "UPDATE items SET stage_acquire = 'pending', stage_normalize = 'pending', "
        "stage_summarize = 'pending', stage_write = 'pending', document_json = NULL, "
        "digest = NULL, updated_at = ? WHERE id = ?",
        (iso_now(), row_id),
    )
    db.commit()


async def run(
    db: sqlite3.Connection,
    settings: Settings,
    row: sqlite3.Row,
    *,
    llm: StructuredLLM,
    vault: LiveSyncVault,
    force: bool = False,
    notifier: Notifier | None = None,
    _acquire: Any = None,
    _summarize: Any = None,
    _write: Any = None,
) -> str:
    acquire = _acquire or acquire_pdf
    summarize = _summarize or summarize_pdf
    write = _write or _write_note

    row_id = str(row["id"])
    if force:
        _reset_for_force(db, row_id)
        row = reload_row(db, row_id)
    meta = _stored(row)
    title = str(meta.get("title") or row_id)

    # ── S2 acquire ───────────────────────────────────────────────────────
    if row["stage_acquire"] != "done":
        set_stage(db, row_id, "acquire", "running")
        try:
            path = _file_for(settings, meta)
            got: Acquired = await acquire(
                llm,
                path,
                settings.pdf,
                title=title,
                item_id=row_id,
                has_vision_tier="vision" in settings.llm.tiers,
            )
        except LLMUnavailable as exc:
            set_stage(db, row_id, "acquire", "pending", error=msg(exc))
            log.warning("pdf.acquire_deferred", item=row_id, error=msg(exc))
            return "deferred"
        except (PdfUnreadable, VisionUnavailable, FileNotFoundError) as exc:
            # Not transient: a re-run reads the same bytes. Say so, once.
            set_stage(db, row_id, "acquire", "failed", error=msg(exc))
            log.error("pdf.acquire_failed", item=row_id, error=msg(exc))
            if notifier:
                await notifier.job_failed(row_id, "acquire", msg(exc))
            return "failed"
        except Exception as exc:  # recorded on the row, reported, not swallowed
            set_stage(db, row_id, "acquire", "failed", error=msg(exc))
            log.exception("pdf.acquire_failed", item=row_id)
            if notifier:
                await notifier.job_failed(row_id, "acquire", msg(exc))
            return "failed"

        meta.update(
            total_pages=got.total_pages, pages_read=len(got.pages), extraction=got.extraction
        )
        db.execute(
            "UPDATE items SET stage_acquire = 'done', stage_normalize = 'done', "
            "document_json = ?, metadata = ?, updated_at = ? WHERE id = ?",
            (_pages_to_json(got.pages), json.dumps(meta, sort_keys=True), iso_now(), row_id),
        )
        db.commit()
        row = reload_row(db, row_id)

    # ── S4 summarise ─────────────────────────────────────────────────────
    if row["stage_summarize"] != "done":
        set_stage(db, row_id, "summarize", "running")
        try:
            known_topics = await fetch_known_topics(vault, settings.vault)
        except Exception as exc:  # a vocabulary hint, never a reason to defer
            log.warning("pdf.known_topics_unavailable", item=row_id, error=msg(exc))
            known_topics = []
        try:
            digest: PdfDigest = await summarize(
                llm,
                item_id=row_id,
                title=title,
                filename=str(meta.get("filename", title)),
                pages=_pages_from_json(row["document_json"]),
                total_pages=int(meta.get("total_pages", 0)),
                known_topics=known_topics,
            )
        except LLMUnavailable as exc:
            set_stage(db, row_id, "summarize", "pending", error=msg(exc))
            log.warning("pdf.summarize_deferred", item=row_id, error=msg(exc))
            return "deferred"
        except Exception as exc:
            set_stage(db, row_id, "summarize", "failed", error=msg(exc))
            log.exception("pdf.summarize_failed", item=row_id)
            if notifier:
                await notifier.job_failed(row_id, "summarize", msg(exc))
            return "failed"
        db.execute(
            "UPDATE items SET stage_summarize = 'done', digest = ?, updated_at = ? WHERE id = ?",
            (digest.model_dump_json(), iso_now(), row_id),
        )
        db.commit()
        row = reload_row(db, row_id)

    # ── S5 vault write ───────────────────────────────────────────────────
    if row["stage_write"] != "done":
        set_stage(db, row_id, "write", "running")
        try:
            note_path = await write(db, vault, settings, row)
        except VaultUnavailable as exc:
            set_stage(db, row_id, "write", "pending", error=msg(exc))
            log.warning("pdf.write_deferred", item=row_id, error=msg(exc))
            return "deferred"
        except Exception as exc:
            set_stage(db, row_id, "write", "failed", error=msg(exc))
            log.exception("pdf.write_failed", item=row_id)
            if notifier:
                await notifier.job_failed(row_id, "write", msg(exc))
            return "failed"
        set_stage(db, row_id, "write", "done")
        log.info("pdf.note_written", item=row_id, note_path=note_path)

    return "done"


async def rewrite(
    db: sqlite3.Connection, settings: Settings, vault: LiveSyncVault, key: str
) -> str:
    """Re-render from the stored digest — no LLM call. `key` is the SHA-256."""
    row = db.execute(
        "SELECT * FROM items WHERE kind = 'pdf' AND item_key = ?", (key,)
    ).fetchone()
    if row is None:
        raise KeyError(key)
    if not row["digest"]:
        raise RewriteUnavailable(key)
    path = await _write_note(db, vault, settings, row)
    db.execute(
        "UPDATE items SET stage_write = 'done', updated_at = ? WHERE id = ?",
        (iso_now(), row["id"]),
    )
    db.commit()
    return path


class PdfKind:
    """Registry entry; see `pipeline/kinds/__init__.py` for the protocol."""

    name = "pdf"

    async def run(self, *args: Any, **kwargs: Any) -> str:
        return await run(*args, **kwargs)

    async def rewrite(self, *args: Any, **kwargs: Any) -> str:
        return await rewrite(*args, **kwargs)
