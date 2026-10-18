"""Getting a PDF into the queue: a pasted link, an upload, or (separately,
`pdf_inbox.py`) a watched folder.

Links and uploads are kept in `pdf.store_dir` as `<sha256>.pdf`, because
unlike a watched folder nothing else holds the original. The content hash is
the item's identity everywhere, so the same document arriving by two routes is
one item, and a re-pasted link is recognised without fetching it again.

A link that can never work (404, not a PDF, too large) is recorded as a
rejected item so the poller does not fetch it again every few minutes; a
transient failure (timeout, 5xx, network) is not recorded and is simply tried
on the next poll.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Literal
from urllib.parse import unquote, urlparse

import httpx

from ..config import PdfConfig
from ..logging_setup import get_logger
from ..utils import iso_now, new_job_id

log = get_logger(__name__)

ADAPTER = "pdf"
_MAGIC = b"%PDF-"
_SAFE_NAME = re.compile(r"[^\w.\- ]+")


class NotAPdf(Exception):
    """The bytes are not a PDF (no `%PDF-` header)."""


class TooLarge(Exception):
    pass


class LinkRejected(Exception):
    """The link will never yield a PDF; recorded so it is not retried."""


class LinkUnavailable(Exception):
    """A transient failure fetching the link; try again next poll."""


@dataclass(slots=True)
class Ingested:
    item_id: str
    job_id: str | None
    status: Literal["created", "existing"]
    note_path: str | None = None


def is_pdf_url(url: str) -> bool:
    return urlparse(url).path.lower().endswith(".pdf")


def _title_from(name: str) -> str:
    stem = PurePosixPath(name).stem or name
    return stem.replace("_", " ").strip() or "document"


def safe_filename(name: str | None, fallback: str = "document.pdf") -> str:
    base = PurePosixPath(unquote(name or "")).name
    cleaned = _SAFE_NAME.sub("", base).strip(" .")
    return cleaned or fallback


def store_path(cfg: PdfConfig, sha: str) -> str:
    return f"{sha}.pdf"


def _write_atomic(target: os.PathLike[str] | str, data: bytes) -> None:
    tmp = f"{target}.tmp"
    with open(tmp, "wb") as handle:
        handle.write(data)
    os.replace(tmp, target)


def find_by_url(db: sqlite3.Connection, url: str) -> sqlite3.Row | None:
    row: sqlite3.Row | None = db.execute(
        "SELECT * FROM items WHERE kind = 'pdf' AND EXISTS ("
        "SELECT 1 FROM json_each(items.metadata, '$.source_urls') WHERE value = ?)",
        (url,),
    ).fetchone()
    return row


def _add_source_url(db: sqlite3.Connection, row: sqlite3.Row, url: str) -> None:
    meta = json.loads(row["metadata"] or "{}")
    urls = meta.setdefault("source_urls", [])
    if url not in urls:
        urls.append(url)
        db.execute(
            "UPDATE items SET metadata = ?, updated_at = ? WHERE id = ?",
            (json.dumps(meta, sort_keys=True), iso_now(), row["id"]),
        )
        db.commit()


def ingest_pdf(
    db: sqlite3.Connection,
    cfg: PdfConfig,
    data: bytes,
    *,
    name: str,
    via: Literal["link", "upload"],
    origin: str,
    source_url: str | None = None,
) -> Ingested:
    """Keep `data` and queue it, unless that exact document is already known."""
    if not data.startswith(_MAGIC) and _MAGIC not in data[:1024]:
        raise NotAPdf(f"{name}: not a PDF")
    if len(data) > cfg.max_upload_mb * 1024 * 1024:
        raise TooLarge(f"{name}: larger than {cfg.max_upload_mb} MB")

    sha = hashlib.sha256(data).hexdigest()
    row_id = f"pdf:{sha}"
    existing = db.execute("SELECT * FROM items WHERE id = ?", (row_id,)).fetchone()
    if existing is not None:
        if source_url:
            _add_source_url(db, existing, source_url)
        return Ingested(row_id, None, "existing", existing["note_path"])

    cfg.store_dir.mkdir(parents=True, exist_ok=True)
    stored = store_path(cfg, sha)
    _write_atomic(cfg.store_dir / stored, data)

    filename = safe_filename(name)
    meta: dict[str, object] = {
        "title": _title_from(filename),
        "filename": filename,
        "via": via,
        "stored_path": stored,
        "size": len(data),
    }
    if source_url:
        meta["source_urls"] = [source_url]
    now = iso_now()
    job_id = new_job_id()
    db.execute(
        "INSERT INTO items (id, kind, item_key, adapter, origin, url, canonical_url, "
        "metadata, stage_resolve, stage_metadata, created_at, updated_at) "
        "VALUES (?, 'pdf', ?, ?, ?, ?, ?, ?, 'done', 'done', ?, ?)",
        (
            row_id, sha, ADAPTER, origin, source_url or filename, source_url or filename,
            json.dumps(meta, sort_keys=True), now, now,
        ),
    )
    db.execute(
        "INSERT INTO jobs (id, item_id, force, status, created_at, updated_at) "
        "VALUES (?, ?, 0, 'queued', ?, ?)",
        (job_id, row_id, now, now),
    )
    db.commit()
    log.info("pdf.enqueued", item=row_id, via=via, size=len(data))
    return Ingested(row_id, job_id, "created")


def record_rejection(db: sqlite3.Connection, url: str, reason: str) -> None:
    """Remember a link that cannot work, so the next poll skips it."""
    sha = hashlib.sha256(url.encode()).hexdigest()
    now = iso_now()
    meta = {"title": url, "via": "link", "source_urls": [url]}
    db.execute(
        "INSERT OR IGNORE INTO items (id, kind, item_key, adapter, origin, url, canonical_url, "
        "metadata, stage_resolve, stage_metadata, stage_errors, last_error, created_at, "
        "updated_at) VALUES (?, 'pdf', ?, ?, 'inbox', ?, ?, ?, 'done', 'failed', ?, ?, ?, ?)",
        (
            f"pdf:url:{sha}", f"url:{sha}", ADAPTER, url, url, json.dumps(meta),
            json.dumps({"metadata": reason}), reason, now, now,
        ),
    )
    db.commit()
    log.warning("pdf.link_rejected", url=url, reason=reason)


async def fetch_pdf(url: str, cfg: PdfConfig) -> tuple[bytes, str]:
    """Download `url`; returns `(bytes, filename)`. Raises `LinkRejected` for
    a definitive no and `LinkUnavailable` for a maybe-later."""
    limit = cfg.max_upload_mb * 1024 * 1024
    if urlparse(url).scheme not in ("http", "https"):
        raise LinkRejected("only http(s) links can be fetched")
    chunks: list[bytes] = []
    size = 0
    try:
        async with (
            httpx.AsyncClient(
                timeout=cfg.download_timeout_s, follow_redirects=True, max_redirects=5
            ) as client,
            client.stream("GET", url) as response,
        ):
            if response.status_code in (404, 410, 401, 403):
                raise LinkRejected(f"HTTP {response.status_code}")
            if response.status_code >= 400:
                raise LinkUnavailable(f"HTTP {response.status_code}")
            async for chunk in response.aiter_bytes():
                size += len(chunk)
                if size > limit:
                    raise LinkRejected(f"larger than {cfg.max_upload_mb} MB")
                chunks.append(chunk)
            final_name = PurePosixPath(unquote(urlparse(str(response.url)).path)).name
    except (httpx.TimeoutException, httpx.TransportError) as exc:
        raise LinkUnavailable(f"{type(exc).__name__}: {exc}") from exc
    data = b"".join(chunks)
    if _MAGIC not in data[:1024]:
        raise LinkRejected("the link did not return a PDF")
    return data, final_name or PurePosixPath(urlparse(url).path).name


async def ingest_link(db: sqlite3.Connection, cfg: PdfConfig, url: str, *, origin: str) -> Ingested:
    """Fetch and queue a PDF link. Safe to call every poll: a link already
    known is answered from the database, with no network call."""
    known = find_by_url(db, url)
    if known is not None:
        return Ingested(known["id"], None, "existing", known["note_path"])
    try:
        data, name = await fetch_pdf(url, cfg)
        return ingest_pdf(db, cfg, data, name=name, via="link", origin=origin, source_url=url)
    except (LinkRejected, NotAPdf, TooLarge) as exc:
        record_rejection(db, url, str(exc))
        raise LinkRejected(str(exc)) from exc
