"""Ingress for the `pdf` kind: find PDFs in the NAS folder and queue them.

Polled on a cron (`pdf.poll_cron`). The folder is the source of truth and is
mounted read-only; this never moves, renames or deletes anything in it.

An item is keyed by the **SHA-256 of the file's content**, so renaming or
moving a PDF does not create a second note, and the same document dropped in
two folders is one item. Hashing every file every poll would read the whole
share, so a file is only hashed when its (path, size, mtime) is not already on
record.

Removal is recorded, never acted on destructively: a PDF that disappears gets
`missing_since` in its metadata and its note is rewritten once to say so.
Because an unmounted or empty share looks exactly like "every file was
deleted", nothing is marked missing unless the folder is readable and still
holds at least one PDF.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

from ..config import PdfConfig
from ..logging_setup import get_logger
from ..utils import iso_now, new_job_id

log = get_logger(__name__)

ADAPTER = "nas"
_HASH_BLOCK = 1 << 20


class InputUnavailable(Exception):
    """The input folder cannot be read — typically the share is not mounted."""


@dataclass(slots=True)
class PollResult:
    new: list[str] = field(default_factory=list)
    moved: list[str] = field(default_factory=list)
    missing: list[str] = field(default_factory=list)
    restored: list[str] = field(default_factory=list)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(_HASH_BLOCK):
            digest.update(block)
    return digest.hexdigest()


def _find_pdfs(root: Path) -> list[Path]:
    if not root.is_dir():
        raise InputUnavailable(f"{root} is not a readable directory")
    found: list[Path] = []
    for dirpath, dirnames, filenames in os.walk(root):
        # QNAP keeps thumbnails in `@eaDir`, and dot-dirs are tool litter.
        dirnames[:] = sorted(d for d in dirnames if not d.startswith((".", "@")))
        for name in sorted(filenames):
            if name.lower().endswith(".pdf") and not name.startswith((".", "@")):
                found.append(Path(dirpath) / name)
    return found


def _queue_job(db: sqlite3.Connection, row_id: str, now: str) -> None:
    db.execute(
        "INSERT INTO jobs (id, item_id, force, status, created_at, updated_at) "
        "VALUES (?, ?, 0, 'queued', ?, ?)",
        (new_job_id(), row_id, now, now),
    )


def _save_metadata(
    db: sqlite3.Connection, row_id: str, meta: dict[str, object], now: str
) -> None:
    db.execute(
        "UPDATE items SET metadata = ?, updated_at = ? WHERE id = ?",
        (json.dumps(meta, sort_keys=True), now, row_id),
    )


def _request_rewrite(db: sqlite3.Connection, row: sqlite3.Row, now: str) -> None:
    """Have the runner re-render a note that already exists. Skips the earlier
    stages on its own: their checkpoints stay `done`."""
    if row["stage_write"] != "done":
        return  # nothing written yet; the pending job will carry the change
    db.execute(
        "UPDATE items SET stage_write = 'pending', updated_at = ? WHERE id = ?", (now, row["id"])
    )
    _queue_job(db, row["id"], now)


def poll_pdfs(
    db: sqlite3.Connection,
    cfg: PdfConfig,
    *,
    today: date | None = None,
    _now: float | None = None,
) -> PollResult:
    root = cfg.input_dir
    files = _find_pdfs(root)
    now_s = _now if _now is not None else time.time()
    now = iso_now()
    result = PollResult()

    rows = db.execute("SELECT * FROM items WHERE kind = 'pdf'").fetchall()
    by_id = {r["id"]: r for r in rows}
    metas = {r["id"]: json.loads(r["metadata"] or "{}") for r in rows}
    all_paths = {p.relative_to(root).as_posix() for p in files}

    def _stat_key(entry: dict[str, object]) -> tuple[object, object, object]:
        path = entry.get("source_path", entry.get("path"))
        return (path, entry.get("size"), entry.get("mtime_ns"))

    # Every (path, size, mtime) already accounted for -> its item. A copy of the
    # same document in a second place is recorded under `duplicates`, so it too
    # is recognised without being read again.
    by_stat: dict[tuple[object, object, object], str] = {}
    for rid, meta in metas.items():
        by_stat[_stat_key(meta)] = rid
        for dup in meta.get("duplicates", []):
            by_stat[_stat_key(dup)] = rid

    seen_ids: set[str] = set()
    seen_paths: set[str] = set()

    def _restore(row_id: str) -> None:
        if metas[row_id].pop("missing_since", None):
            result.restored.append(row_id)
            _request_rewrite(db, by_id[row_id], now)

    for path in files:
        rel = path.relative_to(root).as_posix()
        seen_paths.add(rel)
        stat = path.stat()
        if now_s - stat.st_mtime < cfg.settle_seconds:
            continue  # still being copied in; looked at again next poll

        key = (rel, stat.st_size, stat.st_mtime_ns)
        known = by_stat.get(key)
        if known is not None:
            seen_ids.add(known)
            meta = metas[known]
            dirty = bool(meta.get("missing_since"))
            _restore(known)
            if meta.get("source_path") not in all_paths and meta.get("source_path") != rel:
                # The first copy is gone and this is the one left: the note's
                # `source` and the acquire stage must point at a file that exists.
                meta["duplicates"] = [
                    d for d in meta.get("duplicates", []) if d.get("path") != rel
                ]
                meta.update(source_path=rel, size=stat.st_size, mtime_ns=stat.st_mtime_ns)
                result.moved.append(known)
                dirty = True
            if dirty:
                _save_metadata(db, known, meta, now)
            continue

        sha = file_sha256(path)
        row_id = f"pdf:{sha}"
        seen_ids.add(row_id)
        if row_id in by_id:
            meta = metas[row_id]
            recorded = meta.get("source_path")
            if recorded != rel and recorded in all_paths:
                # A second copy of a document whose first copy is still there.
                dups = meta.setdefault("duplicates", [])
                dups.append({"path": rel, "size": stat.st_size, "mtime_ns": stat.st_mtime_ns})
            else:
                meta.update(source_path=rel, size=stat.st_size, mtime_ns=stat.st_mtime_ns)
                if recorded != rel:
                    result.moved.append(row_id)
            _restore(row_id)
            _save_metadata(db, row_id, meta, now)
            by_stat[key] = row_id
            continue

        meta = {
            "title": path.stem,
            "filename": path.name,
            "source_path": rel,
            "size": stat.st_size,
            "mtime_ns": stat.st_mtime_ns,
        }
        db.execute(
            "INSERT INTO items (id, kind, item_key, adapter, origin, url, canonical_url, "
            "metadata, stage_resolve, stage_metadata, created_at, updated_at) "
            "VALUES (?, 'pdf', ?, ?, 'inbox', ?, ?, ?, 'done', 'done', ?, ?)",
            (row_id, sha, ADAPTER, rel, rel, json.dumps(meta, sort_keys=True), now, now),
        )
        _queue_job(db, row_id, now)
        db.commit()
        # Same-poll duplicates must find this row, not try to insert it again.
        by_id[row_id] = db.execute("SELECT * FROM items WHERE id = ?", (row_id,)).fetchone()
        metas[row_id] = meta
        by_stat[key] = row_id
        result.new.append(row_id)
        log.info("pdf.enqueued", item=row_id, path=rel)

    # An empty or unmounted share must not read as "everything was deleted".
    if files:
        stamp = (today or date.today()).isoformat()
        for row in rows:
            rid = row["id"]
            meta = metas[rid]
            still_here = rid in seen_ids or meta.get("source_path") in seen_paths
            if still_here or meta.get("missing_since"):
                continue
            meta["missing_since"] = stamp
            _save_metadata(db, rid, meta, now)
            _request_rewrite(db, row, now)
            result.missing.append(rid)
            log.warning("pdf.missing", item=rid, path=meta.get("source_path"))
    elif rows:
        log.warning("pdf.input_empty", dir=str(root), known=len(rows))

    db.commit()
    return result
