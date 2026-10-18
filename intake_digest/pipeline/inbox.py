"""Vault inbox watcher (design §4.3).

Polls the intake queue note — `vault.inbox_note`, by default
`00 intake/_intake_queue.md` (the `_` prefix pins it to the top of the folder
in Obsidian's default name sort) — and any `legacy_inbox_notes`. Any line
carrying a bare video or PDF link that is not yet a wikilink is enqueued
(`origin='inbox'`); a link ending in `.pdf` is fetched and kept as a PDF item. Everything the human
typed that is not a URL line, or a URL still new/running/failed, is left in
place untouched.

A line that resolves to a finished note is *removed* from the queue and
appended to `vault.completed_note` instead of being rewritten in place — the
queue is meant to hold only what still needs attention, empty by default. A
line that is already a wikilink when polled (an older note written before
this split existed, or the human's own link) migrates out the same way, on
whichever poll first sees it.

Enqueue is idempotent: a URL that resolves to a known video dedupes in
`resolve.py` with no network call, so re-processing an already-queued line
every poll costs nothing until its note appears.
"""

from __future__ import annotations

import contextlib
import json
import re
import sqlite3

from ..config import AcquisitionConfig, PdfConfig, VaultConfig
from ..logging_setup import get_logger
from ..utils import epoch_ms
from ..vault.livesync import LiveSyncVault, VaultUnavailable
from .pdf_intake import LinkRejected, LinkUnavailable, ingest_link, is_pdf_url
from .resolve import enqueue

log = get_logger(__name__)

_URL = re.compile(r"https?://[^\s<>()\[\]]+")
#: Greedy, not `[^\]]+`: a queue line holds at most one video, so `[[` to the
#: *last* `]]` on the line is that line's whole wikilink. A video title can
#: itself contain a bracket pair -- "... | [un]prompted 2026]]" is a real
#: alias in this vault -- and `[^\]]+` stops at that inner `]`, so the line
#: never matches at all and the video it names never leaves the queue.
_WIKILINK = re.compile(r"\[\[.*\]\]")
#: Leading whitespace plus an optional markdown bullet — preserved when a
#: line is rewritten so a checklist stays a checklist.
_PREFIX = re.compile(r"^(\s*(?:[-*]\s+)?)")


async def poll_inbox(
    db: sqlite3.Connection,
    vault: LiveSyncVault,
    acquisition_cfg: AcquisitionConfig,
    vault_cfg: VaultConfig,
    pdf_cfg: PdfConfig | None = None,
) -> int:
    """Enqueue new links in the intake queue notes and move finished ones out
    to the completed note. Returns the number of lines changed.

    The current queue note is created if missing (somewhere to paste into);
    the legacy ones are polled if they exist and never recreated. A vault that
    will not answer is a no-op (returns 0), not an error.
    """
    changed = 0
    sources = [(vault_cfg.inbox_note, True)] + [
        (path, False) for path in vault_cfg.legacy_inbox_notes if path != vault_cfg.inbox_note
    ]
    for queue_path, create in sources:
        try:
            changed += await _poll_queue(
                db, vault, acquisition_cfg, vault_cfg, pdf_cfg, queue_path, create=create
            )
        except VaultUnavailable as exc:
            log.warning("inbox.vault_unavailable", error=str(exc))
            return changed
    try:
        changed += await _sync_completed(db, vault, vault_cfg)
    except VaultUnavailable as exc:
        log.warning("inbox.vault_unavailable", error=str(exc))
    return changed


def _moved_pointer(vault_cfg: VaultConfig) -> str:
    target = vault_cfg.completed_note.removesuffix(".md")
    return f"Moved to [[{target}]].\n"


async def _sync_completed(
    db: sqlite3.Connection, vault: LiveSyncVault, vault_cfg: VaultConfig
) -> int:
    """Two things that reach the completed note without passing through a queue
    line: lines folded in from a legacy completed note (once, after which that
    note becomes a pointer), and uploaded PDFs, which are never in any queue.
    Returns the number of lines added."""
    pointer = _moved_pointer(vault_cfg)
    existing = (await vault.read_note(vault_cfg.completed_note)) or ""
    text = existing
    added = 0

    folded: list[str] = []
    for legacy in vault_cfg.legacy_completed_notes:
        if legacy == vault_cfg.completed_note:
            continue
        old = await vault.read_note(legacy)
        if old is None or not old.strip() or old == pointer:
            continue
        have = set(text.splitlines())
        fresh = [
            ln
            for ln in old.splitlines()
            if not ln.strip() or ln.startswith("## ") or ln not in have
        ]
        first = next((ln for ln in fresh if ln.strip()), "")
        if not first.startswith("## "):
            fresh.insert(0, "## Older")  # undated lines must not join the last day
        added += sum(1 for ln in fresh if ln.strip() and not ln.startswith("## "))
        text = text.rstrip("\n") + "\n\n" + "\n".join(fresh) + "\n"
        folded.append(legacy)

    uploads = db.execute(
        "SELECT id, note_path, metadata, created_at FROM items "
        "WHERE kind = 'pdf' AND origin = 'upload' AND note_path IS NOT NULL "
        "AND stage_write = 'done' AND NOT EXISTS "
        "(SELECT 1 FROM meta WHERE key = 'completed_logged:' || items.id) "
        "ORDER BY created_at"
    ).fetchall()
    finished: list[tuple[str, str | None]] = []
    for row in uploads:
        title = ""
        with contextlib.suppress(json.JSONDecodeError, TypeError):
            title = str(json.loads(row["metadata"] or "{}").get("title") or "")
        link = f"[[{row['note_path']}|{title}]]" if title else f"[[{row['note_path']}]]"
        finished.append((f"- {link}", str(row["created_at"])[:10]))

    if not folded and not finished:
        return 0
    await vault.project(
        vault_cfg.completed_note,
        _group_completed(db, text, finished),
        mtime_ms=epoch_ms(),
        merge=False,
    )
    for legacy in folded:
        await vault.project(legacy, pointer, mtime_ms=epoch_ms(), merge=False)
    for row in uploads:
        db.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES (?, '1')",
            (f"completed_logged:{row['id']}",),
        )
    db.commit()
    log.info(
        "inbox.completed_synced",
        folded=folded,
        uploads=len(finished),
        completed=vault_cfg.completed_note,
    )
    return added + len(finished)


async def _resolve_pdf_line(
    db: sqlite3.Connection, pdf_cfg: PdfConfig, line: str, url: str
) -> list[tuple[str, str | None]] | None:
    """A queue line holding a PDF link. Fetches it the first time (and keeps a
    copy); on later polls the link is known and nothing is fetched. The line
    leaves the queue only once the summary note exists."""
    try:
        got = await ingest_link(db, pdf_cfg, url, origin="inbox")
    except LinkRejected as exc:
        log.warning("inbox.pdf_rejected", url=url, reason=str(exc))
        return None  # stays in the queue, recorded so it is not refetched
    except LinkUnavailable as exc:
        log.warning("inbox.pdf_unavailable", url=url, error=str(exc))
        return None
    if got.note_path is None:
        return None
    row = db.execute(
        "SELECT created_at, metadata FROM items WHERE id = ?", (got.item_id,)
    ).fetchone()
    title = ""
    with contextlib.suppress(json.JSONDecodeError, TypeError):
        title = str(json.loads(row["metadata"] or "{}").get("title") or "")
    link = f"[[{got.note_path}|{title}]]" if title else f"[[{got.note_path}]]"
    prefix = _PREFIX.match(line)
    return [((prefix.group(1) if prefix else "") + link, str(row["created_at"])[:10])]


async def _poll_queue(
    db: sqlite3.Connection,
    vault: LiveSyncVault,
    acquisition_cfg: AcquisitionConfig,
    vault_cfg: VaultConfig,
    pdf_cfg: PdfConfig | None,
    queue_path: str,
    *,
    create: bool,
) -> int:
    current = await vault.read_note(queue_path)
    if current is None or not current.strip():
        if not create:
            return 0
        # Missing, or left empty by an earlier version: put the note back so
        # there is somewhere to paste into.
        written = await vault.project(
            queue_path, QUEUE_PLACEHOLDER + "\n", mtime_ms=epoch_ms(), merge=False, revive=True
        )
        event = "inbox.queue_restored" if written else "inbox.queue_restore_skipped"
        log.info(event, queue=queue_path)
        return 0

    remaining: list[str] = []
    finished: list[tuple[str, str | None]] = []
    changed = 0
    for line in current.splitlines():
        url_match = _URL.search(line)
        if (
            pdf_cfg is not None
            and pdf_cfg.enabled
            and url_match is not None
            and not _WIKILINK.search(line)
            and is_pdf_url(url_match.group(0))
        ):
            done = await _resolve_pdf_line(db, pdf_cfg, line, url_match.group(0))
        else:
            done = _resolve_line(db, line, acquisition_cfg, vault_cfg)
        if done is None:
            remaining.append(line)
            continue
        changed += 1
        finished.extend(done)

    if not changed:
        return 0

    rebuilt = "\n".join(remaining)
    if not rebuilt.strip():
        rebuilt = QUEUE_PLACEHOLDER
    if current.endswith("\n") or rebuilt == QUEUE_PLACEHOLDER:
        rebuilt += "\n"
    await vault.project(queue_path, rebuilt, mtime_ms=epoch_ms(), merge=False)

    completed_path = vault_cfg.completed_note
    existing = (await vault.read_note(completed_path)) or ""
    await vault.project(
        completed_path, _group_completed(db, existing, finished), mtime_ms=epoch_ms(), merge=False
    )

    log.info(
        "inbox.updated", queue=queue_path, completed=completed_path, lines_changed=changed
    )
    return changed


#: What an otherwise-empty queue note holds. LiveSync/Obsidian treat a 0-byte
#: note as gone, so the queue always keeps one line of text. It is not a URL,
#: so the watcher leaves it alone.
QUEUE_PLACEHOLDER = "Paste video or PDF links below, one per line."

_DATE_HEADING = re.compile(r"^##\s+(\d{4}-\d{2}-\d{2})\s*$")
_OLDER = "Older"
_NOTE_PATH = re.compile(r"\[\[([^\]|]+)")


def _line_day(db: sqlite3.Connection, line: str) -> str | None:
    """The request day of the video a completed-note line links to, from the
    `items` table via its note path; None when the line links nothing we know."""
    link = _NOTE_PATH.search(line)
    if link is None:
        return None
    row = db.execute(
        "SELECT created_at FROM items WHERE note_path = ?", (link.group(1),)
    ).fetchone()
    return str(row["created_at"])[:10] if row and row["created_at"] else None


def _group_completed(
    db: sqlite3.Connection, existing: str, finished: list[tuple[str, str | None]]
) -> str:
    """Re-render the completed note as `## YYYY-MM-DD` sections (the day each
    video was requested, newest first) with `## Older` last. Anything already
    in the note that is not under a date heading -- everything written before
    the grouping existed -- is `Older`, as is a line whose request date cannot
    be found. The video's own publish date lives in its summary, not here."""
    sections: dict[str, list[str]] = {}
    current = _OLDER
    for line in existing.splitlines():
        heading = _DATE_HEADING.match(line)
        if heading:
            current = heading.group(1)
        elif line.strip() == f"## {_OLDER}":
            current = _OLDER
        elif line.strip():
            # Lines from before the grouping, or filed under Older, are dated
            # from the database when it knows them -- Older is only for the
            # ones it cannot place.
            day = _line_day(db, line) if current == _OLDER else None
            sections.setdefault(day or current, []).append(line)
    for line, day in finished:
        sections.setdefault(day or _OLDER, []).append(line)

    days = sorted((d for d in sections if d != _OLDER), reverse=True)
    parts = [f"## {d}\n" + "\n".join(sections[d]) for d in days]
    if _OLDER in sections:
        parts.append(f"## {_OLDER}\n" + "\n".join(sections[_OLDER]))
    return "\n\n".join(parts) + "\n"


def _request_day(db: sqlite3.Connection, video_id: str) -> str | None:
    row = db.execute("SELECT created_at FROM items WHERE item_key = ?", (video_id,)).fetchone()
    return str(row["created_at"])[:10] if row and row["created_at"] else None


def _resolve_line(
    db: sqlite3.Connection,
    line: str,
    acquisition_cfg: AcquisitionConfig,
    vault_cfg: VaultConfig,
) -> list[tuple[str, str | None]] | None:
    """``None`` to leave ``line`` in the queue untouched; otherwise the
    ``(line, request_day)`` pair(s) to add to the completed note in its place."""
    if _WIKILINK.search(line):
        # Already resolved — either this poll's own doing on an earlier line
        # of a playlist, or one written before the queue/completed split
        # existed. Either way it does not belong in the queue any more. Its
        # request day is recovered from the note it links to, if we know it.
        return [(line, _line_day(db, line))]

    match = _URL.search(line)
    if match is None:
        return None  # human prose, a heading, a blank line — not ours to move
    url = match.group(0)

    try:
        results = enqueue(db, acquisition_cfg, url, origin="inbox")
    except Exception as exc:  # a bad URL or a yt-dlp break must not kill the poll
        log.warning("inbox.enqueue_failed", url=url, error=f"{type(exc).__name__}: {exc}")
        return None

    if not results:
        return None

    links = [_note_link(db, r.video_id) for r in results]
    days = [_request_day(db, r.video_id) for r in results]
    if any(link is None for link in links):
        # At least one video in this line has no note yet — leave the line as
        # it is and try again next poll.
        return None

    prefix = _PREFIX.match(line)
    head = prefix.group(1) if prefix else ""
    if len(links) == 1:
        return [(f"{head}{links[0]}", days[0])]
    # A playlist URL expanded to several videos: one bullet per note.
    bullet = head if head.strip() else "- "
    return [(f"{bullet}{link}", day) for link, day in zip(links, days, strict=True)]


def _note_link(db: sqlite3.Connection, video_id: str) -> str | None:
    """`[[<note path>|<title>]]` for a video whose note exists, else None.
    Matches the wikilink form the S5 writer uses on topic pages
    (`vault/write.py::_update_topic_page`) — full path, `.md` included."""
    row = db.execute(
        "SELECT note_path, metadata FROM items WHERE item_key = ?", (video_id,)
    ).fetchone()
    if row is None or not row["note_path"]:
        return None
    title = ""
    with contextlib.suppress(json.JSONDecodeError, TypeError):
        title = str(json.loads(row["metadata"] or "{}").get("title") or "")
    return f"[[{row['note_path']}|{title}]]" if title else f"[[{row['note_path']}]]"
