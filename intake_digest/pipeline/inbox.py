"""Vault inbox watcher (design §4.3).

Polls one note in the notes folder — `vault.inbox_note`, by default
`13 video-summaries/_video-queue.md` (the `_` prefix pins it to the top of
the folder in Obsidian's default name sort, above every
`<YYYY-MM-DD>-<slug>` digest note). Any line carrying a bare video URL that
is not yet a wikilink is enqueued (`origin='inbox'`). Everything the human
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

from ..config import AcquisitionConfig, VaultConfig
from ..logging_setup import get_logger
from ..utils import epoch_ms
from ..vault.livesync import LiveSyncVault, VaultUnavailable
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
) -> int:
    """Enqueue new URLs in the inbox note and move finished ones out to the
    completed note. Returns the number of lines changed. A missing inbox
    note, or a vault that will not answer, is a no-op (returns 0), not an
    error. A missing completed note is normal on the first run — it is
    created the first time something has to move into it.
    """
    queue_path = vault_cfg.inbox_note
    try:
        current = await vault.read_note(queue_path)
    except VaultUnavailable as exc:
        log.warning("inbox.vault_unavailable", error=str(exc))
        return 0
    if current is None or not current.strip():
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
QUEUE_PLACEHOLDER = "Paste video URLs below, one per line."

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
