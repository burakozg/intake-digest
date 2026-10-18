"""The queued-job runner (design §6).

`pipeline/resolve.py::enqueue` (and each other kind's intake) leaves a `jobs`
row at `status='queued'`. This module consumes them: `run_due_jobs` picks up
queued jobs and hands each to its kind (`pipeline/kinds/`), which walks the
item from wherever its per-stage checkpoints left off to a note in the vault.

What a stage *does* is the kind's business; what the runner owns is the queue:
oldest first, one job per item per pass, and the three non-terminal outcomes
(parked, deferred, failed) mapped back onto `jobs.status`.

Scheduled every `scheduler.job_poll_seconds` from `scheduler.py`, once per
kind. Each kind's runner is strictly sequential — `max_instances=1` on its
scheduler job — so no two items of one kind are in flight at once, while a
slow kind (a vision-heavy PDF) cannot hold up another (a video).
"""

from __future__ import annotations

import sqlite3
from typing import Any

from ..config import Settings
from ..llm.base import StructuredLLM
from ..logging_setup import get_logger
from ..notify import Notifier
from ..vault.livesync import LiveSyncVault
from .kinds import RewriteUnavailable, get_kind
from .stages import set_job_status

__all__ = ["RewriteUnavailable", "rewrite_from_stored_digest", "run_due_jobs", "run_job"]

log = get_logger(__name__)

#: How many queued jobs one `run_due_jobs` pass will advance. Sequential and
#: some stages are slow (an ASR download, a reduce call), so a pass is
#: deliberately short; the next scheduled fire picks up the rest.
_DEFAULT_BATCH = 5


async def run_job(
    db: sqlite3.Connection,
    settings: Settings,
    row: sqlite3.Row,
    *,
    llm: StructuredLLM,
    vault: LiveSyncVault,
    force: bool = False,
    notifier: Notifier | None = None,
    **seams: Any,
) -> str:
    """Advance one item as far as it will go this call, by its kind's
    pipeline. Returns one of ``"done"`` | ``"parked"`` | ``"deferred"`` |
    ``"failed"``. `seams` are the kind's own `_`-prefixed test hooks."""
    return await get_kind(str(row["kind"])).run(
        db, settings, row, llm=llm, vault=vault, force=force, notifier=notifier, **seams
    )


async def rewrite_from_stored_digest(
    db: sqlite3.Connection,
    settings: Settings,
    vault: LiveSyncVault,
    video_id: str,
) -> str:
    """Re-render a video's note from the digest already on the row — S5 only,
    no LLM call (design §6 / §8's `POST /videos/{id}/rewrite`). Returns the
    note path."""
    return await get_kind("video").rewrite(db, settings, vault, video_id)


async def run_due_jobs(
    db: sqlite3.Connection,
    settings: Settings,
    *,
    llm: StructuredLLM | None = None,
    vault: LiveSyncVault | None = None,
    notifier: Notifier | None = None,
    limit: int = _DEFAULT_BATCH,
    kind: str | None = None,
    _run_job: Any = None,
) -> int:
    """Advance up to `limit` queued jobs, oldest first. Returns how many
    reached a finished note this pass.

    `kind` restricts the pass to one kind's items — the scheduler runs one
    pass per kind so they never queue behind each other. None takes them all.

    Videos parked for ASR (`asr_queued_at` set) are skipped — they are
    `pipeline/asr_drain.py`'s to clear, and reappear here on the pass after
    it does.
    """
    runner = _run_job or run_job

    # 'running' is included so a job orphaned by a mid-pass crash is picked
    # back up: only one runner exists (max_instances=1 on the scheduler job),
    # so anything still 'running' at query time is a leftover, never a live
    # concurrent run. The videos row's stage checkpoints carry the real
    # resume point.
    pending: list[sqlite3.Row] = db.execute(
        "SELECT j.id AS job_id, j.item_id AS item_id, j.force AS force "
        "FROM jobs j JOIN items v ON v.id = j.item_id "
        "WHERE j.status IN ('queued', 'running') AND v.asr_queued_at IS NULL "
        "AND (? IS NULL OR v.kind = ?) "
        "ORDER BY j.created_at ASC",
        (kind, kind),
    ).fetchall()
    if not pending:
        return 0

    # One video may have several queued jobs (a repeated force=true). Keep
    # the oldest, drop the rest from this pass.
    seen: set[str] = set()
    queue: list[sqlite3.Row] = []
    for job in pending:
        if job["item_id"] in seen:
            continue
        seen.add(job["item_id"])
        queue.append(job)
        if len(queue) >= limit:
            break

    if llm is None:
        from ..llm.client import LLMClient

        llm = LLMClient(settings, db)
    if vault is None:
        vault = LiveSyncVault(settings.vault, settings.vault_password())

    completed = 0
    for job in queue:
        job_id = str(job["job_id"])
        video_row: sqlite3.Row | None = db.execute(
            "SELECT * FROM items WHERE id = ?", (str(job["item_id"]),)
        ).fetchone()
        if video_row is None:
            set_job_status(db, job_id, "failed")
            continue

        set_job_status(db, job_id, "running")
        try:
            outcome = await runner(
                db,
                settings,
                video_row,
                force=bool(job["force"]),
                llm=llm,
                vault=vault,
                notifier=notifier,
            )
        except Exception:
            log.exception("runner.job_crashed", video_id=job["item_id"])
            set_job_status(db, job_id, "failed")
            if notifier:
                await notifier.job_failed(str(job["item_id"]), "runner", "unhandled exception")
            continue

        if outcome == "done":
            set_job_status(db, job_id, "done")
            completed += 1
        elif outcome == "failed":
            set_job_status(db, job_id, "failed")
        else:  # parked | deferred — try again on a later pass
            set_job_status(db, job_id, "queued")

    if completed:
        log.info("runner.pass_done", completed=completed, considered=len(queue))
    return completed
