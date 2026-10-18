"""Stage and job bookkeeping shared by every kind's pipeline.

A kind (`pipeline/kinds/`) owns *what* its stages do; this module owns how a
stage is recorded. Each stage has a fixed UPDATE statement rather than an
interpolated column name, so nothing dynamic reaches the SQL.
"""

from __future__ import annotations

import json
import sqlite3

from ..utils import iso_now

#: Static per-stage checkpoint writes. "transcript" is the video kind's name
#: for the S2 acquire stage — kept as an alias because it is what is stored in
#: `stage_errors`/`attempts` JSON on existing rows and shown by the API.
STAGE_UPDATE: dict[str, str] = {
    "acquire": "UPDATE items SET stage_acquire = ?, updated_at = ? WHERE id = ?",
    "transcript": "UPDATE items SET stage_acquire = ?, updated_at = ? WHERE id = ?",
    "normalize": "UPDATE items SET stage_normalize = ?, updated_at = ? WHERE id = ?",
    "summarize": "UPDATE items SET stage_summarize = ?, updated_at = ? WHERE id = ?",
    "write": "UPDATE items SET stage_write = ?, updated_at = ? WHERE id = ?",
}


def msg(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"


def reload_row(db: sqlite3.Connection, row_id: str) -> sqlite3.Row:
    row: sqlite3.Row = db.execute("SELECT * FROM items WHERE id = ?", (row_id,)).fetchone()
    return row


def set_stage(
    db: sqlite3.Connection,
    row_id: str,
    stage: str,
    status: str,
    *,
    error: str | None = None,
) -> None:
    db.execute(STAGE_UPDATE[stage], (status, iso_now(), row_id))
    if error is not None:
        prev: sqlite3.Row = db.execute(
            "SELECT stage_errors, attempts FROM items WHERE id = ?", (row_id,)
        ).fetchone()
        errors = json.loads(prev["stage_errors"] or "{}")
        errors[stage] = error
        attempts = json.loads(prev["attempts"] or "{}")
        attempts[stage] = int(attempts.get(stage, 0)) + 1
        db.execute(
            "UPDATE items SET stage_errors = ?, attempts = ?, last_error = ?, updated_at = ? "
            "WHERE id = ?",
            (json.dumps(errors), json.dumps(attempts), error, iso_now(), row_id),
        )
    db.commit()


def set_job_status(db: sqlite3.Connection, job_id: str, status: str) -> None:
    db.execute(
        "UPDATE jobs SET status = ?, updated_at = ? WHERE id = ?", (status, iso_now(), job_id)
    )
    db.commit()
