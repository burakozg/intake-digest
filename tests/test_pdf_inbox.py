"""Ingress for PDFs: what the poller queues, and — more importantly — what it
refuses to conclude. A renamed file is not a new document, and an empty or
unmounted share is not "everything was deleted".
"""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import date
from pathlib import Path

import pytest
from pdfs import LONG, make_pdf

from intake_digest.config import PdfConfig
from intake_digest.db import connect
from intake_digest.pipeline.pdf_inbox import InputUnavailable, file_sha256, poll_pdfs

NOW = 2_000_000_000.0
OLD = NOW - 3600  # well past the settle window


@pytest.fixture
def db(tmp_path: Path) -> sqlite3.Connection:
    return connect(tmp_path / "s.sqlite")


@pytest.fixture
def root(tmp_path: Path) -> Path:
    path = tmp_path / "inbox"
    path.mkdir()
    return path


def _put(root: Path, rel: str, text: str = LONG, *, mtime: float = OLD) -> Path:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(make_pdf([text]))
    os.utime(path, (mtime, mtime))
    return path


def _poll(db: sqlite3.Connection, root: Path, **kw: object):
    cfg = PdfConfig(input_dir=root)
    return poll_pdfs(db, cfg, _now=NOW, today=date(2026, 10, 18), **kw)  # type: ignore[arg-type]


def _items(db: sqlite3.Connection) -> list[sqlite3.Row]:
    return db.execute("SELECT * FROM items WHERE kind = 'pdf' ORDER BY id").fetchall()


def _mark_written(db: sqlite3.Connection) -> None:
    db.execute("UPDATE items SET stage_write = 'done', note_path = 'n.md'")
    db.execute("UPDATE jobs SET status = 'done'")
    db.commit()


def test_a_new_pdf_is_queued_keyed_by_its_content_hash(db, root: Path) -> None:
    path = _put(root, "Reports/2026/q3.pdf")

    result = _poll(db, root)

    sha = file_sha256(path)
    assert result.new == [f"pdf:{sha}"]
    (row,) = _items(db)
    assert (row["kind"], row["item_key"], row["adapter"], row["origin"]) == (
        "pdf", sha, "pdf", "folder",
    )
    meta = json.loads(row["metadata"])
    assert meta["source_path"] == "Reports/2026/q3.pdf" and meta["title"] == "q3"
    assert db.execute("SELECT status FROM jobs WHERE item_id = ?", (row["id"],)).fetchone()[0] == (
        "queued"
    )


def test_polling_again_changes_nothing(db, root: Path) -> None:
    _put(root, "a.pdf")
    _poll(db, root)
    before = [tuple(r) for r in db.execute("SELECT * FROM items")]
    jobs = db.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]

    result = _poll(db, root)

    assert not (result.new or result.moved or result.missing or result.restored)
    assert [tuple(r) for r in db.execute("SELECT * FROM items")] == before
    assert db.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == jobs


def test_a_file_still_being_copied_is_left_for_the_next_poll(db, root: Path) -> None:
    _put(root, "partial.pdf", mtime=NOW - 5)
    assert _poll(db, root).new == []
    assert _items(db) == []


def test_non_pdfs_and_nas_litter_are_ignored(db, root: Path) -> None:
    (root / "notes.txt").write_text("x")
    _put(root, "@eaDir/thumb.pdf")
    _put(root, ".hidden/secret.pdf")
    _put(root, "UPPER.PDF")

    result = _poll(db, root)

    assert len(result.new) == 1
    assert json.loads(_items(db)[0]["metadata"])["source_path"] == "UPPER.PDF"


def test_a_renamed_pdf_is_the_same_document(db, root: Path) -> None:
    old = _put(root, "draft.pdf")
    _poll(db, root)
    _mark_written(db)

    old.rename(root / "final.pdf")
    result = _poll(db, root)

    assert result.new == [] and len(result.moved) == 1 and result.missing == []
    (row,) = _items(db)
    assert json.loads(row["metadata"])["source_path"] == "final.pdf"
    # The note names the path, but a rename is not worth rewriting it for.
    assert row["stage_write"] == "done"


def test_the_same_document_in_two_folders_is_one_item(db, root: Path) -> None:
    _put(root, "a/x.pdf")
    _put(root, "b/x.pdf")
    _poll(db, root)
    assert len(_items(db)) == 1


def test_a_second_copy_is_remembered_and_not_reread_or_churned(db, root: Path) -> None:
    _put(root, "a/x.pdf")
    _put(root, "b/x.pdf")
    _poll(db, root)
    before = [tuple(r) for r in db.execute("SELECT * FROM items")]

    result = _poll(db, root)

    assert not (result.new or result.moved or result.missing)
    assert [tuple(r) for r in db.execute("SELECT * FROM items")] == before


def test_losing_the_first_copy_of_a_duplicate_is_not_a_removal(db, root: Path) -> None:
    first = _put(root, "a/x.pdf")
    _put(root, "b/x.pdf")
    _poll(db, root)
    _mark_written(db)

    first.unlink()
    result = _poll(db, root)

    assert result.missing == []
    # ...and the surviving copy becomes the one the item points at.
    assert json.loads(_items(db)[0]["metadata"])["source_path"] == "b/x.pdf"


def test_a_pdf_that_leaves_is_marked_missing_and_its_note_rewritten_once(db, root: Path) -> None:
    path = _put(root, "a.pdf")
    _put(root, "keep.pdf", "Different text. " + LONG)
    _poll(db, root)
    _mark_written(db)

    path.unlink()
    result = _poll(db, root)

    assert len(result.missing) == 1
    gone = db.execute("SELECT * FROM items WHERE id = ?", (result.missing[0],)).fetchone()
    assert json.loads(gone["metadata"])["missing_since"] == "2026-10-18"
    assert gone["stage_write"] == "pending"  # the runner re-renders; no LLM stage repeats
    queued = db.execute(
        "SELECT COUNT(*) FROM jobs WHERE item_id = ? AND status = 'queued'", (gone["id"],)
    ).fetchone()[0]
    assert queued == 1

    # Reported once, not on every poll afterwards.
    again = _poll(db, root)
    assert again.missing == []
    assert db.execute("SELECT COUNT(*) FROM jobs WHERE status = 'queued'").fetchone()[0] == 1


def test_a_pdf_that_returns_is_restored(db, root: Path) -> None:
    path = _put(root, "a.pdf")
    _put(root, "keep.pdf", "Different text. " + LONG)
    _poll(db, root)
    _mark_written(db)
    path.unlink()
    _poll(db, root)
    db.execute("UPDATE items SET stage_write = 'done'")
    db.commit()

    _put(root, "a.pdf")
    result = _poll(db, root)

    assert len(result.restored) == 1
    row = db.execute("SELECT * FROM items WHERE id = ?", (result.restored[0],)).fetchone()
    assert "missing_since" not in json.loads(row["metadata"])
    assert row["stage_write"] == "pending"


def test_an_empty_folder_does_not_mean_everything_was_deleted(db, root: Path) -> None:
    """An unmounted share can look exactly like this."""
    path = _put(root, "a.pdf")
    _poll(db, root)
    _mark_written(db)
    path.unlink()

    result = _poll(db, root)

    assert result.missing == []
    assert "missing_since" not in json.loads(_items(db)[0]["metadata"])


def test_an_unreadable_folder_raises_and_touches_nothing(db, tmp_path: Path) -> None:
    with pytest.raises(InputUnavailable):
        _poll(db, tmp_path / "not-mounted")
    assert _items(db) == []


def test_a_pdf_edited_in_place_is_a_new_document_and_the_old_note_stays(db, root: Path) -> None:
    path = _put(root, "a.pdf")
    _poll(db, root)
    _mark_written(db)

    path.write_bytes(make_pdf([LONG + " Revised."]))
    os.utime(path, (OLD, OLD))
    result = _poll(db, root)

    assert len(result.new) == 1 and result.missing == []
    assert len(_items(db)) == 2
