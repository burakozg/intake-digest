from __future__ import annotations

from pathlib import Path

from intake_digest.config import AcquisitionConfig
from intake_digest.db import connect
from intake_digest.pipeline.resolve import enqueue, metadata_from_row
from intake_digest.sources import wired

SLUG = "how-ai-leaders-are-actually-rewiring-business"
PAGE = (Path(__file__).parent / "fixtures" / "wired_page.html").read_text()


def test_enqueue_stores_a_wired_row(tmp_path: Path) -> None:
    db = connect(tmp_path / "t.db")
    cfg = AcquisitionConfig()
    meta = wired.parse_page(PAGE, SLUG, max_duration_minutes=60)
    [result] = enqueue(
        db, cfg, f"https://www.wired.com/video/watch/{SLUG}", _fetch=lambda _u: meta
    )
    assert result.status == "created"
    row = db.execute("SELECT * FROM items WHERE item_key = ?", (SLUG,)).fetchone()
    assert row["adapter"] == "wired" and row["id"] == f"wired:{SLUG}"
    assert metadata_from_row(row).adapter == "wired"
