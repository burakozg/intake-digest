"""S2 for PDFs: text layer where there is one, the vision tier where there is not."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from pdfs import LONG, make_pdf

from intake_digest.config import PdfConfig
from intake_digest.llm.models import PageText
from intake_digest.pipeline.pdf_extract import PdfUnreadable, VisionUnavailable, acquire_pdf


class FakeVision:
    """Answers `vision` calls from a script: one list of page texts per call."""

    def __init__(self, *answers: list[str]) -> None:
        self.answers = list(answers)
        self.calls: list[dict[str, Any]] = []

    async def complete_structured(self, tier: str, system: str, user: str, model: type, **kw: Any):
        self.calls.append({"tier": tier, "user": user, "images": kw.get("images")})
        return PageText(pages=self.answers.pop(0)), None


def _pdf(tmp_path: Path, pages: list[str | None]) -> Path:
    path = tmp_path / "doc.pdf"
    path.write_bytes(make_pdf(pages))
    return path


async def _acquire(llm: Any, path: Path, cfg: PdfConfig | None = None, vision: bool = True):
    return await acquire_pdf(
        llm, path, cfg or PdfConfig(), title="doc", item_id="pdf:x", has_vision_tier=vision
    )


@pytest.mark.asyncio
async def test_text_pdf_is_read_without_the_vision_tier(tmp_path: Path) -> None:
    got = await _acquire(None, _pdf(tmp_path, [LONG + "One.", LONG + "Two."]), vision=False)

    assert got.extraction == "text"
    assert [p.number for p in got.pages] == [1, 2]
    assert "One." in got.pages[0].text and "Two." in got.pages[1].text
    assert got.total_pages == 2 and got.omitted == 0


@pytest.mark.asyncio
async def test_scanned_pages_go_to_vision_and_keep_their_page_numbers(tmp_path: Path) -> None:
    llm = FakeVision(["text of page two"])
    got = await _acquire(llm, _pdf(tmp_path, [LONG + "One.", None, LONG + "Three."]))

    assert got.extraction == "mixed"
    assert [(p.number, p.text.startswith(("This", "text"))) for p in got.pages] == [
        (1, True), (2, True), (3, True),
    ]
    assert got.pages[1].text == "text of page two"
    assert len(llm.calls) == 1 and len(llm.calls[0]["images"]) == 1
    assert llm.calls[0]["images"][0].startswith(b"\x89PNG")  # a real rendered image


@pytest.mark.asyncio
async def test_fully_scanned_pdf_is_vision(tmp_path: Path) -> None:
    llm = FakeVision(["a", "b"])
    got = await _acquire(llm, _pdf(tmp_path, [None, None]))
    assert got.extraction == "vision"
    assert [p.text for p in got.pages] == ["a", "b"]


@pytest.mark.asyncio
async def test_scans_without_a_vision_tier_fail_with_a_reason(tmp_path: Path) -> None:
    with pytest.raises(VisionUnavailable, match=r"llm\.tiers\.vision"):
        await _acquire(None, _pdf(tmp_path, [LONG, None]), vision=False)


@pytest.mark.asyncio
async def test_vision_batches_respect_the_configured_size(tmp_path: Path) -> None:
    llm = FakeVision(["1", "2"], ["3"])
    cfg = PdfConfig(vision_pages_per_call=2)
    got = await _acquire(llm, _pdf(tmp_path, [None, None, None]), cfg)

    assert [len(c["images"]) for c in llm.calls] == [2, 1]
    assert [p.text for p in got.pages] == ["1", "2", "3"]


@pytest.mark.asyncio
async def test_a_wrong_page_count_is_redone_one_page_at_a_time(tmp_path: Path) -> None:
    """Text attributed to the wrong page is worse than a slow read."""
    llm = FakeVision(["merged"], ["first"], ["second"])
    got = await _acquire(llm, _pdf(tmp_path, [None, None]))

    assert [p.text for p in got.pages] == ["first", "second"]
    assert [len(c["images"]) for c in llm.calls] == [2, 1, 1]


@pytest.mark.asyncio
async def test_only_max_pages_are_read_and_the_rest_counted(tmp_path: Path) -> None:
    got = await _acquire(None, _pdf(tmp_path, [LONG] * 5), PdfConfig(max_pages=2), vision=False)

    assert len(got.pages) == 2 and got.total_pages == 5 and got.omitted == 3


@pytest.mark.asyncio
async def test_garbage_is_unreadable_not_a_crash(tmp_path: Path) -> None:
    path = tmp_path / "doc.pdf"
    path.write_bytes(b"this is not a pdf")
    with pytest.raises(PdfUnreadable):
        await _acquire(None, path, vision=False)


@pytest.mark.asyncio
async def test_a_pdf_with_no_readable_text_anywhere_is_unreadable(tmp_path: Path) -> None:
    llm = FakeVision(["", ""])
    with pytest.raises(PdfUnreadable, match="no text"):
        await _acquire(llm, _pdf(tmp_path, [None, None]))
