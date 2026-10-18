"""S2 for the `pdf` kind: get the text of a PDF's pages.

Text is read with pypdf. A page that yields almost none is a scan, and is
rendered to an image and read by the `vision` tier instead; a document can be
a mix. Only the first `max_pages` pages are read — a vision call per page of a
900-page scan is a bill, not a feature — and the result says how many were
left out, so the note can tell the reader the summary is of part of it.

The pypdf and pdfium work is CPU-bound and synchronous, so it runs in a
thread; the vision calls go through the same `StructuredLLM` boundary as every
other LLM call.
"""

from __future__ import annotations

import asyncio
import io
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from ..config import PdfConfig
from ..llm.base import StructuredLLM
from ..llm.models import PageText
from ..llm.prompts import load_prompt
from ..logging_setup import get_logger

log = get_logger(__name__)

Extraction = Literal["text", "vision", "mixed"]


class PdfUnreadable(Exception):
    """The file is not a PDF we can open — corrupt, or encrypted."""


class VisionUnavailable(Exception):
    """The PDF has scanned pages but no `vision` tier is configured."""


@dataclass(slots=True)
class Page:
    number: int  # 1-based
    text: str


@dataclass(slots=True)
class Acquired:
    pages: list[Page]
    total_pages: int
    extraction: Extraction

    @property
    def omitted(self) -> int:
        return self.total_pages - len(self.pages)


def _read_text_layer(path: Path, max_pages: int) -> tuple[int, list[str]]:
    from pypdf import PdfReader
    from pypdf.errors import PyPdfError

    try:
        reader = PdfReader(str(path))
        if reader.is_encrypted:
            raise PdfUnreadable(f"{path.name}: encrypted")
        total = len(reader.pages)
        texts: list[str] = []
        for page in reader.pages[:max_pages]:
            try:
                texts.append(page.extract_text() or "")
            except Exception:  # a single bad page must not sink the document
                log.warning("pdf.page_text_failed", file=path.name)
                texts.append("")
    except PyPdfError as exc:
        raise PdfUnreadable(f"{path.name}: {exc}") from exc
    return total, texts


def _render_pages(path: Path, numbers: list[int], dpi: int) -> list[bytes]:
    import pypdfium2 as pdfium

    doc = pdfium.PdfDocument(str(path))
    try:
        out: list[bytes] = []
        for number in numbers:
            image = doc[number - 1].render(scale=dpi / 72).to_pil()
            buf = io.BytesIO()
            image.save(buf, format="PNG")
            out.append(buf.getvalue())
        return out
    finally:
        doc.close()


async def _read_scanned(
    llm: StructuredLLM,
    path: Path,
    numbers: list[int],
    cfg: PdfConfig,
    *,
    title: str,
    item_id: str,
) -> dict[int, str]:
    prompt = load_prompt("pdf_vision", "v1")
    texts: dict[int, str] = {}

    async def call(batch: list[int]) -> list[str]:
        images = await asyncio.to_thread(_render_pages, path, batch, cfg.render_dpi)
        system, user = prompt.render(
            count=len(batch), first_page=batch[0], last_page=batch[-1], title=title
        )
        result, _ = await llm.complete_structured(
            "vision",
            system,
            user,
            PageText,
            video_id=item_id,
            prompt_version=prompt.versioned_name,
            images=images,
        )
        return result.pages

    step = cfg.vision_pages_per_call
    for start in range(0, len(numbers), step):
        batch = numbers[start : start + step]
        pages = await call(batch)
        if len(pages) != len(batch):
            # The model merged or dropped pages; attributing text to the wrong
            # page is worse than being slow, so redo this batch one page at a time.
            log.warning("pdf.vision_page_count_mismatch", asked=len(batch), got=len(pages))
            pages = []
            for number in batch:
                single = await call([number])
                if len(single) != 1:
                    raise ValueError(
                        f"vision model returned {len(single)} pages for page {number}"
                    )
                pages.append(single[0])
        texts.update(zip(batch, pages, strict=True))
    return texts


async def acquire_pdf(
    llm: StructuredLLM | None,
    path: Path,
    cfg: PdfConfig,
    *,
    title: str,
    item_id: str,
    has_vision_tier: bool,
) -> Acquired:
    total, raw = await asyncio.to_thread(_read_text_layer, path, cfg.max_pages)
    scanned = [i + 1 for i, t in enumerate(raw) if len(t.strip()) < cfg.min_chars_per_page]

    read: dict[int, str] = {i + 1: t for i, t in enumerate(raw)}
    if scanned:
        if not has_vision_tier or llm is None:
            raise VisionUnavailable(
                f"{path.name}: {len(scanned)} scanned page(s) "
                "but llm.tiers.vision is not configured"
            )
        read.update(
            await _read_scanned(llm, path, scanned, cfg, title=title, item_id=item_id)
        )

    pages = [Page(number=n, text=t.strip()) for n, t in sorted(read.items())]
    if not any(p.text for p in pages):
        raise PdfUnreadable(f"{path.name}: no text could be read from any page")

    extraction: Extraction = (
        "text" if not scanned else "vision" if len(scanned) == len(raw) else "mixed"
    )
    log.info("pdf.acquired", item=item_id, pages=len(pages), total=total, extraction=extraction)
    return Acquired(pages=pages, total_pages=total, extraction=extraction)
