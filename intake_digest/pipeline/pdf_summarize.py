"""S4 for the `pdf` kind: the same two passes as a video, over pages.

Pass A (map) reduces page-bounded slices to raw material; pass B (reduce)
writes one `PdfDigest`. Slices break on page boundaries, never mid-page, so
every claim can be traced to a page range and no overlap is needed.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..llm.base import StructuredLLM
from ..llm.models import ChunkSummary, PdfDigest
from ..llm.prompts import load_prompt
from ..logging_setup import get_logger
from .pdf_extract import Page
from .summarize import CHARS_PER_TOKEN, CHUNK_TARGET_TOKENS

log = get_logger(__name__)


@dataclass(slots=True)
class PageChunk:
    first_page: int
    last_page: int
    text: str


def chunk_pages(pages: list[Page]) -> list[PageChunk]:
    """Group consecutive pages up to the token budget. A single page larger
    than the budget is its own slice rather than being cut."""
    budget = CHUNK_TARGET_TOKENS * CHARS_PER_TOKEN
    chunks: list[PageChunk] = []
    current: list[Page] = []
    size = 0

    def flush() -> None:
        if current:
            chunks.append(
                PageChunk(
                    first_page=current[0].number,
                    last_page=current[-1].number,
                    text="\n\n".join(f"[page {p.number}]\n{p.text}" for p in current if p.text),
                )
            )

    for page in pages:
        if current and size + len(page.text) > budget:
            flush()
            current, size = [], 0
        current.append(page)
        size += len(page.text)
    flush()
    return [c for c in chunks if c.text.strip()]


def _render_material(chunks: list[PageChunk], summaries: list[ChunkSummary]) -> str:
    lines: list[str] = []
    for chunk, summary in zip(chunks, summaries, strict=True):
        if not summary.covered:
            continue
        span = (
            f"page {chunk.first_page}"
            if chunk.first_page == chunk.last_page
            else f"pages {chunk.first_page}-{chunk.last_page}"
        )
        lines.append(f"[{span}] {summary.covered}")
        for claim in summary.claims:
            lines.append(f"  claim: {claim}")
        if summary.entities:
            lines.append(f"  entities: {', '.join(summary.entities)}")
    return "\n".join(lines)


async def summarize_pdf(
    llm: StructuredLLM,
    *,
    item_id: str,
    title: str,
    filename: str,
    pages: list[Page],
    total_pages: int,
    known_topics: list[str] | None = None,
) -> PdfDigest:
    chunks = chunk_pages(pages)
    if not chunks:
        raise ValueError(f"pdf {item_id}: no text, nothing to summarise")

    map_prompt = load_prompt("pdf_map", "v1")
    summaries: list[ChunkSummary] = []
    for index, chunk in enumerate(chunks):
        system, user = map_prompt.render(
            index=index + 1,
            total=len(chunks),
            title=title,
            first_page=chunk.first_page,
            last_page=chunk.last_page,
            content=chunk.text,
        )
        result, _ = await llm.complete_structured(
            "map", system, user, ChunkSummary,
            video_id=item_id, prompt_version=map_prompt.versioned_name,
        )
        summaries.append(result)
        log.info("pdf.chunk_done", item=item_id, chunk=index + 1, total=len(chunks))

    reduce_prompt = load_prompt("pdf_reduce", "v1")
    read = len(pages)
    system, user = reduce_prompt.render(
        filename=filename,
        pages=total_pages,
        pages_read=read,
        omitted_pages=total_pages - read,
        chunk_summaries=_render_material(chunks, summaries),
        known_topics=known_topics or [],
    )
    digest, _ = await llm.complete_structured(
        "reduce", system, user, PdfDigest,
        video_id=item_id, prompt_version=reduce_prompt.versioned_name,
    )
    log.info("pdf.summarized", item=item_id, chunks=len(chunks))
    return digest
