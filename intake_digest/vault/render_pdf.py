"""The `15 PDFs/` note: whole-file, owned outright by this application.

Shaped so that `clippings-topics` can read it as it reads a clipping: a
`title`, a `created` date and a `source`, with the summary first — that
reader only looks at the start of the body, so the part worth extracting
topics from comes before the section detail.

Deterministic on purpose (the vault is replicated, and an unchanged PDF must
not rewrite its note): no render timestamp, stable ordering, and the only
date-bearing key beyond `created` is `pdf_missing_since`, written once when
the file leaves the folder. No wikilinks — topic linking is that reader's job —
and none to the PDF itself, which is not in the vault.
"""

from __future__ import annotations

from ..llm.models import PdfDigest
from .notes import yaml_flow_list, yaml_str
from .render import RenderedNote


def render_pdf_note(
    *,
    path: str,
    digest: PdfDigest,
    created: str,
    source: str,
    sha256: str,
    pages_read: int,
    total_pages: int,
    extraction: str,
    summary_model: str = "",
    missing_since: str | None = None,
) -> RenderedNote:
    front = [
        "---",
        "type: pdf",
        f"title: {yaml_str(digest.title)}",
        f"created: {created}",
        f"source: {yaml_str(source)}",
        f"tags: {yaml_flow_list(['pdf'])}",
        f"pdf_sha256: {sha256}",
        f"pdf_pages: {total_pages}",
        f"pdf_extraction: {extraction}",
        f"pdf_relevance: {digest.relevance}",
    ]
    if pages_read < total_pages:
        front.append(f"pdf_pages_read: {pages_read}")
        front.append("pdf_truncated: true")
    if summary_model:
        front.append(f"summary_model: {yaml_str(summary_model)}")
    if missing_since:
        front.append(f"pdf_missing_since: {missing_since}")
    front.append("---")

    body: list[str] = [f"# {digest.title}", ""]
    where = f"`{source}`"
    plural = "s" if total_pages != 1 else ""
    if missing_since:
        body.append(
            f"> The original PDF ({where}) is no longer in the folder "
            f"(found missing {missing_since})."
        )
    else:
        body.append(f"Original: {where} on the NAS · {total_pages} page{plural}")
    if pages_read < total_pages:
        body.append("")
        body.append(
            f"> Only the first {pages_read} of {total_pages} pages were read; "
            "this summary does not cover the rest."
        )
    body += ["", "## Summary", "", digest.tldr.strip(), "", digest.summary_md.strip(), ""]
    body += ["## Key points", ""] + [f"- {p.strip()}" for p in digest.key_points] + [""]
    if digest.sections:
        body += ["## Sections", ""]
        for section in digest.sections:
            body.append(f"- **{section.heading.strip()}** — {section.summary.strip()}")
        body.append("")
    if digest.entities:
        body += ["## Mentions", "", ", ".join(digest.entities), ""]
    if digest.claims_to_verify:
        body += ["## To verify", ""] + [f"- {c.strip()}" for c in digest.claims_to_verify] + [""]

    markdown = "\n".join(front) + "\n\n" + "\n".join(body).rstrip() + "\n"
    return RenderedNote(path=path, markdown=markdown)
