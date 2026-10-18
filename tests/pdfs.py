"""Build tiny real PDFs for tests, with no PDF-writing dependency.

A page given text gets a content stream drawing it in Helvetica, so pypdf
extracts it back; a page given `None` is blank, which is what a scan looks
like to a text extractor (no text layer at all).
"""

from __future__ import annotations


def make_pdf(pages: list[str | None]) -> bytes:
    objects: list[bytes] = []

    def add(body: bytes) -> int:
        objects.append(body)
        return len(objects)

    catalog = add(b"")  # patched below: needs the pages object number
    pages_obj = add(b"")
    font = add(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")

    kids: list[int] = []
    for text in pages:
        if text is None:
            content = b""
        else:
            safe = text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
            content = f"BT /F1 12 Tf 50 700 Td ({safe}) Tj ET".encode("latin-1")
        stream = add(b"<< /Length %d >>\nstream\n" % len(content) + content + b"\nendstream")
        page = add(
            f"<< /Type /Page /Parent {pages_obj} 0 R /MediaBox [0 0 612 792] "
            f"/Resources << /Font << /F1 {font} 0 R >> >> /Contents {stream} 0 R >>".encode()
        )
        kids.append(page)

    objects[catalog - 1] = f"<< /Type /Catalog /Pages {pages_obj} 0 R >>".encode()
    refs = " ".join(f"{k} 0 R" for k in kids)
    objects[pages_obj - 1] = f"<< /Type /Pages /Kids [{refs}] /Count {len(kids)} >>".encode()

    out = bytearray(b"%PDF-1.4\n")
    offsets: list[int] = []
    for number, body in enumerate(objects, start=1):
        offsets.append(len(out))
        out += f"{number} 0 obj\n".encode() + body + b"\nendobj\n"
    xref = len(out)
    out += f"xref\n0 {len(objects) + 1}\n".encode() + b"0000000000 65535 f \n"
    for offset in offsets:
        out += f"{offset:010d} 00000 n \n".encode()
    out += (
        f"trailer\n<< /Size {len(objects) + 1} /Root {catalog} 0 R >>\n"
        f"startxref\n{xref}\n%%EOF\n"
    ).encode()
    return bytes(out)


LONG = "This sentence is long enough to count as a real text layer on a page. "
