"""Wired adapter: `wired.com/video/watch/<slug>` pages.

Wired publishes its own transcript in the page HTML (`<p data-testid=
"transcript-caption-N">` per caption line), so there is no yt-dlp, no
caption track and no ASR here — one GET yields both the metadata (a
schema.org `VideoObject` JSON-LD block) and the transcript.

The transcript carries **no timestamps**. Each line's start time is therefore
estimated by spreading the video's duration over the lines in proportion to
their length — good enough to order highlights and to cut ~60s paragraphs, but
not real, which is why `render.py` shows Wired highlight times unlinked.
"""

from __future__ import annotations

import html
import json
import re
from typing import Any
from urllib.parse import urlparse

from ..transcripts.normalize import Paragraph, merge_paragraphs
from .youtube import VideoMetadata, VideoRejected

ADAPTER = "wired"

_HOSTS = {"wired.com", "www.wired.com"}
_PATH = re.compile(r"^/video/watch/(?P<slug>[a-z0-9][a-z0-9-]*)/?$")
_LD_JSON = re.compile(
    r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>', re.DOTALL
)
_CAPTION = re.compile(
    r'<p data-testid="transcript-caption-\d+">(.*?)</p>', re.DOTALL
)
_ISO_DURATION = re.compile(r"^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?$")

#: A browser-ish UA: Wired serves the full page (transcript included) to it.
_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; intake-digest)"}


def resolve_url(url: str) -> str | None:
    """The page slug (used as `video_id`) for a Wired video URL, else None."""
    try:
        parsed = urlparse(url)
    except ValueError:
        return None
    if (parsed.hostname or "") not in _HOSTS:
        return None
    match = _PATH.match(parsed.path)
    return match.group("slug") if match else None


def canonical_url(slug: str) -> str:
    return f"https://www.wired.com/video/watch/{slug}"


def _duration_s(raw: str | None) -> int:
    match = _ISO_DURATION.match(raw or "")
    if not match:
        return 0
    hours, minutes, seconds = match.groups()
    return int(int(hours or 0) * 3600 + int(minutes or 0) * 60 + float(seconds or 0))


def _video_object(page: str) -> dict[str, Any] | None:
    for block in _LD_JSON.findall(page):
        try:
            data = json.loads(block)
        except json.JSONDecodeError:
            continue
        for item in data if isinstance(data, list) else [data]:
            if isinstance(item, dict) and item.get("@type") == "VideoObject":
                return item
    return None


def caption_lines(page: str) -> list[str]:
    lines = (re.sub(r"\s+", " ", html.unescape(m)).strip() for m in _CAPTION.findall(page))
    return [line for line in lines if line]


def parse_page(page: str, slug: str, *, max_duration_minutes: int) -> VideoMetadata:
    """Pure: page HTML -> `VideoMetadata`, or `VideoRejected`."""
    video = _video_object(page)
    if video is None:
        raise VideoRejected("unresolvable", "no VideoObject on the page", slug)

    duration_s = _duration_s(video.get("duration"))
    if duration_s > max_duration_minutes * 60:
        raise VideoRejected(
            "too_long",
            f"duration {duration_s}s exceeds max_duration_minutes={max_duration_minutes}",
            slug,
        )
    if not caption_lines(page):
        raise VideoRejected("no_transcript", "page carries no transcript", slug)

    thumbnails = video.get("thumbnailUrl")
    thumbnail = thumbnails[0] if isinstance(thumbnails, list) and thumbnails else thumbnails
    return VideoMetadata(
        video_id=slug,
        title=str(video.get("name") or ""),
        channel="WIRED",
        channel_id="",
        duration_s=duration_s,
        upload_date=str(video.get("uploadDate") or "")[:10] or None,
        description=str(video.get("description") or ""),
        language="en",
        has_manual_subs=True,
        manual_sub_langs=["en"],
        thumbnail_url=thumbnail if isinstance(thumbnail, str) else None,
        adapter=ADAPTER,
    )


def build_paragraphs(lines: list[str], duration_s: int) -> list[Paragraph]:
    """Estimate a start time per caption line (proportional to characters
    spoken so far) and merge into the usual 60-90s paragraphs."""
    total = sum(len(line) + 1 for line in lines) or 1
    fragments: list[tuple[float, str]] = []
    spoken = 0
    for line in lines:
        fragments.append((duration_s * spoken / total, line))
        spoken += len(line) + 1
    return merge_paragraphs(fragments)


def _get(slug: str, timeout_s: float = 30.0) -> str:
    import httpx

    resp = httpx.get(
        canonical_url(slug), headers=_HEADERS, timeout=timeout_s, follow_redirects=True
    )
    resp.raise_for_status()
    return resp.text


def fetch_metadata(slug: str, *, max_duration_minutes: int) -> VideoMetadata:
    return parse_page(_get(slug), slug, max_duration_minutes=max_duration_minutes)


def fetch_paragraphs(slug: str, duration_s: int) -> list[Paragraph]:
    lines = caption_lines(_get(slug))
    if not lines:
        raise VideoRejected("no_transcript", "page carries no transcript", slug)
    return build_paragraphs(lines, duration_s)
