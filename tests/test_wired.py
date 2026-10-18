from __future__ import annotations

from pathlib import Path

import pytest

from intake_digest.sources import wired
from intake_digest.sources.youtube import VideoRejected

SLUG = "how-ai-leaders-are-actually-rewiring-business"
PAGE = (Path(__file__).parent / "fixtures" / "wired_page.html").read_text()


@pytest.mark.parametrize(
    "url",
    [
        f"https://www.wired.com/video/watch/{SLUG}",
        f"https://wired.com/video/watch/{SLUG}/",
        f"https://www.wired.com/video/watch/{SLUG}?utm_source=x",
    ],
)
def test_resolves_slug(url: str) -> None:
    assert wired.resolve_url(url) == SLUG


@pytest.mark.parametrize(
    "url",
    [
        "https://www.wired.com/story/some-article",
        "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
        "https://example.com/video/watch/x",
    ],
)
def test_ignores_other_urls(url: str) -> None:
    assert wired.resolve_url(url) is None


def test_parse_page_metadata() -> None:
    meta = wired.parse_page(PAGE, SLUG, max_duration_minutes=60)
    assert meta.adapter == "wired"
    assert meta.video_id == SLUG
    assert meta.title == "The AI Playbook Top Companies Use"
    assert meta.channel == "WIRED"
    assert meta.duration_s == 26 * 60 + 1
    assert meta.upload_date == "2026-08-03"
    assert meta.manual_sub_langs == ["en"]


def test_too_long_is_rejected() -> None:
    with pytest.raises(VideoRejected) as exc:
        wired.parse_page(PAGE, SLUG, max_duration_minutes=10)
    assert exc.value.reason == "too_long"


def test_page_without_transcript_is_rejected() -> None:
    bare = PAGE.split("<body>")[0] + "<body></body>"
    with pytest.raises(VideoRejected) as exc:
        wired.parse_page(bare, SLUG, max_duration_minutes=60)
    assert exc.value.reason == "no_transcript"


def test_caption_lines_are_unescaped() -> None:
    lines = wired.caption_lines(PAGE)
    assert lines[0] == "I'm Katie Drummond, Global Editorial Director at Wired,"
    assert "McKinsey & Company's" in lines[2]


def test_paragraph_times_are_estimated_and_increasing() -> None:
    lines = [f"Sentence number {i} goes here." for i in range(200)]
    paras = wired.build_paragraphs(lines, 1500)
    assert paras[0].start_s == 0
    starts = [p.start_s for p in paras]
    assert starts == sorted(starts) and len(paras) > 5
    assert starts[-1] < 1500
