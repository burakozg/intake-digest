"""What an item *is*: the seam between the generic queue and a source.

The queue (`jobs`), the stage checkpoints (`pipeline/stages.py`), the runner
(`pipeline/runner.py`), the vault writer and the LLM client are the same for
every sort of input. What differs is how an item is acquired, what its summary
looks like and what its note says — and that lives behind `Kind`. Adding an
input type means adding one module here and registering it below; nothing in
the runner or the scheduler changes.

`video` is the only kind registered at present.
"""

from __future__ import annotations

from typing import Any, Protocol


class RewriteUnavailable(Exception):
    """A rewrite was asked for but the row has no stored digest to render
    from — the item has not been summarised yet."""


class Kind(Protocol):
    """A kind supplies two coroutines. They are typed loosely on purpose: each
    kind has its own `_`-prefixed test seams and its own LLM-client and vault
    types, and a Protocol cannot express "a superset of these keywords".

    ``run(db, settings, row, *, llm, vault, force=False, notifier=None,
    **seams) -> str`` advances one item as far as it will go and returns
    ``"done"`` | ``"parked"`` | ``"deferred"`` | ``"failed"``; the middle two
    leave the job queued for a later pass.

    ``rewrite(db, settings, vault, key) -> str`` re-renders the note from the
    stored digest with no LLM call and returns the note path. It raises
    `KeyError` for an unknown item and `RewriteUnavailable` for one with no
    digest yet.
    """

    name: str

    async def run(self, *args: Any, **kwargs: Any) -> str: ...

    async def rewrite(self, *args: Any, **kwargs: Any) -> str: ...


def _registry() -> dict[str, Kind]:
    # Imported here, not at module top: each kind imports this package for
    # `RewriteUnavailable`, so a top-level import would be circular.
    from .video import VideoKind

    return {k.name: k for k in (VideoKind(),)}


_KINDS: dict[str, Kind] | None = None


def get_kind(name: str) -> Kind:
    global _KINDS
    if _KINDS is None:
        _KINDS = _registry()
    try:
        return _KINDS[name]
    except KeyError:
        raise KeyError(f"unknown item kind {name!r}") from None


def kind_names() -> tuple[str, ...]:
    global _KINDS
    if _KINDS is None:
        _KINDS = _registry()
    return tuple(_KINDS)
