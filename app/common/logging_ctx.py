from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any

_run_ctx: ContextVar[dict[str, Any] | None] = ContextVar("run_ctx", default=None)


def get_run_ctx() -> dict[str, Any]:
    return dict(_run_ctx.get() or {})


def set_run_ctx(
    *,
    run_id: str | None = None,
    user_id: str | None = None,
    profile_key: str | None = None,
) -> None:
    ctx = get_run_ctx()
    if run_id is not None:
        ctx["run_id"] = str(run_id)
    if user_id is not None:
        ctx["user_id"] = str(user_id)
    if profile_key is not None:
        ctx["profile_key"] = str(profile_key)
    _run_ctx.set(ctx)


def clear_run_ctx() -> None:
    _run_ctx.set(None)


@contextmanager
def run_ctx_scope(
    *,
    run_id: str | None = None,
    user_id: str | None = None,
    profile_key: str | None = None,
) -> Iterator[None]:
    ctx = get_run_ctx()
    if run_id is not None:
        ctx["run_id"] = str(run_id)
    if user_id is not None:
        ctx["user_id"] = str(user_id)
    if profile_key is not None:
        ctx["profile_key"] = str(profile_key)
    token = _run_ctx.set(ctx)
    try:
        yield
    finally:
        _run_ctx.reset(token)
