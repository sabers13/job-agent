from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Dict, Iterator, Optional

_run_ctx: ContextVar[Optional[Dict[str, Any]]] = ContextVar("run_ctx", default=None)


def get_run_ctx() -> Dict[str, Any]:
    return dict(_run_ctx.get() or {})


def set_run_ctx(
    *,
    run_id: Optional[str] = None,
    user_id: Optional[str] = None,
    profile_key: Optional[str] = None,
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
    run_id: Optional[str] = None,
    user_id: Optional[str] = None,
    profile_key: Optional[str] = None,
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
