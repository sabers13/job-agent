from __future__ import annotations

import datetime
import os
import re
import unicodedata
from pathlib import Path
from typing import Any, Union

try:
    from pydantic import BaseModel
except Exception:  # pragma: no cover
    BaseModel = None  # type: ignore

def slugify(text: str) -> str:
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^a-zA-Z0-9]+", "-", text).strip("-").lower()
    return re.sub(r"-{2,}", "-", text)

def ensure_dir(p: Union[str, Path]) -> None:
    Path(p).mkdir(parents=True, exist_ok=True)

def timestamp_iso() -> str:
    return datetime.datetime.now().isoformat(timespec="seconds")


def to_jsonable(x: Any) -> Any:
    """
    Convert common non-JSON types to JSON-serializable equivalents.
    Keeps behavior deterministic (e.g., sets are sorted).
    """
    if BaseModel is not None and isinstance(x, BaseModel):
        dump = getattr(x, "model_dump", None)
        return dump(mode="json") if callable(dump) else x.dict()  # type: ignore[attr-defined]

    if isinstance(x, dict):
        return {str(k): to_jsonable(v) for k, v in x.items()}

    if isinstance(x, (list, tuple)):
        return [to_jsonable(v) for v in x]

    if isinstance(x, set):
        return sorted([to_jsonable(v) for v in x], key=lambda t: str(t))

    try:
        import pathlib

        if isinstance(x, pathlib.Path):
            return str(x)
    except Exception:
        pass

    return x
