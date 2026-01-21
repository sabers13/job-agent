from __future__ import annotations

import datetime
import hashlib
import json
import os
import re
import secrets
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

def safe_filename(name: str) -> str:
    base = Path(name).name
    if not base:
        return "file"
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", base)
    return cleaned or "file"

def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def timestamp_iso() -> str:
    return datetime.datetime.now().isoformat(timespec="seconds")


def atomic_write_text(path: Union[str, Path], text: str, *, encoding: str = "utf-8") -> None:
    """
    Atomic write: write to a temp file next to the target and os.replace() into place.
    This prevents partial reads of JSON/text under concurrent readers or crashes.
    """
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_name(f"{p.name}.tmp.{os.getpid()}.{secrets.token_hex(6)}")
    try:
        with tmp.open("w", encoding=encoding) as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, p)
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except Exception:
            pass


def atomic_write_json(
    path: Union[str, Path],
    payload: Any,
    *,
    encoding: str = "utf-8",
    indent: int = 2,
) -> None:
    text = json.dumps(
        to_jsonable(payload),
        ensure_ascii=False,
        indent=indent,
    )
    atomic_write_text(path, text, encoding=encoding)


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

    if isinstance(x, (datetime.datetime, datetime.date)):
        # Datetimes in status payloads should remain readable and stable.
        return x.isoformat()

    try:
        import pathlib

        if isinstance(x, pathlib.Path):
            return str(x)
    except Exception:
        pass

    return x
