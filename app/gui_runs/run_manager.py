from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

OUTPUT_ROOT = Path("output/gui_runs")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def create_run_dir() -> str:
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    run_dir = OUTPUT_ROOT / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    return run_id


def get_run_dir(run_id: str) -> Path:
    return OUTPUT_ROOT / run_id


def status_path(run_id: str) -> Path:
    return get_run_dir(run_id) / "status.json"


def log_path(run_id: str) -> Path:
    return get_run_dir(run_id) / "run.log"


def write_status(run_id: str, data: Dict[str, Any]) -> None:
    sp = status_path(run_id)
    sp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def load_status(run_id: str) -> Optional[Dict[str, Any]]:
    sp = status_path(run_id)
    if not sp.exists():
        return None
    try:
        return json.loads(sp.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def read_log_chunk(run_id: str, offset: int, max_bytes: int = 4096) -> tuple[str, int]:
    """
    Read up to `max_bytes` from run.log starting at `offset`.
    Returns (chunk_text, new_offset).
    """
    lp = log_path(run_id)
    if not lp.exists():
        return "", offset

    size = lp.stat().st_size
    if offset >= size:
        return "", size

    read_upto = min(size, offset + max_bytes)
    with lp.open("r", encoding="utf-8", errors="replace") as f:
        f.seek(offset)
        chunk = f.read(read_upto - offset)
    return chunk, read_upto
