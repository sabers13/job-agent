from __future__ import annotations

import json
import secrets
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from app.common.utils import atomic_write_json
from app.config.settings import settings

OUTPUTS_BASE = settings.output_dir
LEGACY_OUTPUT_ROOT = OUTPUTS_BASE / "gui_runs"
RUN_INDEX_DIR = OUTPUTS_BASE / "_run_index"
LOG_CHUNK_MAX_BYTES = 64 * 1024  # 64KB hard cap per request


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def run_output_root(base: Path, user_id: str, profile_key: str, run_id: str) -> Path:
    return base / user_id / profile_key / run_id


def _run_index_path(run_id: str) -> Path:
    return RUN_INDEX_DIR / f"{run_id}.json"


def _write_run_index(run_id: str, user_id: str, profile_key: str, output_root: Path) -> None:
    RUN_INDEX_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "run_id": run_id,
        "user_id": user_id,
        "profile_key": profile_key,
        "output_root": str(output_root),
    }
    atomic_write_json(_run_index_path(run_id), payload)


def _load_run_index(run_id: str) -> Optional[Dict[str, Any]]:
    path = _run_index_path(run_id)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def create_run_dir(user_id: str, profile_key: str) -> str:
    # Collision-proof: allow multiple runs per second across users/profiles.
    base = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    for _ in range(10):
        run_id = f"{base}-{secrets.token_hex(4)}"  # 8 hex chars
        run_dir = run_output_root(OUTPUTS_BASE, user_id, profile_key, run_id)
        try:
            run_dir.mkdir(parents=True, exist_ok=False)
            _write_run_index(run_id, user_id, profile_key, run_dir)
            return run_id
        except FileExistsError:
            continue
    # Extremely unlikely fallback
    run_id = f"{base}-{secrets.token_hex(8)}"
    run_dir = run_output_root(OUTPUTS_BASE, user_id, profile_key, run_id)
    run_dir.mkdir(parents=True, exist_ok=False)
    _write_run_index(run_id, user_id, profile_key, run_dir)
    return run_id


def get_run_dir(user_id: str, profile_key: str, run_id: str) -> Path:
    return run_output_root(OUTPUTS_BASE, user_id, profile_key, run_id)


def get_run_dir_from_index(run_id: str) -> Optional[Path]:
    idx = _load_run_index(run_id)
    if idx:
        output_root = idx.get("output_root")
        if output_root:
            return Path(output_root)
        user_id = idx.get("user_id")
        profile_key = idx.get("profile_key")
        if user_id and profile_key:
            return run_output_root(OUTPUTS_BASE, str(user_id), str(profile_key), run_id)
    legacy = LEGACY_OUTPUT_ROOT / run_id
    if legacy.exists():
        return legacy
    return None


def status_path(run_id: str) -> Path:
    run_dir = get_run_dir_from_index(run_id)
    if run_dir:
        return run_dir / "status.json"
    return LEGACY_OUTPUT_ROOT / run_id / "status.json"


def log_path(run_id: str) -> Path:
    run_dir = get_run_dir_from_index(run_id)
    if run_dir:
        return run_dir / "run.log"
    return LEGACY_OUTPUT_ROOT / run_id / "run.log"


def write_status(run_id: str, data: Dict[str, Any]) -> None:
    user_id = data.get("user_id")
    profile_key = data.get("profile_key")
    if user_id and profile_key:
        run_dir = run_output_root(OUTPUTS_BASE, str(user_id), str(profile_key), run_id)
        run_dir.mkdir(parents=True, exist_ok=True)
        _write_run_index(run_id, str(user_id), str(profile_key), run_dir)
    sp = status_path(run_id)
    atomic_write_json(sp, data)


def load_status(run_id: str) -> Optional[Dict[str, Any]]:
    sp = status_path(run_id)
    if sp.exists():
        try:
            return json.loads(sp.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return None
    legacy = LEGACY_OUTPUT_ROOT / run_id / "status.json"
    if legacy.exists():
        try:
            return json.loads(legacy.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return None
    return None


def latest_path(user_id: str, profile_key: str) -> Path:
    return OUTPUTS_BASE / user_id / profile_key / "latest.json"


def write_latest(user_id: str, profile_key: str, data: Dict[str, Any]) -> None:
    path = latest_path(user_id, profile_key)
    path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(path, data)


def _utf8_sequence_length(lead: int) -> int:
    """Total byte width of the UTF-8 sequence starting with `lead`.

    Returns 1 for a byte that cannot start a sequence (a stray continuation byte), so
    a malformed log still advances one byte per call instead of stalling.
    """
    if lead < 0x80:
        return 1
    if lead >= 0xF0:
        return 4
    if lead >= 0xE0:
        return 3
    if lead >= 0xC0:
        return 2
    return 1


def _last_sequence_start(data: bytes) -> int | None:
    """Index where the final UTF-8 sequence in `data` begins, or None if undecidable.

    Only the last four bytes are examined: no valid sequence is longer, so a run of
    continuation bytes beyond that is malformed rather than merely incomplete.
    """
    for i in range(len(data) - 1, max(-1, len(data) - 5), -1):
        if (data[i] & 0xC0) != 0x80:  # not a continuation byte
            return i
    return None


def read_log_chunk(run_id: str, offset: int, max_bytes: int = 4096) -> tuple[str, int]:
    """
    Read up to `max_bytes` BYTES from run.log starting at byte `offset`.
    Returns (chunk_text, new_offset_bytes).

    `next_offset` is always a UTF-8 codepoint boundary. A chunk that would end in the
    middle of a multi-byte character is shortened to the last complete one and the
    remainder is delivered by the following poll, so a caller that walks the file with
    the returned offsets reassembles it exactly, whatever chunk size it asks for.

    Two cases deliberately do not shorten:

    - **At EOF.** A truncated tail is genuinely truncated -- there is no later poll to
      complete it -- so it is decoded with U+FFFD and the offset reaches the end.
      Withholding it instead would leave `finished` false and the GUI polling forever.
    - **When the sequence is wider than `max_bytes`.** Shortening would return an empty
      chunk without advancing the offset, and the caller would poll that same offset
      forever. The read is extended to complete the character instead. This is the only
      case where a chunk exceeds `max_bytes`, and never by more than 3 bytes.
    """
    lp = log_path(run_id)
    if not lp.exists():
        return "", max(0, offset)

    offset = max(0, int(offset or 0))
    max_bytes = int(max_bytes or 0)
    if max_bytes <= 0:
        max_bytes = 4096
    max_bytes = min(max_bytes, LOG_CHUNK_MAX_BYTES)

    size = lp.stat().st_size
    if offset >= size:
        return "", size

    read_upto = min(size, offset + max_bytes)
    with lp.open("rb") as f:
        f.seek(offset)
        data = f.read(read_upto - offset)

        if read_upto < size:  # more bytes follow; at EOF `replace` is correct
            start = _last_sequence_start(data)
            if start is not None and start + _utf8_sequence_length(data[start]) > len(data):
                if start > 0:
                    data = data[:start]  # hand the partial character to the next poll
                else:
                    # The whole chunk is one incomplete character. Trimming would
                    # return "" at an unchanged offset and the caller would spin.
                    needed = start + _utf8_sequence_length(data[start]) - len(data)
                    data += f.read(min(needed, size - read_upto))
                read_upto = offset + len(data)

    chunk = data.decode("utf-8", errors="replace")
    return chunk, read_upto
