from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional, Set


def pool_path_for_profile(profile_dir: Path) -> Path:
    return profile_dir / "url_pool.jsonl"


def normalize_url(url: str) -> str:
    if not url:
        return ""
    cleaned = url.strip()
    if not cleaned:
        return ""
    return cleaned.split("#", 1)[0].strip()


def load_pool_set(path: Path) -> Set[str]:
    if not path.exists():
        return set()
    seen: Set[str] = set()
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            raw = line.strip()
            if not raw:
                continue
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                continue
            url = normalize_url(str(payload.get("url") or ""))
            if url:
                seen.add(url)
    return seen


def append_pool_entries(
    path: Path,
    urls: Iterable[str],
    *,
    run_id: str,
    seed_slug: Optional[str] = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    with path.open("a", encoding="utf-8") as handle:
        for url in urls:
            normalized = normalize_url(url)
            if not normalized:
                continue
            payload = {
                "url": normalized,
                "seen_at": now,
                "run_id": run_id,
                "seed_slug": seed_slug,
            }
            handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
