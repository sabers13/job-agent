from __future__ import annotations

import asyncio
import json
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from app.fetching.polite_fetch import (
    AccessDeniedError,
    FetchError,
    RobotsDisallowedError,
    TransientFetchError,
    fetch_job_html,
)
from app.config.settings import settings

from .url_pool import normalize_url, pool_path_for_profile

UNAVAILABLE_MARKERS = (
    "Diese Stellenanzeige ist nicht mehr verfügbar",
    "Das gesuchte Stellenangebot ist leider nicht mehr verfügbar",
)


class _RateLimiter:
    def __init__(self, min_delay: float = 1.2, max_delay: float = 2.0) -> None:
        self._min = float(min_delay)
        self._max = float(max_delay)
        self._lock = asyncio.Lock()
        self._next_ts = 0.0

    async def wait_turn(self) -> None:
        async with self._lock:
            now = time.monotonic()
            if now < self._next_ts:
                await asyncio.sleep(self._next_ts - now)
            self._next_ts = time.monotonic() + random.uniform(self._min, self._max)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _append_unavailable(
    path: Path,
    urls: Iterable[str],
    *,
    run_id: str,
    reason: str = "stepstone_unavailable",
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    now = _now_iso()
    with path.open("a", encoding="utf-8") as handle:
        for url in urls:
            payload = {"url": url, "seen_at": now, "run_id": run_id, "reason": reason}
            handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _load_pool_entries(path: Path) -> Tuple[List[Tuple[str, Optional[str]]], List[str]]:
    entries: List[Tuple[str, Optional[str]]] = []
    unique_urls: List[str] = []
    seen: set[str] = set()
    if not path.exists():
        return entries, unique_urls
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            raw = line.rstrip("\n")
            if not raw:
                entries.append((line, None))
                continue
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                entries.append((line, None))
                continue
            url_norm = normalize_url(str(payload.get("url") or ""))
            entries.append((line, url_norm if url_norm else None))
            if url_norm and url_norm not in seen:
                seen.add(url_norm)
                unique_urls.append(url_norm)
    return entries, unique_urls


def _telemetry_status_hint(telemetry: Dict[str, Any]) -> Optional[int]:
    if "status" in telemetry and isinstance(telemetry["status"], int):
        return telemetry["status"]
    attempts = telemetry.get("attempts")
    if isinstance(attempts, list) and attempts:
        for attempt in reversed(attempts):
            status = attempt.get("status")
            if isinstance(status, int):
                return status
    return None


async def _check_unavailable_polite(
    url: str,
    *,
    sem: asyncio.Semaphore,
    timeout_sec: float,
    preferred_backend: Optional[str],
    limiter: Optional[_RateLimiter] = None,
) -> Tuple[str, bool, Optional[str], bool]:
    """
    Returns: (url, unavailable, error_str, access_denied)
    access_denied=True means we should NOT prune (keep URL) and just count it.
    """
    async with sem:
        try:
            if limiter is not None:
                await limiter.wait_turn()
            html, telemetry = await asyncio.wait_for(
                fetch_job_html(url, preferred_backend=preferred_backend),
                timeout=max(1.0, float(timeout_sec)),
            )
            status = _telemetry_status_hint(telemetry)
            if status in (404, 410):
                return url, True, None, False
            if html and any(marker in html for marker in UNAVAILABLE_MARKERS):
                return url, True, None, False
            return url, False, None, False
        except asyncio.TimeoutError:
            return url, False, f"timeout_after={timeout_sec}s", False
        except AccessDeniedError as exc:
            return url, False, str(exc), True
        except (RobotsDisallowedError, TransientFetchError, FetchError) as exc:
            return url, False, str(exc), False


def prune_unavailable_stepstone_urls(
    profile_dir: Path,
    *,
    max_urls: int = 300,
    concurrency: int = 3,
    timeout: float = 10.0,
    logger=None,
    run_id: str,
    preferred_backend: Optional[str] = None,
) -> Dict[str, Any]:
    pool_path = pool_path_for_profile(profile_dir)
    entries, unique_urls = _load_pool_entries(pool_path)
    urls_to_check = unique_urls[: max(0, int(max_urls))]

    if logger:
        logger.info(f"Loaded {len(unique_urls)} unique URLs from pool")
        logger.info(f"Checking up to {len(urls_to_check)} URLs for availability")

    removed: set[str] = set()
    access_denied = 0
    fetch_errors = 0
    aborted = False

    async def _run() -> None:
        nonlocal access_denied, fetch_errors, aborted
        backend = preferred_backend
        if backend is None:
            backend = "pw" if settings.use_playwright_default else "http"
        sem = asyncio.Semaphore(max(1, int(concurrency)))
        limiter = _RateLimiter(min_delay=1.2, max_delay=2.0)
        tasks = [
            asyncio.create_task(
                _check_unavailable_polite(
                    url,
                    sem=sem,
                    timeout_sec=timeout,
                    preferred_backend=backend,
                    limiter=limiter,
                )
            )
            for url in urls_to_check
        ]
        try:
            for coro in asyncio.as_completed(tasks):
                try:
                    url, unavailable, err, denied = await coro
                except asyncio.CancelledError:
                    continue
                if denied:
                    access_denied += 1
                    aborted = True
                    if logger:
                        logger.warning(
                            f"Access denied while checking {url} (kept in pool): {err}"
                        )
                    for t in tasks:
                        if not t.done():
                            t.cancel()
                    break
                if err:
                    fetch_errors += 1
                    if logger:
                        logger.warning(f"Fetch error while checking {url} (kept in pool): {err}")
                    continue
                if unavailable:
                    removed.add(url)
                    if logger:
                        logger.info(f"Marked unavailable: {url}")
        finally:
            for t in tasks:
                if not t.done():
                    t.cancel()

    if urls_to_check:
        asyncio.run(_run())

    removed_count = len(removed)
    if removed_count and entries:
        tmp_path = pool_path.with_suffix(".tmp")
        with tmp_path.open("w", encoding="utf-8") as handle:
            for raw_line, url_norm in entries:
                if url_norm and url_norm in removed:
                    continue
                if raw_line.endswith("\n"):
                    handle.write(raw_line)
                else:
                    handle.write(raw_line + "\n")
        tmp_path.replace(pool_path)
        if logger:
            logger.info(f"Rewrote active pool without {removed_count} URLs")

    if removed:
        unavailable_path = profile_dir / "url_pool_unavailable.jsonl"
        _append_unavailable(unavailable_path, sorted(removed), run_id=run_id)

    kept_active = max(0, len(unique_urls) - removed_count)
    return {
        "checked_total": len(urls_to_check),
        "removed_unavailable": removed_count,
        "kept_active": kept_active,
        "access_denied": access_denied,
        "fetch_errors": fetch_errors,
        "aborted_due_to_access_denied": aborted,
    }
