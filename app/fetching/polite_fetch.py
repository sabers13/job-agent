from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse
import urllib.robotparser

import httpx
from loguru import logger

from app.config.settings import settings

DEFAULT_USER_AGENT = settings.fetch_user_agent
DEFAULT_ACCEPT_LANGUAGE = settings.fetch_accept_language

headless_mode = settings.headless
use_playwright_default = settings.use_playwright_default

FETCH_TIMEOUT = float(settings.fetch_http_timeout_sec)
HTTP_RETRIES = int(settings.fetch_http_retries)
HTTP_BACKOFF_BASE = float(settings.fetch_http_backoff_base)

ROBOTS_TTL = float(settings.fetch_robots_ttl_sec)
DELAY_MIN = float(settings.fetch_delay_min_sec)
DELAY_MAX = float(settings.fetch_delay_max_sec)
FAILURE_BACKOFF = float(settings.fetch_failure_backoff_sec)

PLAYWRIGHT_WAIT_UNTIL = settings.playwright_wait_until
PLAYWRIGHT_TIMEOUT_MS = int(settings.playwright_timeout_ms)

ACCESS_DENIED_MARKERS: Tuple[str, ...] = tuple(x.strip().lower() for x in settings.fetch_access_denied_markers)


class FetchError(Exception):
    """Base fetch error."""

    def __init__(
        self,
        message: str,
        *,
        backend: Optional[str] = None,
        status: Optional[int] = None,
        data: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(message)
        self.backend = backend
        self.status = status
        self.data = data or {}


class AccessDeniedError(FetchError):
    """Raised when the remote host rejects the request explicitly."""


class RobotsDisallowedError(FetchError):
    """Raised when robots.txt disallows fetching the requested URL."""


class TransientFetchError(FetchError):
    """Raised when a recoverable fetch error occurs."""


@dataclass
class RobotsEntry:
    parser: Optional[urllib.robotparser.RobotFileParser]
    fetched_at: float


@dataclass
class DomainState:
    lock: asyncio.Lock
    last_request: float = 0.0
    consecutive_failures: int = 0


ROBOTS_CACHE: Dict[str, RobotsEntry] = {}
ROBOTS_LOCKS: Dict[str, asyncio.Lock] = {}
DOMAIN_STATE: Dict[str, DomainState] = {}
STATE_INIT_LOCK = asyncio.Lock()


async def _get_domain_state(domain: str) -> DomainState:
    state = DOMAIN_STATE.get(domain)
    if state is not None:
        return state
    async with STATE_INIT_LOCK:
        state = DOMAIN_STATE.get(domain)
        if state is None:
            state = DomainState(lock=asyncio.Lock())
            DOMAIN_STATE[domain] = state
    return state


async def _get_robots_lock(domain: str) -> asyncio.Lock:
    lock = ROBOTS_LOCKS.get(domain)
    if lock is not None:
        return lock
    async with STATE_INIT_LOCK:
        lock = ROBOTS_LOCKS.get(domain)
        if lock is None:
            lock = asyncio.Lock()
            ROBOTS_LOCKS[domain] = lock
    return lock


async def _fetch_robots(url: str, user_agent: str) -> Optional[urllib.robotparser.RobotFileParser]:
    parsed = urlparse(url)
    scheme = parsed.scheme or "https"
    domain = parsed.netloc
    robots_url = urljoin(f"{scheme}://{domain}", "/robots.txt")
    logger.debug("Fetching robots.txt for {} as {}", domain, user_agent)
    headers = {
        "User-Agent": user_agent,
        "Accept": "text/plain,*/*;q=0.8",
    }
    try:
        async with httpx.AsyncClient(timeout=15.0, headers=headers) as client:
            resp = await client.get(robots_url)
            if resp.status_code >= 400:
                logger.warning(
                    "robots.txt unavailable for {} (status={}); treating as allow-all",
                    robots_url,
                    resp.status_code,
                )
                return None
            parser = urllib.robotparser.RobotFileParser()
            parser.parse(resp.text.splitlines())
            return parser
    except Exception as exc:
        logger.warning("robots.txt fetch failed for {}: {}", robots_url, exc)
        return None


async def _robots_parser(url: str, user_agent: str) -> Optional[urllib.robotparser.RobotFileParser]:
    parsed = urlparse(url)
    domain = parsed.netloc
    entry = ROBOTS_CACHE.get(domain)
    now = time.time()
    if entry and now - entry.fetched_at < ROBOTS_TTL:
        return entry.parser

    lock = await _get_robots_lock(domain)
    async with lock:
        entry = ROBOTS_CACHE.get(domain)
        if entry and now - entry.fetched_at < ROBOTS_TTL:
            return entry.parser
        parser = await _fetch_robots(url, user_agent)
        ROBOTS_CACHE[domain] = RobotsEntry(parser=parser, fetched_at=time.time())
        return parser


async def _ensure_robots_allowed(url: str, user_agent: str) -> None:
    parser = await _robots_parser(url, user_agent)
    if parser is None:
        return
    allowed = parser.can_fetch(user_agent, url)
    if not allowed:
        raise RobotsDisallowedError(
            f"robots.txt disallows fetching {url}",
            backend="robots",
            data={"url": url},
        )


async def _mark_success(domain: str) -> None:
    state = await _get_domain_state(domain)
    async with state.lock:
        state.consecutive_failures = 0


async def _mark_failure(domain: str) -> None:
    state = await _get_domain_state(domain)
    async with state.lock:
        state.consecutive_failures += 1


async def _respect_rate_limit(domain: str) -> None:
    state = await _get_domain_state(domain)
    min_delay = max(0.0, DELAY_MIN)
    max_delay = max(min_delay, DELAY_MAX)

    async with state.lock:
        now = time.monotonic()
        if state.last_request <= 0:
            state.last_request = now
            return

        gap = min_delay
        if max_delay > min_delay:
            gap += random.uniform(0, max_delay - min_delay)
        if FAILURE_BACKOFF > 0 and state.consecutive_failures > 0:
            gap += FAILURE_BACKOFF * min(state.consecutive_failures, 6)

        wait_for = (state.last_request + gap) - now
        if wait_for > 0:
            await asyncio.sleep(wait_for)
        state.last_request = time.monotonic()


def _looks_access_denied(text: str) -> bool:
    lower = text.lower()
    return any(marker in lower for marker in ACCESS_DENIED_MARKERS)


async def _http_attempt(
    url: str,
    domain: str,
    user_agent: str,
    attempt_index: int,
) -> Tuple[str, Dict[str, Any]]:
    await _respect_rate_limit(domain)
    headers = {
        "User-Agent": user_agent,
        "Accept-Language": DEFAULT_ACCEPT_LANGUAGE,
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "DNT": "1",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Upgrade-Insecure-Requests": "1",
    }

    start = time.perf_counter()
    try:
        async with httpx.AsyncClient(timeout=FETCH_TIMEOUT, headers=headers, follow_redirects=True) as client:
            resp = await client.get(url)
    except httpx.RequestError as exc:
        elapsed = time.perf_counter() - start
        logger.warning("HTTP fetch error for {} attempt {}: {}", url, attempt_index, exc)
        await _mark_failure(domain)
        raise TransientFetchError(
            f"HTTP fetch error: {exc}",
            backend="http",
            data={"elapsed": elapsed, "attempt": attempt_index},
        ) from exc

    elapsed = time.perf_counter() - start
    status = resp.status_code
    body_preview = resp.text[:2048] if resp.text else ""

    attempt_meta = {
        "attempt": attempt_index,
        "status": status,
        "elapsed": round(elapsed, 3),
        "backend": "http",
        "final_url": str(resp.request.url),
    }

    if status in {403, 429, 451}:
        retry_after = resp.headers.get("Retry-After")
        logger.warning(
            "HTTP {} for {} attempt {} (retry_after={}s)",
            status,
            url,
            attempt_index,
            retry_after,
        )
        data = {**attempt_meta, "retry_after": retry_after}
        await _mark_failure(domain)
        raise AccessDeniedError(
            f"HTTP {status} from upstream",
            backend="http",
            status=status,
            data=data,
        )

    if status >= 500:
        logger.warning("HTTP {} (server error) for {} attempt {}", status, url, attempt_index)
        data = attempt_meta.copy()
        await _mark_failure(domain)
        raise TransientFetchError(
            f"HTTP {status} server error",
            backend="http",
            status=status,
            data=data,
        )

    if status >= 400:
        logger.error("HTTP {} (client error) for {} attempt {}", status, url, attempt_index)
        data = attempt_meta.copy()
        await _mark_failure(domain)
        raise FetchError(
            f"HTTP {status} client error",
            backend="http",
            status=status,
            data=data,
        )

    text = resp.text
    if _looks_access_denied(text):
        logger.warning("HTTP body indicates Access Denied for {} attempt {}", url, attempt_index)
        data = attempt_meta.copy()
        data["reason"] = "body_marker"
        data["preview"] = body_preview[:256]
        await _mark_failure(domain)
        raise AccessDeniedError(
            "Access denied markers detected in response body",
            backend="http",
            status=status,
            data=data,
        )

    logger.info(
        "HTTP fetch ok {} status {} attempt {} elapsed {:.3f}s",
        url,
        status,
        attempt_index,
        elapsed,
    )
    await _mark_success(domain)
    return text, attempt_meta


async def _playwright_attempt(
    url: str,
    domain: str,
    user_agent: str,
    attempt_index: int,
) -> Tuple[str, Dict[str, Any]]:
    await _respect_rate_limit(domain)
    start = time.perf_counter()
    from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

    browser = None
    context = None
    page = None
    try:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=headless_mode)
            context = await browser.new_context(user_agent=user_agent, accept_downloads=False)
            page = await context.new_page()
            response = await page.goto(
                url,
                wait_until=PLAYWRIGHT_WAIT_UNTIL,
                timeout=PLAYWRIGHT_TIMEOUT_MS,
            )
            await page.wait_for_timeout(350)
            html = await page.content()
            final_url = page.url
            status = response.status if response else 200
            elapsed = time.perf_counter() - start
            meta = {
                "attempt": attempt_index,
                "status": status,
                "elapsed": round(elapsed, 3),
                "backend": "pw",
                "final_url": final_url,
            }
            if _looks_access_denied(html):
                logger.warning("Playwright body indicates Access Denied for {} attempt {}", url, attempt_index)
                data = meta.copy()
                data["reason"] = "body_marker"
                await _mark_failure(domain)
                raise AccessDeniedError(
                    "Access denied markers detected in Playwright response",
                    backend="pw",
                    status=status,
                    data=data,
                )

            logger.info(
                "Playwright fetch ok {} status {} attempt {} elapsed {:.3f}s",
                url,
                status,
                attempt_index,
                elapsed,
            )
            await _mark_success(domain)
            return html, meta
    except PlaywrightTimeoutError as exc:
        elapsed = time.perf_counter() - start
        logger.warning("Playwright timeout for {} attempt {}: {}", url, attempt_index, exc)
        await _mark_failure(domain)
        raise TransientFetchError(
            f"Playwright timeout: {exc}",
            backend="pw",
            data={"elapsed": round(elapsed, 3), "attempt": attempt_index},
        ) from exc
    except AccessDeniedError:
        raise
    except Exception as exc:
        elapsed = time.perf_counter() - start
        logger.exception("Playwright fetch failed for {} attempt {}", url, attempt_index)
        await _mark_failure(domain)
        raise FetchError(
            f"Playwright error: {exc}",
            backend="pw",
            data={"elapsed": round(elapsed, 3), "attempt": attempt_index},
        ) from exc
    finally:
        try:
            if page:
                await page.close()
        except Exception:
            pass
        try:
            if context:
                await context.close()
        except Exception:
            pass
        try:
            if browser:
                await browser.close()
        except Exception:
            pass


def _decide_backend_order(preferred: Optional[str]) -> List[str]:
    if preferred == "http":
        return ["http"]
    if preferred == "pw":
        return ["pw"]
    if use_playwright_default:
        return ["pw", "http"]
    return ["http", "pw"]


def _http_retry_backoff(attempt: int) -> float:
    multiplier = 2 ** max(0, attempt - 1)
    jitter = random.uniform(0.5, 1.5)
    return HTTP_BACKOFF_BASE * multiplier * jitter


async def fetch_job_html(
    url: str,
    *,
    preferred_backend: Optional[str] = None,
) -> Tuple[str, Dict[str, Any]]:
    """
    Fetch HTML for the provided job URL with polite rate limiting, robots.txt compliance,
    and Access Denied mitigation. Returns a tuple (html, telemetry).
    """
    parsed = urlparse(url)
    if not parsed.scheme.startswith("http"):
        raise FetchError(f"Unsupported URL scheme for {url}", backend="http")

    user_agent = DEFAULT_USER_AGENT
    domain = parsed.netloc

    await _ensure_robots_allowed(url, user_agent)

    backend_order = _decide_backend_order(preferred_backend)
    telemetry: Dict[str, Any] = {
        "url": url,
        "domain": domain,
        "preferred_backend": preferred_backend or "auto",
        "attempts": [],
    }

    last_error: Optional[FetchError] = None

    for backend in backend_order:
        if backend == "http":
            for attempt in range(1, HTTP_RETRIES + 1):
                try:
                    html, attempt_meta = await _http_attempt(url, domain, user_agent, attempt)
                    telemetry["attempts"].append({**attempt_meta, "ok": True})
                    telemetry["backend"] = "http"
                    telemetry["ok"] = True
                    return html, telemetry
                except AccessDeniedError as exc:
                    data = {**exc.data, "ok": False, "error": str(exc)}
                    telemetry["attempts"].append(data)
                    last_error = exc
                    if attempt < HTTP_RETRIES:
                        backoff = _http_retry_backoff(attempt)
                        logger.info(
                            "Backoff {:.1f}s before HTTP retry {} for {}",
                            backoff,
                            attempt + 1,
                            url,
                        )
                        await asyncio.sleep(backoff)
                        continue
                    break
                except TransientFetchError as exc:
                    data = {**exc.data, "ok": False, "error": str(exc)}
                    telemetry["attempts"].append(data)
                    last_error = exc
                    if attempt < HTTP_RETRIES:
                        backoff = _http_retry_backoff(attempt)
                        logger.info(
                            "Backoff {:.1f}s before transient HTTP retry {} for {}",
                            backoff,
                            attempt + 1,
                            url,
                        )
                        await asyncio.sleep(backoff)
                        continue
                    break
                except FetchError as exc:
                    data = {**exc.data, "ok": False, "error": str(exc)}
                    telemetry["attempts"].append(data)
                    last_error = exc
                    break
        elif backend == "pw":
            try:
                html, attempt_meta = await _playwright_attempt(url, domain, user_agent, 1)
                telemetry["attempts"].append({**attempt_meta, "ok": True})
                telemetry["backend"] = "pw"
                telemetry["ok"] = True
                return html, telemetry
            except FetchError as exc:
                data = {**exc.data, "ok": False, "error": str(exc)}
                telemetry["attempts"].append(data)
                last_error = exc
                continue
        else:
            logger.error("Unknown backend '{}'", backend)

    if last_error is None:
        raise FetchError("No backend succeeded and no error captured", backend=preferred_backend)

    telemetry["ok"] = False
    telemetry["error"] = str(last_error)
    raise last_error
