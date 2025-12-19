import asyncio
from typing import Any, Dict

from bs4 import BeautifulSoup

from app.config.settings import settings
from ..fetching.http_client import fetch

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    )
}


async def search_stepstone_http(query: Dict[str, Any]) -> Dict[str, Any]:
    """
    Minimal HTTP smoke check: GET the URL and parse <title>.
    """
    url = query.get("url") or "https://www.stepstone.de/en/"

    if settings.request_delay_ms > 0:
        await asyncio.sleep(settings.request_delay_ms / 1000)

    html = await fetch(url, headers=HEADERS, timeout=30.0)
    soup = BeautifulSoup(html, "lxml")
    title = soup.title.string.strip() if soup.title and soup.title.string else "Unknown"

    return {
        "ok": True,
        "backend": "http",
        "url": url,
        "final_url": url,
        "title": title,
    }


async def search_stepstone_pw(query: Dict[str, Any]) -> Dict[str, Any]:
    """
    Minimal Playwright smoke check: open the URL and return page title/final URL.
    """
    url = query.get("url") or "https://www.stepstone.de/en/"
    wait_until = query.get("wait_until", "domcontentloaded")

    if settings.request_delay_ms > 0:
        await asyncio.sleep(settings.request_delay_ms / 1000)

    from playwright.async_api import async_playwright

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=settings.headless)
        page = await browser.new_page()
        await page.goto(url, wait_until=wait_until)
        title = await page.title()
        final_url = page.url
        await browser.close()

    return {
        "ok": True,
        "backend": "playwright",
        "url": url,
        "final_url": final_url,
        "title": title,
    }


async def search_stepstone(query: Dict[str, Any], backend_override: str | None = None) -> Dict[str, Any]:
    """
    Choose backend for smoke check:
      - backend_override in {"pw","http"} takes precedence
      - else settings.use_playwright_default decides
    """
    backend = backend_override or ("pw" if settings.use_playwright_default else "http")
    if backend == "pw":
        return await search_stepstone_pw(query)
    if backend == "http":
        return await search_stepstone_http(query)
    raise ValueError("backend_override must be 'pw' or 'http'")
