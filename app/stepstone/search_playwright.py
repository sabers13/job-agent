# app/stepstone/search_playwright.py
from __future__ import annotations
import asyncio, re, random, math, json
from typing import Dict, List, Optional, Any
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse
from datetime import datetime, timezone

from loguru import logger
from playwright.async_api import async_playwright, TimeoutError as PWTimeout
from playwright._impl._errors import Error as PWError

from .dates import (
    isoformat_utc,
    parse_stepstone_listing_date,
)

JOB_LINK_RE = re.compile(
    r"(?:/jobs--|/stellenangebote--|/job/)[^\"'#]+(?:inline\.html|\.html)",
    re.IGNORECASE,
)
RESULT_COUNT_RE = re.compile(r"(\d[\d\.]*)\s+Treffer", re.IGNORECASE)
PAGE_LAST_RE = re.compile(r"data-page-last=\"(\d+)\"")
JSON_LD_RE = re.compile(r"<script[^>]+type=\"application/ld\+json\"[^>]*>(.*?)</script>", re.DOTALL | re.IGNORECASE)
PER_PAGE_DEFAULT = 25
def _with_page(url: str, page_num: int) -> str:
    u = urlparse(url)
    qs = parse_qs(u.query)
    if "page" in qs:
        qs["page"] = [str(page_num)]
    elif "of" in qs:
        qs["of"] = [str(page_num)]
    else:
        qs["page"] = [str(page_num)]
    new_q = urlencode({k: v[0] if isinstance(v, list) and len(v)==1 else v for k, v in qs.items()}, doseq=True)
    return urlunparse((u.scheme, u.netloc, u.path, u.params, new_q, u.fragment))

def _estimate_total_pages_from_html(html: str, *, per_page: int = PER_PAGE_DEFAULT) -> Optional[int]:
    if not html:
        return None
    match = PAGE_LAST_RE.search(html)
    if match:
        try:
            value = int(match.group(1))
            if value > 0:
                return value
        except Exception:
            pass
    match = RESULT_COUNT_RE.search(html)
    if match:
        try:
            total_hits = int(match.group(1).replace(".", ""))
            if total_hits > 0 and per_page > 0:
                return max(1, math.ceil(total_hits / per_page))
        except Exception:
            pass
    for block in JSON_LD_RE.findall(html or ""):
        block = block.strip()
        if not block:
            continue
        try:
            data = json.loads(block)
        except Exception:
            continue
        total_hits = _find_total_in_jsonld(data)
        if total_hits and per_page > 0:
            return max(1, math.ceil(total_hits / per_page))
    return None

def _find_total_in_jsonld(node: Any) -> Optional[int]:
    keys = ("numberOfItems", "totalJobPosting", "totalJobPostings", "totalItems", "totalResults")
    if isinstance(node, dict):
        for key in keys:
            if key in node:
                raw = node[key]
                try:
                    val = int(str(raw).replace(".", "").replace(",", ""))
                    if val > 0:
                        return val
                except Exception:
                    continue
        for value in node.values():
            result = _find_total_in_jsonld(value)
            if result:
                return result
    elif isinstance(node, list):
        for item in node:
            result = _find_total_in_jsonld(item)
            if result:
                return result
    return None

async def _accept_cookies(page):
    selectors = [
        '#onetrust-accept-btn-handler',
        'button[aria-label="Accept all"]',
        'button:has-text("Accept")',
        'button:has-text("Alle akzeptieren")',
    ]
    for sel in selectors:
        try:
            b = await page.wait_for_selector(sel, timeout=1500)
            await b.click()
            return
        except:
            pass

async def _extract_links(page) -> List[str]:
    hrefs = await page.eval_on_selector_all(
        "a[href]", "els => els.map(a => a.getAttribute('href')).filter(Boolean)"
    )
    base = page.url
    out: List[str] = []
    for h in hrefs:
        if JOB_LINK_RE.search(h):
            out.append(urljoin(base, h))
    # unique keep-order
    seen, uniq = set(), []
    for u in out:
        if u not in seen:
            seen.add(u); uniq.append(u)
    return uniq

async def _extract_job_entries(
    page,
    include_titles_any: List[str],
    exclude_titles_any: List[str],
) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    seen: set[str] = set()

    nodes = await page.query_selector_all("[data-at='job-item'], article")
    now = datetime.now(timezone.utc)
    for node in nodes:
        link = None
        title_text = None
        anchors = await node.query_selector_all("a[href]")
        for anchor in anchors:
            href = await anchor.get_attribute("href") or ""
            if JOB_LINK_RE.search(href):
                link = urljoin(page.url, href)
                if title_text is None:
                    raw_title = await anchor.inner_text()
                    title_text = (raw_title or "").strip()
                break
        if not link or link in seen:
            continue
        seen.add(link)

        title_lower = (title_text or "").lower()
        if include_titles_any and not any(tok in title_lower for tok in include_titles_any):
            continue
        if exclude_titles_any and any(tok in title_lower for tok in exclude_titles_any):
            continue

        posted_label = None
        date_node = await node.query_selector("[data-at='job-item-date'], .job-item__date, .job-item__date--desktop")
        if date_node:
            posted_label = (await date_node.inner_text() or "").strip()
        if not posted_label:
            text_blob = (await node.inner_text() or "").strip()
            for line in text_blob.splitlines():
                if "vor" in line.lower() or "erschienen" in line.lower():
                    posted_label = line.strip()
                    break

        posted_dt = parse_stepstone_listing_date(posted_label, now=now) if posted_label else None
        entries.append(
            {
                "url": link,
                "title": title_text,
                "posted_label": posted_label,
                "posted_dt": posted_dt,
            }
        )

    if not entries:
        fallback_links = await _extract_links(page)
        for link in fallback_links:
            entries.append({"url": link, "title": None, "posted_label": None, "posted_dt": None})
    return entries

async def _safe_goto(page, url: str, try_accept_cookies: bool = False) -> None:
    """
    Navigate with 3 attempts, switching wait strategies and falling back to JS redirect.
    Mitigates sporadic net::ERR_HTTP2_PROTOCOL_ERROR.
    """
    attempts = [
        dict(wait_until="domcontentloaded"),
        dict(wait_until="commit"),
        dict(wait_until="load"),
    ]
    last_err = None
    for i, opts in enumerate(attempts):
        try:
            await page.goto(url, timeout=45000, **opts)
            if try_accept_cookies:
                await _accept_cookies(page)
            # small idle to let JS populate results
            await page.wait_for_timeout(500)
            return
        except Exception as e:
            last_err = e
            # final fallback: JS redirect
            if i == len(attempts) - 1:
                try:
                    await page.evaluate("url => location.href = url", url)
                    await page.wait_for_load_state("domcontentloaded", timeout=45000)
                    if try_accept_cookies:
                        await _accept_cookies(page)
                    await page.wait_for_timeout(500)
                    return
                except Exception as e2:
                    last_err = e2
    raise last_err

async def search_stepstone_pw(
    seed_url: str,
    pages: Optional[int] = None,
    delay_sec: float = 1.6,
    include_titles_any: Optional[List[str]] = None,
    exclude_titles_any: Optional[List[str]] = None,
    max_jobs: Optional[int] = None,
    max_pages_guard: int = 80,
    stop_urls: Optional[List[str]] = None,
) -> Dict:
    include_titles_any = [t.lower() for t in (include_titles_any or [])]
    exclude_titles_any = [t.lower() for t in (exclude_titles_any or [])]
    # stop_urls are kept for backwards compatibility but no longer drive crawling
    # stop_set = {s.strip() for s in (stop_urls or []) if s}

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-http2",  # <- important for this CDN
                "--disable-features=IsolateOrigins,site-per-process",
            ],
        )
        ctx = await browser.new_context(
            locale="de-DE",
            ignore_https_errors=True,
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130 Safari/537.36",
            extra_http_headers={
                "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "DNT": "1",
                "Upgrade-Insecure-Requests": "1",
            },
        )
        page = await ctx.new_page()

        all_jobs: List[Dict[str, Any]] = []
        seen_urls: set[str] = set()
        page_hits: List[Dict] = []
        target_pages: Optional[int] = pages if pages and pages > 0 else None
        guard = max(1, max_pages_guard or 80)
        estimated_pages = None
        per_page = PER_PAGE_DEFAULT
        empty_streak = 0

        try:
            # warm-up: hit root domain first, accept cookies once
            root = "https://www.stepstone.de/"
            await _safe_goto(page, root, try_accept_cookies=True)

            p = 1
            while True:
                page_url = _with_page(seed_url, p)
                try:
                    await _safe_goto(page, page_url)
                except (PWTimeout, PWError) as exc:
                    logger.warning("playwright goto failed for {} page {}: {}", seed_url, p, exc)
                    break
                try:
                    await page.wait_for_selector('[data-at="job-item"]', timeout=12000)
                except Exception:
                    # fallback: give the client additional time to render lazy results
                    await page.wait_for_timeout(2000)

                # gentle scroll to trigger lazy lists
                for _ in range(3):
                    await page.mouse.wheel(0, 1600)
                    await page.wait_for_timeout(350)

                page_html = await page.content()
                jobs = await _extract_job_entries(page, include_titles_any, exclude_titles_any)

                added = 0
                for job in jobs:
                    url = job["url"]
                    if url in seen_urls:
                        continue
                    seen_urls.add(url)
                    all_jobs.append(job); added += 1
                    if max_jobs and len(all_jobs) >= max_jobs:
                        break

                page_hits.append({"page": p, "url": page.url, "found": added})

                empty_streak = empty_streak + 1 if added == 0 else 0

                if p == 1 and target_pages is None:
                    estimated_pages = _estimate_total_pages_from_html(page_html, per_page=per_page)
                    if estimated_pages:
                        target_pages = min(estimated_pages, guard)

                if target_pages is None:
                    target_pages = guard

                if max_jobs:
                    pages_from_jobs = max(1, math.ceil(max_jobs / per_page))
                    target_pages = min(target_pages, pages_from_jobs)

                if max_jobs and len(all_jobs) >= max_jobs:
                    break
                if target_pages and p >= target_pages:
                    break
                if empty_streak >= 5:
                    logger.debug("Stopping {} after {} empty pages ({} total urls)", seed_url, empty_streak, len(all_jobs))
                    break

                p += 1
                await page.wait_for_timeout(int(1200 * delay_sec * random.uniform(0.8, 1.4)))
        finally:
            await ctx.close()
            await browser.close()

    return {
        "ok": True,
        "seed_url": seed_url,
        "pages_requested": pages,
        "pages_scanned": len(page_hits),
        "page_hits": page_hits,
        "count": len(all_jobs),
        "estimated_total_pages": estimated_pages,
        "target_pages": target_pages,
        "urls": [job["url"] for job in all_jobs],
        "jobs": [
            {
                "url": job["url"],
                "title": job.get("title"),
                "posted_label": job.get("posted_label"),
                "posted_iso": isoformat_utc(job["posted_dt"]) if isinstance(job.get("posted_dt"), datetime) else None,
            }
            for job in all_jobs
        ],
    }
