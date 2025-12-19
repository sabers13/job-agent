# app/stepstone/search_http.py
from __future__ import annotations
import time, re, random, json
from typing import List, Dict, Optional, Any
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse
import math
from datetime import datetime, timezone

import httpx
from bs4 import BeautifulSoup
from loguru import logger

from .dates import (
    isoformat_utc,
    parse_stepstone_listing_date,
)

# StepStone detail URLs commonly end with inline.html (both en/de variants)
JOB_LINK_RE = re.compile(
    r"(?:/jobs--|/stellenangebote--|/job/)[^\"'#]+(?:inline\.html|\.html)",
    re.IGNORECASE,
)
RESULT_COUNT_RE = re.compile(r"(\d[\d\.]*)\s+Treffer", re.IGNORECASE)
PAGE_LAST_RE = re.compile(r"data-page-last=\"(\d+)\"")
JSON_LD_RE = re.compile(r"<script[^>]+type=\"application/ld\+json\"[^>]*>(.*?)</script>", re.DOTALL | re.IGNORECASE)
PER_PAGE_DEFAULT = 25

def _abs(base: str, href: str) -> str:
    return urljoin(base, href)

def _extract_job_links(html: str, base_url: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    hrefs = set()

    # Generic scan (robust to DOM shifts)
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if JOB_LINK_RE.search(href):
            hrefs.add(_abs(base_url, href))

    # Fallback selectors if StepStone changes markup
    for a in soup.select('[data-at="job-item"] a[href], article a[href]'):
        href = a.get("href", "")
        if JOB_LINK_RE.search(href):
            hrefs.add(_abs(base_url, href))

    return sorted(hrefs)

def _with_page(url: str, page: int) -> str:
    """Insert or override ?page=N (or ?of=N) for pagination."""
    u = urlparse(url)
    qs = parse_qs(u.query)
    if "page" in qs:
        qs["page"] = [str(page)]
    elif "of" in qs:
        qs["of"] = [str(page)]
    else:
        qs["page"] = [str(page)]
    new_q = urlencode({k: v[0] if isinstance(v, list) and len(v)==1 else v for k, v in qs.items()}, doseq=True)
    return urlunparse((u.scheme, u.netloc, u.path, u.params, new_q, u.fragment))

def _extract_posted_label(card: BeautifulSoup) -> Optional[str]:
    for selector in ("[data-at='job-item-date']", ".job-item__date", ".job-item__date--desktop"):
        node = card.select_one(selector)
        if node:
            text = node.get_text(" ", strip=True)
            if text:
                return text
    for node in card.select("span, div"):
        text = node.get_text(" ", strip=True)
        if text and ("vor" in text.lower() or "erschienen" in text.lower()):
            return text
    return None


def _extract_job_entries(
    html: str,
    base_url: str,
    include_titles_any: List[str],
    exclude_titles_any: List[str],
) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    entries: List[Dict[str, Any]] = []
    seen_links: set[str] = set()
    now = datetime.now(timezone.utc)

    for card in soup.select("[data-at='job-item'], article"):
        link_el = None
        title_text = None
        for a in card.select("a[href]"):
            href = a.get("href", "")
            if JOB_LINK_RE.search(href):
                link_el = _abs(base_url, href)
                title_text = a.get_text(strip=True) or title_text
                break
        if not link_el or link_el in seen_links:
            continue
        seen_links.add(link_el)

        title_lower = (title_text or "").lower()
        if include_titles_any and not any(tok in title_lower for tok in include_titles_any):
            continue
        if exclude_titles_any and any(tok in title_lower for tok in exclude_titles_any):
            continue

        posted_label = _extract_posted_label(card)
        posted_dt = parse_stepstone_listing_date(posted_label, now=now) if posted_label else None

        entries.append(
            {
                "url": link_el,
                "title": title_text,
                "posted_label": posted_label,
                "posted_dt": posted_dt,
            }
        )

    if not entries:
        # Fallback to legacy behaviour when markup shifts
        for link in _extract_job_links(html, base_url):
            entries.append(
                {
                    "url": link,
                    "title": None,
                    "posted_label": None,
                    "posted_dt": None,
                }
            )
    return entries

def _estimate_total_pages(html: str, *, per_page: int = PER_PAGE_DEFAULT) -> Optional[int]:
    """
    Best-effort extraction of the total number of result pages from the StepStone HTML.
    """
    if not html:
        return None

    # Try explicit pagination attribute
    match = PAGE_LAST_RE.search(html)
    if match:
        try:
            val = int(match.group(1))
            if val > 0:
                return val
        except Exception:
            pass

    # Fallback to "X Treffer" text (German locale)
    match = RESULT_COUNT_RE.search(html)
    if match:
        try:
            total_hits = int(match.group(1).replace(".", ""))
            if total_hits > 0 and per_page > 0:
                return max(1, math.ceil(total_hits / per_page))
        except Exception:
            pass

    # JSON-LD parsing
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

def search_stepstone(
    seed_url: str,
    pages: Optional[int] = None,
    delay_sec: float = 1.2,
    include_titles_any: Optional[List[str]] = None,
    exclude_titles_any: Optional[List[str]] = None,
    max_jobs: Optional[int] = None,
    max_pages_guard: int = 80,
    stop_urls: Optional[List[str]] = None,
) -> Dict:
    include_titles_any = [t.lower() for t in (include_titles_any or [])]
    exclude_titles_any = [t.lower() for t in (exclude_titles_any or [])]
    # stop_set logic disabled; keep placeholder to show intentional no-op
    # stop_set = {s.strip() for s in (stop_urls or []) if s}

    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130 Safari/537.36"
    }
    client = httpx.Client(timeout=30, follow_redirects=True, headers=headers)

    all_jobs: List[Dict[str, Any]] = []
    seen_urls: set[str] = set()
    page_hits: List[Dict] = []

    target_pages: Optional[int] = pages if pages and pages > 0 else None
    guard = max(1, max_pages_guard or 80)
    per_page = PER_PAGE_DEFAULT
    estimated_pages = None

    try:
        p = 1
        empty_streak = 0
        while True:
            page_url = _with_page(seed_url, p)
            r = client.get(page_url)
            html = r.text
            jobs = _extract_job_entries(html, str(r.url), include_titles_any, exclude_titles_any)

            added = 0
            for job in jobs:
                url = job["url"]
                if url in seen_urls:
                    continue
                seen_urls.add(url)
                all_jobs.append(job)
                added += 1
                if max_jobs and len(all_jobs) >= max_jobs:
                    break

            page_hits.append({"page": p, "url": str(r.url), "found": added})

            if p == 1 and target_pages is None:
                estimated_pages = _estimate_total_pages(html, per_page=per_page)
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
            empty_streak = empty_streak + 1 if added == 0 else 0
            if empty_streak >= 5:
                logger.debug("Stopping {} after {} empty pages ({} total urls)", seed_url, empty_streak, len(all_jobs))
                break

            p += 1

            # polite delay with jitter (helps with throttling)
            time.sleep(max(0.2, delay_sec * random.uniform(0.7, 1.4)))
    finally:
        client.close()

    serialized_jobs: List[Dict[str, Any]] = []
    for job in all_jobs:
        dt = job.get("posted_dt")
        serialized_jobs.append(
            {
                "url": job["url"],
                "title": job.get("title"),
                "posted_label": job.get("posted_label"),
                "posted_iso": isoformat_utc(dt) if isinstance(dt, datetime) else None,
            }
        )

    return {
        "ok": True,
        "seed_url": seed_url,
        "pages_requested": pages,
        "pages_scanned": len(page_hits),
        "page_hits": page_hits,
        "count": len(serialized_jobs),
        "estimated_total_pages": estimated_pages,
        "target_pages": target_pages,
        "urls": [item["url"] for item in serialized_jobs],
        "jobs": serialized_jobs,
    }
