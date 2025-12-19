from __future__ import annotations
from typing import Any, Dict, Optional, List
from bs4 import BeautifulSoup
import json
import re
from html import unescape

def _first_not_none(*vals):
    for v in vals:
        if v not in (None, "", []):
            return v
    return None

def _as_text(html_str: Optional[str]) -> Optional[str]:
    if not html_str:
        return None
    # Strip HTML tags conservatively
    soup = BeautifulSoup(html_str, "lxml")
    text = soup.get_text(" ", strip=True)
    return re.sub(r"\s+", " ", text).strip() if text else None

def _normalize_location(job_json: Dict[str, Any]) -> Optional[str]:
    loc = job_json.get("jobLocation")
    if isinstance(loc, list) and loc:
        loc = loc[0]
    if isinstance(loc, dict):
        addr = loc.get("address") or {}
        comps = [
            addr.get("addressLocality"),
            addr.get("addressRegion"),
            addr.get("addressCountry"),
        ]
        comps = [c for c in comps if c]
        return ", ".join(comps) if comps else None
    return None

def _normalize_salary(job_json: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    sal = job_json.get("baseSalary")
    if not isinstance(sal, dict):
        return None
    # schema.org can nest in "value"
    val = sal.get("value")
    if isinstance(val, dict):
        currency = val.get("currency")
        unit = val.get("unitText")
        min_v = val.get("minValue")
        max_v = val.get("maxValue")
        val_v = val.get("value")
        return {
            "currency": currency,
            "unit": unit,
            "min": min_v,
            "max": max_v,
            "value": val_v,
        }
    return None

def _pick_jobposting(ld_blocks: List[Any]) -> Optional[Dict[str, Any]]:
    # Find object with @type == "JobPosting" (handle lists/nested graphs)
    for block in ld_blocks:
        if isinstance(block, dict):
            # Standalone
            if block.get("@type") == "JobPosting":
                return block
            # Graph
            graph = block.get("@graph")
            if isinstance(graph, list):
                for node in graph:
                    if isinstance(node, dict) and node.get("@type") == "JobPosting":
                        return node
        elif isinstance(block, list):
            for node in block:
                if isinstance(node, dict) and node.get("@type") == "JobPosting":
                    return node
    return None

def extract_jobposting_from_html(html: str) -> Dict[str, Any]:
    """
    Parse <script type='application/ld+json'>, find a JobPosting object,
    and map to a unified schema. If missing, return minimal Unknown fields.
    """
    soup = BeautifulSoup(html, "lxml")
    scripts = soup.find_all("script", attrs={"type": "application/ld+json"})
    ld_blocks: List[Any] = []
    for s in scripts:
        try:
            # Some sites HTML-escape the JSON
            raw = s.string or s.text
            if not raw:
                continue
            raw = unescape(raw).strip()
            data = json.loads(raw)
            ld_blocks.append(data)
        except Exception:
            continue

    jp = _pick_jobposting(ld_blocks) or {}

    title = _first_not_none(jp.get("title"), soup.find("h1").get_text(strip=True) if soup.find("h1") else None)
    org = jp.get("hiringOrganization") or {}
    if isinstance(org, dict):
        company = _first_not_none(org.get("name"), org.get("legalName"))
    else:
        company = None

    desc_html = jp.get("description")
    description_text = _as_text(desc_html)
    location = _normalize_location(jp)
    emp_type = _first_not_none(jp.get("employmentType"))
    date_posted = _first_not_none(jp.get("datePosted"), jp.get("datePublished"))
    valid_through = _first_not_none(jp.get("validThrough"))
    salary = _normalize_salary(jp)

    url = _first_not_none(jp.get("url"))
    identifier = jp.get("identifier")
    job_id = None
    if isinstance(identifier, dict):
        job_id = _first_not_none(identifier.get("value"), identifier.get("@id"))

    return {
        "schema_source": "ld+json",
        "raw_present": bool(jp),
        "title": title or "Unknown",
        "company": company or "Unknown",
        "location": location or "Unknown",
        "employment_type": emp_type or "Unknown",
        "date_posted": date_posted or "Unknown",
        "valid_through": valid_through or None,
        "url": url,
        "job_id": job_id,
        "salary": salary,
        "description_html": desc_html or None,
        "description_text": description_text or None,
    }
