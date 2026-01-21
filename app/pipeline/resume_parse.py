from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional


def extract_text_from_file(path: Path, mime_type: Optional[str] = None) -> str:
    suffix = path.suffix.lower()
    if mime_type == "text/plain" or suffix == ".txt":
        return path.read_text(encoding="utf-8", errors="replace")
    if suffix == ".docx" or mime_type in ("application/vnd.openxmlformats-officedocument.wordprocessingml.document",):
        from docx import Document

        doc = Document(str(path))
        return "\n".join(p.text for p in doc.paragraphs if p.text)
    if suffix == ".pdf" or mime_type == "application/pdf":
        from pypdf import PdfReader

        reader = PdfReader(str(path))
        parts = []
        for page in reader.pages:
            text = page.extract_text() if page else ""
            if text:
                parts.append(text)
        return "\n".join(parts)
    return path.read_text(encoding="utf-8", errors="replace")


def _extract_email(text: str) -> Optional[str]:
    match = re.search(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", text, re.IGNORECASE)
    return match.group(0) if match else None


def _extract_phone(text: str) -> Optional[str]:
    match = re.search(r"\+?\d[\d\s().-]{7,}\d", text)
    return match.group(0).strip() if match else None


def _extract_links(text: str) -> List[str]:
    raw = re.findall(r"https?://[^\s)]+", text)
    seen = set()
    links = []
    for link in raw:
        cleaned = link.strip().rstrip(".,;")
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            links.append(cleaned)
    return links


def _split_sections(text: str) -> Dict[str, List[str]]:
    headings = {
        "summary": "summary",
        "profile": "summary",
        "experience": "experience",
        "work experience": "experience",
        "education": "education",
        "skills": "skills",
        "projects": "projects",
        "certifications": "certifications",
        "certification": "certifications",
    }
    sections: Dict[str, List[str]] = {}
    current = "summary"
    sections.setdefault(current, [])
    for line in text.splitlines():
        raw = line.strip()
        if not raw:
            continue
        key = raw.lower().strip(":")
        if key in headings:
            current = headings[key]
            sections.setdefault(current, [])
            continue
        if raw.endswith(":") and len(raw) <= 40:
            key = raw.lower().strip(":")
            if key in headings:
                current = headings[key]
                sections.setdefault(current, [])
                continue
        sections.setdefault(current, []).append(raw)
    return sections


def _categorize_skills(items: List[str]) -> Dict[str, List[str]]:
    buckets = {"languages": [], "frameworks": [], "tools": [], "cloud": []}
    language_keys = {"python", "sql", "r", "java", "scala", "javascript", "typescript", "c#", "c++"}
    framework_keys = {"fastapi", "django", "flask", "spark", "pyspark", "pandas", "numpy"}
    tool_keys = {"power bi", "tableau", "excel", "git", "jira", "power query", "dax"}
    cloud_keys = {"aws", "azure", "gcp", "google cloud", "azure data factory"}
    for item in items:
        val = item.strip()
        if not val:
            continue
        lowered = val.lower()
        bucket = "tools"
        if any(k in lowered for k in language_keys):
            bucket = "languages"
        elif any(k in lowered for k in framework_keys):
            bucket = "frameworks"
        elif any(k in lowered for k in cloud_keys):
            bucket = "cloud"
        elif any(k in lowered for k in tool_keys):
            bucket = "tools"
        if val not in buckets[bucket]:
            buckets[bucket].append(val)
    return buckets


def _collect_bullets(lines: List[str]) -> List[str]:
    bullets: List[str] = []
    for line in lines:
        cleaned = line.lstrip("-•* ").strip()
        if cleaned:
            bullets.append(cleaned)
    return bullets


def parse_resume_text(text: str) -> Dict[str, Any]:
    text = text or ""
    sections = _split_sections(text)
    skills_raw = []
    for line in sections.get("skills", []):
        for part in re.split(r"[;,/]", line):
            if part.strip():
                skills_raw.append(part.strip())
    skills = _categorize_skills(skills_raw)

    parsed: Dict[str, Any] = {
        "name": None,
        "title": None,
        "location": None,
        "email": _extract_email(text),
        "phone": _extract_phone(text),
        "links": _extract_links(text),
        "summary": " ".join(sections.get("summary", [])[:6]).strip() or None,
        "skills": skills,
        "experience": [],
        "education": [],
        "projects": [],
        "certifications": [],
    }

    if sections.get("experience"):
        parsed["experience"].append(
            {
                "company": None,
                "role": None,
                "start": None,
                "end": None,
                "bullets": _collect_bullets(sections.get("experience", [])),
            }
        )
    if sections.get("education"):
        parsed["education"].append(
            {
                "school": None,
                "degree": None,
                "field": None,
                "start": None,
                "end": None,
                "bullets": _collect_bullets(sections.get("education", [])),
            }
        )
    if sections.get("projects"):
        parsed["projects"].append(
            {
                "name": None,
                "bullets": _collect_bullets(sections.get("projects", [])),
                "links": [],
            }
        )
    if sections.get("certifications"):
        parsed["certifications"] = sections.get("certifications", [])

    return parsed


def parse_resume_file(path: Path, mime_type: Optional[str] = None) -> Dict[str, Any]:
    text = extract_text_from_file(path, mime_type=mime_type)
    parsed = parse_resume_text(text)
    return {"text": text, "parsed": parsed}
