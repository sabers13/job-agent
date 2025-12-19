from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

from ..common.utils import ensure_dir, slugify, timestamp_iso, to_jsonable


def _score_bucket(score: int | None) -> str | None:
    if score is None:
        return None
    if score >= 90:
        return "100-90_excellent"
    if score >= 80:
        return "90-80_good"
    if score >= 70:
        return "80-70_acceptable"
    return None


def write_bundle(
    root: str,
    job: Dict,
    assets: Dict,
    scoring: Dict | None = None,
    seed_slug: str | None = None,
    enrichment_meta: Dict | None = None,
) -> str:
    company = job.get("company") or "unknown"
    title = job.get("title") or "position"
    loc = job.get("location") or ""
    base_slug = slugify(f"{company}-{title}-{loc}")[:80]
    slug = f"{slugify(seed_slug)}-{base_slug}" if seed_slug else base_slug

    score = (scoring or {}).get("score")
    prefix = f"{score:02d}" if isinstance(score, int) else "nn"

    bucket = _score_bucket(score if isinstance(score, int) else None)
    root_path = Path(root)
    if bucket:
        root_path = root_path / bucket

    out_dir = root_path / f"{prefix}_{slug}"
    ensure_dir(out_dir)

    for name, content in assets.items():
        (out_dir / name).write_text(content, encoding="utf-8")

    meta = {
        "created_at": timestamp_iso(),
        "job": job,
        "scoring": scoring,
        "enrichment_meta": enrichment_meta
        or {"ok": False, "error_type": "not_run", "error_message": None, "model": None},
        "dir": str(out_dir),
    }
    (out_dir / "metadata.json").write_text(
        json.dumps(to_jsonable(meta), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return str(out_dir)


def write_summary(reports: List[Dict], out_dir: str = "output") -> str:
    lines = ["# Job Search Summary", ""]
    for r in reports:
        job = r.get("job", {}) or {}
        scoring = r.get("scoring", {}) or {}
        title = job.get("title") or "Unknown"
        company = job.get("company") or "Unknown"
        score = scoring.get("score", "n/a")
        loc = job.get("location") or "Unknown"
        bundle_dir = r.get("output_dir") or "(not written)"
        lines.append(f"## {title} — {company} — Score: {score}")
        lines.append(f"- Location: {loc}")
        lines.append(f"- Bundle: `{bundle_dir}`")
        reasons = scoring.get("reasons") or []
        if reasons:
            lines.append("- Reasons:")
            lines.extend([f"  - {x}" for x in reasons])
        lines.append("")

    ensure_dir(Path(out_dir))
    path = str(Path(out_dir) / "REPORT_SUMMARY.md")
    Path(path).write_text("\n".join(lines), encoding="utf-8")
    return path
