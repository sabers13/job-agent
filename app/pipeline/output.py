from __future__ import annotations

import json
import os
import re
import shutil
from pathlib import Path
from typing import Dict, List, Optional

from app.config.settings import settings
from app.pipeline.potential_bucket import decide_potential
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


def _safe_folder(name: str) -> str:
    cleaned = re.sub(r"[^a-z0-9_-]+", "_", name.strip().lower())
    return cleaned.strip("_") or "misc"


def write_bundle(
    root: str,
    job: Dict,
    assets: Dict,
    scoring: Dict | None = None,
    seed_slug: str | None = None,
    enrichment_meta: Dict | None = None,
    category: str | None = None,
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
    is_potential_category = bool(category and _safe_folder(category) == "potential_applications")
    if category and not is_potential_category:
        root_path = root_path / _safe_folder(category)
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

    merged = {**job, **(scoring or {})}
    decision = decide_potential(merged, final_cutoff=70.0, llm_cutoff=70.0)
    if is_potential_category or decision.is_potential:
        base_root = Path(root)
        run_root = base_root.parent if base_root.name == "bundles" else base_root
        pot_root = run_root / "potential_applications"
        ensure_dir(pot_root)
        pot_job_dir = pot_root / out_dir.name
        ensure_dir(pot_job_dir)
        (pot_job_dir / "potential_reason.json").write_text(
            json.dumps(
                {
                    "reason": decision.reason,
                    "final_score": decision.final_score,
                    "llm_score": decision.llm_score,
                    "source_job_dir": str(out_dir),
                },
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        for item in out_dir.iterdir():
            if item.is_file():
                shutil.copy2(item, pot_job_dir / item.name)
    return str(out_dir)


def write_summary(
    reports: List[Dict],
    out_dir: Optional[str] = None,
    metrics: Optional[Dict] = None,
) -> str:
    lines = ["# Job Search Summary", ""]
    if metrics:
        lines.extend(
            [
                "## Run Metrics",
                f"- Discovered URLs: {metrics.get('discovered_total', 0)}",
                f"- Skipped (already seen): {metrics.get('skipped_pool_existing', 0)}",
                f"- Accepted new: {metrics.get('accepted_new', 0)}",
                f"- Processed: {metrics.get('processed', 0)}",
                f"- Potential applications: {metrics.get('potential_applications_count', 0)}",
                f"- Potential applications path: {metrics.get('potential_applications_path', None)}",
                f"- Stale: {metrics.get('stale', 0)}",
                f"- Rejected low score: {metrics.get('rejected_low_score', 0)}",
                f"- Error: {metrics.get('error', 0)}",
                f"- Bundle failed: {metrics.get('bundle_failed', 0)}",
                "",
            ]
        )
    for r in reports:
        job = r.get("job", {}) or {}
        scoring = r.get("scoring", {}) or {}
        title = job.get("title") or "Unknown"
        company = job.get("company") or "Unknown"
        score = scoring.get("score", "n/a")
        llm_score = scoring.get("llm_score", None)
        keep_threshold = settings.score_keep_threshold
        potential_tag = ""
        try:
            if (
                isinstance(score, (int, float))
                and score < keep_threshold
                and llm_score is not None
                and float(llm_score) >= float(keep_threshold)
            ):
                potential_tag = " [POTENTIAL]"
        except Exception:
            potential_tag = ""
        loc = job.get("location") or "Unknown"
        bundle_dir = r.get("output_dir") or "(not written)"
        lines.append(f"## {title} — {company} — Score: {score}{potential_tag}")
        lines.append(f"- Location: {loc}")
        lines.append(f"- Bundle: `{bundle_dir}`")
        reasons = scoring.get("reasons") or []
        if reasons:
            lines.append("- Reasons:")
            lines.extend([f"  - {x}" for x in reasons])
        lines.append("")

    if not out_dir:
        env_root = os.getenv("JOBAGENT_OUTPUT_ROOT")
        out_dir = env_root or str(settings.output_dir)
    ensure_dir(Path(out_dir))
    path = str(Path(out_dir) / "REPORT_SUMMARY.md")
    Path(path).write_text("\n".join(lines), encoding="utf-8")
    return path
