from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

from jinja2 import Environment, FileSystemLoader, select_autoescape

TEMPLATES_DIR = Path(__file__).resolve().parents[2] / "templates"


def _env() -> Environment:
    return Environment(
        loader=FileSystemLoader(str(TEMPLATES_DIR)),
        autoescape=select_autoescape(disabled_extensions=("md", "txt")),
    )


def _language_summary(job: Dict[str, Any]) -> Dict[str, Any]:
    items = job.get("language_requirements") or []
    de = next((x for x in items if (x.get("language", "").lower().startswith("ger"))), {})
    en = next((x for x in items if (x.get("language", "").lower().startswith("eng"))), {})
    return {
        "german_level": (de.get("cefr_guess") or "Unknown"),
        "german_conf": de.get("confidence"),
        "german_customer_facing": bool(de.get("customer_facing")),
        "english_ok": bool(job.get("english_ok")) or bool(en),
        "posting_language": (de.get("job_post_language") or job.get("posting_language") or "Unknown"),
        "phrases": (de.get("evidence_phrases") or [])[:5],
    }


def generate_bundle(job: Dict, scoring: Dict | None = None) -> Dict[str, str]:
    """
    Build report-only bundle assets. Downstream writer will persist REPORT.md and metadata.json.
    """
    env = _env()
    scoring = scoring or {}
    score_val = scoring.get("score")

    lang_summary = _language_summary(job)
    ctx = {
        "job": job,
        "scoring": scoring,
        "derived": {
            "lang": lang_summary,
            "role_display": job.get("title") or "the role",
            "company_display": job.get("company") or "your company",
            "seniority": job.get("seniority") or "Junior",
        },
    }

    outputs: Dict[str, str] = {}
    report_template = TEMPLATES_DIR / "report_md.j2"
    if report_template.exists():
        outputs["REPORT.md"] = env.get_template("report_md.j2").render(**ctx, score_val=score_val)
    else:
        # Minimal fallback if template is missing
        title = job.get("title") or "Unknown role"
        company = job.get("company") or "Unknown company"
        outputs["REPORT.md"] = f"# {title} — {company}\n\nScore: {score_val if score_val is not None else 'n/a'}\n"

    return outputs
