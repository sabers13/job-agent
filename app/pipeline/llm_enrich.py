from __future__ import annotations
import json, re
import os
from dataclasses import dataclass
from typing import Dict, Any, Optional, Tuple
from openai import OpenAI

from app.config.focus import DEFAULT_FOCUS
from app.config.settings import settings
from loguru import logger

_CLIENT = None


def _client():
    global _CLIENT
    if _CLIENT is None:
        _CLIENT = OpenAI()
    return _CLIENT


@dataclass
class EnrichmentMeta:
    ok: bool
    model: Optional[str] = None
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    raw_excerpt: Optional[str] = None


def _build_system_prompt(focus=DEFAULT_FOCUS) -> str:
    f = focus or DEFAULT_FOCUS
    return (
        "You are an expert recruiter for junior data/BI roles in Germany.\n"
        "Given a job ad (plain text) and partial metadata, return a STRICT JSON object with fields:\n"
        "- seniority (Junior|Working Student|Internship|Mid|Senior|Unknown)\n"
        '- english_ok (true|false|null) — true only if the ad explicitly states English is accepted/working language\n'
        "- language_requirements: an array of objects, one per language explicitly referenced in the ad. Each object must include:\n"
        "    {\n"
        '      "language": string (e.g. "German", "English"),\n'
        '      "cefr_guess": "A1"|"A2"|"B1"|"B2"|"C1"|"C2"|"Unknown",\n'
        '      "confidence": number 0.0–1.0 (be conservative; vague hints <=0.3),\n'
        '      "evidence_phrases": list of short substrings from the ad that support the guess,\n'
        '      "customer_facing": true|false (does the role require external/customer contact?),\n'
        '      "job_post_language": "German"|"English"|"Mixed"|"Unknown"\n'
        "    }\n"
        "- german_requirement (None|A1|A2|B1|B2|C1|Native|Unknown) — provide the best single-level summary for backwards compatibility\n"
        "- skills_detected (list of strings)\n"
        "- skill_hits (object of keyword:count for Python, SQL, Power BI, DAX, Power Query, Pandas, NumPy)\n"
        "- reasons_include (list, concise)\n"
        "- reasons_exclude (list, concise)\n"
        "The target profile:\n"
        f"- Preferred titles (any): {', '.join(sorted(getattr(f, 'titles_any', [])))}\n"
        f"- Excluded titles (any): {', '.join(sorted(getattr(f, 'exclude_titles_any', [])))}\n"
        f"- Core skills (must-have): {', '.join(sorted(getattr(f, 'include_skills_any', [])))}\n"
        f"- Nice-to-have skills: {', '.join(sorted(getattr(f, 'nice_to_have', [])))}\n"
        f"- Preferred locations: {', '.join(sorted(getattr(f, 'locations_any', [])))}\n"
        "Rules:\n"
        "- Do NOT fabricate unknowns; if unsure, set fields to null/Unknown and keep confidence <=0.3.\n"
        '- Only guess a CEFR level when the ad explicitly names a language or provides strong evidence (e.g. "sehr gute Deutschkenntnisse"). '
        "Generic phrases like \"communication skills\" without a language should leave the language as Unknown.\n"
        '- If the ad is entirely in German and customer-facing, you may give German="B2" with confidence <=0.5; otherwise keep it Unknown with low confidence.\n'
        "- Mark customer_facing=true when the ad emphasises Kundenkontakt/consulting/sales/external stakeholders.\n"
        "- Set job_post_language based on the predominant language of the ad.\n"
        "- english_ok=true only if English is explicitly acceptable or stated as the working language.\n"
        "Output ONLY JSON.\n"
    )


def _safe_jsonable(obj):
    """Recursively convert sets/tuples and other non-JSON types into JSON-safe structures."""
    if isinstance(obj, set):
        return sorted(_safe_jsonable(item) for item in obj)
    if isinstance(obj, tuple):
        return [_safe_jsonable(item) for item in obj]
    if isinstance(obj, list):
        return [_safe_jsonable(item) for item in obj]
    if isinstance(obj, dict):
        return {k: _safe_jsonable(v) for k, v in obj.items()}
    return obj


def _load_resume_snapshot() -> Optional[Dict[str, Any]]:
    path = os.getenv("JOBAGENT_RESUME_SNAPSHOT")
    if not path:
        return None
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except Exception:
        return None
    parsed = data.get("parsed_json")
    excerpt = data.get("text_excerpt") or ""
    return {
        "resume_id": data.get("resume_id"),
        "sha256": data.get("sha256"),
        "parsed_json": parsed,
        "text_excerpt": excerpt[:4000],
    }


def _build_user_prompt(job: Dict[str, Any], focus=DEFAULT_FOCUS) -> str:
    title = job.get("title") or "Unknown"
    company = job.get("company") or "Unknown"
    location = job.get("location") or "Unknown"
    desc = job.get("description_text") or ""
    focus_payload = {}
    if hasattr(focus, "model_dump"):
        focus_payload = focus.model_dump()
    else:
        focus_payload = getattr(focus, "__dict__", {}) or {}
    focus_payload = _safe_jsonable(focus_payload)
    meta = {
        "title": title,
        "company": company,
        "location": location,
        "employment_type": job.get("employment_type"),
        "date_posted": job.get("date_posted"),
        "url": job.get("url"),
        "focus": focus_payload,
    }
    resume_ctx = _load_resume_snapshot()
    resume_block = ""
    if resume_ctx:
        resume_block = (
            "\n\nResume CONTEXT:\n"
            f"{json.dumps(resume_ctx, ensure_ascii=False, indent=2)}"
        )
    return f"""Job META:
{json.dumps(meta, ensure_ascii=False, indent=2)}
{resume_block}

Job DESCRIPTION (text):
\"\"\"
{desc[:20000]}
\"\"\""""


def enrich_jobposting(job: Dict[str, Any], focus=DEFAULT_FOCUS) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Calls OpenAI to enrich fields. Returns (merged dict, enrichment_meta).
    Enrichment failures do not raise; metadata captures the failure.
    """
    active_focus = focus or DEFAULT_FOCUS
    prompt = _build_user_prompt(job, active_focus)
    system_prompt = _build_system_prompt(active_focus)
    client = _client()
    raw_text = ""
    try:
        temperature = 0.2
        if "gpt-5" in settings.openai_model:
            temperature = 1
        resp = client.chat.completions.create(
            model=settings.openai_model,
            temperature=temperature,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt},
            ],
        )
        content = resp.choices[0].message.content or "{}"
        raw_text = content
        m = re.search(r"\{.*\}", content, re.DOTALL)
        data = {}
        if m:
            try:
                data = json.loads(m.group(0))
            except Exception:
                data = {}
        enriched = job.copy()
        for k in (
            "seniority",
            "english_ok",
            "german_requirement",
            "skills_detected",
            "skill_hits",
            "reasons_include",
            "reasons_exclude",
            "language_requirements",
        ):
            if k in data:
                enriched[k] = data[k]
        meta = EnrichmentMeta(
            ok=True,
            model=settings.openai_model,
            raw_excerpt=raw_text[:400],
        ).__dict__
        return enriched, meta
    except Exception as exc:
        logger.warning(
            "LLM enrichment failed for job '%s' (%s): %s",
            job.get("title"),
            job.get("url"),
            exc,
        )
        meta = EnrichmentMeta(
            ok=False,
            model=settings.openai_model,
            error_type=type(exc).__name__,
            error_message=str(exc),
            raw_excerpt=raw_text[:400] if raw_text else None,
        ).__dict__
        return job, meta


LLM_SCORING_VERSION = "1.0.0"


def llm_score_job(job: Dict[str, Any], focus: Any, heuristic_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ask an LLM to provide a holistic score and German requirement summary.
    Returns a dict with llm_score and related metadata.
    """
    client = _client()
    payload = {
        "candidate_profile": {
            "profile_name": getattr(focus, "profile_name", ""),
            "description": getattr(focus, "description", None),
            "titles_any": sorted(getattr(focus, "titles_any", [])),
            "exclude_titles_any": sorted(getattr(focus, "exclude_titles_any", [])),
            "include_skills_any": sorted(getattr(focus, "include_skills_any", [])),
            "nice_to_have": sorted(getattr(focus, "nice_to_have", [])),
            "locations_any": sorted(getattr(focus, "locations_any", [])),
        },
        "job": {
            "title": job.get("title"),
            "company": job.get("company"),
            "location": job.get("location"),
            "employment_type": job.get("employment_type"),
            "description_text": job.get("description_text"),
        },
        "heuristic_summary": {
            "score": heuristic_result.get("heuristic_score") or heuristic_result.get("score"),
            "components": heuristic_result.get("components"),
            "reasons": heuristic_result.get("reasons"),
            "meta": heuristic_result.get("meta", {}),
        },
    }
    resume_ctx = _load_resume_snapshot()
    if resume_ctx:
        payload["resume_context"] = resume_ctx

    system_prompt = (
        "You are an assistant that evaluates how well a job fits a specific candidate.\n"
        "You are given a candidate profile, a full job description text, and a heuristic analysis.\n"
        "Tasks:\n"
        "1) Read the FULL job description text.\n"
        "2) Decide the German requirement: none | preferred | required | hard_blocker and estimate min CEFR level (A2,B1,B2,C1,C2).\n"
        "3) Provide a holistic suitability score 0-100 (0-20 impossible, 20-50 weak, 50-70 stretch, 70-85 good, 85-100 excellent).\n"
        "4) List risk_flags and critical_blockers.\n"
        "Output ONLY JSON with keys: llm_score, german_requirement{type,min_level,justification}, risk_flags, critical_blockers, summary.\n"
    )

    content: str = ""
    raw_excerpt: Optional[str] = None
    try:
        temperature = 0.2
        if "gpt-5" in settings.openai_model_scoring:
            temperature = 1
        resp = client.chat.completions.create(
            model=settings.openai_model_scoring,
            temperature=temperature,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
            ],
        )
        content = resp.choices[0].message.content or "{}"
        raw_excerpt = content[:600] if content else None

        m = re.search(r"\{.*\}", content, re.DOTALL)
        if not m:
            return {
                "llm_scoring_version": LLM_SCORING_VERSION,
                "llm_score": None,
                "german_requirement": None,
                "risk_flags": [],
                "critical_blockers": [],
                "summary": None,
                "confidence": None,
                "llm_ok": False,
                "ok": False,  # alias
                "error_type": "parse_error",
                "error_message": "No JSON object found in model output.",
                "raw_excerpt": raw_excerpt,
            }

        try:
            data = json.loads(m.group(0))
        except Exception as exc:
            return {
                "llm_scoring_version": LLM_SCORING_VERSION,
                "llm_score": None,
                "german_requirement": None,
                "risk_flags": [],
                "critical_blockers": [],
                "summary": None,
                "confidence": None,
                "llm_ok": False,
                "ok": False,  # alias
                "error_type": "parse_error",
                "error_message": f"Failed to parse JSON: {type(exc).__name__}: {exc}",
                "raw_excerpt": raw_excerpt,
            }

        llm_score = data.get("llm_score")
        llm_ok = isinstance(llm_score, (int, float))

        conf_raw = data.get("confidence")
        llm_confidence = None
        try:
            if conf_raw is not None:
                llm_confidence = float(conf_raw)
        except Exception:
            llm_confidence = None

        if not llm_ok:
            return {
                "llm_scoring_version": LLM_SCORING_VERSION,
                "llm_score": llm_score,
                "german_requirement": data.get("german_requirement"),
                "risk_flags": data.get("risk_flags") or [],
                "critical_blockers": data.get("critical_blockers") or [],
                "summary": data.get("summary"),
                "confidence": llm_confidence,
                "llm_ok": False,
                "ok": False,  # alias
                "error_type": "schema_error",
                "error_message": "Parsed JSON but llm_score is missing or not a number.",
                "raw_excerpt": raw_excerpt,
            }

        return {
            "llm_scoring_version": LLM_SCORING_VERSION,
            "llm_score": float(llm_score),
            "german_requirement": data.get("german_requirement"),
            "risk_flags": data.get("risk_flags") or [],
            "critical_blockers": data.get("critical_blockers") or [],
            "summary": data.get("summary"),
            "confidence": llm_confidence,
            "llm_ok": True,
            "ok": True,  # alias
            "error_type": None,
            "error_message": None,
            "raw_excerpt": raw_excerpt,
        }
    except Exception as exc:
        # On any LLM failure, fall back gracefully
        return {
            "llm_score": None,
            "llm_scoring_version": LLM_SCORING_VERSION,
            "german_requirement": None,
            "risk_flags": [],
            "critical_blockers": [],
            "summary": None,
            "confidence": None,
            "llm_ok": False,
            "ok": False,
            "error_type": type(exc).__name__,
            "error_message": str(exc),
            "raw_excerpt": raw_excerpt,
        }
