from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from loguru import logger

from ..fetching.polite_fetch import (
    FetchError,
    RobotsDisallowedError,
    AccessDeniedError as FetchAccessDeniedError,
    fetch_job_html,
)
from app.config.settings import settings
from app.config.focus import FocusConfig, DEFAULT_FOCUS
from .output import write_bundle
from .state import cache_get, cache_put
from .llm_enrich import enrich_jobposting
from .models import UnifiedJobPosting
from .parsers import extract_jobposting_from_html
from .scoring import score_job
from .templating import generate_bundle

CachePayload = Dict[str, Any]


def _parse_iso8601(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        value = ts.strip()
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        return datetime.fromisoformat(value)
    except Exception:
        return None


async def fetch_job_details(
    url: str,
    *,
    backend: Optional[str] = None,
    enrich: bool = False,
    score: bool = False,
    cutoff_iso: Optional[str] = None,
    use_cache: bool = True,
    focus: Optional[FocusConfig] = None,
    use_llm_scoring: Optional[bool] = None,
    apply_blocker_cap: Optional[bool] = None,
) -> Dict[str, Any]:
    """
    Fetch a StepStone job posting, optionally enrich and score it, and compute
    whether it is stale compared to an ISO8601 cutoff timestamp.
    Mirrors the behaviour of the FastAPI /job_details endpoint so orchestration
    layers (Prefect, HTTP) stay consistent.
    """
    cache_enabled = use_cache and settings.cache_enabled and (settings.cache_per_profile or focus is None)
    # Optional cache short-circuit
    if cache_enabled:
        try:
            cached = cache_get(url, focus=focus)
            if cached:
                cutoff_dt = _parse_iso8601(cutoff_iso)
                posted_dt = _parse_iso8601((cached.get("job") or {}).get("date_posted"))
                stale = bool(cutoff_dt and posted_dt and posted_dt < cutoff_dt)

                cached["cutoff_iso"] = cutoff_iso
                cached["stale"] = stale
                cached_job = cached.get("job")
                if isinstance(cached_job, dict):
                    if stale:
                        cached_job["stale"] = True
                    else:
                        cached_job.pop("stale", None)
                return cached
        except Exception:
            logger.warning("cache_get failed; continuing without cache", exc_info=True)

    try:
        html, fetch_meta = await fetch_job_html(url, preferred_backend=backend)
        logger.info(
            "job_details fetch success url={} backend={} attempts={}",
            url,
            fetch_meta.get("backend"),
            fetch_meta.get("attempts"),
        )
    except RobotsDisallowedError:
        raise
    except FetchAccessDeniedError:
        raise
    except FetchError:
        raise

    base = extract_jobposting_from_html(html)
    base.setdefault("url", url)

    core = UnifiedJobPosting(
        **{
            "title": base.get("title") or "Unknown",
            "company": base.get("company") or "Unknown",
            "location": base.get("location") or "Unknown",
            "employment_type": base.get("employment_type"),
            "date_posted": base.get("date_posted"),
            "valid_through": base.get("valid_through"),
            "url": base.get("url"),
            "job_id": base.get("job_id"),
            "salary": base.get("salary"),
            "description_html": base.get("description_html"),
            "description_text": base.get("description_text"),
        }
    ).model_dump(mode="json")

    enrichment_meta = None
    active_focus = focus or DEFAULT_FOCUS
    final_job: Dict[str, Any] = core
    if enrich:
        try:
            final_job, enrichment_meta = enrich_jobposting(core, focus=active_focus)
        except Exception as exc:
            enrichment_meta = {
                "ok": False,
                "model": settings.openai_model,
                "error_type": "enrich_wrapper_failure",
                "error_message": f"Unexpected error in enrichment wrapper: {exc}",
            }
            logger.exception("Unexpected error in enrichment wrapper")
    scoring = score_job(
        final_job,
        focus=active_focus,
        use_llm_scoring=use_llm_scoring,
        apply_blocker_cap=apply_blocker_cap,
    ) if score else None

    if scoring:
        final_job = {**final_job, "junior_fit_score": scoring["score"]}

    result: Dict[str, Any] = {
        "ok": True,
        "backend": fetch_meta.get("backend") or backend,
        "job": final_job,
        "scoring": scoring,
        "fetch_meta": fetch_meta,
        "cutoff_iso": cutoff_iso,
        "enrichment_meta": enrichment_meta,
        "stale": False,
    }

    cutoff_dt = _parse_iso8601(cutoff_iso)
    posted_dt = _parse_iso8601(final_job.get("date_posted"))
    stale = bool(cutoff_dt and posted_dt and posted_dt < cutoff_dt)
    result["stale"] = stale
    if stale:
        result["job"]["stale"] = True

    if cache_enabled:
        try:
            cache_put(url, result, focus=focus)
        except Exception:
            logger.warning("cache_put failed; continuing without cache", exc_info=True)

    return result


def write_job_bundle(job: Dict[str, Any], scoring: Optional[Dict[str, Any]] = None, *, seed_slug: Optional[str] = None, enrichment_meta: Optional[Dict[str, Any]] = None) -> str:
    """
    Generate assets for a job posting and persist them to disk.
    Returns the directory path where the bundle was written.
    """
    assets = generate_bundle(job, scoring)
    return write_bundle("output", job, assets, scoring, seed_slug=seed_slug, enrichment_meta=enrichment_meta)
