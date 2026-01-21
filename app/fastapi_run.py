from __future__ import annotations

import json
import os
import re
import subprocess
import uuid
import logging
from pathlib import Path
from urllib.parse import quote
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Literal

from dotenv import load_dotenv
from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Query, Request, Response, status, UploadFile, File, Form
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse, HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from loguru import logger as _logger
from pydantic import BaseModel, Field
from sqlalchemy.exc import DBAPIError, OperationalError

from app.config.settings import settings
from app.config import profile_store
from app.config.focus import DEFAULT_FOCUS, FocusConfig, get_focus_config
from app.gui_runs import run_manager
from app.auth.constants import AUTH_COOKIE_NAME
from app.auth.deps import get_current_user
from app.db.crud_profiles import (
    create_profile_for_user,
    delete_profile_for_user,
    get_profile_for_user,
    get_focus_profile_model_for_user,
    list_profiles_for_user,
    update_profile_for_user,
    upsert_profile_for_user,
)
from app.db.health import check_db
from app.db.session import SessionLocal, db_session, is_transient_db_error, ping_db
from app.db.models import Resume
from app.api.schemas import (
    AggregateReportRequest,
    BundleRequest,
    JobDetailsRequest,
    RunSingleRequest,
    RunSingleResponse,
    SearchStepstoneListResponse,
    SearchStepstoneListRequest,
    JobDetailsResponse,
    BundleResponse,
    AggregateReportResponse,
    FetchMeta as FetchMetaSchema,
    ScoringResult,
    UnifiedJobPostingOut,
    ResumeUploadResponse,
    ResumeListItem,
    ResumeDetailResponse,
)
from app.api.auth_routes import router as auth_router
from .pipeline.templating import generate_bundle
from .pipeline.output import write_bundle, write_summary
from .pipeline.models import UnifiedJobPosting, FocusProfileModel
from .pipeline.resume_parse import parse_resume_file
from app.common.utils import ensure_dir, safe_filename, sha256_bytes
from app.common.logging_ctx import get_run_ctx, run_ctx_scope
from .pipeline.url_pool_maintenance import prune_unavailable_stepstone_urls
from .stepstone.search_http import search_stepstone as crawl_http
from .stepstone.search_playwright import search_stepstone_pw as crawl_pw
from .stepstone.smoke import search_stepstone as ss_search 
from .pipeline.state import load_state, save_state
from .fetching.polite_fetch import (
    RobotsDisallowedError,
    AccessDeniedError as FetchAccessDeniedError,
    FetchError,
)
from .pipeline.pipeline import fetch_job_details as pipeline_fetch_job_details
from .stepstone.dates import parse_iso8601_utc


def _loguru_patcher(record):
    ctx = get_run_ctx()
    record["extra"].setdefault("run_id", ctx.get("run_id", "-"))
    record["extra"].setdefault("user_id", ctx.get("user_id", "-"))
    record["extra"].setdefault("profile_key", ctx.get("profile_key", "-"))
    return record


_logger.configure(patcher=_loguru_patcher)
logger = _logger

load_dotenv()

app = FastAPI(title="Job Fetching Agent (DE · Junior · EN-friendly)", version="0.3.1")
templates = Jinja2Templates(directory="templates")
app.include_router(auth_router)

APP_STATE = {
    "config_ok": True,
    "config_errors": [],
    "output_ok": True,
    "db_ok": None,
}


@app.on_event("startup")
def _startup_checks():
    try:
        root = Path(settings.output_dir)
        root.mkdir(parents=True, exist_ok=True)
        probe = root / ".write_probe"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink(missing_ok=True)
        APP_STATE["output_ok"] = True
    except Exception as exc:
        APP_STATE["output_ok"] = False
        APP_STATE["config_ok"] = False
        APP_STATE["config_errors"].append(f"output_root_not_writable: {exc}")

    try:
        db = check_db()
        APP_STATE["db_ok"] = bool(db.get("ok"))
    except Exception:
        APP_STATE["db_ok"] = False


@app.exception_handler(OperationalError)
async def sqlalchemy_operational_error_handler(request: Request, exc: OperationalError):
    return JSONResponse(
        status_code=503,
        content={
            "detail": "Database temporarily unavailable",
            "transient": is_transient_db_error(exc),
        },
    )


@app.exception_handler(DBAPIError)
async def sqlalchemy_dbapi_error_handler(request: Request, exc: DBAPIError):
    if is_transient_db_error(exc):
        return JSONResponse(
            status_code=503,
            content={"detail": "Database temporarily unavailable", "transient": True},
        )
    return JSONResponse(status_code=500, content={"detail": "Database error"})

use_playwright_default = settings.use_playwright_default
headless_mode = settings.headless


# -------------------------
# Health + Playwright check
# -------------------------

class Health(BaseModel):
    ok: bool
    use_playwright: bool
    headless: bool
    message: Optional[str] = None
    config_ok: Optional[bool] = None
    db_ok: Optional[bool] = None
    output_ok: Optional[bool] = None

@app.get("/health", response_model=Health)
async def health():
    config_ok = bool(APP_STATE.get("config_ok", True))
    output_ok = APP_STATE.get("output_ok", True) is not False
    db_ok = APP_STATE.get("db_ok", None) is not False
    ok = config_ok and output_ok and db_ok
    msg = "ready" if ok else "not_ready"
    return Health(
        ok=ok,
        use_playwright=use_playwright_default,
        headless=headless_mode,
        message=msg,
        config_ok=config_ok,
        db_ok=APP_STATE.get("db_ok", None),
        output_ok=APP_STATE.get("output_ok", True),
    )


@app.get("/health/db")
async def health_db():
    details = await run_in_threadpool(check_db)
    if not details.get("ok"):
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content=details,
        )
    return details


@app.get("/health/config")
def health_config():
    status_code = status.HTTP_200_OK
    if not APP_STATE["config_ok"]:
        status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    elif APP_STATE["db_ok"] is False:
        status_code = status.HTTP_503_SERVICE_UNAVAILABLE

    return JSONResponse(
        status_code=status_code,
        content={
            "config_ok": APP_STATE["config_ok"],
            "output_ok": APP_STATE["output_ok"],
            "db_ok": APP_STATE["db_ok"],
            "errors": APP_STATE["config_errors"],
        },
    )


@app.get("/playwright_check")
async def playwright_check():
    if not use_playwright_default:
        raise HTTPException(status_code=400, detail="Playwright disabled in .env")
    try:
        from playwright.async_api import async_playwright
        ua = None
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=headless_mode)
            page = await browser.new_page()
            await page.goto("https://httpbin.org/user-agent", wait_until="domcontentloaded")
            data = await page.content()
            await browser.close()
        import re, html
        text = html.unescape(data)
        m = re.search(r'\"user-agent\"\\s*:\\s*\"([^\"]+)\"', text)
        ua = m.group(1) if m else "unknown"
        return JSONResponse({"ok": True, "user_agent": ua})
    except Exception as e:
        logger.exception("Playwright check failed")
        raise HTTPException(status_code=500, detail=f"Playwright error: {e}")


# -------------------------
# Helpers
# -------------------------

def _resume_root(user_id: str, resume_id: str) -> Path:
    return settings.output_dir / user_id / settings.resumes_dir_name / resume_id


def _active_resume_for_user(db, user_id: uuid.UUID) -> Resume | None:
    return (
        db.query(Resume)
        .filter(Resume.user_id == user_id, Resume.is_active == True)  # noqa: E712
        .order_by(Resume.updated_at.desc())
        .first()
    )


def _write_resume_snapshot(db, user_id: uuid.UUID, run_root: Path) -> Path | None:
    resume = _active_resume_for_user(db, user_id)
    if not resume:
        return None
    parsed_payload = None
    if resume.parsed_json:
        try:
            parsed_payload = json.loads(resume.parsed_json)
        except json.JSONDecodeError:
            parsed_payload = None
    snapshot = {
        "resume_id": str(resume.id),
        "sha256": resume.sha256,
        "parsed_json": parsed_payload,
        "text_excerpt": (resume.text_content or "")[:12000],
        "filename": resume.filename,
        "mime_type": resume.mime_type,
        "created_at": resume.created_at.isoformat() if resume.created_at else None,
    }
    run_root.mkdir(parents=True, exist_ok=True)
    path = run_root / "resume_snapshot.json"
    path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _augment_with_potential_applications(status: dict) -> dict:
    """
    Adds:
      - metrics.potential_applications_count
      - artifacts.potential_applications_path
    Prefers summary.json if available; otherwise counts potential_applications/ dirs.
    """
    try:
        output_root = status.get("output_root")
        if not output_root:
            return status
        run_dir = Path(output_root)

        summary_path = status.get("summary_path") or str(run_dir / "summary.json")
        sp = Path(summary_path)
        if sp.exists():
            try:
                summary = json.loads(sp.read_text(encoding="utf-8"))
                pot_count = summary.get("potential_applications_count")
                pot_path = summary.get("potential_applications_path")
                if pot_count is not None:
                    status.setdefault("metrics", {})["potential_applications_count"] = int(pot_count)
                if pot_path:
                    status.setdefault("artifacts", {})["potential_applications_path"] = pot_path
                if pot_count is not None or pot_path:
                    return status
            except Exception:
                pass

        pot_dir = run_dir / "potential_applications"
        if pot_dir.exists() and pot_dir.is_dir():
            count = sum(1 for p in pot_dir.iterdir() if p.is_dir())
            status.setdefault("metrics", {})["potential_applications_count"] = count
            status.setdefault("artifacts", {})["potential_applications_path"] = str(pot_dir)
        else:
            status.setdefault("metrics", {})["potential_applications_count"] = 0
            status.setdefault("artifacts", {}).setdefault("potential_applications_path", None)
        return status
    except Exception:
        return status


class _TemporaryEnv:
    def __init__(self, updates: Dict[str, str]) -> None:
        self._updates = updates
        self._previous: Dict[str, str | None] = {}

    def __enter__(self):
        for key, value in self._updates.items():
            self._previous[key] = os.environ.get(key)
            os.environ[key] = value

    def __exit__(self, exc_type, exc, tb):
        for key, prior in self._previous.items():
            if prior is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = prior

def _filter_listings_by_cutoff(result: Dict[str, Any], cutoff_iso: Optional[str]) -> Dict[str, Any]:
    dt = parse_iso8601_utc(cutoff_iso)
    if not dt:
        return result
    jobs = result.get("jobs") or []
    if not jobs:
        return result
    filtered: List[Dict[str, Any]] = []
    for job in jobs:
        posted_iso = job.get("posted_iso")
        posted_dt = parse_iso8601_utc(posted_iso)
        if posted_dt and posted_dt < dt:
            continue
        filtered.append(job)
    if not filtered:
        return result
    result["jobs"] = filtered
    result["urls"] = [job.get("url") for job in filtered if job.get("url")]
    result["count"] = len(result["urls"])
    result["list_cutoff_iso"] = cutoff_iso
    return result


# -------------------------
# (Legacy) /search_stepstone
# -------------------------

@app.get("/search_stepstone")
async def search_stepstone(
    url: Optional[str] = Query(default=None, description="URL to visit; default StepStone EN homepage"),
    backend: Optional[str] = Query(default=None, description="Override backend: 'pw' or 'http'")
):
    try:
        query = {"url": url} if url else {}
        if backend not in (None, "pw", "http"):
            raise HTTPException(status_code=400, detail="backend must be 'pw' or 'http'")
        data = await ss_search(query, backend_override=backend)
        return JSONResponse(data)
    except Exception as e:
        logger.exception("search_stepstone error")
        raise HTTPException(status_code=500, detail=str(e))


# -------------------------
# NEW (L8): /search_stepstone_list
# -------------------------

@app.post("/search_stepstone_list", response_model=SearchStepstoneListResponse)
async def search_stepstone_list(req: SearchStepstoneListRequest) -> SearchStepstoneListResponse:
    stop_urls = [str(u) for u in (req.stop_urls or [])] or None
    if req.use_playwright:
        # Playwright path is already async
        result = await crawl_pw(
            str(req.seed_url),
            req.pages,
            req.delay_sec,
            req.include_titles_any,
            req.exclude_titles_any,
            req.max_jobs,
            req.max_pages or 80,
            stop_urls=stop_urls,
        )
    else:
        # Keep the fast HTTP path available
        result = await run_in_threadpool(
            crawl_http,
            str(req.seed_url),
            req.pages,
            req.delay_sec,
            req.include_titles_any,
            req.exclude_titles_any,
            req.max_jobs,
            req.max_pages or 80,
            stop_urls=stop_urls,
        )
    filtered = _filter_listings_by_cutoff(result, req.list_cutoff_iso)
    return SearchStepstoneListResponse(**filtered)


# --------------------------------------
# SINGLE: /job_details (enrich + scoring)
# --------------------------------------


@app.post("/job_details", response_model=JobDetailsResponse)
async def job_details(req: JobDetailsRequest) -> JobDetailsResponse:
    backend = req.backend or "auto"
    if backend not in ("auto", "pw", "http"):
        raise HTTPException(status_code=400, detail="backend must be 'pw', 'http', or 'auto'")

    active_focus = DEFAULT_FOCUS
    if req.profile_key:
        try:
            active_focus = get_focus_config(req.profile_key)
        except KeyError:
            logger.warning("Unknown profile_key '{}' provided; using DEFAULT_FOCUS", req.profile_key)
        except Exception:
            logger.exception("Failed to load focus profile '{}'; using DEFAULT_FOCUS", req.profile_key)

    try:
        result = await pipeline_fetch_job_details(
            str(req.url),
            backend=backend,
            enrich=bool(req.enrich),
            score=bool(req.score),
            cutoff_iso=req.cutoff_iso,
            focus=active_focus,
            use_llm_scoring=req.use_llm_scoring,
            apply_blocker_cap=req.apply_blocker_cap,
            use_cache=bool(req.use_cache),
        )
        if not result.get("backend"):
            default_backend = "pw" if use_playwright_default else "http"
            result["backend"] = backend if backend in ("pw", "http") else default_backend
        job_payload = result.get("job") or {}
        scoring_payload = result.get("scoring")
        fetch_meta_payload = result.get("fetch_meta")

        job_model = UnifiedJobPostingOut(**job_payload)
        scoring_model = ScoringResult(**scoring_payload) if isinstance(scoring_payload, dict) else None
        fetch_meta_model = FetchMetaSchema(**fetch_meta_payload) if isinstance(fetch_meta_payload, dict) else None

        return JobDetailsResponse(
            ok=bool(result.get("ok", True)),
            backend=result.get("backend"),
            job=job_model,
            scoring=scoring_model,
            fetch_meta=fetch_meta_model,
            cutoff_iso=result.get("cutoff_iso"),
            stale=bool(result.get("stale", False)),
            enrichment_meta=result.get("enrichment_meta"),
        )
    except RobotsDisallowedError as e:
        logger.warning("robots disallow {}: {}", req.url, e)
        raise HTTPException(status_code=451, detail=str(e))
    except FetchAccessDeniedError as e:
        logger.warning("access denied for {}: {}", req.url, e)
        raise HTTPException(status_code=429, detail=str(e))
    except FetchError as e:
        logger.warning("fetch failure for {}: {}", req.url, e)
        raise HTTPException(status_code=502, detail=f"Fetch failed: {e}")
    except Exception as e:
        logger.exception("job_details error")
        raise HTTPException(status_code=500, detail=f"Failed to parse job: {e}")


# -------------------------
# /bundle
# -------------------------


@app.post("/bundle", response_model=BundleResponse)
async def bundle(req: BundleRequest) -> BundleResponse:
    try:
        assets = generate_bundle(req.job, req.scoring)
        score_val = req.scoring.get("score") if isinstance(req.scoring, dict) else None
        llm_score = req.scoring.get("llm_score") if isinstance(req.scoring, dict) else None
        keep_threshold = settings.score_keep_threshold
        is_potential = (
            score_val is not None
            and score_val < keep_threshold
            and llm_score is not None
            and float(llm_score) >= float(keep_threshold)
        )
        if score_val is not None and score_val < keep_threshold and not is_potential:
            raise HTTPException(
                status_code=422,
                detail=f"Rejected: score {score_val} < keep_threshold {keep_threshold}",
            )
        out_dir = write_bundle(
            req.output_root,
            req.job,
            assets,
            req.scoring,
            category="potential_applications" if is_potential else None,
        )
        return BundleResponse(ok=True, output_dir=out_dir, files=list(assets.keys()))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Bundle failed: {e}")


# -------------------------
# NEW (L10): /aggregate_report
# -------------------------


@app.post("/aggregate_report", response_model=AggregateReportResponse)
async def aggregate_report(req: AggregateReportRequest) -> AggregateReportResponse:
    try:
        path = await run_in_threadpool(write_summary, req.reports, req.output_root)
        return AggregateReportResponse(ok=True, path=path)
    except Exception as e:
        logger.exception("aggregate_report error")
        raise HTTPException(status_code=500, detail=str(e))


# ---------- Profile API ----------

class ProfileListItem(BaseModel):
    key: str
    profile_name: str
    description: Optional[str] = None


class BatchSearchConfig(BaseModel):
    max_age_days: int = 4
    cutoff_iso: Optional[str] = None


class StartBatchRunRequest(BaseModel):
    profile_key: str
    search: BatchSearchConfig = BatchSearchConfig()
    use_llm_enrich: bool = True
    use_llm_scoring: bool = True
    apply_blocker_cap: bool = True
    seed_urls: Optional[List[str]] = None
    orchestrator: Literal["prefect_subprocess", "prefect_inprocess"] = "prefect_subprocess"


class BatchRunStatus(BaseModel):
    run_id: str
    status: str
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    profile_key: Optional[str] = None
    params: Dict[str, Any] = {}
    user_id: Optional[str] = None
    stage: Optional[str] = None
    output_root: Optional[str] = None
    artifacts: Dict[str, Optional[str]] = Field(default_factory=dict)
    return_codes: Dict[str, int] = Field(default_factory=dict)
    metrics: Dict[str, Any] = Field(default_factory=dict)
    summary_path: Optional[str] = None
    error: Optional[str] = None


class RunLogsResponse(BaseModel):
    ok: bool
    run_id: str
    chunk: str
    next_offset: int
    finished: bool


class RunSummaryResponse(BaseModel):
    ok: bool
    run_id: str
    summary_md: Optional[str] = None
    analysis_summary: Optional[Any] = None


class MeResponse(BaseModel):
    user_id: str
    email: str | None = None


class PotentialApplicationListItem(BaseModel):
    job_key: str
    final_score: Optional[float] = None
    llm_score: Optional[float] = None
    reason: Optional[str] = None
    title: Optional[str] = None
    company: Optional[str] = None
    location: Optional[str] = None
    url: Optional[str] = None


class PotentialApplicationsResponse(BaseModel):
    run_id: str
    count: int
    items: List[PotentialApplicationListItem]


class PotentialApplicationDetailResponse(BaseModel):
    run_id: str
    job_key: str
    job_json: Optional[Dict[str, Any]] = None
    metadata_json: Optional[Dict[str, Any]] = None
    reason_json: Optional[Dict[str, Any]] = None


class PruneUrlPoolRequest(BaseModel):
    max_urls: int = 300
    concurrency: int = 3
    timeout_sec: float = 12.0


PRUNE_URL_POOL_MAX_URLS_CAP = int(os.getenv("URL_POOL_PRUNE_MAX_URLS_CAP", "300"))
PRUNE_URL_POOL_CONCURRENCY_CAP = int(os.getenv("URL_POOL_PRUNE_CONCURRENCY_CAP", "3"))
PRUNE_URL_POOL_TIMEOUT_CAP = float(os.getenv("URL_POOL_PRUNE_TIMEOUT_CAP", "20"))


class MaintenanceRunResponse(BaseModel):
    run_id: str
    status: str


class MyProfileCreate(BaseModel):
    profile_key: str
    profile_name: Optional[str] = None
    description: Optional[str] = None
    focus_config_json: Dict[str, Any] | str = Field(default_factory=dict)


class MyProfileUpdate(BaseModel):
    profile_name: Optional[str] = None
    description: Optional[str] = None
    focus_config_json: Dict[str, Any] | str = Field(default_factory=dict)


def _profile_payload_from_db(prof) -> Dict[str, Any]:
    try:
        payload = json.loads(prof.focus_config_json or "{}")
    except Exception:
        payload = {}
    if isinstance(payload, dict):
        payload.setdefault("profile_key", prof.profile_key)
        payload.setdefault("profile_name", getattr(prof, "profile_name", None) or prof.profile_key)
        payload.setdefault("description", getattr(prof, "description", None))
        payload.setdefault("search_seeds", [])
    return payload if isinstance(payload, dict) else {}


def gui_login_redirect(request: Request) -> RedirectResponse:
    next_path = request.url.path
    if request.url.query:
        next_path += "?" + request.url.query
    return RedirectResponse(url=f"/gui/login?next={quote(next_path, safe='')}", status_code=303)


def _resolve_focus_profile_model_for_user(user_id: str, profile_key: str):
    """Resolve FocusProfileModel with precedence: user DB profile -> built-in profiles."""
    from app.pipeline.models import FocusProfileModel

    with db_session() as db:
        prof = get_profile_for_user(db, uuid.UUID(user_id), profile_key)
    if prof is not None:
        payload = _profile_payload_from_db(prof)
        return FocusProfileModel(**payload)

    data = profile_store.get_profile(profile_key)
    if not data:
        raise HTTPException(status_code=404, detail=f"Unknown profile '{profile_key}'")
    return FocusProfileModel(**data)


@app.get("/api/profiles", response_model=dict, dependencies=[Depends(get_current_user)])
def list_profiles():
    profiles = profile_store.load_profiles()
    items = [
        {
            "key": key,
            "profile_name": p.get("profile_name", key),
            "description": p.get("description"),
        }
        for key, p in profiles.items()
    ]
    return {"profiles": items}


@app.get("/api/my/profiles", response_model=dict, dependencies=[Depends(get_current_user)])
def list_my_profiles(user=Depends(get_current_user)):
    with db_session() as db:
        rows = list_profiles_for_user(db, user.id)
    items = []
    for p in rows:
        try:
            payload = json.loads(p.focus_config_json or "{}")
        except Exception:
            payload = {}
        items.append(
            {
                "key": p.profile_key,
                "profile_name": getattr(p, "profile_name", None) or payload.get("profile_name") or p.profile_key,
                "description": getattr(p, "description", None) or payload.get("description"),
            }
        )
    return {"profiles": items}


@app.get("/api/my/me", response_model=MeResponse, dependencies=[Depends(get_current_user)])
def get_my_me(user=Depends(get_current_user)):
    return MeResponse(user_id=str(user.id), email=getattr(user, "email", None))


@app.get("/api/my/profile/{key}", response_model=FocusProfileModel, dependencies=[Depends(get_current_user)])
def get_my_profile(key: str, user=Depends(get_current_user)):
    with db_session() as db:
        prof = get_profile_for_user(db, user.id, key)
    if not prof:
        raise HTTPException(status_code=404, detail="Profile not found")
    payload = _profile_payload_from_db(prof)
    return FocusProfileModel(**payload)


@app.get("/api/my/profile/{profile_key}/latest", response_model=dict, dependencies=[Depends(get_current_user)])
def get_my_profile_latest(profile_key: str, user=Depends(get_current_user)):
    path = run_manager.latest_path(str(user.id), profile_key)
    if not path.exists():
        raise HTTPException(status_code=404, detail="Latest run not found")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="Latest run data corrupted")


@app.post(
    "/api/my/profile/{profile_key}/url_pool/prune_stepstone",
    response_model=MaintenanceRunResponse,
    dependencies=[Depends(get_current_user)],
)
def prune_profile_url_pool_stepstone(
    profile_key: str,
    req: PruneUrlPoolRequest,
    background_tasks: BackgroundTasks,
    user=Depends(get_current_user),
):
    with db_session() as db:
        prof = get_profile_for_user(db, user.id, profile_key)
    if not prof:
        raise HTTPException(status_code=404, detail="Profile not found")

    user_id = str(user.id)
    run_id = run_manager.create_run_dir(user_id, profile_key)
    run_dir = run_manager.get_run_dir(user_id, profile_key, run_id)

    effective_max_urls = max(1, min(int(req.max_urls), PRUNE_URL_POOL_MAX_URLS_CAP))
    effective_concurrency = max(1, min(int(req.concurrency), PRUNE_URL_POOL_CONCURRENCY_CAP))
    effective_timeout = max(2.0, min(float(req.timeout_sec), PRUNE_URL_POOL_TIMEOUT_CAP))

    status = {
        "run_id": run_id,
        "status": "running",
        "started_at": run_manager._now_iso(),
        "finished_at": None,
        "profile_key": profile_key,
        "params": {
            "max_urls": effective_max_urls,
            "concurrency": effective_concurrency,
            "timeout_sec": effective_timeout,
            "requested": {
                "max_urls": req.max_urls,
                "concurrency": req.concurrency,
                "timeout_sec": req.timeout_sec,
            },
        },
        "summary_path": None,
        "error": None,
        "user_id": user_id,
        "stage": "prune_url_pool",
        "output_root": str(run_dir),
        "artifacts": {},
        "return_codes": {},
        "metrics": {},
    }
    run_manager.write_status(run_id, status)

    background_tasks.add_task(
        _run_prune_url_pool,
        run_id=run_id,
        user_id=user_id,
        profile_key=profile_key,
        max_urls=effective_max_urls,
        concurrency=effective_concurrency,
        timeout_sec=effective_timeout,
    )

    return MaintenanceRunResponse(run_id=run_id, status="running")


@app.post("/api/my/resume", response_model=ResumeUploadResponse, dependencies=[Depends(get_current_user)])
async def upload_resume(
    file: UploadFile = File(...),
    set_active: bool = Form(True),
    user=Depends(get_current_user),
):
    if not file.filename:
        raise HTTPException(status_code=400, detail="Missing filename")
    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Empty file")

    user_id = user.id
    user_id_str = str(user.id)
    digest = sha256_bytes(content)
    filename = safe_filename(file.filename)
    mime_type = file.content_type or "application/octet-stream"

    with db_session() as db:
        existing = (
            db.query(Resume)
            .filter(Resume.user_id == user_id, Resume.sha256 == digest)
            .first()
        )
        if existing:
            if set_active:
                db.query(Resume).filter(Resume.user_id == user_id).update(
                    {"is_active": False},
                    synchronize_session=False,
                )
                existing.is_active = True
                db.commit()
            return ResumeUploadResponse(
                resume_id=str(existing.id),
                is_active=bool(existing.is_active),
                filename=existing.filename,
                sha256=existing.sha256,
            )

        resume_id = uuid.uuid4()
        resume_dir = _resume_root(user_id_str, str(resume_id))
        ensure_dir(resume_dir)
        ext = Path(filename).suffix or ""
        original_path = resume_dir / f"original{ext}"
        original_path.write_bytes(content)

        text_content = None
        parsed_json = None
        try:
            parsed = parse_resume_file(original_path, mime_type=mime_type)
            text_content = parsed.get("text") or ""
            parsed_json = json.dumps(parsed.get("parsed") or {}, ensure_ascii=False, indent=2)
            (resume_dir / "resume.txt").write_text(text_content, encoding="utf-8")
            (resume_dir / "resume.parsed.json").write_text(parsed_json, encoding="utf-8")
        except Exception:
            pass

        if set_active:
            db.query(Resume).filter(Resume.user_id == user_id).update(
                {"is_active": False},
                synchronize_session=False,
            )

        row = Resume(
            id=resume_id,
            user_id=user_id,
            filename=filename,
            mime_type=mime_type,
            sha256=digest,
            storage_path=str(original_path),
            text_content=text_content,
            parsed_json=parsed_json,
            is_active=bool(set_active),
        )
        db.add(row)
        db.commit()

        return ResumeUploadResponse(
            resume_id=str(row.id),
            is_active=bool(row.is_active),
            filename=row.filename,
            sha256=row.sha256,
        )


@app.get("/api/my/resumes", response_model=List[ResumeListItem], dependencies=[Depends(get_current_user)])
def list_resumes(user=Depends(get_current_user)):
    with db_session() as db:
        rows = (
            db.query(Resume)
            .filter(Resume.user_id == user.id)
            .order_by(Resume.created_at.desc())
            .all()
        )
    return [
        ResumeListItem(
            id=str(r.id),
            filename=r.filename,
            created_at=r.created_at.isoformat() if r.created_at else None,
            is_active=bool(r.is_active),
        )
        for r in rows
    ]


@app.get("/api/my/resume/{resume_id}", response_model=ResumeDetailResponse, dependencies=[Depends(get_current_user)])
def get_resume_detail(resume_id: str, user=Depends(get_current_user)):
    try:
        resume_uuid = uuid.UUID(resume_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid resume_id")
    with db_session() as db:
        row = (
            db.query(Resume)
            .filter(Resume.user_id == user.id, Resume.id == resume_uuid)
            .first()
        )
        if not row:
            raise HTTPException(status_code=404, detail="Resume not found")

        parsed = None
        if row.parsed_json:
            try:
                parsed = json.loads(row.parsed_json)
            except json.JSONDecodeError:
                parsed = None

        excerpt = (row.text_content or "")[:12000] if row.text_content else None
        return ResumeDetailResponse(
            id=str(row.id),
            filename=row.filename,
            mime_type=row.mime_type,
            sha256=row.sha256,
            storage_path=row.storage_path,
            created_at=row.created_at.isoformat() if row.created_at else None,
            updated_at=row.updated_at.isoformat() if row.updated_at else None,
            is_active=bool(row.is_active),
            parsed_json=parsed,
            text_excerpt=excerpt,
        )


@app.post("/api/my/resume/{resume_id}/activate", response_model=dict, dependencies=[Depends(get_current_user)])
def activate_resume(resume_id: str, user=Depends(get_current_user)):
    try:
        resume_uuid = uuid.UUID(resume_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid resume_id")
    with db_session() as db:
        row = (
            db.query(Resume)
            .filter(Resume.user_id == user.id, Resume.id == resume_uuid)
            .first()
        )
        if not row:
            raise HTTPException(status_code=404, detail="Resume not found")
        db.query(Resume).filter(Resume.user_id == user.id).update(
            {"is_active": False},
            synchronize_session=False,
        )
        row.is_active = True
        db.commit()
    return {"ok": True, "resume_id": resume_id}


@app.post("/api/my/profile", response_model=FocusProfileModel, dependencies=[Depends(get_current_user)])
def upsert_my_profile(body: MyProfileCreate, response: Response, user=Depends(get_current_user)):
    with db_session() as db:
        existed = get_profile_for_user(db, user.id, body.profile_key) is not None

        prof = upsert_profile_for_user(
            db=db,
            user_id=user.id,
            profile_key=body.profile_key,
            profile_name=(body.profile_name or body.profile_key),
            description=body.description,
            profile_json=body.focus_config_json or {},
        )

        db.commit()
        db.refresh(prof)

        response.headers["X-Upsert-Action"] = "updated" if existed else "created"
        payload = _profile_payload_from_db(prof)
        return FocusProfileModel(**payload)


@app.post("/api/my/profile/{key}", response_model=FocusProfileModel, dependencies=[Depends(get_current_user)])
def upsert_my_profile_by_key(key: str, body: MyProfileUpdate, response: Response, user=Depends(get_current_user)):
    with db_session() as db:
        existed = get_profile_for_user(db, user.id, key) is not None

        prof = upsert_profile_for_user(
            db=db,
            user_id=user.id,
            profile_key=key,
            profile_name=body.profile_name or key,
            description=body.description,
            profile_json=body.focus_config_json or {},
        )
        db.commit()
        db.refresh(prof)
        response.headers["X-Upsert-Action"] = "updated" if existed else "created"
        payload = _profile_payload_from_db(prof)
        return FocusProfileModel(**payload)


@app.delete("/api/my/profile/{key}", response_model=dict, dependencies=[Depends(get_current_user)])
def delete_my_profile(key: str, user=Depends(get_current_user)):
    with db_session() as db:
        ok = delete_profile_for_user(db, user.id, key)
        db.commit()
    if not ok:
        raise HTTPException(status_code=404, detail="Profile not found")
    return {"ok": True}


@app.get("/api/profile/{key}", response_model=FocusProfileModel, dependencies=[Depends(get_current_user)])
def get_profile_api(key: str):
    data = profile_store.get_profile(key)
    if not data:
        raise HTTPException(status_code=404, detail="Profile not found")
    return FocusProfileModel(**data)


@app.post("/api/profile/{key}", response_model=FocusProfileModel, dependencies=[Depends(get_current_user)])
def upsert_profile_api(key: str, profile: FocusProfileModel):
    stored = profile_store.upsert_profile(key, profile.model_dump())
    return FocusProfileModel(**stored)


@app.delete("/api/profile/{key}", response_model=dict, dependencies=[Depends(get_current_user)])
def delete_profile_api(key: str):
    if not profile_store.get_profile(key):
        raise HTTPException(status_code=404, detail="Profile not found")
    profile_store.delete_profile(key)
    return {"ok": True}


@app.get("/gui/login", response_class=HTMLResponse)
def gui_login(request: Request, next: str = "/gui/run"):
    return templates.TemplateResponse("gui_login.html", {"request": request, "next": next})


@app.get("/gui/profiles", response_class=HTMLResponse)
def gui_profiles(request: Request):
    try:
        user = get_current_user(request, None)
    except HTTPException as e:
        if e.status_code == status.HTTP_401_UNAUTHORIZED:
            return gui_login_redirect(request)
        raise
    user_ctx = {"id": str(user.id), "email": user.email}
    return templates.TemplateResponse("gui_profiles.html", {"request": request, "user": user_ctx})


# ---------- Run single job ----------


def _compute_cutoff_iso(max_age_days: int) -> str:
    dt = datetime.now(timezone.utc) - timedelta(days=max_age_days)
    return dt.isoformat().replace("+00:00", "Z")


def _slugify(text: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", text.lower()).strip("-")
    return slug or "seed"


def _build_seeds_from_focus(focus) -> Optional[List[Dict[str, Any]]]:
    seeds = getattr(focus, "search_seeds", None) or []
    if not seeds:
        return None
    payload: List[Dict[str, Any]] = []
    for idx, raw in enumerate(seeds):
        if not raw:
            continue
        url = raw.strip()
        slug = ""
        if url.startswith("http://") or url.startswith("https://"):
            slug = _slugify(url.split("/")[-1] or f"seed-{idx+1}")
        else:
            slug = _slugify(url)
            url = f"https://www.stepstone.de/jobs/{slug}/"
        payload.append(
            {
                "slug": slug or f"seed-{idx+1}",
                "seed_url": url,
                "use_playwright": False,
                "delay_sec": 1.2,
                "max_pages": 80,
                "max_jobs": None,
                "exclude_titles_any": sorted(getattr(focus, "exclude_titles_any", [])),
            }
        )
    return payload or None


def _build_seeds_from_urls(seed_urls: List[str]) -> List[Dict[str, Any]]:
    payload: List[Dict[str, Any]] = []
    for idx, raw in enumerate(seed_urls):
        if not raw:
            continue
        url = raw.strip()
        if not url:
            continue
        slug = _slugify(url.split("/")[-1] or f"seed-{idx+1}")
        payload.append(
            {
                "slug": slug or f"seed-{idx+1}",
                "seed_url": url,
                "use_playwright": False,
                "delay_sec": 1.2,
                "max_pages": 80,
                "max_jobs": None,
            }
        )
    return payload


def _run_prefect_batch(
    run_id: str,
    user_id: str,
    profile_key: str,
    search_cfg: BatchSearchConfig,
    use_llm_enrich: bool,
    use_llm_scoring: bool,
    apply_blocker_cap: bool,
    focus_config_path: str | None = None,
    seeds_json_path: str | None = None,
) -> None:
    with run_ctx_scope(run_id=run_id, user_id=user_id, profile_key=profile_key):
        log_file = run_manager.log_path(run_id)
        cutoff_iso = search_cfg.cutoff_iso or _compute_cutoff_iso(search_cfg.max_age_days)
        run_root = run_manager.get_run_dir(user_id, profile_key, run_id)

        status = run_manager.load_status(run_id) or {}
        status.setdefault("run_id", run_id)
        status.setdefault("started_at", run_manager._now_iso())
        status.setdefault("finished_at", None)
        status.setdefault("profile_key", profile_key)
        status.setdefault("summary_path", None)
        status.setdefault("error", None)
        status.setdefault("artifacts", {})
        status.setdefault("return_codes", {})
        status.setdefault("metrics", {})
        status["params"] = {
            "max_age_days": search_cfg.max_age_days,
            "cutoff_iso": cutoff_iso,
            "use_llm_enrich": use_llm_enrich,
            "use_llm_scoring": use_llm_scoring,
            "apply_blocker_cap": apply_blocker_cap,
        }
        status.update(
            {
                "status": "running",
                "user_id": user_id,
                "stage": "starting",
                "output_root": str(run_root),
            }
        )
        run_manager.write_status(run_id, status)

        env = os.environ.copy()
        env["JOBAGENT_FOCUS_PROFILE"] = profile_key
        env["JOBAGENT_PROFILE_KEY"] = profile_key
        env["JOBAGENT_RUN_ID"] = run_id
        env["JOBAGENT_OUTPUT_ROOT"] = str(run_root)
        resume_snapshot = run_root / "resume_snapshot.json"
        if resume_snapshot.exists():
            env["JOBAGENT_RESUME_SNAPSHOT"] = str(resume_snapshot)
        if focus_config_path:
            env["JOBAGENT_FOCUS_CONFIG_PATH"] = focus_config_path
        if seeds_json_path:
            env["JOBAGENT_STEPSTONE_SEEDS_JSON_PATH"] = seeds_json_path
        env["JOBAGENT_USE_LLM_ENRICH"] = "true" if use_llm_enrich else "false"
        env["JOBAGENT_USE_LLM_SCORING"] = "true" if use_llm_scoring else "false"
        env["JOBAGENT_APPLY_BLOCKER_CAP"] = "true" if apply_blocker_cap else "false"
        if getattr(settings, "openai_model", None):
            env["JOBAGENT_OPENAI_MODEL_ENRICH"] = getattr(settings, "openai_model", "")
        if getattr(settings, "openai_model_scoring", None):
            env["JOBAGENT_OPENAI_MODEL_SCORING"] = getattr(settings, "openai_model_scoring", "")

    # Override seeds per profile, if provided
        try:
            focus = get_focus_config(profile_key)
            seeds_payload = _build_seeds_from_focus(focus)
            if seeds_payload:
                env["JOBAGENT_STEPSTONE" "_SEEDS_JSON"] = json.dumps(seeds_payload, ensure_ascii=False)
        except Exception:
            pass

        process_cmd = [
            "python",
            "-m",
            "app.prefect_run",
            "process",
            f"--cutoff-iso={cutoff_iso}",
            "--backend=auto",
            f"--profile-key={profile_key}",
            "--use-llm-scoring" if use_llm_scoring else "--no-use-llm-scoring",
            "--apply-blocker-cap" if apply_blocker_cap else "--no-apply-blocker-cap",
            f"--run-id={run_id}",
        ]

        commands = [
            [
                "python",
                "-m",
                "app.prefect_run",
                "crawl",
                f"--list-max-age-days={search_cfg.max_age_days}",
                f"--run-id={run_id}",
            ],
            process_cmd,
        ]

        overall_ok = True
        error_msg = None
        return_codes = status.get("return_codes") or {}

        try:
            with log_file.open("a", encoding="utf-8") as log:
                for idx, cmd in enumerate(commands):
                    stage = "crawl" if idx == 0 else "process"
                    status["stage"] = stage
                    status["return_codes"] = return_codes
                    run_manager.write_status(run_id, status)

                    log.write(f"$ {' '.join(cmd)}\n")
                    log.flush()
                    try:
                        proc = subprocess.run(
                            cmd,
                            cwd=None,
                            env=env,
                            stdout=log,
                            stderr=log,
                            text=True,
                        )
                    except Exception as exc:
                        overall_ok = False
                        error_msg = f"Subprocess error: {exc}"
                        break

                    return_codes[stage] = proc.returncode
                    status["return_codes"] = return_codes
                    run_manager.write_status(run_id, status)
                    if proc.returncode != 0:
                        overall_ok = False
                        error_msg = f"Command {' '.join(cmd)} exited with {proc.returncode}"
                        break

                    if stage == "process":
                        report_md = run_root / "REPORT_SUMMARY.md"
                        analysis_json = run_root / "analysis_summary.json"
                        metrics_json = run_root / "run_metrics.json"
                        artifacts = status.get("artifacts") or {}
                        if report_md.exists():
                            status["summary_path"] = str(report_md)
                            artifacts["report_summary_md"] = str(report_md)
                        if analysis_json.exists():
                            artifacts["analysis_summary_json"] = str(analysis_json)
                        if metrics_json.exists():
                            artifacts["run_metrics_json"] = str(metrics_json)
                            try:
                                status["metrics"] = json.loads(metrics_json.read_text(encoding="utf-8"))
                            except json.JSONDecodeError:
                                status["metrics"] = {}
                        status["artifacts"] = artifacts
                        run_manager.write_status(run_id, status)
        except Exception as exc:
            overall_ok = False
            error_msg = f"Unexpected error: {exc}"
        finally:
            status["finished_at"] = run_manager._now_iso()
            if overall_ok:
                status["status"] = "completed"
                status["stage"] = "completed"
                status["error"] = None
            else:
                status["status"] = "failed"
                status["stage"] = "failed"
                status["error"] = error_msg
            run_manager.write_status(run_id, status)
            run_manager.write_latest(
                user_id,
                profile_key,
                {
                    "run_id": run_id,
                    "status": status.get("status"),
                    "started_at": status.get("started_at"),
                    "finished_at": status.get("finished_at"),
                    "output_root": status.get("output_root"),
                    "summary_path": status.get("summary_path"),
                    "artifacts": status.get("artifacts") or {},
                },
            )


def _run_prefect_inprocess_batch(
    run_id: str,
    user_id: str,
    profile_key: str,
    search_cfg: BatchSearchConfig,
    use_llm_enrich: bool,
    use_llm_scoring: bool,
    apply_blocker_cap: bool,
    focus_config_path: str | None = None,
    seeds_json_path: str | None = None,
) -> None:
    with run_ctx_scope(run_id=run_id, user_id=user_id, profile_key=profile_key):
        from app.prefect_run import SeedConfig, crawl_and_save_flow, process_run_flow

        log_file = run_manager.log_path(run_id)
        cutoff_iso = search_cfg.cutoff_iso or _compute_cutoff_iso(search_cfg.max_age_days)
        run_root = run_manager.get_run_dir(user_id, profile_key, run_id)

        status = run_manager.load_status(run_id) or {}
        status.setdefault("run_id", run_id)
        status.setdefault("started_at", run_manager._now_iso())
        status.setdefault("finished_at", None)
        status.setdefault("profile_key", profile_key)
        status.setdefault("summary_path", None)
        status.setdefault("error", None)
        status.setdefault("artifacts", {})
        status.setdefault("return_codes", {})
        status.setdefault("metrics", {})
        status["params"] = {
            "max_age_days": search_cfg.max_age_days,
            "cutoff_iso": cutoff_iso,
            "use_llm_enrich": use_llm_enrich,
            "use_llm_scoring": use_llm_scoring,
            "apply_blocker_cap": apply_blocker_cap,
            "orchestrator": "prefect_inprocess",
        }
        status.update(
            {
                "status": "running",
                "user_id": user_id,
                "stage": "starting",
                "output_root": str(run_root),
            }
        )
        run_manager.write_status(run_id, status)

        env_updates = {
            "JOBAGENT_FOCUS_PROFILE": profile_key,
            "JOBAGENT_PROFILE_KEY": profile_key,
            "JOBAGENT_RUN_ID": run_id,
            "JOBAGENT_OUTPUT_ROOT": str(run_root),
            "JOBAGENT_USE_LLM_ENRICH": "true" if use_llm_enrich else "false",
            "JOBAGENT_USE_LLM_SCORING": "true" if use_llm_scoring else "false",
            "JOBAGENT_APPLY_BLOCKER_CAP": "true" if apply_blocker_cap else "false",
        }
        if focus_config_path:
            env_updates["JOBAGENT_FOCUS_CONFIG_PATH"] = focus_config_path
        if seeds_json_path:
            env_updates["JOBAGENT_STEPSTONE_SEEDS_JSON_PATH"] = seeds_json_path
        resume_snapshot = run_root / "resume_snapshot.json"
        if resume_snapshot.exists():
            env_updates["JOBAGENT_RESUME_SNAPSHOT"] = str(resume_snapshot)

        handler = logging.FileHandler(log_file, encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
        root_logger = logging.getLogger()
        root_logger.addHandler(handler)
        prefect_logger = logging.getLogger("prefect")
        prefect_logger.addHandler(handler)

        overall_ok = True
        error_msg = None
        return_codes = status.get("return_codes") or {}

        try:
            with log_file.open("a", encoding="utf-8") as log:
                log.write("$ inprocess prefect flow\n")
                log.flush()

            seeds = None
            if seeds_json_path:
                try:
                    payload = json.loads(Path(seeds_json_path).read_text(encoding="utf-8"))
                    seeds = [SeedConfig(**item) for item in payload]
                except Exception:
                    seeds = None
            if seeds is None:
                try:
                    focus = get_focus_config(profile_key)
                    seeds_payload = _build_seeds_from_focus(focus)
                    if seeds_payload:
                        seeds = [SeedConfig(**item) for item in seeds_payload]
                except Exception:
                    seeds = None

            with _TemporaryEnv(env_updates):
                status["stage"] = "crawl"
                run_manager.write_status(run_id, status)
                crawl_and_save_flow(seeds=seeds, list_cutoff_iso=cutoff_iso, run_id=run_id)
                return_codes["crawl"] = 0
                status["return_codes"] = return_codes
                run_manager.write_status(run_id, status)

                status["stage"] = "process"
                run_manager.write_status(run_id, status)
                process_run_flow(
                    cutoff_iso=cutoff_iso,
                    profile_key=profile_key,
                    backend="auto",
                    use_llm_scoring=use_llm_scoring,
                    apply_blocker_cap=apply_blocker_cap,
                    run_id=run_id,
                )
                return_codes["process"] = 0
                status["return_codes"] = return_codes
                run_manager.write_status(run_id, status)

                report_md = run_root / "REPORT_SUMMARY.md"
                analysis_json = run_root / "analysis_summary.json"
                metrics_json = run_root / "run_metrics.json"
                artifacts = status.get("artifacts") or {}
                if report_md.exists():
                    status["summary_path"] = str(report_md)
                    artifacts["report_summary_md"] = str(report_md)
                if analysis_json.exists():
                    artifacts["analysis_summary_json"] = str(analysis_json)
                if metrics_json.exists():
                    artifacts["run_metrics_json"] = str(metrics_json)
                    try:
                        status["metrics"] = json.loads(metrics_json.read_text(encoding="utf-8"))
                    except json.JSONDecodeError:
                        status["metrics"] = {}
                status["artifacts"] = artifacts
                run_manager.write_status(run_id, status)
        except Exception as exc:
            overall_ok = False
            error_msg = f"In-process error: {exc}"
            if status.get("stage") == "crawl":
                return_codes["crawl"] = 1
            elif status.get("stage") == "process":
                return_codes["process"] = 1
        finally:
            status["finished_at"] = run_manager._now_iso()
            if overall_ok:
                status["status"] = "completed"
                status["stage"] = "completed"
                status["error"] = None
            else:
                status["status"] = "failed"
                status["stage"] = "failed"
                status["error"] = error_msg
            status["return_codes"] = return_codes
            run_manager.write_status(run_id, status)
            run_manager.write_latest(
                user_id,
                profile_key,
                {
                    "run_id": run_id,
                    "status": status.get("status"),
                    "started_at": status.get("started_at"),
                    "finished_at": status.get("finished_at"),
                    "output_root": status.get("output_root"),
                    "summary_path": status.get("summary_path"),
                    "artifacts": status.get("artifacts") or {},
                },
            )
            root_logger.removeHandler(handler)
            prefect_logger.removeHandler(handler)
            handler.close()


def _run_prune_url_pool(
    run_id: str,
    user_id: str,
    profile_key: str,
    max_urls: int,
    concurrency: int,
    timeout_sec: float,
) -> None:
    with run_ctx_scope(run_id=run_id, user_id=user_id, profile_key=profile_key):
        log_file = run_manager.log_path(run_id)
        run_root = run_manager.get_run_dir(user_id, profile_key, run_id)
        profile_dir = run_root.parent

        status = run_manager.load_status(run_id) or {}
        status.update(
            {
                "run_id": run_id,
                "status": "running",
                "started_at": run_manager._now_iso(),
                "finished_at": None,
                "profile_key": profile_key,
                "user_id": user_id,
                "stage": "prune_url_pool",
                "output_root": str(run_root),
                "metrics": {},
            }
        )
        run_manager.write_status(run_id, status)

    class _LogSink:
        def __init__(self, handle):
            self.handle = handle

        def _write(self, level: str, message: str) -> None:
            self.handle.write(f"[{level}] {message}\n")
            self.handle.flush()

        def info(self, message: str) -> None:
            self._write("info", message)

        def warning(self, message: str) -> None:
            self._write("warn", message)

        def error(self, message: str) -> None:
            self._write("error", message)

        try:
            with log_file.open("a", encoding="utf-8") as log:
                logger_sink = _LogSink(log)
                log.write("$ prune_url_pool_stepstone\n")
                log.flush()
                metrics = prune_unavailable_stepstone_urls(
                    profile_dir,
                    max_urls=max_urls,
                    concurrency=concurrency,
                    timeout=timeout_sec,
                    logger=logger_sink,
                    run_id=run_id,
                )
            status["metrics"] = metrics
            metrics_path = run_root / "run_metrics.json"
            metrics_path.write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")
            artifacts = status.get("artifacts") or {}
            artifacts["run_metrics_json"] = str(metrics_path)
            artifacts["url_pool_jsonl"] = str(profile_dir / "url_pool.jsonl")
            artifacts["url_pool_unavailable_jsonl"] = str(profile_dir / "url_pool_unavailable.jsonl")
            status["artifacts"] = artifacts
            status["status"] = "completed"
            status["finished_at"] = run_manager._now_iso()
            status["error"] = None
        except Exception as exc:
            status["status"] = "failed"
            status["finished_at"] = run_manager._now_iso()
            status["error"] = f"Prune failed: {exc}"
        finally:
            run_manager.write_status(run_id, status)


@app.post("/api/run_single", response_model=RunSingleResponse, dependencies=[Depends(get_current_user)])
async def run_single(req: RunSingleRequest, user=Depends(get_current_user)) -> RunSingleResponse:
    with db_session() as db:
        profile_model = get_focus_profile_model_for_user(db, user.id, req.profile_key)
    if not profile_model:
        raise HTTPException(status_code=404, detail="Profile not found")
    focus = FocusConfig.from_profile(profile_model)

    backend = req.backend or "auto"
    if backend not in ("auto", "pw", "http"):
        raise HTTPException(status_code=400, detail="backend must be 'pw', 'http', or 'auto'")

    user_id_str = str(user.id)
    run_id = run_manager.create_run_dir(user_id_str, req.profile_key)
    run_root = run_manager.get_run_dir(user_id_str, req.profile_key, run_id)
    resume_snapshot_path = None
    with db_session() as db:
        resume_snapshot_path = _write_resume_snapshot(db, user.id, run_root)

    env_updates = {}
    if resume_snapshot_path:
        env_updates["JOBAGENT_RESUME_SNAPSHOT"] = str(resume_snapshot_path)

    with _TemporaryEnv(env_updates):
        result = await pipeline_fetch_job_details(
            url=str(req.url),
            backend=backend,
            enrich=req.enrich,
            score=True,
            cutoff_iso=req.cutoff_iso,
            focus=focus,
            use_llm_scoring=req.use_llm_scoring,
            apply_blocker_cap=req.apply_blocker_cap,
            use_cache=False,
        )

    if not result.get("ok", False):
        raise HTTPException(status_code=502, detail="Job processing failed")

    job_payload = result.get("job") or {}
    scoring_payload = result.get("scoring")
    fetch_meta_payload = result.get("fetch_meta")

    job_model = UnifiedJobPostingOut(**job_payload)
    scoring_model = ScoringResult(**scoring_payload) if isinstance(scoring_payload, dict) else None
    fetch_meta_model = FetchMetaSchema(**fetch_meta_payload) if isinstance(fetch_meta_payload, dict) else None

    job_details = JobDetailsResponse(
        ok=result.get("ok", True),
        backend=result.get("backend"),
        job=job_model,
        scoring=scoring_model,
        fetch_meta=fetch_meta_model,
        cutoff_iso=result.get("cutoff_iso"),
        stale=bool(result.get("stale", False)),
        enrichment_meta=result.get("enrichment_meta"),
    )

    return RunSingleResponse(
        ok=True,
        profile_key=req.profile_key,
        details=job_details,
    )


@app.post("/api/start_batch_run", response_model=BatchRunStatus, dependencies=[Depends(get_current_user)])
def start_batch_run(req: StartBatchRunRequest, background_tasks: BackgroundTasks, user=Depends(get_current_user)):
    with db_session() as db:
        profile_model = get_focus_profile_model_for_user(db, user.id, req.profile_key)
    if not profile_model:
        raise HTTPException(status_code=404, detail="Profile not found")

    user_id = str(user.id)
    run_id = run_manager.create_run_dir(user_id, req.profile_key)
    run_dir = run_manager.get_run_dir(user_id, req.profile_key, run_id)
    focus_override_path = run_dir / "focus_profile_override.json"
    focus_override_payload = {"profile_key": req.profile_key, **profile_model.model_dump()}
    focus_override_path.write_text(json.dumps(focus_override_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    seeds_json_path = None
    if req.seed_urls:
        seeds_payload = _build_seeds_from_urls(req.seed_urls)
        if seeds_payload:
            seeds_json_path = run_dir / "seed_override.json"
            seeds_json_path.write_text(json.dumps(seeds_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    if req.orchestrator == "prefect_inprocess":
        background_tasks.add_task(
            _run_prefect_inprocess_batch,
            run_id=run_id,
            user_id=str(user.id),
            profile_key=req.profile_key,
            search_cfg=req.search,
            use_llm_enrich=req.use_llm_enrich,
            use_llm_scoring=req.use_llm_scoring,
            apply_blocker_cap=req.apply_blocker_cap,
            focus_config_path=str(focus_override_path),
            seeds_json_path=str(seeds_json_path) if seeds_json_path else None,
        )
    else:
        background_tasks.add_task(
            _run_prefect_batch,
            run_id=run_id,
            user_id=str(user.id),
            profile_key=req.profile_key,
            search_cfg=req.search,
            use_llm_enrich=req.use_llm_enrich,
            use_llm_scoring=req.use_llm_scoring,
            apply_blocker_cap=req.apply_blocker_cap,
            focus_config_path=str(focus_override_path),
            seeds_json_path=str(seeds_json_path) if seeds_json_path else None,
        )

    resume_snapshot_path = None
    with db_session() as db:
        resume_snapshot_path = _write_resume_snapshot(db, user.id, run_dir)

    cutoff_iso = req.search.cutoff_iso or _compute_cutoff_iso(req.search.max_age_days)
    status = {
        "run_id": run_id,
        "status": "running",
        "started_at": run_manager._now_iso(),
        "finished_at": None,
        "profile_key": req.profile_key,
        "params": {
            **req.search.model_dump(),
            "cutoff_iso": cutoff_iso,
            "use_llm_enrich": req.use_llm_enrich,
            "use_llm_scoring": req.use_llm_scoring,
            "apply_blocker_cap": req.apply_blocker_cap,
            "orchestrator": req.orchestrator,
        },
        "summary_path": None,
        "error": None,
        "user_id": str(user.id),
        "stage": "queued",
        "output_root": str(run_dir),
        "artifacts": {},
        "return_codes": {},
        "metrics": {},
    }
    if resume_snapshot_path:
        status["artifacts"]["resume_snapshot_json"] = str(resume_snapshot_path)
    run_manager.write_status(run_id, status)
    run_manager.write_latest(
        user_id,
        req.profile_key,
        {
            "run_id": run_id,
            "status": status.get("status"),
            "started_at": status.get("started_at"),
            "finished_at": status.get("finished_at"),
            "output_root": status.get("output_root"),
            "summary_path": status.get("summary_path"),
            "artifacts": status.get("artifacts") or {},
        },
    )
    return BatchRunStatus(**status)


@app.get("/api/run_status/{run_id}", response_model=BatchRunStatus, dependencies=[Depends(get_current_user)])
def get_run_status(run_id: str, user=Depends(get_current_user)):
    status = run_manager.load_status(run_id)
    if not status:
        raise HTTPException(status_code=404, detail="Run not found")
    if status.get("user_id") != str(user.id):
        raise HTTPException(status_code=404, detail="Run not found")
    status = _augment_with_potential_applications(status)
    artifacts = status.get("artifacts") or {}
    if artifacts.get("potential_applications_path") is None:
        artifacts.pop("potential_applications_path", None)
    status["artifacts"] = artifacts
    return BatchRunStatus(**status)


@app.get("/api/run_logs/{run_id}", response_model=RunLogsResponse, dependencies=[Depends(get_current_user)])
def get_run_logs(
    run_id: str,
    offset: int = 0,
    max_bytes: int = 4096,
    user=Depends(get_current_user),
):
    status = run_manager.load_status(run_id)
    if status is None:
        raise HTTPException(status_code=404, detail="Run not found")
    if status.get("user_id") != str(user.id):
        raise HTTPException(status_code=404, detail="Run not found")

    offset = max(0, int(offset or 0))
    max_bytes = int(max_bytes or 0)
    if max_bytes <= 0:
        max_bytes = 4096
    max_bytes = min(max_bytes, 64 * 1024)

    chunk, new_offset = run_manager.read_log_chunk(run_id, offset=offset, max_bytes=max_bytes)
    finished = status.get("status") in ("completed", "failed")

    return RunLogsResponse(
        ok=True,
        run_id=run_id,
        chunk=chunk,
        next_offset=new_offset,
        finished=finished and (chunk == ""),
    )


@app.get("/api/run_summary/{run_id}", response_model=RunSummaryResponse, dependencies=[Depends(get_current_user)])
def get_run_summary(run_id: str, user=Depends(get_current_user)):
    status = run_manager.load_status(run_id)
    if not status:
        raise HTTPException(status_code=404, detail="Run not found")
    if status.get("user_id") != str(user.id):
        raise HTTPException(status_code=404, detail="Run not found")

    artifacts = status.get("artifacts") or {}
    summary_path = artifacts.get("report_summary_md") or status.get("summary_path")
    analysis_path = artifacts.get("analysis_summary_json")

    summary_md = None
    if summary_path and Path(summary_path).exists():
        data = Path(summary_path).read_text(encoding="utf-8", errors="replace")
        summary_md = data[:200_000]

    analysis_summary = None
    if analysis_path and Path(analysis_path).exists():
        raw = Path(analysis_path).read_text(encoding="utf-8", errors="replace")
        analysis_summary = json.loads(raw)

    return RunSummaryResponse(
        ok=True,
        run_id=run_id,
        summary_md=summary_md,
        analysis_summary=analysis_summary,
    )

# -------------------------
# Run artifacts: potential applications
# -------------------------

_SAFE_JOBKEY_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._-]{0,200}$")


def _safe_job_key(job_key: str) -> str:
    if not job_key or not _SAFE_JOBKEY_RE.match(job_key) or ".." in job_key or "/" in job_key or "\\" in job_key:
        raise HTTPException(status_code=400, detail="Invalid job_key")
    return job_key


def _read_json_file(path: Path) -> Optional[Dict[str, Any]]:
    try:
        if not path.exists():
            return None
        raw = path.read_text(encoding="utf-8", errors="replace")
        return json.loads(raw)
    except Exception:
        return None


def _pick_first_json(paths: List[Path]) -> Optional[Dict[str, Any]]:
    for path in paths:
        obj = _read_json_file(path)
        if isinstance(obj, dict):
            return obj
    return None


def _coerce_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def _extract_best_effort_fields(
    job: Optional[Dict[str, Any]],
    meta: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    job = job or {}
    meta = meta or {}
    title = job.get("title") or job.get("job_title") or meta.get("title") or meta.get("job_title")
    company = job.get("company") or meta.get("company") or job.get("employer") or meta.get("employer")
    location = job.get("location") or meta.get("location") or job.get("city") or meta.get("city")
    url = job.get("url") or meta.get("url")
    return {"title": title, "company": company, "location": location, "url": url}

@app.get(
    "/api/run_artifacts/{run_id}/potential_applications",
    response_model=PotentialApplicationsResponse,
    dependencies=[Depends(get_current_user)],
)
def list_potential_applications(
    run_id: str,
    limit: int = Query(200, ge=1, le=1000),
    user=Depends(get_current_user),
):
    status = run_manager.load_status(run_id)
    if not status or status.get("user_id") != str(user.id):
        raise HTTPException(status_code=404, detail="Run not found")

    output_root = status.get("output_root")
    if not output_root:
        return PotentialApplicationsResponse(run_id=run_id, count=0, items=[])

    run_dir = Path(output_root)
    pot_dir = run_dir / "potential_applications"
    if not pot_dir.exists() or not pot_dir.is_dir():
        return PotentialApplicationsResponse(run_id=run_id, count=0, items=[])

    items: List[PotentialApplicationListItem] = []
    for child in sorted([p for p in pot_dir.iterdir() if p.is_dir()], key=lambda p: p.name):
        if len(items) >= int(limit):
            break
        job_key = child.name
        if not _SAFE_JOBKEY_RE.match(job_key) or ".." in job_key or "/" in job_key or "\\" in job_key:
            continue

        reason = _read_json_file(child / "potential_reason.json")
        job_obj = _pick_first_json(
            [child / "job.json", child / "job_details.json", child / "job_posting.json"]
        )
        meta_obj = _pick_first_json([child / "metadata.json", child / "meta.json"])

        final_score = _coerce_float(
            (reason or {}).get("final_score")
            or (job_obj or {}).get("final_score")
            or (job_obj or {}).get("fit_score")
        )
        llm_score = _coerce_float(
            (reason or {}).get("llm_score")
            or (job_obj or {}).get("llm_score")
            or (job_obj or {}).get("llm_fit_score")
        )
        reason_text = None
        if isinstance(reason, dict):
            reason_text = reason.get("reason") or reason.get("note") or reason.get("explanation")

        fields = _extract_best_effort_fields(job_obj, meta_obj)
        items.append(
            PotentialApplicationListItem(
                job_key=job_key,
                final_score=final_score,
                llm_score=llm_score,
                reason=reason_text or "final<70 and llm>70",
                title=fields.get("title"),
                company=fields.get("company"),
                location=fields.get("location"),
                url=fields.get("url"),
            )
        )

    return PotentialApplicationsResponse(run_id=run_id, count=len(items), items=items)


@app.get(
    "/api/run_artifacts/{run_id}/potential_applications/{job_key}",
    response_model=PotentialApplicationDetailResponse,
    dependencies=[Depends(get_current_user)],
)
def get_potential_application_detail(
    run_id: str,
    job_key: str,
    user=Depends(get_current_user),
):
    job_key = _safe_job_key(job_key)
    status = run_manager.load_status(run_id)
    if not status or status.get("user_id") != str(user.id):
        raise HTTPException(status_code=404, detail="Run not found")

    output_root = status.get("output_root")
    if not output_root:
        raise HTTPException(status_code=404, detail="Not found")

    run_dir = Path(output_root)
    child = run_dir / "potential_applications" / job_key
    if not child.exists() or not child.is_dir():
        raise HTTPException(status_code=404, detail="Not found")

    reason = _read_json_file(child / "potential_reason.json")
    job_obj = _pick_first_json([child / "job.json", child / "job_details.json", child / "job_posting.json"])
    meta_obj = _pick_first_json([child / "metadata.json", child / "meta.json"])

    return PotentialApplicationDetailResponse(
        run_id=run_id,
        job_key=job_key,
        job_json=job_obj,
        metadata_json=meta_obj,
        reason_json=reason,
    )


@app.get("/gui/run", response_class=HTMLResponse)
def gui_run(request: Request):
    try:
        user = get_current_user(request, None)
    except HTTPException as e:
        if e.status_code == status.HTTP_401_UNAUTHORIZED:
            return gui_login_redirect(request)
        raise
    user_ctx = {"id": str(user.id), "email": user.email}
    return templates.TemplateResponse("gui_run.html", {"request": request, "user": user_ctx})


@app.get("/gui/logout")
def gui_logout():
    resp = RedirectResponse(url="/gui/login", status_code=303)
    resp.delete_cookie(AUTH_COOKIE_NAME, path="/")
    return resp


# -------------------------
# Run state persistence
# -------------------------

class RunState(BaseModel):
    last_run: Optional[str] = None
    run_dir: Optional[str] = None


@app.get("/run_state", response_model=RunState)
async def get_run_state():
    try:
        data = load_state()
        return RunState(**data)
    except Exception as exc:
        logger.exception("Failed to load run_state")
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/run_state", response_model=RunState)
async def set_run_state(state: RunState):
    try:
        stored = save_state(state.model_dump())
        return RunState(**stored)
    except Exception as exc:
        logger.exception("Failed to persist run_state")
        raise HTTPException(status_code=500, detail=str(exc))
