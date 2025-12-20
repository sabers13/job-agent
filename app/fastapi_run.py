from __future__ import annotations

import json
import os
import re
import subprocess
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Query, Request, status
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse, HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from loguru import logger
from pydantic import BaseModel

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
    list_profiles_for_user,
    update_profile_for_user,
)
from app.db.session import db_session
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
)
from app.api.auth_routes import router as auth_router
from app.db.health import check_db
from .pipeline.templating import generate_bundle
from .pipeline.output import write_bundle, write_summary
from .pipeline.models import UnifiedJobPosting, FocusProfileModel
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
load_dotenv()

app = FastAPI(title="Job Fetching Agent (DE · Junior · EN-friendly)", version="0.3.1")
templates = Jinja2Templates(directory="templates")
app.include_router(auth_router)

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

@app.get("/health", response_model=Health)
async def health():
    return Health(ok=True, use_playwright=use_playwright_default, headless=headless_mode, message="ready")


@app.get("/health/db")
async def health_db():
    ok = await run_in_threadpool(check_db)
    return {"ok": bool(ok)}


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
        out_dir = write_bundle(req.output_root, req.job, assets, req.scoring)
        return BundleResponse(ok=True, output_dir=out_dir, files=list(assets.keys()))
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


class BatchRunStatus(BaseModel):
    run_id: str
    status: str
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    profile_key: Optional[str] = None
    params: Dict[str, Any] = {}
    summary_path: Optional[str] = None
    error: Optional[str] = None


class RunLogsResponse(BaseModel):
    ok: bool
    run_id: str
    chunk: str
    next_offset: int
    finished: bool


class MyProfileCreate(BaseModel):
    profile_key: str
    profile_name: str
    description: Optional[str] = None
    focus_config_json: Dict[str, Any] | str = {}


class MyProfileUpdate(BaseModel):
    profile_name: Optional[str] = None
    description: Optional[str] = None
    focus_config_json: Dict[str, Any] | str = {}


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


@app.get("/api/my/profile/{key}", response_model=FocusProfileModel, dependencies=[Depends(get_current_user)])
def get_my_profile(key: str, user=Depends(get_current_user)):
    with db_session() as db:
        prof = get_profile_for_user(db, user.id, key)
    if not prof:
        raise HTTPException(status_code=404, detail="Profile not found")
    payload = _profile_payload_from_db(prof)
    return FocusProfileModel(**payload)


@app.post("/api/my/profile", response_model=FocusProfileModel, dependencies=[Depends(get_current_user)])
def create_my_profile(body: MyProfileCreate, user=Depends(get_current_user)):
    with db_session() as db:
        existing = get_profile_for_user(db, user.id, body.profile_key)
        if existing:
            raise HTTPException(status_code=409, detail="Profile key already exists")
        profile_json = body.focus_config_json or {}
        prof = create_profile_for_user(
            db=db,
            user_id=user.id,
            profile_key=body.profile_key,
            profile_name=body.profile_name,
            description=body.description,
            profile_json=profile_json,
        )
        db.commit()
        db.refresh(prof)
        payload = _profile_payload_from_db(prof)
        return FocusProfileModel(**payload)


@app.post("/api/my/profile/{key}", response_model=FocusProfileModel, dependencies=[Depends(get_current_user)])
def update_my_profile(key: str, body: MyProfileUpdate, user=Depends(get_current_user)):
    with db_session() as db:
        prof = get_profile_for_user(db, user.id, key)
        if not prof:
            raise HTTPException(status_code=404, detail="Profile not found")
        profile_json = body.focus_config_json or {}
        updated = update_profile_for_user(
            db=db,
            user_id=user.id,
            profile_key=key,
            profile_name=body.profile_name or prof.profile_name,
            description=body.description if body.description is not None else prof.description,
            profile_json=profile_json,
        )
        if not updated:
            raise HTTPException(status_code=404, detail="Profile not found")
        db.commit()
        db.refresh(updated)
        payload = _profile_payload_from_db(updated)
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
            return RedirectResponse(url="/gui/login", status_code=303)
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


def _run_prefect_batch(
    run_id: str,
    profile_key: str,
    search_cfg: BatchSearchConfig,
    use_llm_enrich: bool,
    use_llm_scoring: bool,
    apply_blocker_cap: bool,
    focus_config_path: str | None = None,
) -> None:
    run_dir = run_manager.get_run_dir(run_id)
    log_file = run_manager.log_path(run_id)
    cutoff_iso = search_cfg.cutoff_iso or _compute_cutoff_iso(search_cfg.max_age_days)

    status = {
        "run_id": run_id,
        "status": "running",
        "started_at": run_manager._now_iso(),
        "finished_at": None,
        "profile_key": profile_key,
        "params": {
            "max_age_days": search_cfg.max_age_days,
            "cutoff_iso": cutoff_iso,
            "use_llm_enrich": use_llm_enrich,
            "use_llm_scoring": use_llm_scoring,
            "apply_blocker_cap": apply_blocker_cap,
        },
        "summary_path": None,
        "error": None,
    }
    run_manager.write_status(run_id, status)

    env = os.environ.copy()
    env["JOBAGENT_FOCUS_PROFILE"] = profile_key
    env["JOBAGENT_PROFILE_KEY"] = profile_key
    if focus_config_path:
        env["JOBAGENT_FOCUS_CONFIG_PATH"] = focus_config_path
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
    ]

    commands = [
        ["python", "-m", "app.prefect_run", "crawl", f"--list-max-age-days={search_cfg.max_age_days}"],
        process_cmd,
    ]

    overall_ok = True
    error_msg = None

    with log_file.open("a", encoding="utf-8") as log:
        for cmd in commands:
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
                if proc.returncode != 0:
                    overall_ok = False
                    error_msg = f"Command {' '.join(cmd)} exited with {proc.returncode}"
                    break
            except Exception as exc:
                overall_ok = False
                error_msg = f"Subprocess error: {exc}"
                break

    status["finished_at"] = run_manager._now_iso()
    if overall_ok:
        status["status"] = "completed"
    else:
        status["status"] = "failed"
        status["error"] = error_msg
    run_manager.write_status(run_id, status)


@app.post("/api/run_single", response_model=RunSingleResponse, dependencies=[Depends(get_current_user)])
async def run_single(req: RunSingleRequest, user=Depends(get_current_user)) -> RunSingleResponse:
    try:
        profile_model = _resolve_focus_profile_model_for_user(str(user.id), req.profile_key)
        focus = FocusConfig.from_profile(profile_model)
    except HTTPException:
        raise
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Unknown profile '{req.profile_key}'")

    backend = req.backend or "auto"
    if backend not in ("auto", "pw", "http"):
        raise HTTPException(status_code=400, detail="backend must be 'pw', 'http', or 'auto'")

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
    try:
        profile_model = _resolve_focus_profile_model_for_user(str(user.id), req.profile_key)
    except HTTPException:
        raise
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Unknown profile '{req.profile_key}'")
    except Exception:
        raise HTTPException(status_code=500, detail="Failed to load focus profile")

    run_id = run_manager.create_run_dir()
    run_dir = run_manager.get_run_dir(run_id)
    focus_override_path = run_dir / "focus_profile_override.json"
    focus_override_payload = {"profile_key": req.profile_key, **profile_model.model_dump()}
    focus_override_path.write_text(json.dumps(focus_override_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    background_tasks.add_task(
        _run_prefect_batch,
        run_id=run_id,
        profile_key=req.profile_key,
        search_cfg=req.search,
        use_llm_enrich=req.use_llm_enrich,
        use_llm_scoring=req.use_llm_scoring,
        apply_blocker_cap=req.apply_blocker_cap,
        focus_config_path=str(focus_override_path),
    )

    status = {
        "run_id": run_id,
        "status": "running",
        "started_at": run_manager._now_iso(),
        "finished_at": None,
        "profile_key": req.profile_key,
        "params": req.search.model_dump(),
        "summary_path": None,
        "error": None,
    }
    run_manager.write_status(run_id, status)
    return BatchRunStatus(**status)


@app.get("/api/run_status/{run_id}", response_model=BatchRunStatus, dependencies=[Depends(get_current_user)])
def get_run_status(run_id: str):
    status = run_manager.load_status(run_id)
    if not status:
        raise HTTPException(status_code=404, detail="Run not found")
    return BatchRunStatus(**status)


@app.get("/api/run_logs/{run_id}", response_model=RunLogsResponse, dependencies=[Depends(get_current_user)])
def get_run_logs(run_id: str, offset: int = 0, max_bytes: int = 4096):
    status = run_manager.load_status(run_id)
    if status is None:
        raise HTTPException(status_code=404, detail="Run not found")

    chunk, new_offset = run_manager.read_log_chunk(run_id, offset=offset, max_bytes=max_bytes)
    finished = status.get("status") in ("completed", "failed")

    return RunLogsResponse(
        ok=True,
        run_id=run_id,
        chunk=chunk,
        next_offset=new_offset,
        finished=finished and (chunk == ""),
    )


@app.get("/gui/run", response_class=HTMLResponse)
def gui_run(request: Request):
    try:
        user = get_current_user(request, None)
    except HTTPException as e:
        if e.status_code == status.HTTP_401_UNAUTHORIZED:
            return RedirectResponse(url="/gui/login", status_code=303)
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
