from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, EmailStr, Field, HttpUrl

from app.pipeline.models import UnifiedJobPosting


class _Base(BaseModel):
    model_config = ConfigDict(extra="allow")


# --- Requests --------------------------------------------------------------


class JobDetailsRequest(_Base):
    """
    Canonical request for a single job fetch/parse/enrich/score.
    """

    url: HttpUrl
    backend: str = "auto"
    enrich: bool = True
    score: bool = True
    profile_key: Optional[str] = None
    use_llm_scoring: bool = True
    apply_blocker_cap: bool = True
    cutoff_iso: Optional[str] = None
    use_cache: bool = True


class SearchStepstoneListRequest(_Base):
    seed_url: HttpUrl
    pages: int = 2
    max_pages: Optional[int] = 80
    delay_sec: float = 1.2
    include_titles_any: Optional[List[str]] = None
    exclude_titles_any: Optional[List[str]] = None
    max_jobs: Optional[int] = None
    use_playwright: bool = False
    stop_urls: Optional[List[HttpUrl]] = None
    list_cutoff_iso: Optional[str] = None


class BundleRequest(_Base):
    job: Dict[str, Any]
    scoring: Optional[Dict[str, Any]] = None
    output_root: str = "output"


class AggregateReportRequest(_Base):
    reports: List[Dict[str, Any]]
    output_root: str = "output"


# --- Responses --------------------------------------------------------------

class FetchMeta(_Base):
    backend: Optional[str] = None
    attempts: Optional[Any] = None


class ScoringResult(_Base):
    score: int
    version: Optional[str] = None
    heuristic_score: Optional[float] = None
    heuristic_version: Optional[str] = None
    alpha: Optional[float] = None
    components: Optional[Dict[str, Any]] = None
    reasons: Optional[List[str]] = None
    llm_enabled: Optional[bool] = None
    blocker_cap_enabled: Optional[bool] = None
    llm_ok: Optional[bool] = None
    llm_score: Optional[float] = None
    llm_scoring_version: Optional[str] = None
    critical_blockers: Optional[List[str]] = None
    llm_raw_excerpt: Optional[str] = None
    llm_debug: Optional[str] = None


class RunSingleRequest(_Base):
    profile_key: str
    url: HttpUrl
    backend: str = "auto"
    enrich: bool = True
    use_llm_scoring: bool = True
    apply_blocker_cap: bool = True
    cutoff_iso: Optional[str] = None


class RunSingleResponse(_Base):
    ok: bool
    profile_key: str
    details: JobDetailsResponse


class UnifiedJobPostingOut(UnifiedJobPosting):
    model_config = ConfigDict(extra="allow")


class JobDetailsResponse(_Base):
    ok: bool
    backend: Optional[str] = None
    job: UnifiedJobPostingOut
    scoring: Optional[ScoringResult] = None
    fetch_meta: FetchMeta
    cutoff_iso: Optional[str] = None
    stale: bool
    enrichment_meta: Optional[Dict[str, Any]] = None


class BundleResponse(_Base):
    ok: bool
    output_dir: str
    files: List[str]


class AggregateReportResponse(_Base):
    ok: bool
    path: str


class ResumeUploadResponse(_Base):
    resume_id: str
    is_active: bool
    filename: str
    sha256: str


class ResumeListItem(_Base):
    id: str
    filename: str
    created_at: Optional[str] = None
    is_active: bool


class ResumeDetailResponse(_Base):
    id: str
    filename: str
    mime_type: str
    sha256: str
    storage_path: str
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    is_active: bool
    parsed_json: Optional[Any] = None
    text_excerpt: Optional[str] = None


class JobListItem(_Base):
    url: str
    posted_iso: Optional[str] = None
    title: Optional[str] = None
    company: Optional[str] = None
    location: Optional[str] = None


class SearchStepstoneListResponse(_Base):
    ok: bool = True
    urls: List[str] = []
    jobs: List[JobListItem] = []
    count: int = 0
    list_cutoff_iso: Optional[str] = None


# --- Auth -----------------------------------------------------------------


class SignupRequest(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8, max_length=128)


class SignupResponse(BaseModel):
    id: str
    email: EmailStr


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


class MeResponse(BaseModel):
    id: str
    email: EmailStr
