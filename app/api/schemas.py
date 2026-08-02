from __future__ import annotations

from typing import Any

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
    profile_key: str | None = None
    use_llm_scoring: bool = True
    apply_blocker_cap: bool = True
    cutoff_iso: str | None = None
    use_cache: bool = True


class SearchStepstoneListRequest(_Base):
    seed_url: HttpUrl
    pages: int = 2
    max_pages: int | None = 80
    delay_sec: float = 1.2
    include_titles_any: list[str] | None = None
    exclude_titles_any: list[str] | None = None
    max_jobs: int | None = None
    use_playwright: bool = False
    stop_urls: list[HttpUrl] | None = None
    list_cutoff_iso: str | None = None


class BundleRequest(_Base):
    job: dict[str, Any]
    scoring: dict[str, Any] | None = None
    output_root: str = "output"


class AggregateReportRequest(_Base):
    reports: list[dict[str, Any]]
    output_root: str = "output"


# --- Responses --------------------------------------------------------------


class FetchMeta(_Base):
    backend: str | None = None
    attempts: Any | None = None


class ScoringResult(_Base):
    score: int
    version: str | None = None
    heuristic_score: float | None = None
    heuristic_version: str | None = None
    alpha: float | None = None
    components: dict[str, Any] | None = None
    reasons: list[str] | None = None
    llm_enabled: bool | None = None
    blocker_cap_enabled: bool | None = None
    llm_ok: bool | None = None
    llm_score: float | None = None
    llm_scoring_version: str | None = None
    critical_blockers: list[str] | None = None
    llm_raw_excerpt: str | None = None
    llm_debug: str | None = None


class RunSingleRequest(_Base):
    profile_key: str
    url: HttpUrl
    backend: str = "auto"
    enrich: bool = True
    use_llm_scoring: bool = True
    apply_blocker_cap: bool = True
    cutoff_iso: str | None = None


class RunSingleResponse(_Base):
    ok: bool
    profile_key: str
    details: JobDetailsResponse


class UnifiedJobPostingOut(UnifiedJobPosting):
    model_config = ConfigDict(extra="allow")


class JobDetailsResponse(_Base):
    ok: bool
    backend: str | None = None
    job: UnifiedJobPostingOut
    scoring: ScoringResult | None = None
    fetch_meta: FetchMeta
    cutoff_iso: str | None = None
    stale: bool
    enrichment_meta: dict[str, Any] | None = None


class BundleResponse(_Base):
    ok: bool
    output_dir: str
    files: list[str]


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
    created_at: str | None = None
    is_active: bool


class ResumeDetailResponse(_Base):
    id: str
    filename: str
    mime_type: str
    sha256: str
    storage_path: str
    created_at: str | None = None
    updated_at: str | None = None
    is_active: bool
    parsed_json: Any | None = None
    text_excerpt: str | None = None


class JobListItem(_Base):
    url: str
    posted_iso: str | None = None
    title: str | None = None
    company: str | None = None
    location: str | None = None


class SearchStepstoneListResponse(_Base):
    ok: bool = True
    urls: list[str] = []
    jobs: list[JobListItem] = []
    count: int = 0
    list_cutoff_iso: str | None = None


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
