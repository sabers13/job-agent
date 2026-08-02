from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, HttpUrl, field_validator


class UnifiedJobPosting(BaseModel):
    # Core (from JSON-LD or scraped)
    title: str
    company: str
    location: str
    employment_type: str | None = None
    date_posted: str | None = None
    valid_through: str | None = None
    url: HttpUrl | None = None
    job_id: str | None = None
    salary: dict[str, Any] | None = None
    description_text: str | None = None
    description_html: str | None = None

    # Enriched (LLM-added)
    seniority: str | None = None  # e.g., Junior, Working Student, Internship, Mid, Senior
    english_ok: bool | None = None
    german_requirement: str | None = None  # e.g., None, A2, B1, B2, C1, Native
    skills_detected: list[str] | None = None  # detected skills
    skill_hits: dict[str, int] | None = None  # counts for keyword hits { "Python": 3, ... }
    reasons_include: list[str] | None = None
    reasons_exclude: list[str] | None = None

    # Scoring placeholder (L5 will use)
    junior_fit_score: float | None = None

    @field_validator("seniority", mode="before")
    @classmethod
    def _norm_seniority(cls, v):
        if not v:
            return v
        return str(v).strip().title()


# -------------------------
# API / scoring models
# -------------------------


class FetchMeta(BaseModel):
    backend: str | None = None
    status: int | None = None
    attempts: int | list[dict[str, Any]] | None = None
    elapsed: float | None = None
    final_url: HttpUrl | None = None

    class Config:
        extra = "allow"


class LLMDetail(BaseModel):
    german_requirement: dict[str, Any] | None = None
    risk_flags: list[str] | None = None
    critical_blockers: list[str] | None = None
    summary: str | None = None
    error: str | None = None


class JobScoring(BaseModel):
    score: float
    heuristic_score: float | None = None
    llm_score: float | None = None
    components: dict[str, float] = {}
    reasons: list[str] = []
    meta: dict[str, Any] = {}
    llm_detail: LLMDetail | None = None
    heuristic_version: str | None = None
    llm_scoring_version: str | None = None
    version: str | None = None

    class Config:
        extra = "allow"


class JobDetailsResponse(BaseModel):
    ok: bool
    backend: str | None = None
    job: UnifiedJobPosting
    scoring: JobScoring | None = None
    fetch_meta: FetchMeta | None = None
    cutoff_iso: str | None = None
    stale: bool = False


# -------------------------
# Profile constraints (Step 5)
# -------------------------


class BlockerCaps(BaseModel):
    hard: int = 35
    soft: int = 55


class Constraints(BaseModel):
    # Candidate constraints / policies
    german_level: str = "Unknown"  # Candidate's actual level: A0/A1/A2/B1/B2/C1/C2/Unknown
    relocation_ok: bool = True  # Candidate willing to relocate?
    strict_language_blocker: bool = (
        True  # If job needs high German and candidate level is Unknown -> treat as blocker
    )
    blocker_caps: BlockerCaps = Field(default_factory=BlockerCaps)


class FocusProfileModel(BaseModel):
    profile_key: str = ""
    profile_name: str
    description: str | None = None
    search_seeds: list[str] = []

    # seniority / experience
    target_seniority: str | None = "junior"  # e.g. intern/junior/mid/senior
    max_allowed_seniority: str | None = "mid"
    max_required_experience_years: int | None = 3
    experience_penalty_strength: float = 1.0

    # skills
    core_skills: list[str] = []
    nice_to_have_skills: list[str] = []

    # job preferences
    preferred_titles: list[str] = []
    excluded_titles: list[str] = []
    preferred_locations: list[str] = []
    excluded_locations: list[str] = []

    # language / misc
    min_german_level: str | None = "B1"  # e.g. "none", "A2", "B1", ...
    requires_student_status: bool = True

    # NEW (candidate constraints / cap policy)
    constraints: Constraints = Field(default_factory=Constraints)
