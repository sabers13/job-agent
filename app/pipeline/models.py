from __future__ import annotations

from typing import Any, Dict, List, Optional, Set

from pydantic import BaseModel, Field, HttpUrl, field_validator

class UnifiedJobPosting(BaseModel):
    # Core (from JSON-LD or scraped)
    title: str
    company: str
    location: str
    employment_type: Optional[str] = None
    date_posted: Optional[str] = None
    valid_through: Optional[str] = None
    url: Optional[HttpUrl] = None
    job_id: Optional[str] = None
    salary: Optional[Dict[str, Any]] = None
    description_text: Optional[str] = None
    description_html: Optional[str] = None

    # Enriched (LLM-added)
    seniority: Optional[str] = None                  # e.g., Junior, Working Student, Internship, Mid, Senior
    english_ok: Optional[bool] = None
    german_requirement: Optional[str] = None         # e.g., None, A2, B1, B2, C1, Native
    skills_detected: Optional[List[str]] = None      # detected skills
    skill_hits: Optional[Dict[str, int]] = None      # counts for keyword hits { "Python": 3, ... }
    reasons_include: Optional[List[str]] = None
    reasons_exclude: Optional[List[str]] = None

    # Scoring placeholder (L5 will use)
    junior_fit_score: Optional[float] = None

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
    backend: Optional[str] = None
    status: Optional[int] = None
    attempts: Optional[int | List[Dict[str, Any]]] = None
    elapsed: Optional[float] = None
    final_url: Optional[HttpUrl] = None

    class Config:
        extra = "allow"


class LLMDetail(BaseModel):
    german_requirement: Optional[Dict[str, Any]] = None
    risk_flags: Optional[List[str]] = None
    critical_blockers: Optional[List[str]] = None
    summary: Optional[str] = None
    error: Optional[str] = None


class JobScoring(BaseModel):
    score: float
    heuristic_score: Optional[float] = None
    llm_score: Optional[float] = None
    components: Dict[str, float] = {}
    reasons: List[str] = []
    meta: Dict[str, Any] = {}
    llm_detail: Optional[LLMDetail] = None
    heuristic_version: Optional[str] = None
    llm_scoring_version: Optional[str] = None
    version: Optional[str] = None

    class Config:
        extra = "allow"


class JobDetailsResponse(BaseModel):
    ok: bool
    backend: Optional[str] = None
    job: UnifiedJobPosting
    scoring: Optional[JobScoring] = None
    fetch_meta: Optional[FetchMeta] = None
    cutoff_iso: Optional[str] = None
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
    strict_language_blocker: bool = True  # If job needs high German and candidate level is Unknown -> treat as blocker
    blocker_caps: BlockerCaps = Field(default_factory=BlockerCaps)


class FocusProfileModel(BaseModel):
    profile_name: str
    description: Optional[str] = None
    search_seeds: List[str] = []

    # seniority / experience
    target_seniority: Optional[str] = "junior"          # e.g. intern/junior/mid/senior
    max_allowed_seniority: Optional[str] = "mid"
    max_required_experience_years: Optional[int] = 3
    experience_penalty_strength: float = 1.0

    # skills
    core_skills: List[str] = []
    nice_to_have_skills: List[str] = []

    # job preferences
    preferred_titles: List[str] = []
    excluded_titles: List[str] = []
    preferred_locations: List[str] = []
    excluded_locations: List[str] = []

    # language / misc
    min_german_level: Optional[str] = "B1"      # e.g. "none", "A2", "B1", ...
    requires_student_status: bool = True

    # NEW (candidate constraints / cap policy)
    constraints: Constraints = Field(default_factory=Constraints)
