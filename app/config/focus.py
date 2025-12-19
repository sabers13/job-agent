from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Set, Optional, TYPE_CHECKING, List

from app.config import profile_store

if TYPE_CHECKING:
    from app.pipeline.models import FocusProfileModel


@dataclass(frozen=True)
class FocusConfig:
    profile_name: str = "junior_data_bi"
    description: Optional[str] = None
    search_seeds: List[str] = field(default_factory=list)
    target_seniority: Optional[str] = "junior"
    max_allowed_seniority: Optional[str] = "mid"
    max_required_experience_years: Optional[int] = 3
    experience_penalty_strength: float = 1.0
    titles_any: Set[str] = field(
        default_factory=lambda: {
            "Junior Data Analyst",
            "Junior BI Analyst",
            "Junior BI Developer",
            "Analytics Engineer",
            "Python Developer",
            "Working Student Data",
            "Data/BI Intern",
        }
    )
    exclude_titles_any: Set[str] = field(default_factory=lambda: {"Senior", "Lead", "Principal", "Head", "Manager"})
    locations_any: Set[str] = field(default_factory=lambda: {"Deutschland", "NRW", "Dortmund", "Cologne", "Düsseldorf", "Essen"})
    include_skills_any: Set[str] = field(default_factory=lambda: {"Python", "SQL"})
    nice_to_have: Set[str] = field(default_factory=lambda: {"Power BI", "DAX", "Power Query", "Pandas", "NumPy"})
    excluded_locations: Set[str] = field(default_factory=set)
    min_german_level: Optional[str] = "B1"
    requires_student_status: bool = True
    # NEW (candidate constraints / cap policy)
    candidate_german_level: str = "Unknown"
    relocation_ok: bool = True
    strict_language_blocker: bool = True
    blocker_cap_hard: int = 35
    blocker_cap_soft: int = 55

    @classmethod
    def from_profile(cls, profile: "FocusProfileModel") -> "FocusConfig":
        strength = profile.experience_penalty_strength
        try:
            strength = float(strength)
        except Exception:
            strength = 1.0
        strength = max(0.0, min(strength, 3.0))

        constraints = getattr(profile, "constraints", None)
        cand_level = "Unknown"
        relocation_ok = True
        strict_lang = True
        cap_hard = 35
        cap_soft = 55

        if constraints is not None:
            cand_level = getattr(constraints, "german_level", "Unknown") or "Unknown"
            relocation_ok = bool(getattr(constraints, "relocation_ok", True))
            strict_lang = bool(getattr(constraints, "strict_language_blocker", True))
            blocker_caps = getattr(constraints, "blocker_caps", None)
            if blocker_caps is not None:
                cap_hard = int(getattr(blocker_caps, "hard", 35))
                cap_soft = int(getattr(blocker_caps, "soft", 55))

        return cls(
            profile_name=profile.profile_name,
            description=profile.description,
            search_seeds=list(profile.search_seeds or []),
            target_seniority=profile.target_seniority,
            max_allowed_seniority=profile.max_allowed_seniority,
            max_required_experience_years=profile.max_required_experience_years,
            experience_penalty_strength=strength,
            titles_any=set(profile.preferred_titles or []),
            exclude_titles_any=set(profile.excluded_titles or []),
            locations_any=set(profile.preferred_locations or []),
            excluded_locations=set(profile.excluded_locations or []),
            include_skills_any=set(profile.core_skills or []),
            nice_to_have=set(profile.nice_to_have_skills or []),
            min_german_level=profile.min_german_level,
            requires_student_status=bool(profile.requires_student_status),
            candidate_german_level=str(cand_level),
            relocation_ok=relocation_ok,
            strict_language_blocker=strict_lang,
            blocker_cap_hard=cap_hard,
            blocker_cap_soft=cap_soft,
        )


DEFAULT_FOCUS = FocusConfig()


def load_focus_profiles() -> Dict[str, FocusConfig]:
    from app.pipeline.models import FocusProfileModel

    raw = profile_store.load_profiles()
    profiles: Dict[str, FocusConfig] = {}
    for key, payload in raw.items():
        try:
            profiles[key] = FocusConfig.from_profile(FocusProfileModel(**payload))
        except Exception:
            # Skip malformed entries; can be logged later
            continue
    return profiles


def get_focus_config(profile_key: str) -> FocusConfig:
    profiles = load_focus_profiles()
    if profile_key not in profiles:
        raise KeyError(f"Unknown profile key: {profile_key}")
    return profiles[profile_key]
