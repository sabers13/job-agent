from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Any

PROFILES_PATH = Path("config/focus_profiles.json")


def _ensure_file_exists() -> None:
    PROFILES_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not PROFILES_PATH.exists():
        PROFILES_PATH.write_text("{}", encoding="utf-8")


def load_profiles() -> Dict[str, Dict[str, Any]]:
    """Load all profiles as a dict mapping key -> profile dict."""
    _ensure_file_exists()
    text = PROFILES_PATH.read_text(encoding="utf-8")
    if not text.strip():
        return {}
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        # If the file is corrupted, don't crash the whole app
        # (you can log a warning here if you want)
        return {}
    if not isinstance(data, dict):
        return {}
    return data


def save_profiles(data: Dict[str, Dict[str, Any]]) -> None:
    """Overwrite the profile file with the given dict."""
    _ensure_file_exists()
    PROFILES_PATH.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def get_profile_keys() -> list[str]:
    return list(load_profiles().keys())


def get_profile(key: str) -> Dict[str, Any] | None:
    profiles = load_profiles()
    return profiles.get(key)


def upsert_profile(key: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    profiles = load_profiles()
    profiles[key] = payload
    save_profiles(profiles)
    return profiles[key]


def delete_profile(key: str) -> None:
    profiles = load_profiles()
    if key in profiles:
        del profiles[key]
        save_profiles(profiles)


def get_default_profiles_dict() -> Dict[str, Dict[str, Any]]:
    """
    Return built-in default profiles for seeding new users.
    These are templates only; they are not written to disk here.
    """
    return {
        "junior_data_bi": {
            "profile_name": "Junior Data/BI",
            "description": "General junior data/BI focus profile",
            "search_seeds": [],
            "target_seniority": "junior",
            "max_allowed_seniority": "mid",
            "max_required_experience_years": 3,
            "experience_penalty_strength": 1.0,
            "core_skills": ["Python", "SQL"],
            "nice_to_have_skills": ["Power BI", "DAX", "Power Query", "Pandas", "NumPy"],
            "preferred_titles": [
                "Junior Data Analyst",
                "Junior BI Analyst",
                "Junior BI Developer",
                "Analytics Engineer",
                "Python Developer",
                "Working Student Data",
                "Data/BI Intern",
            ],
            "excluded_titles": ["Senior", "Lead", "Principal", "Head", "Manager"],
            "preferred_locations": ["Deutschland", "NRW", "Dortmund", "Cologne", "Düsseldorf", "Essen"],
            "excluded_locations": [],
            "min_german_level": "B1",
            "requires_student_status": True,
        }
    }
