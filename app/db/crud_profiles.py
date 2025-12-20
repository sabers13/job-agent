from __future__ import annotations

import json
import uuid
from typing import Any, List, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config.profile_store import get_default_profiles_dict
from app.db.models import Profile


def _normalize_focus_json(value: Any) -> str:
    """
    Ensure we always store valid JSON text in the DB.
    Accepts dict/list/str; validates str input is JSON.
    """
    if value is None:
        return "{}"
    if isinstance(value, str):
        json.loads(value)
        return value
    return json.dumps(value, ensure_ascii=False)


def list_profiles_for_user(db: Session, user_id: uuid.UUID) -> List[Profile]:
    stmt = select(Profile).where(Profile.user_id == user_id)
    return list(db.scalars(stmt).all())


def get_profile_for_user(db: Session, user_id: uuid.UUID, profile_key: str) -> Optional[Profile]:
    stmt = select(Profile).where(Profile.user_id == user_id, Profile.profile_key == profile_key)
    return db.scalars(stmt).first()


def create_profile_for_user(
    db: Session,
    user_id: uuid.UUID,
    profile_key: str,
    profile_name: str,
    description: Optional[str],
    profile_json: str,
) -> Profile:
    profile_json = _normalize_focus_json(profile_json)
    profile = Profile(
        user_id=user_id,
        profile_key=profile_key,
        profile_name=profile_name,
        description=description,
        focus_config_json=profile_json,
    )
    db.add(profile)
    db.flush()
    return profile


def update_profile_for_user(
    db: Session,
    user_id: uuid.UUID,
    profile_key: str,
    profile_name: str,
    description: Optional[str],
    profile_json: str,
) -> Optional[Profile]:
    existing = get_profile_for_user(db, user_id, profile_key)
    if not existing:
        return None
    profile_json = _normalize_focus_json(profile_json)
    existing.profile_name = profile_name
    existing.description = description
    existing.focus_config_json = profile_json
    db.flush()
    return existing


def upsert_profile_for_user(
    db: Session,
    user_id: uuid.UUID,
    profile_key: str,
    profile_name: str,
    description: Optional[str],
    profile_json: str,
) -> Profile:
    existing = get_profile_for_user(db, user_id, profile_key)
    if existing:
        profile_json = _normalize_focus_json(profile_json)
        existing.profile_name = profile_name
        existing.description = description
        existing.focus_config_json = profile_json
        db.flush()
        return existing
    return create_profile_for_user(db, user_id, profile_key, profile_name, description, profile_json)


def delete_profile_for_user(db: Session, user_id: uuid.UUID, profile_key: str) -> bool:
    profile = get_profile_for_user(db, user_id, profile_key)
    if not profile:
        return False
    db.delete(profile)
    db.flush()
    return True


def seed_default_profiles_for_user(db: Session, user_id: uuid.UUID) -> int:
    # If the user already has profiles, do not seed defaults.
    existing = db.execute(select(Profile.id).where(Profile.user_id == user_id)).first()
    if existing:
        return 0

    defaults = get_default_profiles_dict()
    inserted = 0
    for key, payload in defaults.items():
        profile_name = payload.get("profile_name") or key
        description = payload.get("description")
        profile_json = json.dumps(payload, ensure_ascii=False)
        create_profile_for_user(db, user_id, key, profile_name, description, profile_json)
        inserted += 1
    return inserted
