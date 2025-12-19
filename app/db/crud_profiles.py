from __future__ import annotations

import json
import uuid
from typing import List, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config.profile_store import get_default_profiles_dict
from app.db.models import Profile


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
