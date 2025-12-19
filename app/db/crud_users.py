from __future__ import annotations

import uuid

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models import User


def get_user_by_email(db: Session, email: str) -> User | None:
    stmt = select(User).where(User.email == email)
    return db.execute(stmt).scalars().first()


def get_user_by_id(db: Session, user_id: uuid.UUID) -> User | None:
    stmt = select(User).where(User.id == user_id)
    return db.execute(stmt).scalars().first()


def create_user(db: Session, email: str, password_hash: str) -> User:
    user = User(email=email.lower().strip(), password_hash=password_hash)
    db.add(user)
    db.flush()  # ensures user.id exists
    return user
