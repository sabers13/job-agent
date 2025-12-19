from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from jose import jwt
from passlib.context import CryptContext

from app.config.settings import settings

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(password: str, password_hash: str) -> bool:
    return pwd_context.verify(password, password_hash)


def create_access_token(subject: str, extra: dict[str, Any] | None = None) -> str:
    if not settings.jwt_secret:
        raise RuntimeError("JOBAGENT_JWT_SECRET is not set")

    now = datetime.now(timezone.utc)
    exp = now + timedelta(minutes=int(settings.jwt_expires_min))

    payload: dict[str, Any] = {
        "sub": subject,
        "iat": int(now.timestamp()),
        "exp": int(exp.timestamp()),
    }
    if extra:
        payload.update(extra)

    return jwt.encode(payload, settings.jwt_secret, algorithm=settings.jwt_alg)


def decode_token(token: str) -> dict[str, Any]:
    if not settings.jwt_secret:
        raise RuntimeError("JOBAGENT_JWT_SECRET is not set")
    return jwt.decode(token, settings.jwt_secret, algorithms=[settings.jwt_alg])
