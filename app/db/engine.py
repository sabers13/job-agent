from __future__ import annotations

from sqlalchemy import create_engine

from app.config.settings import settings


def make_engine(url: str):
    return create_engine(
        url,
        echo=bool(settings.db_echo),
        pool_pre_ping=True,
        pool_size=int(settings.db_pool_size),
        max_overflow=int(settings.db_max_overflow),
        future=True,
    )


def get_engine():
    if not settings.database_url:
        raise RuntimeError("JOBAGENT_DATABASE_URL is not set")
    return make_engine(settings.database_url)
