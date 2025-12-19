from __future__ import annotations

from contextlib import contextmanager

from sqlalchemy.orm import sessionmaker

from app.db.engine import get_engine

SessionLocal = sessionmaker(
    bind=get_engine(),
    autoflush=False,
    autocommit=False,
    expire_on_commit=False,
    future=True,
)


@contextmanager
def db_session():
    session = SessionLocal()
    try:
        yield session
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
