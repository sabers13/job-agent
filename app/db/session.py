from __future__ import annotations

from contextlib import contextmanager

from sqlalchemy.orm import sessionmaker

from app.db.engine import get_engine

SessionLocal = sessionmaker(bind=get_engine(), autoflush=False, autocommit=False, future=True)


@contextmanager
def db_session():
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
