from __future__ import annotations

from contextlib import contextmanager
import time
from typing import Callable, Generator, TypeVar

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.orm import Session, sessionmaker

from app.db.engine import get_engine

T = TypeVar("T")

SessionLocal = sessionmaker(
    bind=get_engine(),
    autoflush=False,
    autocommit=False,
    expire_on_commit=False,
    future=True,
)

_TRANSIENT_HINTS = (
    "HYT00",
    "08S01",
    "40001",
    "40197",
    "40501",
    "40613",
)


def is_transient_db_error(exc: BaseException) -> bool:
    parts: list[str] = []
    try:
        parts.append(str(exc))
        if getattr(exc, "args", None):
            parts.extend(str(a) for a in exc.args if a is not None)
        orig = getattr(exc, "orig", None)
        if orig is not None and getattr(orig, "args", None):
            parts.extend(str(a) for a in orig.args if a is not None)
    except Exception:
        parts.append(str(exc))

    haystack = " | ".join(parts).lower()
    return any(h.lower() in haystack for h in _TRANSIENT_HINTS)

def run_db_with_retries(
    op: Callable[[Session], T],
    *,
    max_retries: int = 2,
    base_sleep: float = 0.4,
) -> T:
    attempt = 0
    while True:
        db = SessionLocal()
        try:
            return op(db)
        except (OperationalError, DBAPIError) as exc:
            try:
                db.rollback()
            except Exception:
                pass
            if attempt < max_retries and is_transient_db_error(exc):
                time.sleep(base_sleep * (2**attempt))
                attempt += 1
                continue
            raise
        finally:
            try:
                db.close()
            except Exception:
                pass


def ping_db(session: Session) -> None:
    session.execute(text("SELECT 1"))


def get_db(max_retries: int = 2, base_sleep: float = 0.4) -> Generator[Session, None, None]:
    attempt = 0
    while True:
        db = SessionLocal()
        try:
            ping_db(db)
            yield db
            return
        except (OperationalError, DBAPIError) as exc:
            if attempt < max_retries and is_transient_db_error(exc):
                time.sleep(base_sleep * (2**attempt))
                attempt += 1
                continue
            raise
        finally:
            try:
                db.close()
            except Exception:
                pass


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
