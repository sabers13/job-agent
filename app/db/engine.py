from __future__ import annotations

import os
from urllib.parse import quote_plus, unquote_plus
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, make_url

from app.config.settings import settings


def _ensure_connect_timeout(db_url: str, timeout_sec: int) -> str:
    if "mssql+pyodbc" not in (db_url or ""):
        return db_url
    lower = db_url.lower()
    if "connection timeout=" in lower or "connect timeout=" in lower:
        return db_url

    # Use SQLAlchemy URL parser/renderer to avoid breaking "mssql+pyodbc:///?..."
    url = make_url(db_url)

    q = dict(url.query)
    odbc = q.get("odbc_connect")
    if not odbc:
        # Fallback: append as query param; preserve existing query
        # (for mssql+pyodbc, we prefer odbc_connect, but don't break other forms)
        sep = "&" if "?" in db_url else "?"
        return f"{db_url}{sep}Connection+Timeout={int(timeout_sec)}"

    odbc_decoded = unquote_plus(odbc)
    odbc_lower = odbc_decoded.lower()
    if "connection timeout=" not in odbc_lower and "connect timeout=" not in odbc_lower:
        if not odbc_decoded.endswith(";"):
            odbc_decoded += ";"
        odbc_decoded += f"Connection Timeout={int(timeout_sec)};"

    q["odbc_connect"] = quote_plus(odbc_decoded)

    # render_as_string(hide_password=False) keeps full URL for engine creation
    return url.set(query=q).render_as_string(hide_password=False)


def make_engine(url: str) -> Engine:
    connect_timeout = int(os.getenv("DB_CONNECT_TIMEOUT_SEC", "10"))
    pool_timeout = int(os.getenv("DB_POOL_TIMEOUT_SEC", "15"))
    pool_recycle = int(os.getenv("DB_POOL_RECYCLE_SEC", "1800"))
    db_url = _ensure_connect_timeout(url, connect_timeout)

    return create_engine(
        db_url,
        echo=bool(settings.db_echo),
        pool_pre_ping=True,
        pool_size=int(settings.db_pool_size),
        max_overflow=int(settings.db_max_overflow),
        pool_timeout=pool_timeout,
        pool_recycle=pool_recycle,
        future=True,
    )


def get_engine():
    if not settings.database_url:
        raise RuntimeError("JOBAGENT_DATABASE_URL is not set")
    return make_engine(settings.database_url)
