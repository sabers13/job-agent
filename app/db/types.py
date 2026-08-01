"""Dialect-neutral column types.

Backlog A8. SQL Server has `DATETIMEOFFSET` and PostgreSQL has `timestamptz`, both
of which round-trip an aware `datetime` unchanged. SQLite has neither: its DATETIME
is an ISO string with no offset, so SQLAlchemy hands back a **naive** datetime even
when the column is declared `DateTime(timezone=True)`.

That difference is the kind of dialect drift this restructure exists to remove. A
timestamp that is aware in production and naive under test turns every
`datetime.now(UTC) - row.created_at` into a `TypeError` that only appears on one
dialect, and only at runtime.

`UtcDateTime` normalises both directions: everything is stored as UTC, and
everything comes back tagged UTC, on every dialect.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import DateTime
from sqlalchemy.engine.interfaces import Dialect
from sqlalchemy.types import TypeDecorator


def utcnow() -> datetime:
    """Python-side default for timestamp columns.

    Replaces `server_default=func.sysdatetimeoffset()`, which is SQL-Server-only and
    made every INSERT fail on SQLite with `unknown function: sysdatetimeoffset()`.

    Evaluated by the application rather than the database, which is what makes it
    portable. The cost is that rows written by something other than this application
    -- a manual `INSERT`, a bulk load -- no longer get a timestamp for free.
    """
    return datetime.now(UTC)


class UtcDateTime(TypeDecorator[datetime]):
    """A timezone-aware datetime that behaves identically on every dialect.

    Renders as the same underlying type as before (`DATETIMEOFFSET` on SQL Server,
    `TIMESTAMP WITH TIME ZONE` on PostgreSQL), so this changes no DDL.
    """

    impl = DateTime(timezone=True)
    cache_ok = True

    def process_bind_param(self, value: datetime | None, dialect: Dialect) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            # A naive value is ambiguous. Treating it as UTC is the only choice
            # consistent with utcnow() above; reject nothing, guess nothing else.
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    def process_result_value(self, value: Any, dialect: Dialect) -> datetime | None:
        if value is None:
            return None
        if not isinstance(value, datetime):
            return value
        if value.tzinfo is None:
            # SQLite path: the stored value is UTC by construction (bind above),
            # so tagging it UTC restores the instant rather than shifting it.
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
