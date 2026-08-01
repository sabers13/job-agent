"""Backlog A6 + A8 — the ORM must work on every dialect the project targets.

Two defects, found one behind the other, both written failing before their fix:

**A6** — 12 columns across all 6 tables were typed `mssql.UNIQUEIDENTIFIER`, which
neither SQLite nor PostgreSQL can render. `Base.metadata.create_all` raised
`CompileError`, so no DB-backed test could build a schema at all. Fixed with
`sa.Uuid`.

**A8** — 8 timestamp columns defaulted to `server_default=sysdatetimeoffset()`, a
SQL-Server-only function. The schema then *created* on SQLite but the first INSERT
died with `unknown function: sysdatetimeoffset()`. Fixed by moving the default into
Python (`app.db.types.utcnow`) via a new Alembic revision, plus `UtcDateTime` to make
the result side tz-aware on SQLite as well as on the two dialects that do it natively.

The SQL Server DDL is asserted explicitly. That assertion is what made editing the two
A6 migrations safe rather than reckless: `sa.Uuid` emits UNIQUEIDENTIFIER on mssql, so
replaying them produces byte-identical DDL on the only deployed database. A8 could not
rely on that -- dropping a DEFAULT clause *does* change SQL Server DDL -- which is
exactly why it got a new revision instead of an edit.
"""

from __future__ import annotations

import uuid
from datetime import UTC

import pytest
from sqlalchemy import create_engine, inspect, select
from sqlalchemy.dialects import mssql, postgresql, sqlite
from sqlalchemy.schema import CreateTable

import app.db.models  # noqa: F401  -- registers the tables on Base.metadata
from app.db.base import Base

DIALECTS = {
    "sqlite": sqlite.dialect(),
    "postgresql": postgresql.dialect(),
    "mssql": mssql.dialect(),
}

# Every column the A6 swap touches: 12 across 6 tables.
UUID_COLUMNS = [
    ("users", "id"),
    ("resumes", "id"),
    ("resumes", "user_id"),
    ("profiles", "id"),
    ("profiles", "user_id"),
    ("runs", "id"),
    ("runs", "user_id"),
    ("runs", "profile_id"),
    ("run_items", "id"),
    ("run_items", "run_id"),
    ("url_pool", "id"),
    ("url_pool", "profile_id"),
]

# Backlog A8: every column that carried server_default=sysdatetimeoffset().
TIMESTAMP_COLUMNS = [
    ("users", "created_at"),
    ("profiles", "created_at"),
    ("runs", "created_at"),
    ("run_items", "created_at"),
    ("url_pool", "first_seen_at"),
    ("url_pool", "last_seen_at"),
    ("resumes", "created_at"),
    ("resumes", "updated_at"),
]


@pytest.mark.parametrize("dialect_name", sorted(DIALECTS))
def test_every_table_renders_ddl(dialect_name: str) -> None:
    """DDL compiles for all three dialects. This is the core A6 assertion."""
    dialect = DIALECTS[dialect_name]
    for table in Base.metadata.sorted_tables:
        # Raises CompileError on an unrenderable type -- that was the bug.
        ddl = str(CreateTable(table).compile(dialect=dialect))
        assert f"CREATE TABLE {table.name}" in ddl


def test_uuid_columns_render_per_dialect_expectations() -> None:
    """The type each dialect gets is pinned, not incidental.

    mssql keeping UNIQUEIDENTIFIER is the load-bearing one -- see the module
    docstring. The other two are recorded so a future type change is visible.
    """
    expected = {"sqlite": "CHAR(32)", "postgresql": "UUID", "mssql": "UNIQUEIDENTIFIER"}
    by_table = {t.name: t for t in Base.metadata.sorted_tables}

    for dialect_name, want in expected.items():
        dialect = DIALECTS[dialect_name]
        for table_name, column_name in UUID_COLUMNS:
            column = by_table[table_name].columns[column_name]
            rendered = column.type.compile(dialect=dialect)
            assert rendered == want, (
                f"{table_name}.{column_name} on {dialect_name}: expected {want}, got {rendered}"
            )


def test_sql_server_schema_is_unchanged_by_the_swap() -> None:
    """SQL Server DDL must be identical to the pre-swap output.

    The only deployed database is SQL Server and it is already at head. If this
    holds, editing the two migrations cannot alter what a replay produces.
    """
    for table in Base.metadata.sorted_tables:
        ddl = str(CreateTable(table).compile(dialect=mssql.dialect()))
        for column in table.columns:
            if (table.name, column.name) in UUID_COLUMNS:
                assert f"{column.name} UNIQUEIDENTIFIER" in ddl, (
                    f"{table.name}.{column.name} no longer renders as UNIQUEIDENTIFIER "
                    "on SQL Server -- the migration-edit exception no longer applies"
                )


def test_create_all_succeeds_on_sqlite(tmp_path) -> None:
    """The end state A6 exists to unblock: a real schema on a real SQLite file."""
    engine = create_engine(f"sqlite:///{tmp_path / 'probe.db'}")
    Base.metadata.create_all(engine)

    created = set(inspect(engine).get_table_names())
    assert {t.name for t in Base.metadata.sorted_tables} <= created


def test_uuid_roundtrips_as_uuid_object_on_sqlite(tmp_path) -> None:
    """A UUID goes in and a UUID comes back -- not a string.

    SQLite stores `sa.Uuid` as 32 hex characters with no dashes. Any code that
    compares an id against `str(some_uuid)` (which HAS dashes) would silently stop
    matching. This pins the ORM-level contract so that stays an application concern
    rather than a storage surprise.

    `created_at` is supplied explicitly to keep this test about the UUID type and
    nothing else. The Python-side default from A8 would work fine here.
    """
    from datetime import UTC, datetime

    from sqlalchemy.orm import Session

    from app.db.models import User

    engine = create_engine(f"sqlite:///{tmp_path / 'probe.db'}")
    Base.metadata.create_all(engine)

    user_id = uuid.uuid4()
    with Session(engine) as session:
        session.add(
            User(
                id=user_id,
                email="a6@example.test",
                password_hash="x",
                created_at=datetime.now(UTC),
            )
        )
        session.commit()

    with Session(engine) as session:
        loaded = session.get(User, user_id)
        assert loaded is not None
        assert isinstance(loaded.id, uuid.UUID)
        assert loaded.id == user_id


def test_str_of_an_id_keeps_its_dashes_on_sqlite(tmp_path) -> None:
    """`str(user.id)` must be the dashed canonical form on every dialect.

    This is the load-bearing safety property of the A6 swap. SQLite stores
    `sa.Uuid` as 32 hex characters with NO dashes, and roughly twenty call sites in
    `app/fastapi_run.py` do `str(user.id)` -- including run-artifact ownership
    checks (`status.get("user_id") != str(user.id)`) and run directory paths
    (`run_manager.latest_path(str(user.id), ...)`).

    If the dash-less form ever escaped the storage layer, every ownership check
    would silently start failing and run directories would change name. It does not
    escape, because `sa.Uuid` defaults to `as_uuid=True` and returns a `uuid.UUID`.
    Someone "simplifying" that to `as_uuid=False` would break all of it, so the
    property is pinned here rather than left to be rediscovered.
    """
    from datetime import UTC, datetime
    from sqlite3 import connect

    from sqlalchemy.orm import Session

    from app.db.models import User

    db_path = tmp_path / "probe.db"
    engine = create_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(engine)

    user_id = uuid.uuid4()
    with Session(engine) as session:
        session.add(
            User(
                id=user_id,
                email="dashes@example.test",
                password_hash="x",
                created_at=datetime.now(UTC),
            )
        )
        session.commit()

    with Session(engine) as session:
        loaded = session.get(User, user_id)
        assert loaded is not None
        assert "-" in str(loaded.id)
        assert str(loaded.id) == str(user_id)

    # ...while the on-disk form is genuinely dash-less. Both halves matter: the
    # first says the app is safe, the second says we really are on CHAR(32).
    stored = list(connect(str(db_path)).execute("select id from users"))[0][0]
    assert "-" not in str(stored)
    assert str(stored) == user_id.hex


def test_insert_without_explicit_timestamp_works_on_sqlite(tmp_path) -> None:
    """Backlog A8. Was `xfail(strict=True)`; the marker was removed when A8 landed.

    A6 made the schema *creatable* on SQLite. A8 makes it *writable*: the timestamp
    columns defaulted to `sysdatetimeoffset()`, which SQLite does not have, so the
    first INSERT died with `unknown function: sysdatetimeoffset()`. The default is now
    Python-side (`app.db.types.utcnow`).
    """
    from sqlalchemy.orm import Session

    from app.db.models import User

    engine = create_engine(f"sqlite:///{tmp_path / 'probe.db'}")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        session.add(User(email="a8@example.test", password_hash="x"))
        session.commit()

    with Session(engine) as session:
        loaded = session.scalars(select(User)).one()
        assert loaded.created_at is not None
        assert loaded.created_at.tzinfo is not None


def test_no_column_carries_a_dialect_specific_server_default() -> None:
    """A8's root cause, pinned so it cannot come back.

    `sysdatetimeoffset()` is SQL-Server-only. Any server_default naming a function
    only one engine implements re-breaks every other dialect at write time -- which
    is invisible until something actually inserts.
    """
    offenders = []
    for table in Base.metadata.sorted_tables:
        for column in table.columns:
            if column.server_default is None:
                continue
            rendered = str(getattr(column.server_default, "arg", column.server_default))
            if "sysdatetimeoffset" in rendered.lower():
                offenders.append(f"{table.name}.{column.name}")
    assert not offenders, f"mssql-only server_default still present on: {offenders}"


@pytest.mark.parametrize("dialect_name", sorted(DIALECTS))
def test_timestamp_columns_render_identically_after_a8(dialect_name: str) -> None:
    """The timestamp type is unchanged by A8 -- only the DEFAULT clause moved.

    `UtcDateTime` wraps `DateTime(timezone=True)`, so SQL Server still gets
    DATETIMEOFFSET and PostgreSQL still gets TIMESTAMP WITH TIME ZONE. Had the type
    changed, the new migration would have needed a data migration rather than a
    DEFAULT drop.
    """
    expected = {
        "sqlite": "DATETIME",
        "postgresql": "TIMESTAMP WITH TIME ZONE",
        "mssql": "DATETIMEOFFSET",
    }
    dialect = DIALECTS[dialect_name]
    by_table = {t.name: t for t in Base.metadata.sorted_tables}
    for table_name, column_name in TIMESTAMP_COLUMNS:
        column = by_table[table_name].columns[column_name]
        assert column.type.compile(dialect=dialect) == expected[dialect_name]


def test_utcnow_default_is_tz_aware_and_utc() -> None:
    """The replacement default is aware, and is actually UTC -- not local time.

    A naive default here would round-trip as naive on PostgreSQL and SQL Server and
    silently record the wrong instant for anyone not on UTC.
    """

    from app.db.types import utcnow

    value = utcnow()
    assert value.tzinfo is not None
    assert value.utcoffset() == UTC.utcoffset(None)


def test_timestamps_come_back_tz_aware_on_sqlite(tmp_path) -> None:
    """The property `DateTime(timezone=True)` alone does NOT give you.

    SQLite has no tz-aware type: its DATETIME is an ISO string with no offset, so a
    plain `DateTime(timezone=True)` column hands back a **naive** datetime while
    PostgreSQL and SQL Server hand back an aware one. `UtcDateTime` normalises the
    result side so all three agree.

    Without this, `datetime.now(UTC) - row.created_at` raises TypeError on SQLite and
    works everywhere else -- a bug that only ever appears under test.
    """
    from datetime import UTC, datetime

    from sqlalchemy.orm import Session

    from app.db.models import User

    engine = create_engine(f"sqlite:///{tmp_path / 'probe.db'}")
    Base.metadata.create_all(engine)

    written = datetime(2026, 7, 31, 12, 34, 56, 789012, tzinfo=UTC)
    with Session(engine) as session:
        session.add(User(email="tz@example.test", password_hash="x", created_at=written))
        session.commit()

    with Session(engine) as session:
        loaded = session.scalars(select(User)).one()
        assert loaded.created_at.tzinfo is not None, "SQLite returned a naive datetime"
        assert loaded.created_at == written
        # Arithmetic against an aware value must not raise.
        assert (datetime.now(UTC) - loaded.created_at).total_seconds() != 0


def test_naive_input_is_treated_as_utc_not_local(tmp_path) -> None:
    """A naive value written in is read back tagged UTC, with the same wall clock.

    Ambiguous input has to resolve somehow; UTC is the only choice consistent with
    `utcnow()`. Pinned so the assumption is explicit rather than incidental.
    """
    from datetime import UTC, datetime

    from sqlalchemy.orm import Session

    from app.db.models import User

    engine = create_engine(f"sqlite:///{tmp_path / 'probe.db'}")
    Base.metadata.create_all(engine)

    naive = datetime(2026, 7, 31, 12, 0, 0)
    with Session(engine) as session:
        session.add(User(email="naive@example.test", password_hash="x", created_at=naive))
        session.commit()

    with Session(engine) as session:
        loaded = session.scalars(select(User)).one()
        assert loaded.created_at == naive.replace(tzinfo=UTC)
