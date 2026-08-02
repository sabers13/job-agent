"""Shared fixtures for the contract suite.

Two rules from TEST-STRATEGY drive everything here:

1. **No implicit global state.** Every scoring test builds an explicit profile via
   `profile_factory`. `DEFAULT_FOCUS` is a thing under test (see
   `tests/unit/test_default_focus.py`), never a silent precondition of other tests.
   That is the direct fix for flaw F2 — the old suite asserted `<= -15`, a number that
   was a function of `DEFAULT_FOCUS`, so a config change silently broke a code test.

2. **Offline and deterministic.** No live network, no DB container, no Playwright, no
   LLM. Anything needing those is marked `external` and excluded from the gate.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

FIXTURES = Path(__file__).parent / "fixtures"

# --------------------------------------------------------------------------- #
# Environment, set BEFORE any `app.*` import.
#
# `app/config/settings.py` evaluates env vars as dataclass field defaults, i.e. at
# import time, and `app/db/session.py` calls `get_engine()` at module scope to build
# `SessionLocal`. So importing `app.fastapi_run` with no database URL raises
# `RuntimeError: JOBAGENT_DATABASE_URL is not set` during collection.
#
# These are throwaway values. Every test that touches the DB rebinds `SessionLocal` to
# its own `tmp_path` file via the `db_engine` fixture; this URL only has to be
# *constructible*. It is a file rather than `:memory:` for the reason in `sqlite_url`.
#
# **Assignment, not `setdefault`.** The suite owns these six variables outright.
#
# `setdefault` let a sourced `.env.dev` win, and `.env.dev` is the shell AGENTS.md
# §Commands documents as normal. Its `mssql+pyodbc` URL then survived into
# `TestClient`'s lifespan, where `_startup_checks` -> `check_db` opened a live
# connection to the developer's SQL Server. `app/db/health.py` binds `SessionLocal`
# at import, so `db_engine`'s monkeypatch cannot reach that path and cannot save it.
#
# The result was an oracle whose meaning depended on the shell it ran in: with the
# container up, `/health/db` graded a real database and passed; with it down, the
# run blocked on the ODBC login timeout. Neither is what these tests claim to check.
#
# `tests/test_suite_hermeticity.py` is the executable form of this paragraph.
# --------------------------------------------------------------------------- #
TEST_ENV: dict[str, str] = {
    "JOBAGENT_DATABASE_URL": f"sqlite:///{ROOT / '.pytest_bootstrap.db'}",
    "JOBAGENT_JWT_SECRET": "test-secret-not-used-for-real-tokens",
    "JOBAGENT_ENV": "test",
    "JOBAGENT_USE_PLAYWRIGHT": "false",
    "JOBAGENT_USE_LLM_ENRICH": "false",
    "JOBAGENT_USE_LLM_SCORING": "false",
}
os.environ.update(TEST_ENV)


# --------------------------------------------------------------------------- #
# Network — the fourth claim in this module's docstring, made executable.
#
# "No live network" was prose until CP1-8. `tests/net_guard.py` carries the design
# notes; `tests/test_suite_hermeticity.py` asserts the guard is installed and that it
# actually refuses, so a future edit cannot neuter it and leave the claim standing.
# --------------------------------------------------------------------------- #


@pytest.fixture(scope="session", autouse=True)
def _refuse_outbound_connections() -> Iterator[None]:
    """Session-scoped and autouse: no test may opt out, and none has to opt in."""
    from tests import net_guard

    with pytest.MonkeyPatch.context() as mp:
        net_guard.install(mp)
        yield


# Checked per phase by a hook rather than by a fixture, for two reasons. A swallowed
# refusal must fail the **call** phase — a teardown failure leaves the test counted as
# passed, and `pytest_passed` is `ci/baseline.json`'s one ratchet key. And the check has
# to run outside every `except Exception:` in `app/`, which a fixture body does but an
# assertion inside the test does not.
#
# Each wrapper resets on the failure path instead of reporting: a refusal that already
# propagated is one red test, not one red test plus a stale record failing the next
# phase as well.


@pytest.hookimpl(wrapper=True)
def pytest_runtest_setup(item: pytest.Item):
    from tests import net_guard

    net_guard.reset()
    try:
        result = yield
    except BaseException:
        net_guard.reset()
        raise
    net_guard.check_and_reset("fixture setup")
    return result


@pytest.hookimpl(wrapper=True)
def pytest_runtest_call(item: pytest.Item):
    from tests import net_guard

    try:
        result = yield
    except BaseException:
        net_guard.reset()
        raise
    net_guard.check_and_reset("the test")
    return result


@pytest.hookimpl(wrapper=True)
def pytest_runtest_teardown(item: pytest.Item, nextitem: pytest.Item | None):
    from tests import net_guard

    try:
        result = yield
    except BaseException:
        net_guard.reset()
        raise
    net_guard.check_and_reset("fixture teardown")
    return result


# --------------------------------------------------------------------------- #
# Profiles
# --------------------------------------------------------------------------- #


@pytest.fixture
def profile_factory() -> Callable[..., Any]:
    """Build an explicit `FocusConfig`. Never returns `DEFAULT_FOCUS`.

    **`FocusConfig`, not `FocusProfileModel`.** Those are two different types for the
    same idea and they do not share attribute names — `FocusConfig.exclude_titles_any`
    vs `FocusProfileModel.excluded_titles`, `relocation_ok` at the top level vs nested
    under `constraints`. `score_job` consumes `FocusConfig`; passing the Pydantic model
    raises `AttributeError: no attribute 'exclude_titles_any'`.

    (The duplication is real and is the D1/A7 profile-store question in another guise.
    Recorded here because a fixture is where the next person will trip over it.)

    Every value scoring reads is stated explicitly, so a test's expectations depend on
    the test rather than on whatever `config/focus_profiles.json` says today — the fix
    for flaw F2.
    """
    from app.config.focus import FocusConfig

    def _make(**overrides: Any):
        base: dict[str, Any] = {
            "profile_name": "test_profile",
            "description": "explicit profile for tests",
            "search_seeds": [],
            "target_seniority": "junior",
            "max_allowed_seniority": "mid",
            "max_required_experience_years": 3,
            "experience_penalty_strength": 1.0,
            "titles_any": {"Data Analyst"},
            "exclude_titles_any": {"Senior", "Head"},
            "locations_any": {"Berlin"},
            "excluded_locations": set(),
            "include_skills_any": {"SQL", "Python"},
            "nice_to_have": {"Power BI"},
            "min_german_level": "B1",
            "requires_student_status": False,
            "candidate_german_level": "B2",
            "relocation_ok": True,
            "strict_language_blocker": True,
            "blocker_cap_hard": 35,
            "blocker_cap_soft": 55,
        }
        base.update(overrides)
        return FocusConfig(**base)

    return _make


# --------------------------------------------------------------------------- #
# Jobs
# --------------------------------------------------------------------------- #


@pytest.fixture
def job_factory() -> Callable[..., dict[str, Any]]:
    """Build a scoring-ready job dict, validated through `UnifiedJobPosting`.

    Returns a plain dict because that is what `score_job` consumes; routing it through
    the model first means a field rename breaks the fixture loudly instead of silently
    producing a job with a typo'd key that scores 0 for the wrong reason.
    """
    from app.pipeline.models import UnifiedJobPosting

    def _make(**overrides: Any) -> dict[str, Any]:
        base: dict[str, Any] = {
            "title": "Data Analyst",
            "company": "Test GmbH",
            "location": "Berlin",
            "employment_type": "FULL_TIME",
            "description_text": "Work with SQL and Python on dashboards.",
            "url": "https://example.test/job/1",
            "seniority": "junior",
            "skills_detected": [],
        }
        base.update(overrides)
        return UnifiedJobPosting(**base).model_dump()

    return _make


@pytest.fixture
def job_html() -> Callable[[str], str]:
    """Raw HTML fixture, for the one place parsing itself is under test."""

    def _read(name: str = "job_stepstone_1.html") -> str:
        return (FIXTURES / "jobs" / name).read_text(encoding="utf-8")

    return _read


# --------------------------------------------------------------------------- #
# Run artifacts
# --------------------------------------------------------------------------- #


@pytest.fixture
def run_output_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Redirect all run-artifact writes under `tmp_path`.

    `run_manager` binds `OUTPUTS_BASE`, `LEGACY_OUTPUT_ROOT` and `RUN_INDEX_DIR` at
    import time, so patching `settings.output_dir` alone would do nothing — all three
    module constants have to move together or `create_run_dir` and `status_path`
    disagree about where a run lives.
    """
    import dataclasses

    import app.fastapi_run as fastapi_run
    import app.pipeline.state as pipeline_state
    from app.gui_runs import run_manager

    base = tmp_path / "output"
    base.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(run_manager, "OUTPUTS_BASE", base)
    monkeypatch.setattr(run_manager, "LEGACY_OUTPUT_ROOT", base / "gui_runs")
    monkeypatch.setattr(run_manager, "RUN_INDEX_DIR", base / "_run_index")

    # `run_manager` is not the only writer. Résumé uploads go through
    # `_resume_root()` -> `settings.output_dir`, and run state goes through
    # `pipeline.state` -> `settings.output_dir`. Patching only `run_manager` leaves both
    # writing into the developer's real `output/` tree — which is exactly what happened
    # the first time this fixture was written.
    #
    # `settings` is a frozen dataclass, so `setattr` raises. Rebind the module-level name
    # to a copy instead; every reader resolves `settings` from its own module globals.
    scoped = dataclasses.replace(fastapi_run.settings, output_dir=base)
    monkeypatch.setattr(fastapi_run, "settings", scoped)

    # `pipeline.state` derives STATE_DIR/STATE_FILE/CACHE_DIR from `settings.output_dir`
    # at import time, so rebinding `settings` there is too late — the paths are already
    # computed. Patch the constants themselves, same as `run_manager`.
    monkeypatch.setattr(pipeline_state, "settings", scoped, raising=False)
    monkeypatch.setattr(pipeline_state, "STATE_DIR", base / "_state", raising=False)
    monkeypatch.setattr(pipeline_state, "STATE_FILE", base / "_state" / "run_state.json")
    monkeypatch.setattr(pipeline_state, "CACHE_DIR", base / "_cache", raising=False)
    return base


@pytest.fixture
def make_run(run_output_root: Path) -> Callable[..., tuple[str, Path]]:
    """Create a run directory and return `(run_id, run_dir)`."""
    from app.gui_runs import run_manager

    def _make(user_id: str = "user-1", profile_key: str = "profile-1") -> tuple[str, Path]:
        run_id = run_manager.create_run_dir(user_id, profile_key)
        return run_id, run_manager.get_run_dir(user_id, profile_key, run_id)

    return _make


# --------------------------------------------------------------------------- #
# Database — file-based SQLite, per TEST-STRATEGY 5.5
# --------------------------------------------------------------------------- #


@pytest.fixture
def sqlite_url(tmp_path: Path) -> str:
    """A fresh SQLite **file** per test.

    Not `:memory:`. `make_engine` passes `pool_size`/`max_overflow`/`pool_timeout`
    unconditionally and an in-memory URL gets a `SingletonThreadPool` that rejects all
    three, so collection dies before any test runs (backlog A5). A file URL gets a
    `QueuePool`. Switching to in-memory later also needs `poolclass=StaticPool` and
    `check_same_thread=False`, or the schema vanishes between connections.
    """
    return f"sqlite:///{tmp_path / 'test.db'}"


@pytest.fixture
def db_engine(sqlite_url: str, monkeypatch: pytest.MonkeyPatch):
    """A fresh engine on `tmp_path`, with `SessionLocal` rebound to it.

    The app has **no DB dependency injection** — routes use
    `with db_session() as db:`, a context manager, so `dependency_overrides` cannot
    reach it. `db_session()`, `run_db_with_retries()` and `get_db()` all resolve the
    module-global `SessionLocal` at call time, so rebinding that one name redirects
    every path.

    (That missing seam is itself an argument for the Slice 6 service layer: today the
    only way to point the app at a test database is to reach into a module global.)
    """
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    import app.db.models  # noqa: F401  -- registers tables on Base.metadata
    import app.db.session as session_module
    from app.db.base import Base

    engine = create_engine(sqlite_url)
    Base.metadata.create_all(engine)
    monkeypatch.setattr(
        session_module,
        "SessionLocal",
        sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False),
    )
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture
def db_session(db_engine) -> Iterator[Any]:
    from sqlalchemy.orm import Session

    with Session(db_engine) as session:
        yield session


# --------------------------------------------------------------------------- #
# HTTP client
# --------------------------------------------------------------------------- #


@pytest.fixture
def test_user(db_session):
    """A persisted user. `created_at` is explicit only to keep the row deterministic."""
    import uuid
    from datetime import UTC, datetime

    from app.db.models import User

    user = User(
        id=uuid.uuid4(),
        email="contract@example.com",  # not .test: EmailStr rejects reserved TLDs
        password_hash="not-a-real-hash",
        created_at=datetime.now(UTC),
    )
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)
    return user


@pytest.fixture
def other_user(db_session):
    """A second persisted user, for cross-tenant isolation (CP1-5).

    "Rejects a stranger" and "rejects a *logged-in* stranger" are different contracts and
    only the first was covered: the 401 sweep is exhaustive over all 24 protected routes,
    which made the second look covered too. Ownership was actually asserted for 3 routes,
    all of them filesystem-backed, using a forged `user_id` **string** — never a real
    `User` row, so no DB-backed route could be probed at all.

    This is a real row so that a resource can be *owned* by someone other than the caller.
    """
    import uuid
    from datetime import UTC, datetime

    from app.db.models import User

    user = User(
        id=uuid.uuid4(),
        email="stranger@example.com",  # not .test: EmailStr rejects reserved TLDs
        password_hash="not-a-real-hash",
        created_at=datetime.now(UTC),
    )
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)
    return user


@pytest.fixture
def file_profile_store(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Redirect the file-backed profile store into `tmp_path`.

    `app/config/profile_store.py` hardcodes `PROFILES_PATH = Path("config/focus_profiles.json")`
    — a **repo-relative** path. Without this, any test touching `/api/profile/*` writes
    into the working tree, and a test that fails between create and delete leaves the
    repo dirty. TEST-STRATEGY §6: no writes outside `tmp_path`.
    """
    from app.config import profile_store

    path = tmp_path / "config" / "focus_profiles.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(profile_store, "PROFILES_PATH", path)
    return path


@pytest.fixture
def client(
    db_session,
    test_user,
    run_output_root: Path,
    file_profile_store: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    """Authenticated `TestClient` via `dependency_overrides`.

    Overrides rather than a real login round-trip: a login per test would make every
    contract test depend on the auth implementation, which is exactly the internal
    coupling (flaw F1) this suite exists to avoid. `test_client_unauthed` covers the
    401 half of each contract.
    """
    from fastapi.testclient import TestClient

    from app.auth.deps import get_current_user
    from app.fastapi_run import app

    # Only auth is overridable — it is a real `Depends`. The DB is redirected by
    # `db_engine` rebinding `SessionLocal`, not through the dependency system.
    app.dependency_overrides[get_current_user] = lambda: test_user
    try:
        with TestClient(app) as c:
            yield c
    finally:
        app.dependency_overrides.clear()


@pytest.fixture
def client_unauthed(db_engine, run_output_root: Path, file_profile_store: Path):
    """Unauthenticated client — `get_current_user` is left in place so it 401s."""
    from fastapi.testclient import TestClient

    from app.fastapi_run import app

    app.dependency_overrides.clear()
    try:
        with TestClient(app) as c:
            yield c
    finally:
        app.dependency_overrides.clear()
