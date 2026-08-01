"""The suite owns its environment.

`pytest -q` must mean the same thing in every shell. AGENTS.md §Commands documents
`set -a; source .env.dev; set +a` as the normal working environment, and `.env.dev`
carries a real `mssql+pyodbc://` URL, a real output directory and a Prefect API URL.
While `tests/conftest.py` seeded its variables with `os.environ.setdefault`, all of
those survived into the test run.

That is not a cosmetic leak. `app/db/health.py` binds `SessionLocal` at import, so the
`db_engine` fixture's monkeypatch never reaches `check_db`, and `check_db` is what
`TestClient`'s lifespan calls via `_startup_checks`. With `.env.dev` sourced, the suite
that TEST-STRATEGY §2 calls "offline, no DB container" opened a live connection to the
developer's SQL Server on every client fixture. Whether that presented as a pass or a
hang was a property of the machine, not the code: container up, `/health/db` graded a
real database and went green; container down, the run blocked on the ODBC login
timeout. An oracle that grades a different system depending on who runs it is not an
oracle.

The assertions come in two layers, and the second is the one that matters:

1. The six variables hold the values `conftest` assigned. Not a tautology — it is
   exactly the assertion that reverting to `setdefault` breaks, and it breaks only when
   an ambient value exists to win, which is the case this file exists to cover.
2. The engine the app *actually reaches* is SQLite, and the feature flags the app
   *actually reads* are off. A test that only re-read `os.environ` would restate
   `conftest` instead of checking it; these go through `app.config` and `app.db` the
   way production code does.

Deliberately not covered: `JOBAGENT_OUTPUT_DIR`, which `.env.dev` also sets. It is
outside the six and is neutralised per-test by the `run_output_root` fixture rather
than at import. Noted here so the gap is visible rather than assumed closed.
"""

from __future__ import annotations

import os

import pytest

from tests.conftest import TEST_ENV

WHY = "the suite must own its environment; see this module's docstring"


@pytest.mark.parametrize("name", sorted(TEST_ENV))
def test_the_suite_owns_its_environment_variables(name: str) -> None:
    """Each variable holds conftest's value, not the ambient shell's.

    Parametrised per variable so a failure names the leaking one instead of reporting
    that "the environment differs".
    """
    # Compare into a bool *before* asserting. pytest rewrites assertions to print
    # every operand, and the ambient values these guard against are a DB URL with an
    # embedded password and a JWT signing secret. Asserting on `os.environ[name]`
    # directly dumps both into the failure output, i.e. into CI logs -- for a test
    # whose whole subject is credentials leaking in from the environment.
    ambient = os.environ.get(name)
    overridden = ambient == TEST_ENV[name]

    assert overridden, (
        f"{name} was not overridden by tests/conftest.py -- {WHY}. An ambient value "
        f"({len(ambient or '')} chars, redacted) won: a sourced .env.dev, or .env via "
        f"settings' load_dotenv."
    )


def test_the_database_the_app_reaches_is_sqlite() -> None:
    """The engine bound at import — the one `check_db` captured and no fixture can move."""
    from sqlalchemy.engine import Engine

    import app.db.health as health

    bind = health.SessionLocal().get_bind()
    assert isinstance(bind, Engine), f"expected an Engine, got {type(bind).__name__}"

    # Only the drivername crosses into the assertion. `URL.__repr__` masks the
    # password component but not query parameters, and the mssql+pyodbc URL carries
    # its credentials inside `odbc_connect`.
    drivername = bind.url.drivername

    assert drivername.startswith("sqlite"), (
        f"`check_db` would talk to {drivername!r}, not SQLite -- {WHY}. "
        f"This is the path `db_engine`'s monkeypatch cannot reach, because "
        f"`app/db/health.py` binds `SessionLocal` at import."
    )


def test_settings_agree_that_this_is_an_offline_test_run() -> None:
    """The config layer's view, which is what feature code reads."""
    from app.config.settings import settings

    assert settings.database_url is not None
    scheme = settings.database_url.split("://", 1)[0]  # never assert on the full URL
    assert scheme.startswith("sqlite"), f"settings.database_url is {scheme!r} -- {WHY}"
    assert settings.env == "test", f"settings.env is {settings.env!r} -- {WHY}"

    # `use_playwright_default` defaults to **True** in settings.py, so a leak here does
    # not merely mis-grade: it launches a browser from a suite that must not.
    assert settings.use_playwright_default is False, f"Playwright is enabled -- {WHY}"
    assert settings.use_llm_scoring is False, f"LLM scoring is enabled -- {WHY}"
