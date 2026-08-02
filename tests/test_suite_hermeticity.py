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

The assertions come in three layers, and the second and third are the ones that matter:

1. The six variables hold the values `conftest` assigned. Not a tautology — it is
   exactly the assertion that reverting to `setdefault` breaks, and it breaks only when
   an ambient value exists to win, which is the case this file exists to cover.
2. The engine the app *actually reaches* is SQLite, and the feature flags the app
   *actually reads* are off. A test that only re-read `os.environ` would restate
   `conftest` instead of checking it; these go through `app.config` and `app.db` the
   way production code does.
3. **The network is refused.** Added for CP1-8, and the reason it had to be added is
   the point: this file was written to make `conftest`'s "Offline and deterministic.
   No live network, no DB container, no Playwright, no LLM" executable, and it covered
   three of those four. The database half was closed because CP1-7 produced a failing
   symptom that pointed at it. Nobody generalised, so the network half stayed prose —
   and `/search_stepstone` reached `https://www.stepstone.de/en/` on every gate run,
   from two tests, both of which passed either way.

   §3 therefore asserts the *guard* rather than the absence of traffic. "No test
   happened to connect today" is a property of the tests that exist; "a connection is
   refused, and a refusal fails the test even when an `except Exception:` eats it" is a
   property of the suite, and it is the one that holds for tests not yet written.

Deliberately not covered: `JOBAGENT_OUTPUT_DIR`, which `.env.dev` also sets. It is
outside the six and is neutralised per-test by the `run_output_root` fixture rather
than at import. Noted here so the gap is visible rather than assumed closed.

Also deliberately not covered: loopback. The guard permits it — see `tests/net_guard.py`
for why — so §3 pins that boundary as a decision instead of leaving the next reader to
discover it from a passing connection.
"""

from __future__ import annotations

import os
import socket

import pytest

from tests import net_guard
from tests.conftest import TEST_ENV

WHY = "the suite must own its environment; see this module's docstring"

# RFC 5737 TEST-NET-1 and RFC 2606's reserved TLD. Both are guaranteed never to route
# or resolve to a real host, so if the guard is ever removed these probes fail by
# timing out rather than by quietly reaching somebody's server.
BLACKHOLE_IP = "192.0.2.1"
BLACKHOLE_HOST = "hermeticity-probe.invalid"


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


# --------------------------------------------------------------------------- #
# 3. The network — CP1-8
#
# These probes trip the guard deliberately, so each clears the record it leaves
# behind: `conftest` checks the record at the end of the call phase, and would
# otherwise report a test that proved the guard works as a test that leaked.
# `net_guard.refuses_connection` does both halves.
# --------------------------------------------------------------------------- #

NET_WHY = "the suite must be offline; see this module's docstring and tests/net_guard.py"


def test_the_network_guard_is_installed() -> None:
    """The autouse fixture actually rebound the socket layer.

    Asserted separately from the behavioural probes below so that removing the fixture
    fails with "the guard is not installed" rather than with three timeouts.
    """
    patched = {
        "socket.connect": getattr(socket.socket.connect, "__qualname__", ""),
        "socket.connect_ex": getattr(socket.socket.connect_ex, "__qualname__", ""),
        "getaddrinfo": getattr(socket.getaddrinfo, "__qualname__", ""),
    }
    unguarded = sorted(name for name, qualname in patched.items() if "guarded_" not in qualname)

    assert not unguarded, f"{unguarded} still resolve to the real socket layer -- {NET_WHY}"


def test_an_outbound_connection_is_refused() -> None:
    """The base case: a raw socket to a routable-looking address off this machine."""
    with net_guard.refuses_connection(BLACKHOLE_IP):
        socket.create_connection((BLACKHOLE_IP, 443), timeout=1.0)


def test_resolving_a_remote_name_is_refused() -> None:
    """DNS is egress too, and it happens before any `connect`.

    A lookup that leaves the machine has already told a resolver what the suite is
    doing, and refusing at `connect` alone would name an IP in the failure instead of
    the host — which is the part a reader can act on.
    """
    with net_guard.refuses_connection(BLACKHOLE_HOST):
        socket.getaddrinfo(BLACKHOLE_HOST, 443)


def test_the_http_stack_the_app_uses_cannot_reach_out() -> None:
    """`httpx`, not just `socket` — the guard has to hold under the app's own client.

    `app/fetching/http_client.py` fetches through `httpx`, which owns its connection
    pool and maps low-level errors into its own exception hierarchy. Whatever it
    chooses to raise, what matters is that the guard refused and recorded the target,
    so this asserts on the record rather than on httpx's exception type.
    """
    import httpx

    try:
        httpx.get(f"https://{BLACKHOLE_HOST}/", timeout=1.0)
    except Exception:  # noqa: BLE001 -- httpx wraps transport errors; the record is the assertion
        pass
    refused = [attempt.target for attempt in net_guard.attempts()]
    net_guard.reset()

    assert refused == [BLACKHOLE_HOST], f"httpx reached past the guard: {refused} -- {NET_WHY}"


def test_a_refusal_swallowed_by_a_broad_handler_still_fails_the_phase() -> None:
    """The property that closes the category, rather than one more instance of it.

    CP1-8's route is `except Exception: raise HTTPException(500)` and 78 such handlers
    exist (backlog A3), so a guard that only raises is absorbed by the code it polices
    and re-emitted as a status code some accept set already permits. The refusal is
    therefore recorded as well, and `conftest` checks the record at a point no `except`
    clause in `app/` can reach.

    Written as the swallow itself: catch the refusal exactly as the handler does, then
    assert the phase check still fires.
    """
    try:
        socket.create_connection((BLACKHOLE_IP, 443), timeout=1.0)
    except Exception:  # noqa: BLE001 -- deliberately the shape of app/fastapi_run.py:397
        pass

    with pytest.raises(net_guard.NetworkAccessAttempted, match="refusal was swallowed"):
        net_guard.check_and_reset("this test")


def test_the_phase_check_is_wired_into_every_test(request: pytest.FixtureRequest) -> None:
    """The record is only worth keeping if something reads it.

    `test_a_refusal_swallowed_by_a_broad_handler_still_fails_the_phase` proves
    `check_and_reset` raises; this proves it is *reached*. Delete the hooks from
    `conftest` and every other test in this section still passes while leaks go back to
    being invisible — which is the CP1-8 failure repeating one level up.

    Structural rather than end-to-end, and stated as such: it asserts the three hooks are
    registered as wrappers and that each one names `check_and_reset`. Proving the call
    itself would need a nested pytest run, which is not worth a `pytester` dependency for
    the increment.
    """
    import tests.conftest as conftest_module

    for phase in ("pytest_runtest_setup", "pytest_runtest_call", "pytest_runtest_teardown"):
        hook = getattr(request.config.pluginmanager.hook, phase)
        ours = [impl for impl in hook.get_hookimpls() if impl.plugin is conftest_module]

        assert len(ours) == 1, f"{phase} is not hooked by tests/conftest.py -- {NET_WHY}"
        assert ours[0].wrapper, (
            f"{phase}'s hook is not a wrapper, so it cannot see the phase result"
        )
        assert "check_and_reset" in ours[0].function.__code__.co_names, (
            f"{phase}'s hook no longer consults the guard's record -- {NET_WHY}"
        )


def test_loopback_is_deliberately_allowed() -> None:
    """The guard's boundary, pinned so it reads as a decision and not an oversight.

    Egress is what makes a gate machine-dependent. Loopback does not: a local database
    is already pinned by `test_the_database_the_app_reaches_is_sqlite`, and `-m external`
    needs a loopback port to talk to Playwright's browser. Binding a listener here rather
    than probing a closed port keeps the test deterministic — it must not depend on what
    happens to be listening on this machine.
    """
    with socket.socket() as server:
        server.bind(("127.0.0.1", 0))
        server.listen(1)
        with socket.create_connection(server.getsockname(), timeout=1.0):
            pass

    assert not net_guard.attempts(), f"loopback was refused -- {NET_WHY}"
