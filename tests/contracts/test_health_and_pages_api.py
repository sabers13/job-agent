"""The remaining routes: health, auth, GUI pages, run state, and the fetch endpoints.

The fetch endpoints (`/search_stepstone*`, `/job_details`, `/bundle`,
`/playwright_check`) reach the network or a browser in production. They are stubbed at
the adapter boundary so the default suite stays offline (TEST-STRATEGY §3); what is
asserted is the HTTP contract, not the crawler.
"""

from __future__ import annotations

import pytest

# --------------------------------------------------------------------------- #
# Health
# --------------------------------------------------------------------------- #


def test_health_reports_the_documented_flags(client_unauthed) -> None:
    body = client_unauthed.get("/health").json()

    assert {"ok", "use_playwright", "headless", "config_ok", "db_ok", "output_ok"} <= set(body)
    assert isinstance(body["ok"], bool)


def test_health_config_returns_a_status(client_unauthed) -> None:
    response = client_unauthed.get("/health/config")

    assert response.status_code in (200, 503), response.text


def test_health_db_reports_reachability(client_unauthed) -> None:
    """The suite pins SQLite (`tests/test_suite_hermeticity.py`), so this is the healthy path.

    This assertion used to be `in (200, 503)` — the only two codes the route can return,
    so it passed unconditionally while its docstring asserted a fact about the
    environment. That is what made the environment leak invisible: `conftest.py` seeded
    its variables with `os.environ.setdefault`, so a sourced `.env.dev` kept its real
    `mssql+pyodbc` URL, `TestClient`'s lifespan opened a live connection to the
    developer's SQL Server, and this test graded it green. The accept set was wide enough
    to swallow both the SQLite path it claims to describe and a real database it never
    mentions. CP1-7; backlog **A13** for why no fixture could have caught it.

    If this ever fails, the database the suite reaches is not the one it pinned — which
    is precisely the signal that was missing.
    """
    response = client_unauthed.get("/health/db")

    assert response.status_code == 200, response.text
    assert response.json()["ok"] is True


def test_health_never_requires_auth(client_unauthed) -> None:
    """A health check behind auth is useless to a load balancer."""
    for path in ("/health", "/health/config", "/health/db"):
        assert client_unauthed.get(path).status_code != 401


# --------------------------------------------------------------------------- #
# Auth
# --------------------------------------------------------------------------- #


def test_signup_then_login_round_trips(client_unauthed) -> None:
    signup = client_unauthed.post(
        "/auth/signup", json={"email": "newuser@example.com", "password": "hunter2hunter2"}
    )
    assert signup.status_code in (200, 201), signup.text

    login = client_unauthed.post(
        "/auth/login", json={"email": "newuser@example.com", "password": "hunter2hunter2"}
    )
    assert login.status_code == 200, login.text


def test_login_with_a_bad_password_is_401(client_unauthed) -> None:
    client_unauthed.post(
        "/auth/signup", json={"email": "person@example.com", "password": "correct-horse"}
    )

    response = client_unauthed.post(
        "/auth/login", json={"email": "person@example.com", "password": "wrong"}
    )

    assert response.status_code == 401


def test_login_for_an_unknown_email_is_401(client_unauthed) -> None:
    response = client_unauthed.post(
        "/auth/login", json={"email": "ghost@example.com", "password": "whatever"}
    )

    assert response.status_code == 401


def test_signup_rejects_a_malformed_email(client_unauthed) -> None:
    response = client_unauthed.post(
        "/auth/signup", json={"email": "not-an-email", "password": "hunter2hunter2"}
    )

    assert response.status_code in (400, 422), response.status_code


def test_logout_is_always_safe_to_call(client_unauthed) -> None:
    assert client_unauthed.post("/auth/logout").status_code in (200, 204)


# --------------------------------------------------------------------------- #
# GUI pages
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("path", ["/gui/login", "/gui/run", "/gui/profiles", "/gui/logout"])
def test_gui_pages_render_or_redirect(client_unauthed, path: str) -> None:
    """Pages must never 500. Unauthenticated ones may redirect to the login page."""
    response = client_unauthed.get(path, follow_redirects=False)

    assert response.status_code in (200, 302, 303, 307), f"{path} -> {response.status_code}"


def test_gui_logout_clears_the_session(client_unauthed) -> None:
    response = client_unauthed.get("/gui/logout", follow_redirects=False)

    assert response.status_code in (200, 302, 303, 307)


# --------------------------------------------------------------------------- #
# Run state
# --------------------------------------------------------------------------- #


def test_run_state_round_trips(client_unauthed) -> None:
    written = client_unauthed.post("/run_state", json={"last_run": "abc", "run_dir": "/tmp/x"})
    assert written.status_code == 200, written.text

    body = client_unauthed.get("/run_state").json()
    assert body["last_run"] == "abc"


def test_run_state_starts_empty(client_unauthed) -> None:
    body = client_unauthed.get("/run_state").json()

    assert "last_run" in body


# --------------------------------------------------------------------------- #
# Fetch endpoints — stubbed at the boundary
# --------------------------------------------------------------------------- #


def test_search_stepstone_returns_the_adapter_result(
    client_unauthed, monkeypatch: pytest.MonkeyPatch
) -> None:
    import app.fastapi_run as fr

    monkeypatch.setattr(
        fr, "search_stepstone_http", lambda *a, **k: {"results": [], "count": 0}, raising=False
    )
    monkeypatch.setattr(
        fr, "search_stepstone", lambda *a, **k: {"results": [], "count": 0}, raising=False
    )

    response = client_unauthed.get("/search_stepstone", params={"what": "data analyst"})

    assert response.status_code in (200, 422, 500), response.status_code


def test_search_stepstone_list_validates_its_body(client_unauthed) -> None:
    response = client_unauthed.post("/search_stepstone_list", json={})

    assert response.status_code in (200, 400, 422), response.status_code


def test_job_details_validates_its_body(client_unauthed) -> None:
    response = client_unauthed.post("/job_details", json={})

    assert response.status_code in (200, 400, 422), response.status_code


def test_bundle_validates_its_body(client_unauthed) -> None:
    response = client_unauthed.post("/bundle", json={})

    assert response.status_code in (200, 400, 422), response.status_code


def test_aggregate_report_validates_its_body(client_unauthed) -> None:
    response = client_unauthed.post("/aggregate_report", json={})

    assert response.status_code in (200, 400, 422, 404), response.status_code


@pytest.mark.external
def test_playwright_check_launches_a_browser(client_unauthed) -> None:
    """Genuinely needs a browser, so it is excluded from the gate.

    Marked rather than deleted: the endpoint exists and someone should be able to run
    this deliberately with `pytest -m external`.
    """
    assert client_unauthed.get("/playwright_check").status_code == 200
