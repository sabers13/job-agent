"""Every route is accounted for, and every protected route really 401s.

CP-1 asks Chat to look for "routes silently missing from the 38". This file makes that
mechanical instead: the inventory below is compared against the live app, so adding a
route without a contract test fails here, and deleting one fails here too.

**42 user-defined routes**, not 38 — 38 declared in `app/fastapi_run.py` plus 4 mounted
from `app/api/auth_routes.py` under `/auth`. The checklist's "38" counts only the
god-module's own. All 42 are covered.

The 401 sweep is exhaustive by construction: it is parametrised from the *live*
dependency graph, so a route that quietly loses `Depends(get_current_user)` shows up as
an inventory mismatch rather than as a silent hole.
"""

from __future__ import annotations

import pytest

from tests.contracts.routes import (
    PUBLIC_ROUTES,
    live_protected_routes,
    live_public_routes,
    sample_path,
)

# Path params substituted with values that are syntactically valid but refer to nothing.
# A protected route must 401 *before* it ever looks anything up, so these never resolve.
PROTECTED_SAMPLES = [(method, sample_path(path)) for method, path in live_protected_routes()]


def test_inventory_matches_the_live_app() -> None:
    """The declared public set is exactly the live public set.

    Guards both directions: a new unauthenticated route, or an existing one silently
    losing its auth dependency, both fail here.
    """
    assert sorted(live_public_routes()) == sorted(PUBLIC_ROUTES)


def test_all_42_user_routes_are_accounted_for() -> None:
    total = len(live_protected_routes()) + len(live_public_routes())
    assert total == 42, f"route count changed: {total}"


@pytest.mark.parametrize(("method", "path"), PROTECTED_SAMPLES, ids=lambda v: str(v))
def test_protected_routes_require_authentication(client_unauthed, method: str, path: str) -> None:
    response = client_unauthed.request(method, path)

    assert response.status_code == 401, (
        f"{method} {path} returned {response.status_code}, expected 401. "
        "An unauthenticated caller must never reach the handler."
    )


@pytest.mark.parametrize(("method", "path"), PROTECTED_SAMPLES, ids=lambda v: str(v))
def test_protected_routes_reject_a_garbage_token(client_unauthed, method: str, path: str) -> None:
    response = client_unauthed.request(
        method, path, headers={"Authorization": "Bearer not-a-real-token"}
    )

    assert response.status_code == 401


def test_public_routes_do_not_401(client_unauthed) -> None:
    """Public routes may fail for their own reasons, but never for authentication.

    Recorded deliberately: `/bundle`, `/job_details`, `/search_stepstone*`,
    `/aggregate_report`, `/run_state` and `/playwright_check` are unauthenticated
    pipeline and network endpoints. That is the current contract, pinned here so a
    change is visible. Whether it *should* be the contract is a bucket-C question
    (backlog A12).
    """
    for method, path in live_public_routes():
        if path.startswith("/gui/"):
            continue  # HTML pages, exercised in test_gui_pages.py
        response = client_unauthed.request(method, sample_path(path), json={})
        assert response.status_code != 401, f"{method} {path} unexpectedly requires auth"
