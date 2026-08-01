"""Route inventory, derived from the live app.

Not a test module. `test_route_inventory.py` compares the declared `PUBLIC_ROUTES`
against what the app actually exposes, and parametrises the 401 sweep from the live
protected set — so the sweep cannot fall behind the app.

The declared list exists so that a *change* is a failing test rather than a silently
shrinking parametrisation: deriving both sides from the app would make the sweep pass
happily after someone deleted the auth dependency from every route.
"""

from __future__ import annotations

BUILTIN_PATHS = {"/openapi.json", "/docs", "/docs/oauth2-redirect", "/redoc"}

# Concrete stand-ins for path parameters. Deliberately non-existent: a protected route
# must reject an unauthenticated caller before it looks anything up.
PATH_PARAM_SAMPLES = {
    "{run_id}": "no-such-run",
    "{job_key}": "no-such-job",
    "{key}": "no-such-profile",
    "{profile_key}": "no-such-profile",
    "{resume_id}": "00000000-0000-0000-0000-000000000000",
}

# The 18 routes that are reachable without authentication. Kept as a literal so that a
# route gaining or losing auth fails `test_inventory_matches_the_live_app`.
PUBLIC_ROUTES: list[tuple[str, str]] = [
    ("POST", "/aggregate_report"),
    ("POST", "/auth/login"),
    ("POST", "/auth/logout"),
    ("POST", "/auth/signup"),
    ("POST", "/bundle"),
    ("GET", "/gui/login"),
    ("GET", "/gui/logout"),
    ("GET", "/gui/profiles"),
    ("GET", "/gui/run"),
    ("GET", "/health"),
    ("GET", "/health/config"),
    ("GET", "/health/db"),
    ("POST", "/job_details"),
    ("GET", "/playwright_check"),
    ("GET", "/run_state"),
    ("POST", "/run_state"),
    ("GET", "/search_stepstone"),
    ("POST", "/search_stepstone_list"),
]


def sample_path(path: str) -> str:
    """Substitute every `{param}` with a concrete, non-existent value."""
    for placeholder, value in PATH_PARAM_SAMPLES.items():
        path = path.replace(placeholder, value)
    return path


def _iter_routes():
    from app.fastapi_run import app

    for route in app.routes:
        methods = getattr(route, "methods", None)
        path = getattr(route, "path", None)
        if not methods or path is None or path in BUILTIN_PATHS:
            continue
        yield sorted(methods - {"HEAD", "OPTIONS"})[0], path, route


def _requires_auth(route) -> bool:
    from app.auth.deps import get_current_user

    dependant = getattr(route, "dependant", None)
    if dependant is None:
        return False

    def walk(dep):
        yield dep.call
        for sub in dep.dependencies:
            yield from walk(sub)

    return get_current_user in set(walk(dependant))


def live_protected_routes() -> list[tuple[str, str]]:
    return sorted((method, path) for method, path, route in _iter_routes() if _requires_auth(route))


def live_public_routes() -> list[tuple[str, str]]:
    return sorted(
        (method, path) for method, path, route in _iter_routes() if not _requires_auth(route)
    )
