"""Profile endpoints — response shape and the create/update distinction.

Two parallel profile surfaces exist today: `/api/my/profile*` (DB-backed, user-scoped)
and `/api/profile*` (file-backed store). Both are routed and both are covered, because
this suite has to pass unchanged through Slice 6b — and D1 has decided the DB wins, so
the *file*-backed contract is the one that will change later. Pinning it now means the
change shows up as a deliberate test edit rather than a silent behaviour drift.
"""

from __future__ import annotations

import json

FOCUS_CONFIG = {
    "profile_name": "Contract Profile",
    "target_seniority": "junior",
    "titles_any": ["Data Analyst"],
    "include_skills_any": ["SQL"],
    "locations_any": ["Berlin"],
}


# --------------------------------------------------------------------------- #
# Identity
# --------------------------------------------------------------------------- #


def test_me_returns_the_authenticated_user(client, test_user) -> None:
    body = client.get("/api/my/me").json()

    assert body["user_id"] == str(test_user.id)
    assert body["email"] == test_user.email


def test_auth_me_returns_the_authenticated_user(client, test_user) -> None:
    response = client.get("/auth/me")

    assert response.status_code == 200
    assert str(test_user.id) in response.text


# --------------------------------------------------------------------------- #
# DB-backed profiles
# --------------------------------------------------------------------------- #


def test_my_profiles_is_an_envelope_not_a_bare_list(client) -> None:
    """`{"profiles": [...]}`, not `[...]`. Pinned because a client parses one or the other."""
    body = client.get("/api/my/profiles").json()

    assert isinstance(body, dict)
    assert isinstance(body["profiles"], list)


def test_upsert_then_read_round_trips(client) -> None:
    created = client.post(
        "/api/my/profile",
        json={
            "profile_key": "contract",
            "profile_name": "Contract Profile",
            "description": "made by a contract test",
            "focus_config_json": json.dumps(FOCUS_CONFIG),
        },
    )
    assert created.status_code == 200, created.text

    fetched = client.get("/api/my/profile/contract")
    assert fetched.status_code == 200
    assert fetched.json()["profile_key"] == "contract"


def test_upsert_reports_created_then_updated(client) -> None:
    """The `X-Upsert-Action` header is the only way a caller can tell the two apart.

    Same URL, same 200, different meaning — so the header is the contract.
    """
    payload = {
        "profile_key": "hdr",
        "profile_name": "Header Probe",
        "focus_config_json": json.dumps(FOCUS_CONFIG),
    }

    first = client.post("/api/my/profile", json=payload)
    second = client.post("/api/my/profile", json=payload)

    assert first.headers.get("X-Upsert-Action") == "created"
    assert second.headers.get("X-Upsert-Action") == "updated"


def test_upsert_by_key_matches_the_plural_form(client) -> None:
    response = client.post(
        "/api/my/profile/by-key",
        json={
            "profile_name": "By Key",
            "focus_config_json": json.dumps(FOCUS_CONFIG),
        },
    )

    assert response.status_code == 200, response.text
    assert client.get("/api/my/profile/by-key").status_code == 200


def test_created_profile_appears_in_the_list(client) -> None:
    client.post(
        "/api/my/profile",
        json={
            "profile_key": "listed",
            "profile_name": "Listed",
            "focus_config_json": json.dumps(FOCUS_CONFIG),
        },
    )

    listed = client.get("/api/my/profiles").json()["profiles"]
    keys = [item.get("profile_key") or item.get("key") for item in listed]

    assert "listed" in keys


def test_delete_removes_the_profile(client) -> None:
    client.post(
        "/api/my/profile",
        json={
            "profile_key": "doomed",
            "profile_name": "Doomed",
            "focus_config_json": json.dumps(FOCUS_CONFIG),
        },
    )

    assert client.delete("/api/my/profile/doomed").status_code == 200
    assert client.get("/api/my/profile/doomed").status_code == 404


def test_unknown_profile_is_404(client) -> None:
    assert client.get("/api/my/profile/never-created").status_code == 404


def test_latest_for_a_profile_with_no_runs_is_404(client) -> None:
    assert client.get("/api/my/profile/never-created/latest").status_code == 404


# --------------------------------------------------------------------------- #
# File-backed profiles  (superseded by D1; pinned so the change is visible)
# --------------------------------------------------------------------------- #


def test_profiles_listing_has_the_documented_item_shape(client) -> None:
    body = client.get("/api/profiles").json()

    assert isinstance(body["profiles"], list)
    for item in body["profiles"]:
        assert {"key", "profile_name", "description"} <= set(item)


def test_file_backed_profile_round_trips(client) -> None:
    written = client.post("/api/profile/contract-file", json=FOCUS_CONFIG)
    assert written.status_code == 200, written.text

    fetched = client.get("/api/profile/contract-file")
    assert fetched.status_code == 200

    assert client.delete("/api/profile/contract-file").status_code == 200
