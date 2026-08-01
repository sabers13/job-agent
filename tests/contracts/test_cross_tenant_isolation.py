"""Every DB-backed protected route, probed as the wrong tenant. CP1-5.

The 401 sweep in `test_route_inventory.py` is exhaustive over all 24 protected routes, so
the auth story *reads* as complete. It is not: "rejects a stranger" and "rejects a
**logged-in** stranger" are different properties, and only the first was asserted.

Ownership was covered for exactly three routes — `/api/run_status`, `/api/run_logs`,
`/api/run_summary` — and only because those compare a `user_id` recorded in `status.json`,
which a test can forge as a bare string. No second `User` row existed anywhere in the
suite, so no route backed by a real table could be probed at all. That is the hole this
file closes.

**The contract is 404, not 403.** A stranger must not be able to distinguish "this
resource is not yours" from "this resource does not exist" — 403 confirms the id is real.
The three filesystem-backed routes already got this right; these assert the same shape.

**Read the upsert cases carefully.** `POST /api/my/profile` and `POST /api/my/profile/{key}`
return **200 by design** when the caller names a key another user owns, because they
create a row for the *caller*. A 200 there is correct behaviour; the leak would be the
write landing on the other tenant's row. Those two are therefore asserted on the stored
state rather than on the status code — a status-code sweep alone would have to either
skip them or assert something false about them.
"""

from __future__ import annotations

import json
import uuid
from typing import Any

import pytest

FOCUS_CONFIG = {
    "profile_name": "Stranger's Profile",
    "target_seniority": "junior",
    "titles_any": ["Data Analyst"],
    "include_skills_any": ["SQL"],
    "locations_any": ["Berlin"],
}

VICTIM_PROFILE_KEY = "stranger-profile"


# --------------------------------------------------------------------------- #
# Resources owned by `other_user`
# --------------------------------------------------------------------------- #


def _seed_profile(db_session, owner) -> Any:
    """A DB profile owned by `owner`, created through the app's own CRUD."""
    from app.db.crud_profiles import upsert_profile_for_user

    profile = upsert_profile_for_user(
        db=db_session,
        user_id=owner.id,
        profile_key=VICTIM_PROFILE_KEY,
        profile_name="Stranger's Profile",
        description="seeded by the cross-tenant probe",
        profile_json=FOCUS_CONFIG,
    )
    db_session.commit()
    db_session.refresh(profile)
    return profile


def _seed_resume(db_session, owner, root) -> Any:
    """A résumé row owned by `owner`.

    Inserted directly rather than uploaded: the upload route derives `user_id` from the
    authenticated caller, so there is no way to *upload* a file as someone else — which
    is itself the property under test one layer down.
    """
    from datetime import UTC, datetime

    from app.db.models import Resume

    path = root / str(owner.id) / "_resumes" / "seeded.txt"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("A CV. SQL and Python.", encoding="utf-8")

    resume = Resume(
        id=uuid.uuid4(),
        user_id=owner.id,
        filename="seeded.txt",
        mime_type="text/plain",
        sha256="0" * 64,
        storage_path=str(path),
        text_content="A CV. SQL and Python.",
        is_active=True,
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )
    db_session.add(resume)
    db_session.commit()
    db_session.refresh(resume)
    return resume


def _seed_run(owner) -> str:
    """A completed run owned by `owner`, with a potential-application artifact.

    The artifact matters: `/api/run_artifacts/.../potential_applications` returns an empty
    envelope rather than 404 when a run has no `potential_applications/` directory, so a
    run without one would pass the sweep without the ownership check ever being reached.
    """
    from app.gui_runs import run_manager

    user_id = str(owner.id)
    run_id = run_manager.create_run_dir(user_id, VICTIM_PROFILE_KEY)
    run_dir = run_manager.get_run_dir(user_id, VICTIM_PROFILE_KEY, run_id)

    (run_dir / "run.log").write_text("a log line\n", encoding="utf-8")
    (run_dir / "summary.json").write_text(json.dumps({"accepted": 1}), encoding="utf-8")

    job_dir = run_dir / "potential_applications" / "seeded_job"
    job_dir.mkdir(parents=True, exist_ok=True)
    (job_dir / "potential_reason.json").write_text(
        json.dumps({"reason": "seeded", "final_score": 65.0, "llm_score": 80.0}),
        encoding="utf-8",
    )

    run_manager.write_status(
        run_id,
        {
            "run_id": run_id,
            "user_id": user_id,
            "profile_key": VICTIM_PROFILE_KEY,
            "status": "completed",
            "output_root": str(run_dir),
        },
    )
    return run_id


@pytest.fixture
def other_profile(db_session, other_user):
    return _seed_profile(db_session, other_user)


@pytest.fixture
def other_resume(db_session, other_user, run_output_root):
    return _seed_resume(db_session, other_user, run_output_root)


@pytest.fixture
def other_run(other_user, run_output_root) -> str:
    return _seed_run(other_user)


# --------------------------------------------------------------------------- #
# The sweep — every DB-backed protected route, as the wrong tenant
# --------------------------------------------------------------------------- #
#
# `client` is authenticated as `test_user`. Every request below names a resource owned by
# `other_user`, so each must 404. Split into small tests rather than one parametrised
# sweep because the seeding fixtures differ per resource, and a `request.getfixturevalue`
# indirection would cost more legibility than the parametrisation saves.


def test_a_stranger_cannot_read_another_users_profile(client, other_profile) -> None:
    response = client.get(f"/api/my/profile/{VICTIM_PROFILE_KEY}")

    assert response.status_code == 404, response.text


def test_a_stranger_cannot_delete_another_users_profile(client, db_session, other_profile) -> None:
    """404 *and* the row survives. The status code alone would not prove the second."""
    from app.db.crud_profiles import get_profile_for_user

    response = client.delete(f"/api/my/profile/{VICTIM_PROFILE_KEY}")

    assert response.status_code == 404, response.text
    db_session.expire_all()
    survivor = get_profile_for_user(db_session, other_profile.user_id, VICTIM_PROFILE_KEY)
    assert survivor is not None, "a stranger's DELETE removed another user's profile"


def test_a_stranger_cannot_read_another_users_latest_run(client, other_user, other_run) -> None:
    """`latest.json` is keyed by user id on the filesystem, so the path must not resolve."""
    from app.gui_runs import run_manager

    run_manager.write_latest(
        str(other_user.id), VICTIM_PROFILE_KEY, {"run_id": other_run, "status": "completed"}
    )

    response = client.get(f"/api/my/profile/{VICTIM_PROFILE_KEY}/latest")

    assert response.status_code == 404, response.text


def test_a_stranger_cannot_start_maintenance_on_another_users_profile(
    client, other_profile
) -> None:
    """A 200 here would not just leak — it would spend the owner's crawl budget."""
    response = client.post(
        f"/api/my/profile/{VICTIM_PROFILE_KEY}/url_pool/prune_stepstone",
        json={"max_urls": 1, "concurrency": 1, "timeout_sec": 2.0},
    )

    assert response.status_code == 404, response.text


def test_a_stranger_cannot_read_another_users_resume(client, other_resume) -> None:
    response = client.get(f"/api/my/resume/{other_resume.id}")

    assert response.status_code == 404, response.text


def test_a_stranger_cannot_activate_another_users_resume(client, db_session, other_resume) -> None:
    """404 *and* `is_active` is untouched — activation is a write, so check the write."""
    from app.db.models import Resume

    response = client.post(f"/api/my/resume/{other_resume.id}/activate")

    assert response.status_code == 404, response.text
    db_session.expire_all()
    row = db_session.get(Resume, other_resume.id)
    assert row.is_active is True, "a stranger's activate flipped another user's résumé"


def test_a_stranger_cannot_read_another_users_run_status(client, other_run) -> None:
    assert client.get(f"/api/run_status/{other_run}").status_code == 404


def test_a_stranger_cannot_read_another_users_run_logs(client, other_run) -> None:
    assert client.get(f"/api/run_logs/{other_run}").status_code == 404


def test_a_stranger_cannot_read_another_users_run_summary(client, other_run) -> None:
    assert client.get(f"/api/run_summary/{other_run}").status_code == 404


def test_a_stranger_cannot_list_another_users_potential_applications(client, other_run) -> None:
    response = client.get(f"/api/run_artifacts/{other_run}/potential_applications")

    assert response.status_code == 404, response.text


def test_a_stranger_cannot_read_another_users_potential_application(client, other_run) -> None:
    response = client.get(f"/api/run_artifacts/{other_run}/potential_applications/stranger_job")

    assert response.status_code == 404, response.text


# --------------------------------------------------------------------------- #
# The two upserts — 200 is correct, so assert on the stored state
# --------------------------------------------------------------------------- #


def _stored_profile_name(db_session, user_id: uuid.UUID, key: str) -> Any:
    from app.db.crud_profiles import get_profile_for_user

    db_session.expire_all()
    profile = get_profile_for_user(db_session, user_id, key)
    return None if profile is None else profile.profile_name


def test_upserting_a_key_another_user_owns_creates_a_second_row(
    client, db_session, test_user, other_user, other_profile
) -> None:
    """Same `profile_key`, two owners, two rows. The 200 is the contract, not a leak.

    `(user_id, profile_key)` is the identity — a key is only unique *within* a tenant. So
    the assertion that matters is that the caller's write landed on the caller's row and
    the other tenant's row still says what it said.
    """
    response = client.post(
        "/api/my/profile",
        json={
            "profile_key": VICTIM_PROFILE_KEY,
            "profile_name": "Mine Now",
            "focus_config_json": json.dumps(FOCUS_CONFIG),
        },
    )

    assert response.status_code == 200, response.text
    assert _stored_profile_name(db_session, test_user.id, VICTIM_PROFILE_KEY) == "Mine Now"
    assert _stored_profile_name(db_session, other_user.id, VICTIM_PROFILE_KEY) == (
        "Stranger's Profile"
    ), "an upsert overwrote another user's profile of the same key"


def test_upserting_by_key_does_not_overwrite_another_users_profile(
    client, db_session, test_user, other_user, other_profile
) -> None:
    """The `/{key}` form of the same upsert. Both routes exist; both need the check."""
    response = client.post(
        f"/api/my/profile/{VICTIM_PROFILE_KEY}",
        json={"profile_name": "Mine Now Too", "focus_config_json": json.dumps(FOCUS_CONFIG)},
    )

    assert response.status_code == 200, response.text
    assert _stored_profile_name(db_session, test_user.id, VICTIM_PROFILE_KEY) == "Mine Now Too"
    assert _stored_profile_name(db_session, other_user.id, VICTIM_PROFILE_KEY) == (
        "Stranger's Profile"
    ), "an upsert overwrote another user's profile of the same key"


# --------------------------------------------------------------------------- #
# Listings must not leak either
# --------------------------------------------------------------------------- #


def test_profile_listing_shows_only_the_callers_profiles(client, other_profile) -> None:
    """A scoped detail route is worth little if the listing enumerates every tenant."""
    listed = client.get("/api/my/profiles").json()["profiles"]

    keys = [item.get("profile_key") or item.get("key") for item in listed]
    assert VICTIM_PROFILE_KEY not in keys, f"another user's profile appeared in the listing: {keys}"


def test_resume_listing_shows_only_the_callers_resumes(client, other_resume) -> None:
    listing = client.get("/api/my/resumes")

    assert listing.status_code == 200, listing.text
    assert str(other_resume.id) not in listing.text, "another user's résumé appeared in the listing"


# --------------------------------------------------------------------------- #
# Positive controls — the 404s above must come from ownership, not from breakage
# --------------------------------------------------------------------------- #
#
# Without these the whole file is worthless. A 404 raised because a seeding helper wrote
# to the wrong path, or because a route was renamed, is indistinguishable from a 404
# raised by a working ownership check — and the file would report "isolation verified"
# while proving only that the URLs 404. Each control below runs the *same* `_seed_*`
# helper against `test_user` and asserts 200, so every negative above is pinned to the
# one variable that changed: who owns the resource.


def test_control_the_caller_can_read_their_own_profile(client, db_session, test_user) -> None:
    _seed_profile(db_session, test_user)

    assert client.get(f"/api/my/profile/{VICTIM_PROFILE_KEY}").status_code == 200


def test_control_the_caller_can_read_their_own_resume(
    client, db_session, test_user, run_output_root
) -> None:
    resume = _seed_resume(db_session, test_user, run_output_root)

    assert client.get(f"/api/my/resume/{resume.id}").status_code == 200


def test_control_the_caller_can_read_their_own_run(client, test_user) -> None:
    run_id = _seed_run(test_user)

    assert client.get(f"/api/run_status/{run_id}").status_code == 200


def test_control_the_caller_can_read_their_own_potential_applications(client, test_user) -> None:
    """Also proves the seeded artifact is shaped well enough to be found and returned."""
    run_id = _seed_run(test_user)

    listed = client.get(f"/api/run_artifacts/{run_id}/potential_applications")
    assert listed.status_code == 200, listed.text
    assert listed.json()["count"] == 1, listed.text

    detail = client.get(f"/api/run_artifacts/{run_id}/potential_applications/seeded_job")
    assert detail.status_code == 200, detail.text


def test_control_the_caller_can_read_their_own_latest(client, test_user) -> None:
    from app.gui_runs import run_manager

    run_id = _seed_run(test_user)
    run_manager.write_latest(
        str(test_user.id), VICTIM_PROFILE_KEY, {"run_id": run_id, "status": "completed"}
    )

    assert client.get(f"/api/my/profile/{VICTIM_PROFILE_KEY}/latest").status_code == 200
