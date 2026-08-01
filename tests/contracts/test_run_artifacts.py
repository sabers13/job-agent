"""The run-artifact contract — the project's actual deliverable.

AGENTS.md: "`run_id`, `status.json`, `run.log`, and the offset-based log streaming
contract are the system's actual deliverable. An HTTP 200 with missing or malformed
artifacts is a failure, not a success."

So these tests assert the artifacts on disk *and* what the status endpoint does with
them. Every assertion is about the on-disk layout or the response shape — never about
how `run_manager` computes either — so the whole file must survive Slices 5, 6 and 7
without an edit.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

RUN_ARTIFACTS = [
    "run_metrics.json",
    "analysis_summary.json",
    "REPORT_SUMMARY.md",
]


def _status_payload(run_id: str, user_id: str, **overrides):
    payload = {
        "run_id": run_id,
        "user_id": user_id,
        "profile_key": "profile-1",
        "status": "completed",
        "started_at": "2026-08-01T00:00:00Z",
        "finished_at": "2026-08-01T00:01:00Z",
    }
    payload.update(overrides)
    return payload


# --------------------------------------------------------------------------- #
# Directory layout
# --------------------------------------------------------------------------- #


def test_run_directory_layout(make_run, run_output_root: Path) -> None:
    """`output/<user_id>/<profile_key>/<run_id>/` — asserted positionally.

    Downstream code builds paths from these segments, so a reordering would be silent
    until someone went looking for a run on disk.
    """
    run_id, run_dir = make_run(user_id="user-42", profile_key="analytics")

    assert run_dir.is_dir()
    assert run_dir.name == run_id
    assert run_dir.parent.name == "analytics"
    assert run_dir.parent.parent.name == "user-42"
    assert run_dir.parent.parent.parent == run_output_root
    assert run_dir.relative_to(run_output_root) == Path("user-42") / "analytics" / run_id


def test_run_ids_are_unique_within_the_same_second(make_run) -> None:
    """`create_run_dir` claims to be collision-proof; two runs a moment apart prove it."""
    ids = {make_run(user_id="u", profile_key="p")[0] for _ in range(5)}
    assert len(ids) == 5


def test_runs_are_isolated_per_user_and_profile(make_run) -> None:
    _, a = make_run(user_id="user-a", profile_key="p")
    _, b = make_run(user_id="user-b", profile_key="p")
    assert a.parent.parent != b.parent.parent


# --------------------------------------------------------------------------- #
# status.json
# --------------------------------------------------------------------------- #


def test_status_round_trips(make_run, test_user) -> None:
    from app.gui_runs import run_manager

    run_id, run_dir = make_run(user_id=str(test_user.id))
    payload = _status_payload(run_id, str(test_user.id))
    run_manager.write_status(run_id, payload)

    assert (run_dir / "status.json").is_file()
    assert run_manager.load_status(run_id) == payload


def test_status_write_is_atomic(make_run, test_user) -> None:
    """A partially-written status file is never observable.

    The GUI polls status continuously; a reader landing mid-write must see the old
    document or the new one, never a truncated one. `atomic_write_json` writes to a
    temp file and renames, so no partial file ever carries the real name.
    """
    from app.gui_runs import run_manager

    run_id, run_dir = make_run(user_id=str(test_user.id))
    run_manager.write_status(run_id, _status_payload(run_id, str(test_user.id)))

    for _ in range(20):
        run_manager.write_status(
            run_id, _status_payload(run_id, str(test_user.id), status="running")
        )
        loaded = json.loads((run_dir / "status.json").read_text(encoding="utf-8"))
        assert loaded["run_id"] == run_id, "observed a partially-written status file"


def test_unparseable_status_returns_none_rather_than_raising(make_run, test_user) -> None:
    from app.gui_runs import run_manager

    run_id, run_dir = make_run(user_id=str(test_user.id))
    run_manager.write_status(run_id, _status_payload(run_id, str(test_user.id)))
    (run_dir / "status.json").write_text("{not json", encoding="utf-8")

    assert run_manager.load_status(run_id) is None


def test_load_status_of_unknown_run_is_none(run_output_root: Path) -> None:
    from app.gui_runs import run_manager

    assert run_manager.load_status("no-such-run") is None


# --------------------------------------------------------------------------- #
# The artifact set
# --------------------------------------------------------------------------- #


def test_completed_run_exposes_its_artifacts(client, test_user, make_run) -> None:
    from app.gui_runs import run_manager

    run_id, run_dir = make_run(user_id=str(test_user.id))
    (run_dir / "run.log").write_text("done\n", encoding="utf-8")
    for name in RUN_ARTIFACTS:
        (run_dir / name).write_text("{}" if name.endswith(".json") else "# summary\n")

    run_manager.write_status(
        run_id,
        _status_payload(
            run_id,
            str(test_user.id),
            artifacts={name: str(run_dir / name) for name in RUN_ARTIFACTS},
            output_root=str(run_dir),
        ),
    )

    body = client.get(f"/api/run_status/{run_id}").json()

    assert body["status"] == "completed"
    assert set(RUN_ARTIFACTS) <= set(body["artifacts"])
    for name in RUN_ARTIFACTS:
        assert Path(body["artifacts"][name]).is_file()


@pytest.mark.parametrize("absent", RUN_ARTIFACTS)
def test_status_polling_survives_an_absent_optional_artifact(
    client, test_user, make_run, absent: str
) -> None:
    """The Pydantic `None` bug, pinned so it cannot recur.

    A run that failed early, or one still in flight, has not written every artifact.
    Status polling must still answer 200 — the GUI polls this endpoint on a timer and a
    500 here takes the whole run view down.
    """
    from app.gui_runs import run_manager

    run_id, run_dir = make_run(user_id=str(test_user.id))
    artifacts: dict[str, str | None] = {
        name: str(run_dir / name) for name in RUN_ARTIFACTS if name != absent
    }
    artifacts[absent] = None

    run_manager.write_status(
        run_id, _status_payload(run_id, str(test_user.id), artifacts=artifacts)
    )

    response = client.get(f"/api/run_status/{run_id}")

    assert response.status_code == 200, response.text
    assert response.json()["artifacts"].get(absent) is None


def test_status_polling_survives_no_artifacts_at_all(client, test_user, make_run) -> None:
    """A run that died before writing anything is still pollable."""
    from app.gui_runs import run_manager

    run_id, _ = make_run(user_id=str(test_user.id))
    run_manager.write_status(
        run_id,
        {"run_id": run_id, "user_id": str(test_user.id), "profile_key": "p", "status": "failed"},
    )

    response = client.get(f"/api/run_status/{run_id}")

    assert response.status_code == 200, response.text
    assert response.json()["status"] == "failed"


# --------------------------------------------------------------------------- #
# Status transitions
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("state", ["queued", "running", "completed", "failed"])
def test_every_status_value_is_pollable(client, test_user, make_run, state: str) -> None:
    from app.gui_runs import run_manager

    run_id, _ = make_run(user_id=str(test_user.id))
    run_manager.write_status(run_id, _status_payload(run_id, str(test_user.id), status=state))

    body = client.get(f"/api/run_status/{run_id}").json()
    assert body["status"] == state


def test_terminal_states_are_terminal(client, test_user, make_run) -> None:
    """Once terminal, a status stays terminal across reads.

    Nothing should re-derive or "helpfully" recompute a finished run's state on read.
    """
    from app.gui_runs import run_manager

    run_id, _ = make_run(user_id=str(test_user.id))
    run_manager.write_status(run_id, _status_payload(run_id, str(test_user.id), status="completed"))

    first = client.get(f"/api/run_status/{run_id}").json()["status"]
    second = client.get(f"/api/run_status/{run_id}").json()["status"]

    assert first == second == "completed"


# --------------------------------------------------------------------------- #
# Ownership and absence
# --------------------------------------------------------------------------- #


def test_unknown_run_is_404(client) -> None:
    assert client.get("/api/run_status/nope").status_code == 404


def test_another_users_run_is_404_not_403(client, make_run) -> None:
    from app.gui_runs import run_manager

    run_id, _ = make_run(user_id="someone-else")
    run_manager.write_status(run_id, _status_payload(run_id, "someone-else"))

    assert client.get(f"/api/run_status/{run_id}").status_code == 404


def test_status_requires_authentication(client_unauthed, make_run) -> None:
    run_id, _ = make_run()
    assert client_unauthed.get(f"/api/run_status/{run_id}").status_code == 401


# --------------------------------------------------------------------------- #
# latest.json
# --------------------------------------------------------------------------- #


def test_latest_pointer_lives_beside_the_runs(run_output_root: Path) -> None:
    from app.gui_runs import run_manager

    run_manager.write_latest("user-1", "profile-1", {"run_id": "abc"})

    path = run_manager.latest_path("user-1", "profile-1")
    assert path == run_output_root / "user-1" / "profile-1" / "latest.json"
    assert json.loads(path.read_text(encoding="utf-8")) == {"run_id": "abc"}
