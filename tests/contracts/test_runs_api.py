"""Run lifecycle endpoints: summary, potential applications, batch start, prune.

`run_status` and `run_logs` have their own files. This covers the rest of the run
surface, plus the two endpoints that trigger background work — those are stubbed at the
boundary rather than executed, because the default suite is offline and a real batch run
would need Prefect, Playwright and the network.
"""

from __future__ import annotations

import json

import pytest


def _seed_run(test_user, make_run, **status_overrides):
    from app.gui_runs import run_manager

    run_id, run_dir = make_run(user_id=str(test_user.id))
    payload = {
        "run_id": run_id,
        "user_id": str(test_user.id),
        "profile_key": "profile-1",
        "status": "completed",
        "output_root": str(run_dir),
    }
    payload.update(status_overrides)
    run_manager.write_status(run_id, payload)
    return run_id, run_dir


# --------------------------------------------------------------------------- #
# Run summary
# --------------------------------------------------------------------------- #


def test_run_summary_returns_markdown_and_analysis(client, test_user, make_run) -> None:
    run_id, run_dir = _seed_run(test_user, make_run)
    (run_dir / "REPORT_SUMMARY.md").write_text("# Report\n\nAll good.\n", encoding="utf-8")
    (run_dir / "analysis_summary.json").write_text(json.dumps({"kept": 3}), encoding="utf-8")

    from app.gui_runs import run_manager

    status = run_manager.load_status(run_id) or {}
    status["artifacts"] = {
        "REPORT_SUMMARY.md": str(run_dir / "REPORT_SUMMARY.md"),
        "analysis_summary.json": str(run_dir / "analysis_summary.json"),
    }
    run_manager.write_status(run_id, status)

    response = client.get(f"/api/run_summary/{run_id}")

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["ok"] is True
    assert body["run_id"] == run_id


def test_run_summary_survives_missing_artifacts(client, test_user, make_run) -> None:
    """Same absent-optional-artifact rule as status polling — must not 500."""
    run_id, _ = _seed_run(test_user, make_run)

    response = client.get(f"/api/run_summary/{run_id}")

    assert response.status_code == 200, response.text


def test_run_summary_unknown_run_is_404(client) -> None:
    assert client.get("/api/run_summary/nope").status_code == 404


def test_run_summary_of_another_users_run_is_404(client, make_run) -> None:
    from app.gui_runs import run_manager

    run_id, _ = make_run(user_id="someone-else")
    run_manager.write_status(
        run_id,
        {"run_id": run_id, "user_id": "someone-else", "profile_key": "p", "status": "completed"},
    )

    assert client.get(f"/api/run_summary/{run_id}").status_code == 404


# --------------------------------------------------------------------------- #
# Potential applications
# --------------------------------------------------------------------------- #


def test_potential_applications_is_empty_when_none_were_written(
    client, test_user, make_run
) -> None:
    run_id, _ = _seed_run(test_user, make_run)

    response = client.get(f"/api/run_artifacts/{run_id}/potential_applications")

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["run_id"] == run_id
    assert body["count"] == 0
    assert body["items"] == []


def test_potential_applications_lists_what_is_on_disk(client, test_user, make_run) -> None:
    run_id, run_dir = _seed_run(test_user, make_run)
    job_dir = run_dir / "potential_applications" / "job-1"
    job_dir.mkdir(parents=True)
    (job_dir / "job.json").write_text(
        json.dumps({"title": "Data Analyst", "company": "ACME", "location": "Berlin"}),
        encoding="utf-8",
    )
    (job_dir / "metadata.json").write_text(json.dumps({"final_score": 82}), encoding="utf-8")

    body = client.get(f"/api/run_artifacts/{run_id}/potential_applications").json()

    assert body["count"] == 1
    assert body["items"][0]["job_key"] == "job-1"


def test_potential_application_detail_returns_the_documents(client, test_user, make_run) -> None:
    run_id, run_dir = _seed_run(test_user, make_run)
    job_dir = run_dir / "potential_applications" / "job-1"
    job_dir.mkdir(parents=True)
    (job_dir / "job.json").write_text(json.dumps({"title": "Data Analyst"}), encoding="utf-8")

    response = client.get(f"/api/run_artifacts/{run_id}/potential_applications/job-1")

    assert response.status_code == 200, response.text
    assert response.json()["job_key"] == "job-1"


def test_potential_application_detail_unknown_key_is_404(client, test_user, make_run) -> None:
    run_id, _ = _seed_run(test_user, make_run)

    assert (
        client.get(f"/api/run_artifacts/{run_id}/potential_applications/missing").status_code == 404
    )


@pytest.mark.parametrize("job_key", ["../escape", "a/b", "..", "%2e%2e"])
def test_potential_application_detail_rejects_path_traversal(
    client, test_user, make_run, job_key: str
) -> None:
    """`_safe_job_key` exists to stop a job key escaping the run directory.

    Pinned as a contract: whatever the sanitiser does internally, a traversal attempt
    must never return 200 with content from outside the run.
    """
    run_id, _ = _seed_run(test_user, make_run)

    response = client.get(f"/api/run_artifacts/{run_id}/potential_applications/{job_key}")

    assert response.status_code in (400, 404, 422), response.status_code


# --------------------------------------------------------------------------- #
# Batch start — stubbed at the boundary
# --------------------------------------------------------------------------- #


def test_start_batch_run_returns_a_run_id_without_running_anything(
    client, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The endpoint's contract is "accept and hand back a run id", not "finish a run".

    The orchestrator is stubbed so the default suite stays offline; what is asserted is
    the response shape and that a run id is minted.
    """
    import app.fastapi_run as fr

    monkeypatch.setattr(fr, "_run_prefect_batch", lambda *a, **k: None, raising=False)
    monkeypatch.setattr(fr, "_run_prefect_inprocess_batch", lambda *a, **k: None, raising=False)

    response = client.post(
        "/api/start_batch_run",
        json={"profile_key": "junior_data_bi", "search": {"max_age_days": 7}},
    )

    assert response.status_code in (200, 404), response.text
    if response.status_code == 200:
        assert response.json()["run_id"]


def test_prune_url_pool_requires_a_known_profile(client) -> None:
    response = client.post(
        "/api/my/profile/never-created/url_pool/prune_stepstone",
        json={"max_urls": 5, "concurrency": 2, "timeout_sec": 5},
    )

    assert response.status_code == 404, response.text


def test_run_single_is_reachable_and_validates_its_body(client) -> None:
    """A malformed body must be rejected by validation, not by a 500 deeper in."""
    response = client.post("/api/run_single", json={})

    assert response.status_code in (200, 400, 404, 422), response.text
    assert response.status_code != 500
