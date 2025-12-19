from __future__ import annotations

from fastapi.testclient import TestClient

from app import fastapi_run
from app.config import profile_store
from app.gui_runs import run_manager


def _override_profiles_path(tmp_path):
    profile_store.PROFILES_PATH = tmp_path / "focus_profiles.json"


def _override_runs_root(tmp_path):
    run_manager.OUTPUT_ROOT = tmp_path / "gui_runs"


def _make_client(tmp_path, monkeypatch):
    _override_profiles_path(tmp_path)
    _override_runs_root(tmp_path)
    return TestClient(fastapi_run.app)


def test_profile_crud(tmp_path, monkeypatch):
    client = _make_client(tmp_path, monkeypatch)

    payload = {
        "profile_name": "Test Profile",
        "description": "For API tests",
        "search_seeds": ["junior-data-analyst"],
        "target_seniority": "junior",
        "max_allowed_seniority": "mid",
        "max_required_experience_years": 2,
        "experience_penalty_strength": 1.0,
        "core_skills": ["Python", "SQL"],
        "nice_to_have_skills": [],
        "preferred_titles": [],
        "excluded_titles": [],
        "preferred_locations": [],
        "excluded_locations": [],
        "min_german_level": "B1",
        "requires_student_status": False,
    }

    # Create
    res = client.post("/api/profile/test_profile", json=payload)
    assert res.status_code == 200
    body = res.json()
    assert body["profile_name"] == "Test Profile"

    # Get
    res = client.get("/api/profile/test_profile")
    assert res.status_code == 200
    body = res.json()
    assert body["profile_name"] == "Test Profile"
    assert body["search_seeds"] == ["junior-data-analyst"]

    # List
    res = client.get("/api/profiles")
    assert res.status_code == 200
    items = res.json().get("profiles", [])
    assert any(item["key"] == "test_profile" for item in items)


def test_run_single_with_stubbed_pipeline(tmp_path, monkeypatch):
    client = _make_client(tmp_path, monkeypatch)

    # Create minimal profile first
    client.post(
        "/api/profile/test_profile",
        json={
            "profile_name": "Test Profile",
            "target_seniority": "junior",
            "max_allowed_seniority": "mid",
            "max_required_experience_years": 3,
            "experience_penalty_strength": 1.0,
            "core_skills": [],
            "nice_to_have_skills": [],
            "preferred_titles": [],
            "excluded_titles": [],
            "preferred_locations": [],
            "excluded_locations": [],
            "min_german_level": "B1",
            "requires_student_status": False,
            "search_seeds": [],
        },
    )

    async def _fake_fetch_job_details(*args, **kwargs):
        return {
            "ok": True,
            "backend": "http",
            "job": {"title": "X", "company": "Y", "location": "Z"},
            "scoring": {"score": 99},
            "fetch_meta": {},
            "cutoff_iso": None,
            "stale": False,
        }

    monkeypatch.setattr(fastapi_run, "pipeline_fetch_job_details", _fake_fetch_job_details)

    res = client.post(
        "/api/run_single",
        json={
            "profile_key": "test_profile",
            "url": "https://example.com/job",
            "enrich": False,
            "use_llm_scoring": False,
        },
    )
    assert res.status_code == 200
    body = res.json()
    assert body["ok"] is True
    assert body["details"]["job"]["title"] == "X"


def test_run_logs_endpoint(tmp_path, monkeypatch):
    client = _make_client(tmp_path, monkeypatch)

    run_id = run_manager.create_run_dir()
    run_manager.write_status(
        run_id,
        {
            "run_id": run_id,
            "status": "completed",
            "started_at": "now",
            "finished_at": "later",
            "profile_key": "p",
            "params": {},
            "summary_path": None,
            "error": None,
        },
    )
    log_path = run_manager.log_path(run_id)
    log_path.write_text("line1\nline2\n", encoding="utf-8")

    res = client.get(f"/api/run_logs/{run_id}?offset=0&max_bytes=100")
    assert res.status_code == 200
    data = res.json()
    assert data["chunk"] != ""
    assert data["finished"] is False

    # Second read should have no new data and mark finished
    res = client.get(f"/api/run_logs/{run_id}?offset={data['next_offset']}")
    assert res.status_code == 200
    data2 = res.json()
    assert data2["chunk"] == ""
    assert data2["finished"] is True
