from pathlib import Path

import pytest

from app.pipeline import state


def test_cache_roundtrip(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(state, "STATE_DIR", tmp_path)
    monkeypatch.setattr(state, "CACHE_DIR", tmp_path)
    url = "http://example.com/job"
    payload = {
        "ok": True,
        "job": {"title": "Example", "url": url},
        "fetch_meta": {},
    }
    state.cache_put(url, payload, focus=None)
    loaded = state.cache_get(url, focus=None)
    assert loaded is not None
    assert loaded["job"]["title"] == "Example"


def test_run_state_roundtrip(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(state, "STATE_DIR", tmp_path)
    monkeypatch.setattr(state, "STATE_FILE", tmp_path / "run_state.json")
    saved = state.save_state({"last_run": "2024-01-01T00:00:00Z", "run_dir": "runs/test"})
    loaded = state.load_state()
    assert loaded["last_run"] == saved["last_run"]
    assert loaded["run_dir"] == saved["run_dir"]
