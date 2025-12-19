from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from app.pipeline import state as state_mod


def test_cache_version_invalidation(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(state_mod, "CACHE_DIR", tmp_path)
    monkeypatch.setattr(
        state_mod,
        "settings",
        SimpleNamespace(
            cache_enabled=True,
            cache_ttl_days=7,
            cache_version="new",
            cache_per_profile=False,
            output_dir=tmp_path,
        ),
    )

    url = "http://example.com/job"
    cache_path = state_mod._cache_path(url, focus=None)
    cache_path.write_text(
        json.dumps({"cache_meta": {"cache_version": "old", "cached_at": "2025-01-01T00:00:00Z"}, "payload": {"ok": True}}),
        encoding="utf-8",
    )

    assert state_mod.cache_get(url, focus=None) is None

