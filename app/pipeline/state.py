from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, is_dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from loguru import logger

from app.common.utils import ensure_dir
from app.config.focus import FocusConfig
from app.config.settings import settings

STATE_DIR = settings.output_dir / "_state"
STATE_FILE = STATE_DIR / "run_state.json"

CACHE_DIR = settings.output_dir / "_cache"

DEFAULT_STATE: Dict[str, Any] = {
    "last_run": None,
    "run_dir": None,
    "last_seed": None,
}


def load_state() -> Dict[str, Any]:
    try:
        if not STATE_FILE.exists():
            return DEFAULT_STATE.copy()
        data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        out = DEFAULT_STATE.copy()
        if isinstance(data, dict):
            out.update(data)
        return out
    except Exception:
        logger.warning("load_state failed; using defaults", exc_info=True)
        return DEFAULT_STATE.copy()


def save_state(state: Dict[str, Any]) -> Dict[str, Any]:
    ensure_dir(STATE_DIR)
    persisted = DEFAULT_STATE.copy()
    if isinstance(state, dict):
        persisted.update(state)
        if persisted.get("run_dir") is not None and not isinstance(persisted["run_dir"], str):
            persisted["run_dir"] = None
    STATE_FILE.write_text(json.dumps(persisted, ensure_ascii=False, indent=2), encoding="utf-8")
    return persisted


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _stable_json(obj: Any) -> str:
    """
    Convert FocusConfig (contains sets) into a stable JSON string.
    """
    if is_dataclass(obj):
        obj = asdict(obj)
    if isinstance(obj, dict):
        norm: Dict[str, Any] = {}
        for k, v in obj.items():
            if isinstance(v, set):
                norm[k] = sorted(list(v))
            elif isinstance(v, (list, tuple)):
                norm[k] = v
            elif isinstance(v, dict):
                norm[k] = json.loads(_stable_json(v))
            else:
                norm[k] = v
        return json.dumps(norm, sort_keys=True, ensure_ascii=False)
    return json.dumps(obj, sort_keys=True, ensure_ascii=False)


def _focus_fingerprint(focus: Optional[FocusConfig]) -> Optional[str]:
    if not focus:
        return None
    raw = _stable_json(focus)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _cache_key(url: str, focus: Optional[FocusConfig]) -> str:
    parts = [url.strip(), f"cv:{settings.cache_version}"]
    if focus and settings.cache_per_profile:
        parts.append(f"profile:{focus.profile_name}")
        parts.append(f"fh:{_focus_fingerprint(focus)}")
    base = "|".join(parts)
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def _cache_path(url: str, focus: Optional[FocusConfig]) -> Path:
    ensure_dir(CACHE_DIR)
    return CACHE_DIR / f"{_cache_key(url, focus)}.json"


def cache_get(url: str, focus: Optional[FocusConfig] = None) -> Optional[Dict[str, Any]]:
    if not settings.cache_enabled:
        return None
    p = _cache_path(url, focus)
    if not p.exists():
        return None

    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        meta = (data or {}).get("cache_meta") or {}
        payload = (data or {}).get("payload")

        if meta.get("cache_version") != settings.cache_version:
            return None

        cached_at = meta.get("cached_at")
        if cached_at and settings.cache_ttl_days > 0:
            try:
                dt = datetime.fromisoformat(str(cached_at).replace("Z", "+00:00"))
                if datetime.now(timezone.utc) - dt > timedelta(days=settings.cache_ttl_days):
                    return None
            except Exception:
                return None

        if focus and settings.cache_per_profile:
            if meta.get("focus_profile") != focus.profile_name:
                return None
            if meta.get("focus_hash") != _focus_fingerprint(focus):
                return None

        if isinstance(payload, dict):
            return payload
        return None
    except Exception:
        logger.warning("cache_get failed; ignoring cache", exc_info=True)
        return None


def cache_put(url: str, payload: Dict[str, Any], focus: Optional[FocusConfig] = None) -> None:
    if not settings.cache_enabled:
        return
    try:
        scoring = (payload or {}).get("scoring") or {}
        meta = {
            "cached_at": _now_iso(),
            "cache_version": settings.cache_version,
            "url": url,
            "focus_profile": (focus.profile_name if (focus and settings.cache_per_profile) else None),
            "focus_hash": (_focus_fingerprint(focus) if (focus and settings.cache_per_profile) else None),
            "scoring_versions": {
                "heuristic_version": scoring.get("heuristic_version"),
                "version": scoring.get("version"),
                "llm_scoring_version": scoring.get("llm_scoring_version"),
            },
        }
        p = _cache_path(url, focus)
        p.write_text(
            json.dumps({"cache_meta": meta, "payload": payload}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception:
        logger.warning("cache_put failed; continuing", exc_info=True)

