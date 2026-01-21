#!/usr/bin/env python3
from __future__ import annotations

import argparse
import glob
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set
from urllib.parse import urlsplit, urlunsplit


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def normalize_url(url: str) -> str:
    """
    Conservative canonicalization:
      - trim
      - remove fragment
      - remove query
    """
    u = (url or "").strip()
    if not u:
        return ""

    # If user saved markdown-like links occasionally, unwrap: [text](url)
    if "](" in u and u.endswith(")"):
        try:
            u = u.split("](", 1)[1][:-1]
        except Exception:
            pass

    parts = urlsplit(u)
    parts = parts._replace(fragment="", query="")
    return urlunsplit(parts)


def load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def extract_urls(payload: Dict[str, Any]) -> List[str]:
    # Expected shape: {"urls": [...], "seed": {...}, "metadata": {...}}
    urls = payload.get("urls")
    if isinstance(urls, list):
        return [str(x) for x in urls if x]
    # fallback if someone stored as {"items": [{"url":...}, ...]}
    items = payload.get("items")
    if isinstance(items, list):
        out = []
        for it in items:
            if isinstance(it, dict) and it.get("url"):
                out.append(str(it["url"]))
        return out
    return []


def seed_slug_from_payload(payload: Dict[str, Any], fallback: str) -> Optional[str]:
    seed = payload.get("seed") or {}
    if isinstance(seed, dict) and seed.get("slug"):
        return str(seed["slug"])
    return fallback or None


def load_existing_pool(pool_path: Path) -> Set[str]:
    """
    Returns set of URLs (already normalized) that exist in url_pool.jsonl.
    """
    if not pool_path.exists():
        return set()

    seen: Set[str] = set()
    with pool_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            u = normalize_url(str(obj.get("url") or ""))
            if u:
                seen.add(u)
    return seen


def append_pool_entries(pool_path: Path, entries: Iterable[Dict[str, Any]]) -> int:
    pool_path.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with pool_path.open("a", encoding="utf-8") as f:
        for e in entries:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")
            n += 1
    return n


def find_profile_dir(outputs_base: Path, profile_key: str, user_id: Optional[str]) -> Path:
    """
    Tries to locate outputs/<user_id>/<profile_key>.
    If user_id not given, autodetect if exactly one match exists.
    """
    if user_id:
        return outputs_base / user_id / profile_key

    # autodetect: outputs/*/<profile_key>
    matches = [p for p in outputs_base.glob(f"*/{profile_key}") if p.is_dir()]
    if len(matches) == 1:
        return matches[0]
    if len(matches) == 0:
        raise SystemExit(
            f"Could not find profile directory under {outputs_base}/<user_id>/{profile_key}. "
            f"Provide --user-id or verify outputs base."
        )
    raise SystemExit(
        f"Multiple matches found for profile_key={profile_key}: {matches}\n"
        f"Provide --user-id to disambiguate."
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile-key", required=True, help="e.g. junior_data_bi")
    ap.add_argument("--snapshots", nargs="+", required=True,
                    help='Snapshot directories or json globs, e.g. "scripts/2025-11-04T*/urls-*.json"')
    ap.add_argument("--outputs-base", default="output", help='Outputs base dir (default: "output")')
    ap.add_argument("--user-id", default=None, help="Optional: force user_id directory name")
    ap.add_argument("--run-id-prefix", default="import", help="Prefix for run_id field (default: import)")
    ap.add_argument("--seen-at", default=None,
                    help="Optional ISO or YYYY-MM-DD; if omitted uses now() for entries")
    args = ap.parse_args()

    outputs_base = Path(args.outputs_base)
    profile_dir = find_profile_dir(outputs_base, args.profile_key, args.user_id)
    pool_path = profile_dir / "url_pool.jsonl"

    # Expand snapshot patterns to JSON files
    json_files: List[Path] = []
    for s in args.snapshots:
        expanded = glob.glob(s)
        if expanded:
            json_files.extend(Path(x) for x in expanded)
        else:
            p = Path(s)
            if p.is_dir():
                json_files.extend(p.glob("urls-*.json"))
            elif p.is_file():
                json_files.append(p)

    json_files = sorted({p.resolve() for p in json_files})
    if not json_files:
        raise SystemExit("No snapshot JSON files found from provided --snapshots inputs.")

    # Determine seen_at timestamp
    if args.seen_at:
        # Accept YYYY-MM-DD or full ISO; normalize to UTC ISO.
        raw = args.seen_at.strip()
        if len(raw) == 10 and raw[4] == "-" and raw[7] == "-":
            dt = datetime.fromisoformat(raw).replace(tzinfo=timezone.utc)
        else:
            dt = datetime.fromisoformat(raw)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
        seen_at = dt.isoformat().replace("+00:00", "Z")
    else:
        seen_at = utc_now_iso()

    already = load_existing_pool(pool_path)

    new_entries: List[Dict[str, Any]] = []
    discovered = 0
    added = 0

    for jf in json_files:
        payload = load_json(jf)
        urls = extract_urls(payload)

        # best-effort seed slug: from payload or filename (urls-<slug>.json)
        fname = jf.name
        fallback_slug = None
        if fname.startswith("urls-") and fname.endswith(".json"):
            fallback_slug = fname[len("urls-"):-len(".json")]
        seed_slug = seed_slug_from_payload(payload, fallback_slug)

        # run_id: import-<snapshotfolder>
        # snapshot folder name is the parent dir (e.g. 2025-11-04T...)
        snap_tag = jf.parent.name
        run_id = f"{args.run_id_prefix}-{snap_tag}"

        for u in urls:
            discovered += 1
            nu = normalize_url(u)
            if not nu:
                continue
            if nu in already:
                continue
            already.add(nu)
            new_entries.append({
                "url": nu,
                "seen_at": seen_at,
                "run_id": run_id,
                "seed_slug": seed_slug,
            })
            added += 1

    appended = append_pool_entries(pool_path, new_entries)

    print(f"profile_dir: {profile_dir}")
    print(f"pool_path:   {pool_path}")
    print(f"inputs:      {len(json_files)} files")
    print(f"discovered:  {discovered} raw urls")
    print(f"added:       {added} new urls")
    print(f"appended:    {appended} lines written")


if __name__ == "__main__":
    main()
