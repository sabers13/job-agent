#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, List, Literal, Tuple


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Filter analysis_summary.json or process_result_*.json for jobs above a score threshold."
        ),
    )
    parser.add_argument("input", type=Path, help="Path to analysis_summary.json")
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        help="Optional output path to write the filtered JSON (defaults to stdout).",
    )
    parser.add_argument(
        "--min-score",
        type=float,
        default=70.0,
        help="Minimum score required to keep an entry (default: 70).",
    )
    args = parser.parse_args()

    entries, source_type = _load_entries(args.input)
    filtered = [
        entry
        for entry in entries
        if (score := _extract_score(entry, source_type)) is not None and score >= args.min_score
    ]
    filtered.sort(key=lambda item: _extract_score(item, source_type) or 0, reverse=True)

    payload = json.dumps(filtered, ensure_ascii=False, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload, encoding="utf-8")
    else:
        print(payload)


SourceType = Literal["analysis", "process_result"]


def _load_entries(path: Path) -> Tuple[List[dict[str, Any]], SourceType]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, list):
        return [entry for entry in data if isinstance(entry, dict)], "analysis"
    if isinstance(data, dict):
        raw_results = data.get("results")
        if isinstance(raw_results, list):
            results = [entry for entry in raw_results if isinstance(entry, dict)]
            return results, "process_result"
    raise ValueError(
        f"Unrecognised JSON structure in {path}; expected a list or an object with 'results'."
    )


def _extract_score(entry: dict[str, Any], source_type: SourceType) -> float | None:
    if source_type == "analysis":
        score = entry.get("score")
        return float(score) if isinstance(score, (int, float)) else None
    details = entry.get("details") or {}
    scoring = details.get("scoring") if isinstance(details, dict) else None
    score = scoring.get("score") if isinstance(scoring, dict) else None
    return float(score) if isinstance(score, (int, float)) else None


if __name__ == "__main__":
    main()
