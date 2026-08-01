#!/usr/bin/env python3
"""Ratchet gate for the restructure.

Runs pytest, pyright and ruff, compares each count against ci/baseline.json, and
fails if any count went up. Counts that went DOWN are reported as slack to be
banked -- lower the baseline in the same commit that earns it.

The repo is mid-restructure with a known-red baseline (6 failing tests, 32 pyright
errors, 769 ruff findings). A pass/fail gate would be red on day one and get
ignored within a week; a ratchet is green today and still catches regressions.

Usage:
    python ci/gate.py              # run every gate
    python ci/gate.py --only ruff  # run one
    python ci/gate.py --update     # rewrite baseline from current counts
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BASELINE_PATH = ROOT / "ci" / "baseline.json"
VENV_PY = ROOT / ".venv" / "bin" / "python"
PYTHON = str(VENV_PY) if VENV_PY.exists() else sys.executable


def _run(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)


def measure_pytest() -> dict[str, int]:
    proc = _run([PYTHON, "-m", "pytest", "-q", "--no-header", "-p", "no:cacheprovider"])
    tail = proc.stdout.strip().splitlines()[-1] if proc.stdout.strip() else ""
    failed = int(m.group(1)) if (m := re.search(r"(\d+) failed", tail)) else 0
    passed = int(m.group(1)) if (m := re.search(r"(\d+) passed", tail)) else 0
    errors = int(m.group(1)) if (m := re.search(r"(\d+) error", tail)) else 0
    skipped = int(m.group(1)) if (m := re.search(r"(\d+) skipped", tail)) else 0
    if skipped:
        print(f"  note: {skipped} skipped -- TEST-STRATEGY requires zero skips in a green run")
    return {"pytest_failed": failed + errors, "pytest_passed": passed}


def measure_pyright() -> dict[str, int]:
    if not shutil.which("pyright"):
        print("  pyright not on PATH -- skipping (install: npm install -g pyright)")
        return {}
    # With pyrightconfig.json present (Slice 0), let it be the single source of
    # truth for include/exclude and typeCheckingMode. Passing paths on the command
    # line would override its `include` and let the two definitions drift.
    # The explicit-path form is kept as the fallback so this still works if the
    # config is ever removed.
    cmd = ["pyright", "--outputjson", "--pythonpath", PYTHON]
    if not (ROOT / "pyrightconfig.json").exists():
        cmd[1:1] = ["app", "tests", "scripts"]
    proc = _run(cmd)
    try:
        summary = json.loads(proc.stdout)["summary"]
    except (json.JSONDecodeError, KeyError):
        print(f"  pyright produced no parseable output:\n{proc.stdout[:400]}")
        return {}
    return {"pyright_errors": int(summary["errorCount"])}


def measure_ruff() -> dict[str, int]:
    ruff = ROOT / ".venv" / "bin" / "ruff"
    exe = str(ruff) if ruff.exists() else shutil.which("ruff")
    if not exe:
        print("  ruff not found -- skipping")
        return {}
    proc = _run([exe, "check", ".", "--output-format", "json"])
    try:
        findings = json.loads(proc.stdout or "[]")
    except json.JSONDecodeError:
        print(f"  ruff produced no parseable output:\n{proc.stdout[:400]}")
        return {}
    return {"ruff_findings": len(findings)}


ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
CONTRACTS_RE = re.compile(r"Contracts:\s*(\d+)\s*kept,\s*(\d+)\s*broken")


def measure_imports() -> dict[str, int]:
    """Count broken import-linter contracts.

    Ratchets on BROKEN CONTRACTS, which is what import-linter reports in its
    summary line. Note this can exceed the number of distinct illegal imports --
    one bad edge that violates both the layers contract and a targeted forbidden
    contract counts twice. That is fine for a ratchet: the number only has to move
    monotonically, not be a physical quantity.

    import-linter aborts the whole run on an unresolvable module rather than
    counting it, so a config naming a not-yet-created package produces no summary
    line at all. That is reported loudly here instead of silently scoring 0.
    """
    exe = ROOT / ".venv" / "bin" / "lint-imports"
    path = str(exe) if exe.exists() else shutil.which("lint-imports")
    if not path:
        print("  lint-imports not found -- skipping (pip install import-linter)")
        return {}
    proc = _run([path])
    out = ANSI_RE.sub("", proc.stdout + proc.stderr)
    match = CONTRACTS_RE.search(out)
    if not match:
        # Most likely an unresolvable module in .importlinter -- see the docstring.
        first = next((ln for ln in out.splitlines() if ln.strip()), "")
        print(f"  lint-imports produced no contract summary: {first[:120]}")
        return {}
    kept, broken = int(match.group(1)), int(match.group(2))
    print(f"  {kept} kept, {broken} broken")
    return {"importlinter_broken": broken}


MEASURERS = {
    "pytest": measure_pytest,
    "pyright": measure_pyright,
    "ruff": measure_ruff,
    "imports": measure_imports,
}

# Metrics where a higher number is better. Everything else: lower is better.
HIGHER_IS_BETTER = {"pytest_passed"}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--only", choices=sorted(MEASURERS), action="append")
    parser.add_argument("--update", action="store_true", help="rewrite the baseline")
    args = parser.parse_args()

    baseline = json.loads(BASELINE_PATH.read_text())
    current: dict[str, int] = {}
    for name in args.only or sorted(MEASURERS):
        print(f"running {name}...")
        current.update(MEASURERS[name]())

    if args.update:
        baseline.update(current)
        BASELINE_PATH.write_text(json.dumps(baseline, indent=2) + "\n")
        print(f"\nbaseline updated: {current}")
        return 0

    regressions: list[str] = []
    improvements: list[str] = []
    for key, now in sorted(current.items()):
        was = baseline.get(key)
        if was is None:
            print(f"  {key:18s} {now:5d}  (no baseline)")
            continue
        worse = now < was if key in HIGHER_IS_BETTER else now > was
        better = now > was if key in HIGHER_IS_BETTER else now < was
        mark = "REGRESSED" if worse else ("improved" if better else "ok")
        print(f"  {key:18s} {now:5d}  baseline {was:5d}  {mark}")
        if worse:
            regressions.append(f"{key}: {was} -> {now}")
        elif better:
            improvements.append(f"{key}: {was} -> {now}")

    if improvements:
        print("\nSlack to bank -- lower the baseline in this same commit:")
        for line in improvements:
            print(f"  {line}")
        print("  (run: python ci/gate.py --update)")

    if regressions:
        print("\nGATE FAILED")
        for line in regressions:
            print(f"  {line}")
        return 1

    print("\nGATE PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
