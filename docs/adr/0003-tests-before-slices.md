# 0003 — Characterization tests precede every structural slice

**Status:** Accepted · **Date:** 2026-07-31

## Context

Coverage on the modules the restructure moves:

| Module | Coverage |
| --- | ---: |
| `app/prefect_run.py` | 0% |
| `app/gui_runs/run_manager.py` | 23% |
| `app/fastapi_run.py` | 24% |

`run_manager.py` owns the run-artifact contract, which the project defines as its actual
deliverable — `run_id`, `status.json`, `run.log`, and the offset-based log-streaming protocol.

Refactoring means changing structure while preserving behavior. With no way to observe behavior,
the activity is not refactoring; it is rewriting and hoping the output is the same.

## Decision

No structural slice begins before the characterization suite is green. The bar is ~60% on
`fastapi_run.py` and `run_manager.py`.

The existing 24 tests are **not repaired** — they bind to internals that are about to move. They
move to `tests/legacy/`, out of the gate, deleted per-file as the new suite covers their ground.

Contract tests must pass **unchanged** through the router split. `git diff --stat tests/contracts/`
being empty is a verification command in two slices, not a guideline.

## Consequences

- The largest slices are gated behind a session that ships no user-visible change.
- Contract tests target HTTP endpoints and on-disk artifacts, so they survive the restructure
  rather than breaking with it — which is the entire point.

**Cost:** delays visible progress. The alternative is not knowing whether a slice broke anything,
which on the artifact contract means shipping HTTP 200s over malformed output.

**Open question blocking this work:** `_experience_delta` returns `-1` where the old test expected
`<= -15`. Until it is known whether that is a live bug, a stale test, or a config change,
characterizing scoring risks encoding a defect as the specification.
