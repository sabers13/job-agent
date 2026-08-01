# 0004 — Lock dependencies; `requirements.txt` is intent, the lock is truth

**Status:** Accepted · **Date:** 2026-07-31

## Context

`requirements.txt` used compatible-release ranges (`~=`). Comparing declared ranges against the
environment that actually produces the recorded test baseline found two packages outside their
declared range entirely:

| Package | Declared | Installed |
| --- | --- | --- |
| `pypdf` | `~=4.3` | **6.5.0** — two majors ahead |
| `pytest` | `~=8.3` | **9.1.1** — one major ahead |

This is not a cosmetic drift. `pytest~=8.3` resolves to `>=8.3,<9.0`, so a clean
`pip install -r requirements.txt` installs a *different test runner* than the one that produced
"18 passed, 6 failed". CI built on that file would measure a codebase nobody has run.

`pypdf` is worse: it is load-bearing for résumé parsing, and 4.x → 6.x spans two major versions
of a parsing library whose output feeds scoring.

## Decision

- `requirements.lock.txt` — fully pinned (151 packages), generated from the known-good
  environment via `pip freeze`. **This is what CI and the Docker image install.**
- `pyproject.toml` `[project.dependencies]` — loose ranges, updated to match reality
  (`pypdf~=6.5`, and `pytest~=9.1` under the `dev` extra). Declares intent for humans.
- `requirements.txt` is superseded by the two files above.

## Consequences

- CI, the container, and the dev machine install byte-identical dependency sets. A test failure
  is a code fact, not an environment artifact.
- Upgrades become deliberate: regenerate the lock, watch the ratchet gate, commit both.

**Cost:** two files to keep in sync, and `pip freeze` captures the environment as-is including
anything installed ad hoc. A resolver-based tool (`uv lock`, `pip-compile`) derives the lock from
declared constraints instead, which is strictly better.

**Why not `uv` now:** it is not installed on the dev machine, and adopting a new resolver at the
same time as pinning would conflate "what does this project depend on" with "which tool resolves
it". The lock file works today with no new tooling. Migrating later is:

```bash
uv pip compile pyproject.toml -o requirements.lock.txt
```

which produces the same artifact CI already consumes — so the migration is a one-line change to
how the file is generated, not to anything that reads it.
