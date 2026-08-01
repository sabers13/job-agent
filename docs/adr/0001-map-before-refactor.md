# 0001 — Map the architecture before restructuring

**Status:** Accepted · **Date:** 2026-07-31

## Context

The codebase was written quickly and grew past the point where any one person held its shape in
their head. 67 modules, ~9,900 LOC in `app/`. The instinct was to start splitting
`app/fastapi_run.py`, which is obviously too big at 2,100 lines.

Starting there would have been wrong in a way that only shows up later: the plan would have
encoded assumptions about the dependency structure that nobody had checked.

## Decision

Produce `docs/architecture.md` first, as a committed artifact, using semantic analysis
(pyright LSP `findReferences`, AST parsing) rather than text search. No code changes during
mapping.

## Consequences

The map paid for itself immediately by contradicting three assumptions:

- **`app/config/settings.py`, not `fastapi_run.py`, is the most-imported module** — 14 of 46
  `app` modules, 104 references across 16 files, fan-out 0. It is also the safest kind of
  coupling, so it needs no work at all.
- **`app/pipeline/` does not import `app/stepstone/`.** The target architecture's "pipeline must
  not reach into sources" rule was already satisfied. A slice that was budgeted as significant
  turned out to be about locking in an existing property.
- **There are no runtime import cycles**, only two latent ones created by `__init__.py`
  re-exports. Verified by importing every suspect module in a cold interpreter, not by reading.

Two defects surfaced from the same pass, neither of which anyone was looking for: `get_db` is
dead code with exactly one reference (its own definition), and the `_LogSink` indentation bug
means the URL-pool prune endpoint has always silently failed.

**Cost:** one long session, read-only, producing no shipped behavior. Justified because a wrong
map is paid for by every token spent executing against it.

**Non-obvious:** the map is now cached context for every later session. The discovery cost was
paid once instead of being re-incurred each time.
