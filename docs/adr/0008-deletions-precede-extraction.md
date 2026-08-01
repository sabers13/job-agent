# 0008 — Keep/drop decisions come before the extraction slices

**Status:** Accepted · **Date:** 2026-07-31

## Context

The project was built fast, and parts of it are suspected to be unnecessary or non-functional.
Two confirmed instances surfaced from a light structural pass, neither of which anyone was
looking for:

- **`get_db`** in `app/db/session.py` — dead. LSP `findReferences` returns exactly one result:
  its own definition.
- **`_LogSink`** in `app/fastapi_run.py` — the URL-pool prune endpoint has *always* silently
  failed. A `try` block indented inside a class body runs during class evaluation, the name is
  not yet bound, and a bare `except Exception` swallows it into a `"failed"` status. The endpoint
  returns HTTP 200 and never calls the function it exists to call.

The general rule is that behavior changes wait until after the restructure, so that a change in
output is attributable to either the move or the change, never ambiguously both.

**Deletions are the exception, and the general rule gets this wrong.**

## Decision

Feature *modifications* wait until after the extraction slices. Feature *deletions* are decided
**before** them.

The keep/drop decisions are made against evidence, not recollection — see
`docs/liveness-audit.md`. The audit produces a table of every route and feature with proof of
whether it works and whether anything calls it.

## Consequences

Refactoring code that is about to be deleted is pure waste. Under the general rule, a dead
feature would get a service extracted for it in slice 6 and a router in slice 7, and *then* be
deleted. Every dropped feature is a slice's worth of work not done — so the audit probably saves
more time than it costs.

**Cost:** a session before the extraction slices that ships nothing.

**Evidence that the audit will find more:** an AST pass counted **78** broad
`except Exception`/bare-except handlers with no re-raise across `app/`, 17 of them in
`fastapi_run.py` alone. `_LogSink` is one of those 78. The others are unexamined, and each is a
place where a failure can be reported as success.

That number is the argument for the audit, not a claim that 78 bugs exist. Most are probably
legitimate. The point is that nobody currently knows which.
