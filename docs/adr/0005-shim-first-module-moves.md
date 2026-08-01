# 0005 — Module moves use re-export shims, removed only on a full green run

**Status:** Accepted · **Date:** 2026-07-31

## Context

The restructure moves symbols across package boundaries in ten slices. Each slice must be
independently shippable — mergeable to `main` without waiting for the next one — or the branch
becomes a long-lived fork that is merged once, unreviewably.

A move that updates the definition and all call sites in one commit is not independently
shippable: anything not in the diff (a script, the `n8n workflows/` prototype, a stale import)
breaks silently.

## Decision

Every move leaves a re-export shim at the old import path. The shim is deleted in a **later**
slice, only after the full suite passes with it removed — not just the affected module's tests.

The form depends on how consumers import:

- **Named imports** → explicit re-export with a redundant alias. This marks the names as
  intentional re-exports for pyright and satisfies ruff's F401 without a `noqa`:
  ```python
  from app.sources.stepstone.dates import parse_iso8601_utc as parse_iso8601_utc
  ```
- **Attribute access on a module object** (`run_manager.write_status(...)`) → star-import the
  module and give the target an `__all__`. Enumerating names would be a lie by omission when the
  consumer can reach for any attribute.

## Consequences

- Each slice is revertible on its own.
- Shim identity is verifiable, not assumed. Every move's verification includes proving the two
  paths resolve to the same object:
  ```python
  assert old is new, "shim is a divergent copy"
  ```
  A shim that copies rather than re-exports is the failure mode this catches.

**Cost:** a period where two import paths exist, which is confusing if the removal slice is
skipped. Mitigated by naming the removing slice in a comment on every shim.

**Non-obvious:** the largest slice needs no shims at all. The ~30 private helpers being extracted
from `app/fastapi_run.py` have zero external importers — verified per symbol via LSP
`findReferences`. A 2,100-line module with a fan-in of 0 from `app/` is what makes that slice
tractable despite its size.
