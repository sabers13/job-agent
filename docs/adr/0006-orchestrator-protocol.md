# 0006 — Prefect becomes an opt-in backend behind an `Orchestrator` protocol

**Status:** Proposed · **Date:** 2026-07-31

## Context

Orchestration is Prefect-only (`app/prefect_run.py`, 698 lines) and assumes a separately started
`prefect server`. That forces a two-terminal startup, which directly contradicts the goal of an
application a non-technical user can launch.

`app/fastapi_run.py` already forks between `_run_prefect_batch` (subprocess) and
`_run_prefect_inprocess_batch` (in-process) — roughly 330 lines of near-duplicate logic. The
`StartBatchRunRequest.orchestrator` request field exists and selects between them.

So the abstraction is already present, expressed as an if-statement across two duplicated
functions.

## Decision

Introduce `app/orchestration/` with an `Orchestrator` protocol, a `LocalOrchestrator`
(in-process, default) and a `PrefectOrchestrator` wrapping the existing flows.

**Prefect is not deleted.** It becomes an opt-in backend.

## Consequences

- Single-process startup becomes the default path; the second terminal disappears.
- The duplicated runners collapse into one implementation each.

**Cost — and this is the uncomfortable part:** `app/prefect_run.py` is at **0% coverage**. The
test strategy excludes real Prefect from the default suite (correctly — it needs a server), so
`PrefectOrchestrator` will be verified only by manual runs.

This must be stated rather than glossed: either `LocalOrchestrator` gets offline tests and
`PrefectOrchestrator` is accepted as manually verified, or the slice does not ship. Refactoring
698 lines with no oracle is the exact thing ADR 0003 exists to prevent, and this ADR is a
deliberate, scoped exception to it.

**Deferred:** deduplicating the two batch runners happens in a commit *after* the move, not
during. A merge inside a move makes failures unattributable.
