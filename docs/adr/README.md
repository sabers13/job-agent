# Architecture Decision Records

Short records of decisions that were expensive to reach and would otherwise have to be
re-argued. Each one states the context, the decision, and — most importantly — what it costs.

**0001–0008** were written after the fact, from the reasoning in the restructure planning
sessions (2026-07-31). They are backfilled, not contemporaneous. That is noted here rather than
pretended away. **0009 onward are contemporaneous** — written in the session that made the
decision.

| # | Decision | Status |
| --- | --- | --- |
| [0001](0001-map-before-refactor.md) | Map the architecture before restructuring | Accepted |
| [0002](0002-ratchet-gate-not-passfail.md) | CI gates on a ratchet, not pass/fail | Accepted |
| [0003](0003-tests-before-slices.md) | Characterization tests precede every structural slice | Accepted |
| [0004](0004-dependency-locking.md) | Lock dependencies; `requirements.txt` is intent, the lock is truth | Accepted |
| [0005](0005-shim-first-module-moves.md) | Module moves use re-export shims, removed only on a full green run | Accepted |
| [0006](0006-orchestrator-protocol.md) | Prefect becomes an opt-in backend behind an `Orchestrator` protocol | Proposed |
| [0007](0007-single-process-sqlite-default.md) | Single process, SQLite by default | Proposed |
| [0008](0008-deletions-precede-extraction.md) | Keep/drop decisions come before the extraction slices | Accepted |
| [0009](0009-log-chunks-are-codepoint-aligned.md) | Log chunks are codepoint-aligned; `max_bytes` is a soft limit | Accepted |

## Format

Deliberately minimal — context, decision, consequences. A template nobody fills in is worse than
a short record somebody does.

Status values: **Proposed** (agreed, not yet implemented), **Accepted** (implemented),
**Superseded by NNNN**.
