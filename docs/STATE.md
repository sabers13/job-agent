# State

**Single entry point for any new session.** Read this plus `AGENTS.md` and you have the
project. Everything else is reference, read on demand.

**Update this at the end of every session.** A stale STATE.md is worse than none — it
gets trusted.

_Last updated: 2026-08-01_

---

## Where we are

Phases 0–2 complete. Slice 1 is green, so **CP‑1 is now due and blocks Slice 3**.

| | Status |
| --- | --- |
| Phase 0 — baseline, gates | ✅ |
| Phase 1 — `docs/architecture.md` | ✅ |
| Phase 2 — `refactor-plan.md` + review (`PHASE-2-REVIEW.md`, R1–R10, D1–D4) | ✅ |
| Slice 0 — gate infrastructure | ✅ committed |
| Bugfixes A6, A8, A9 | ✅ committed, each with its proof hash |
| Slice 1 — contract test suite | ✅ committed. 208 passed, 0 failed, 0 skipped |
| **CP‑1 — oracle review** | 🔴 **due now. Blocks Slice 3 and everything after** |
| Slice 2 — lint + packaging | ⬜ first Codex task. Independent of CP‑1 |
| Slice 2.5 — single-process spike | ⬜ written into the plan (R4) |
| Slice 2.9 — extract `app/domain/` | ⬜ written into the plan (R10). Closes A7 |
| Slices 3–10, Phase 5 | ⬜ |

Working on `main` (deliberate). `origin/main` is at `f195ece`; **18 commits unpushed**
(14 from this restructure, plus the 4 pre-baseline chores up to `660a6a0`).
Everything since `f195ece` exists on one machine only.

## Gate (`ci/baseline.json`)

Ratchet: CI fails only when a number goes **up**. Lower it in the commit that earns it.

```
pytest    208 passed, 0 failed, 0 skipped, 1 deselected (external)
pyright   32 errors    (basic; off=0, standard=32, strict=1036)
ruff      747 findings (672 auto-fixable — that is Slice 2)
imports   2 broken     (both from A7, one edge)
```

Coverage is a **reported metric, never a gate** (R3): `fastapi_run.py` 50%,
`run_manager.py` 82%, `scoring.py` 78%, `pipeline/pipeline.py` **22%**, total 46%.

## Blocked / do not touch

- **A10** — PostgreSQL migration blockers. ⛔ Do not fix. Decide **D1** (drop SQL Server?)
  at CP‑3 first; the right fix depends on the answer. Nothing depends on it — SQLite and
  SQL Server both run `upgrade head` clean, verified on a real container.
- **A7** — `db/` imports `pipeline/`. Closed by Slice 2.9, not as a standalone bugfix.
- **A5** — `make_engine` rejects in-memory SQLite. **Still open** — an earlier STATE.md
  claimed it landed; it never did, `app/db/engine.py` is untouched. Not blocking: the DB
  fixtures use file-based SQLite, which works.

## Known gaps in the oracle

Read these before trusting a green gate.

- **`tests/integration/test_pipeline_offline.py` is unwritten.** TEST-STRATEGY §4 calls
  for it; Slice 1 did not deliver it. `pipeline/pipeline.py` therefore sits at **22%** —
  it *dropped* when the legacy end-to-end test left the gate and nothing replaced it.
  The parse → score → artifact path is the least-covered thing in the repo. Fix before
  any slice touches `pipeline/`.
- **`app/prefect_run.py` is at 0%** and Slice 8 rewrites it. Verified differentially
  instead (D2/R7).
- 18 of 42 routes are unauthenticated (**A12**) — pinned as the current contract, not
  endorsed.

## Next three actions

1. **CP‑1** — hand the contract suite to Chat. Blocks Slice 3.
2. **Slice 2** to Codex — mechanical, independent of CP‑1, and it clears 672 ruff
   findings plus the hook friction below.
3. Write `tests/integration/test_pipeline_offline.py`, or explicitly accept 22%.

## Friction worth knowing

- The `ruff-check` hook lints whole staged **files**, so any commit touching
  `app/db/models.py` or most of `app/` is blocked by pre-existing Slice 2 findings.
  `--no-verify` with a stated reason is the expected escape — see AGENTS.md. Slice 2
  removes the need.
- `git add` leaves things staged across commands. A leftover `git add tests/legacy/`
  silently rode into an unrelated commit this session and the history had to be rebuilt
  with `reset --soft`. Stage per commit, and check `git diff --cached --stat` first.

---

## Map

| File | What it is |
| --- | --- |
| `AGENTS.md` (= `CLAUDE.md`) | Conventions, layering, do-nots. Auto-loads. |
| `docs/refactor-plan.md` | The slices. Files, symbols, shims, verify blocks. |
| `docs/PHASE-2-REVIEW.md` | Amendments R1–R10, decisions D1–D4 |
| `docs/TEST-STRATEGY.md` | Why the old suite rotted; §8 has the `_experience_delta` verdict |
| `docs/CHAT-CHECKPOINTS.md` | Where execution stops for a Chat decision |
| `docs/AGENT-WORKFLOW.md` | Claude-plans / Codex-executes loop |
| `docs/backlog.md` | Parked items, buckets A–D |
| `docs/architecture.md` | Module inventory, dependency graph |
| `docs/adr/` | Why each decision was made |
| `docs/liveness-audit.md` | Prompt to run before CP‑3 |

## Restart protocol

- **Claude Code** — new session per slice. `Read AGENTS.md and docs/STATE.md. Execute
  Slice N.` Prefer `/clear` over `/compact`.
- **Codex** — cold every task by design. `AGENTS.md` + `tasks/slice-NN.md` is the whole
  context.
- **Chat** — new session per checkpoint. `Read docs/STATE.md. I'm at CP‑n.`

`/slice-brief` and `/slice-review` do **not** exist yet — `.claude/commands/` is deferred
to Phase 3 ([AGENT-WORKFLOW.md](AGENT-WORKFLOW.md) §8 step 7). Write briefs from an
explicit prompt against §2's schema until then. `make report SLICE=… BASE=…` does exist.

Never paste state into a new session. Point at files.

Any decision made in Chat and not written into a file is lost. Three have been caught
this way: A10's deferral, R10's slice, and A11/A12 (which existed only in test docstrings
until this session's close).
