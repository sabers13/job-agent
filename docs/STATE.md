# State

**Single entry point for any new session.** Read this plus `AGENTS.md` and you have the
project. Everything else is reference, read on demand.

**Update this at the end of every session.** A stale STATE.md is worse than none — it
gets trusted.

_Last updated: 2026-08-01_

---

## Where we are

Phases 0–2 complete. Slice 1 is green, so **CP‑1 is due and blocks Slice 3**.

| | Status |
| --- | --- |
| Phases 0–2 — baseline, architecture, plan + review (R1–R10, D1–D4) | ✅ |
| Slice 0 — gate infrastructure | ✅ `bfbcaaf` |
| Bugfixes A6, A8, A9 | ✅ `75d1924`, `0e5af61`, `20ae054` — each with its proof hash |
| Slice 1 — contract test suite | ✅ `8b0116e`. 208 passed, 0 failed, 0 skipped |
| Slice 2 brief for Codex | ✅ `99218ce` → `tasks/slice-02.md` |
| **CP‑1 — oracle review** | 🔴 **due. Blocks Slice 3 and everything after** |
| Slice 2 — lint + packaging | ⬜ briefed, not started. Independent of CP‑1 |
| Slice 2.5 spike · Slice 2.9 `app/domain/` | ⬜ in the plan (R4, R10) |
| Slices 3–10, Phase 5 | ⬜ |

Working on `main` (deliberate). `origin/main` is at `f195ece`; **20 commits unpushed**.
Everything since `f195ece` exists on one machine only. `refactor/restructure` is a
separate worktree still parked at `660a6a0`.

## Gate (`ci/baseline.json`) — measured, not remembered

```
pytest    208 passed, 0 failed, 0 skipped, 1 deselected (external)
pyright   32 errors    (basic; off=0, standard=32, strict=1036)
ruff      747 findings (676 auto-fixable — that is Slice 2; expect 31 after)
imports   2 broken     (both from A7, one edge)
```

Coverage is a **reported metric, never a gate** (R3): `fastapi_run.py` 50%,
`run_manager.py` 82%, `scoring.py` 78%, `pipeline/pipeline.py` **22%**,
`prefect_run.py` 0%, total 46%.

## Blocked / do not touch

- **A10** — PostgreSQL migration blockers. ⛔ Do not fix. Decide **D1** (drop SQL
  Server?) at CP‑3 first; the right fix depends on the answer. Nothing depends on it —
  SQLite and SQL Server both run `upgrade head` clean, verified on a real container.
- **A1** — `_LogSink`. ruff reports it as `F821` at `fastapi_run.py:1586`. Do **not**
  fix it inside Slice 2; it is a live behaviour bug and needs its own test-first commit.
- **A7** — `db/` imports `pipeline/`. Closed by Slice 2.9, not as a standalone bugfix.
- **A5** — `make_engine` rejects in-memory SQLite. Still open, `app/db/engine.py`
  untouched. Not blocking: the DB fixtures use file-based SQLite.

## Known gaps in the oracle

- **`tests/integration/test_pipeline_offline.py` does not exist.** The directory is
  empty and untracked. `pipeline/pipeline.py` is at **22%** — it *dropped* when the
  legacy end-to-end test left the gate and nothing replaced it. Branch
  `fix/pipeline-offline` was planned this session and **never created**; no work exists.
- `prefect_run.py` at 0% — Slice 8 rewrites it, verified differentially (D2/R7).
- 18 of 42 routes are unauthenticated (**A12**) — pinned as the current contract, not
  endorsed. On the CP‑3 agenda.

## Next three actions

1. **Run Slice 2** — `tasks/slice-02.md` is written and Codex-ready. Mechanical,
   independent of CP‑1, clears 676 findings and the `--no-verify` friction below.
2. **CP‑1** — hand the contract suite to Chat. Blocks Slice 3.
3. **Write `tests/integration/test_pipeline_offline.py`** on `fix/pipeline-offline`, or
   explicitly accept 22% on the parse → score → artifact path.

## Friction worth knowing

- The `ruff-check` hook lints whole staged **files**, so any commit touching
  `app/db/models.py` or most of `app/` is blocked by pre-existing Slice 2 findings.
  `--no-verify` with a stated reason is the expected escape (AGENTS.md). Slice 2 ends it.
- `git add` leaves things staged across commands. A leftover `git add tests/legacy/`
  rode into an unrelated commit and the history had to be rebuilt with `reset --soft`.
  Stage per commit; check `git diff --cached --stat` first.
- `ruff --exclude` *replaces* pyproject's exclude list — use `--extend-exclude`, or it
  silently rewrites `alembic/versions/`. `ruff format` also reformats Python inside
  Markdown. Both are now in AGENTS.md.

---

## Map

| File | What it is |
| --- | --- |
| `AGENTS.md` (= `CLAUDE.md`) | Conventions, layering, do-nots. Auto-loads. |
| `docs/refactor-plan.md` | The slices. Files, symbols, shims, verify blocks. |
| `docs/PHASE-2-REVIEW.md` | Amendments R1–R10, decisions D1–D4 |
| `docs/TEST-STRATEGY.md` | Why the old suite rotted; §8 has the `_experience_delta` verdict |
| `docs/CHAT-CHECKPOINTS.md` | Where execution stops for a Chat decision |
| `docs/AGENT-WORKFLOW.md` | Claude-plans / Codex-executes loop; §2 is the brief schema |
| `docs/backlog.md` | Parked items, buckets A–D |
| `docs/architecture.md` | Module inventory, dependency graph |
| `docs/adr/` | Why each decision was made |
| `docs/liveness-audit.md` | Prompt to run before CP‑3 |
| `tasks/slice-02.md` | The live Codex brief |

## Restart protocol

- **Claude Code** — new session per slice. Prefer `/clear` over `/compact`.
- **Codex** — cold every task. `AGENTS.md` + `tasks/slice-NN.md` is the whole context.
- **Chat** — new session per checkpoint.

`.claude/commands/` does not exist yet (Phase 3, AGENT-WORKFLOW §8 step 7), so briefs are
written from an explicit prompt against §2's schema. `make report SLICE=… BASE=…` works.

Never paste state into a new session. Point at files.

Any decision made in Chat and not written into a file is lost. Three have been caught
this way: A10's deferral, R10's slice, and A11/A12.
