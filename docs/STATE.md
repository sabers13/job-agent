# State

**Single entry point for any new session.** Read this plus `AGENTS.md` and you have the
project. Everything else is reference, read on demand.

**Update this at the end of every session.** A stale STATE.md is worse than none — it
gets trusted.

_Last updated: 2026-08-02_

---

## Where we are

**CP‑1 returned NOT TRUSTWORTHY; CP1-1 … CP1-7 are now all closed.** Fix list:
[CP-1-REVIEW.md](CP-1-REVIEW.md). **The re-run is the only thing left** — Slice 2 stays
**halted** and Slice 3 **blocked** until CP‑1 gives a clean verdict, because the whole
point is that this suite grades them.

| | Status |
| --- | --- |
| Phases 0–2 · Slice 0 · bugfixes A6, A8, A9 | ✅ |
| Slice 1 — contract test suite | ✅ `8b0116e` — but see CP‑1: green ≠ trustworthy |
| Slice 2 brief for Codex | ✅ `99218ce` → `tasks/slice-02.md` |
| CP1-4 — log-chunk UTF‑8 corruption | ✅ `ea93e34` red → `a539f56` green |
| Suite hermeticity — `.env.dev` leak | ✅ `bf88f64` |
| **CP1-5 — cross-tenant isolation** | ✅ **probe clean, no auth bug.** 15 probes + 5 controls |
| CP1-1, CP1-2, CP1-3, CP1-6, CP1-7 | ✅ one commit, `tests/` + constant promotions |
| **CP‑1 re-run** | ⬜ **the remaining blocker** |
| **Slice 2 — lint + packaging** | 🛑 **HALTED** until the CP‑1 re-run is clean |
| Slice 3 | 🛑 blocked by the CP‑1 re-run |
| Slice 2.5 spike · Slice 2.9 `app/domain/` · Slices 4–10 · Phase 5 | ⬜ |

CP‑1's verdict: **seven tests whose name and docstring claimed a property the test could
not fail to detect the absence of.** An incomplete oracle is safe — low coverage is
visible. One that asserts what it does not check is not, because every later gate is
graded against it. Two of the seven were live bugs, each found by following the test that
claimed to pin it. Both are now closed:

- **CP1-4** — `read_log_chunk` split UTF‑8 codepoints at chunk boundaries and decoded
  with `errors="replace"`, destroying the bytes on both sides. Corruption inside a
  must-never-break contract. CP‑1's *suggested* fix has a starvation bug — see the
  contract note in [refactor-plan.md](refactor-plan.md) Slice 5 before touching this.
- **Suite hermeticity** — `conftest.py` used `os.environ.setdefault`, so a sourced
  `.env.dev` kept its real `mssql+pyodbc` URL and the "offline" suite opened a live
  connection to SQL Server in `TestClient`'s lifespan. Why 208/0 reproduced on one
  machine and not another. Read backlog **A13** before retrying this as a fixture fix.

Working on `main` (deliberate). **`origin/main` is at `f562202`** — the restructure is
pushed and no longer single-machine. (It read `f195ece` with a "nothing has been pushed"
warning until 2026-08-02; the `f195ece..f562202` push had landed and the warning was
stale. A backup claim is the one kind of stale state that costs you the work.)
`refactor/restructure` is a separate worktree parked at `660a6a0`.

> Do not record an absolute commit count here. Writing it creates a commit, which makes
> the number wrong the instant it is written — `9b03785 "correct unpushed count to 22"`
> was itself the 23rd, and it stalled a Codex run on a STATE-versus-disk mismatch.

## Gate (`ci/baseline.json`) — measured, not remembered

```
pytest    248 passed, 0 failed, 0 skipped, 1 deselected (external)
pyright   32 errors    (basic; off=0, standard=32, strict=1036)
ruff      747 findings (676 auto-fixable — that is Slice 2; expect 31 after)
imports   2 broken     (both from A7, one edge)
```

Banked twice. 208 → 226 in `d9f4ce7` (8 hermeticity tests, 10 from CP1-4), then **226 →
248** with the CP‑1 batch: 20 in `test_cross_tenant_isolation.py`, 1 for CP1-3's
`test_rescoring_an_already_scored_job_is_stable`, 1 for CP1-6's
`test_http_honours_the_same_cap_as_the_function`. `pytest_passed` is the one
`HIGHER_IS_BETTER` key, so this is the ratchet moving in the permitted direction.

**The other four did not move**, and that is the result to check rather than skim: CP1-6
promoted constants, which is behaviour-preserving by construction, so a change in `ruff`
or `imports` would have meant the promotion did something it was not supposed to.

**The old 208 was not a comparable number** — measured with an ambient environment
leaking in, it meant something different on every machine. **248 is verified identical
with and without `.env.dev` sourced**, re-checked on 2026-08-02. Re-run that both ways
whenever this number moves; it is the only thing that makes it mean anything.

Coverage is a **reported metric, never a gate** (R3): `fastapi_run.py` 50%,
`run_manager.py` 82%, `scoring.py` 78%, `pipeline/pipeline.py` **22%**,
`prefect_run.py` 0%, total 46%.

## Blocked / do not touch

- **A10** — PostgreSQL migration blockers. ⛔ Do not fix. Decide **D1** (drop SQL Server?)
  at CP‑3 first; the right fix depends on the answer. Nothing depends on it.
- **A1** — `_LogSink`, `F821` at `fastapi_run.py:1586`. Not inside Slice 2 — live
  behaviour bug, needs its own test-first commit.
- **A7** — `db/` imports `pipeline/`. Closed by Slice 2.9, not standalone.
- **A5** — `make_engine` rejects in-memory SQLite. Open; DB fixtures use file-based.
- **A13** — `check_db` unreachable from fixtures. Environment-layer fix landed and is
  sufficient; the seam defect itself waits for the Slice 6 service layer.

## Known gaps in the oracle

[CP-1-REVIEW.md](CP-1-REVIEW.md) supersedes this list — these are the gaps that were
*visible* before it. Its finding is that the dangerous ones were invisible.

- **`tests/integration/test_pipeline_offline.py` does not exist**; `pipeline/pipeline.py`
  at **22%**. Branch `fix/pipeline-offline` was planned and never created.
- `app/pipeline/parsers.py` at **zero** coverage (CP‑1 S6) — `job_html` and
  `tests/fixtures/jobs/job_stepstone_1.html` are read by nothing. With the missing
  integration test, parse → score → artifact is covered at one of three stages.
- `prefect_run.py` at 0% — Slice 8 rewrites it, verified differentially (D2/R7).
- 18 of 42 routes unauthenticated (**A12**) — pinned, not endorsed; CP‑3 agenda. **CP1-5
  closed the other half**: the 24 protected routes are now tested for "rejects a
  *logged-in* stranger", not only "rejects a stranger", and all of them do.
- **`/api/profile/{key}` GET/POST/DELETE are not tenant-scoped** — the file-backed store,
  no `/my/`. Any authenticated user reads and writes the same global
  `config/focus_profiles.json`. Found while running the CP1-5 probe and deliberately left
  alone: it is not DB-backed, so it was outside the probe's scope, and D1 has already
  decided the DB surface supersedes it. **CP‑3 agenda, beside A12.**

## Next three actions

All CP‑1 remediation. **Slice 2 does not resume until CP‑1 is clean** — its own
verification is graded against this oracle.

1. **Write `tests/integration/test_pipeline_offline.py`** on `fix/pipeline-offline`, or
   explicitly accept 22%. Slice 3 moves `stepstone/` into `sources/` and this is the only
   test that would notice if the parse → score handoff broke during the move.
2. **Re-run CP‑1.** Only a clean verdict unblocks Slice 3 and resumes Slice 2. CP1-1 …
   CP1-7 are closed, so this is the whole remaining blocker.
3. **Then Slice 2 resumes** from `tasks/slice-02.md`, and Slice 3 unblocks.

Then S1–S6 (minus CP1-7, promoted) before Slice 5; L1–L4 before Slice 7. **S5 is the
cheapest and is now the odd one out** — `test_blockers_still_dominate_when_the_llm_is_mocked`
still asserts a magic `<= 35` two functions after an identical property is asserted
relationally as `<= blocking.blocker_cap_hard`. Left alone deliberately: it is an S item and
this batch was scoped to the B list.

> **CP-1's items are `CP1-n`; `backlog.md` bucket B is `Bn`.** They used to be two
> colliding `B1`–`B7` sequences held apart by a naming convention. Renumbered mechanically
> on 2026-08-02, because a convention fails silently the first time a brief drops the
> prefix — and a cold Codex run has no way to notice.

## Friction worth knowing

- The `ruff-check` hook lints whole staged **files**, so a commit touching most of `app/`
  is blocked by pre-existing Slice 2 findings. `--no-verify` with a stated reason is the
  expected escape (AGENTS.md). Measure before/after counts to show you added none.
- `git add` leaves things staged across commands. Stage per commit; check
  `git diff --cached --stat` first.
- `ruff --exclude` *replaces* pyproject's exclude list — use `--extend-exclude`.
  `ruff format` also reformats Python inside Markdown. Both are in AGENTS.md.
- pytest's assertion rewriting prints every operand — never assert directly on a value
  that may hold a secret. In AGENTS.md §Conventions.

---

## Map

| File | What it is |
| --- | --- |
| `AGENTS.md` (= `CLAUDE.md`) | Conventions, layering, do-nots. Auto-loads. |
| `docs/CP-1-REVIEW.md` | **The live fix list.** CP1-1 … CP1-7 block Slice 3 |
| `docs/refactor-plan.md` | The slices. Files, symbols, shims, verify blocks. |
| `docs/PHASE-2-REVIEW.md` | Amendments R1–R10, decisions D1–D4 |
| `docs/TEST-STRATEGY.md` | Why the old suite rotted; §8 has the `_experience_delta` verdict |
| `docs/CHAT-CHECKPOINTS.md` | Where execution stops for a Chat decision |
| `docs/SESSION-HANDOFF.md` | OPEN/CLOSE prompts for every surface |
| `docs/AGENT-WORKFLOW.md` | Claude-plans / Codex-executes loop; §2 is the brief schema |
| `docs/backlog.md` | Parked items, buckets A–D |
| `docs/architecture.md` | Module inventory, dependency graph |
| `docs/adr/` | Why each decision was made |
| `docs/liveness-audit.md` | Prompt to run before CP‑3 |
| `tasks/slice-02.md` | The Codex brief — **halted, do not execute** |

## Restart protocol

- **Claude Code** — new session per slice. Prefer `/clear` over `/compact`.
- **Codex** — cold every task. `AGENTS.md` + `tasks/slice-NN.md` is the whole context.
- **Chat** — new session per checkpoint.

`.claude/commands/` does not exist yet (Phase 3, AGENT-WORKFLOW §8 step 7), so briefs are
written from an explicit prompt against §2's schema. `make report SLICE=… BASE=…` works.

Never paste state into a new session. Point at files.

Any decision made in Chat and not written into a file is lost. Three have been caught
this way: A10's deferral, R10's slice, and A11/A12.
