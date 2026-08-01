# State

**Single entry point for any new session.** Read this plus `AGENTS.md` and you have the
project. Everything else is reference, read on demand.

**Update this at the end of every session.** A stale STATE.md is worse than none — it
gets trusted.

_Last updated: 2026-08-01_

---

## Where we are

**CP‑1 ran and returned NOT TRUSTWORTHY.** Fix list: [CP-1-REVIEW.md](CP-1-REVIEW.md).
Slice 2 is **halted**; Slice 3 stays **blocked**.

| | Status |
| --- | --- |
| Phases 0–2 · Slice 0 · bugfixes A6, A8, A9 | ✅ |
| Slice 1 — contract test suite | ✅ `8b0116e` — but see CP‑1: green ≠ trustworthy |
| Slice 2 brief for Codex | ✅ `99218ce` → `tasks/slice-02.md` |
| **CP‑1 — oracle review** | 🔴 **NOT TRUSTWORTHY.** B1, B2, B3, B5, B6, B7 open |
| CP‑1 B4 — log-chunk UTF‑8 corruption | ✅ `ea93e34` red → `a539f56` green |
| Suite hermeticity — `.env.dev` leak | ✅ `bf88f64` |
| **Slice 2 — lint + packaging** | 🛑 **HALTED** until CP‑1 is clean |
| Slice 3 | 🛑 blocked by CP‑1 |
| Slice 2.5 spike · Slice 2.9 `app/domain/` · Slices 4–10 · Phase 5 | ⬜ |

CP‑1's verdict: **seven tests whose name and docstring claimed a property the test could
not fail to detect the absence of.** An incomplete oracle is safe — low coverage is
visible. One that asserts what it does not check is not, because every later gate is
graded against it. Two of the seven were live bugs, each found by following the test that
claimed to pin it. Both are now closed:

- **CP‑1 B4** — `read_log_chunk` split UTF‑8 codepoints at chunk boundaries and decoded
  with `errors="replace"`, destroying the bytes on both sides. Corruption inside a
  must-never-break contract. CP‑1's *suggested* fix has a starvation bug — see the
  contract note in [refactor-plan.md](refactor-plan.md) Slice 5 before touching this.
- **Suite hermeticity** — `conftest.py` used `os.environ.setdefault`, so a sourced
  `.env.dev` kept its real `mssql+pyodbc` URL and the "offline" suite opened a live
  connection to SQL Server in `TestClient`'s lifespan. Why 208/0 reproduced on one
  machine and not another. Read backlog **A13** before retrying this as a fixture fix.

Working on `main` (deliberate). `origin/main` is at `f195ece`; **nothing since then has
been pushed** — the whole restructure exists on one machine only. `refactor/restructure`
is a separate worktree parked at `660a6a0`.

> Do not record an absolute commit count here. Writing it creates a commit, which makes
> the number wrong the instant it is written — `9b03785 "correct unpushed count to 22"`
> was itself the 23rd, and it stalled a Codex run on a STATE-versus-disk mismatch.

## Gate (`ci/baseline.json`) — measured, not remembered

```
pytest    226 passed, 0 failed, 0 skipped, 1 deselected (external)
pyright   32 errors    (basic; off=0, standard=32, strict=1036)
ruff      747 findings (676 auto-fixable — that is Slice 2; expect 31 after)
imports   2 broken     (both from A7, one edge)
```

208 → 226 banked in `d9f4ce7`: 8 hermeticity tests, 10 from CP‑1 B4. `pytest_passed` is
the one `HIGHER_IS_BETTER` key, so this is the ratchet moving in the permitted direction.

**The old 208 was not a comparable number** — measured with an ambient environment
leaking in, it meant something different on every machine. 226 is verified identical with
and without `.env.dev` sourced.

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
- 18 of 42 routes unauthenticated (**A12**) — pinned, not endorsed; CP‑3 agenda. Distinct
  from CP‑1 **B5**, which is that the other 24 are tested for "rejects a stranger" but
  not "rejects a *logged-in* stranger".

## Next four actions

All CP‑1 remediation. **Slice 2 does not resume until CP‑1 is clean** — its own
verification is graded against this oracle.

1. **CP‑1 B1, B2, B3, B6, B7** — one commit against `tests/`, plus two constant
   promotions in `app/`. B1–B3 are `copy.deepcopy` fixes to the scoring invariants;
   B6 promotes the accept threshold and `LOG_CHUNK_MAX_BYTES` so tests assert *about*
   the value instead of duplicating it; B7 is `test_health_db_reports_reachability`.
2. **CP‑1 B5** — `other_user` fixture plus a cross-tenant sweep. Ownership is asserted
   for 3 of 24 routes. **If any route returns 200, stop: live authorisation bug, an
   escalation, not a test fix.**
3. **Write `tests/integration/test_pipeline_offline.py`** on `fix/pipeline-offline`, or
   explicitly accept 22%. Slice 3 moves `stepstone/` into `sources/` and this is the only
   test that would notice if the parse → score handoff broke during the move.
4. **Re-run CP‑1.** Only a clean verdict unblocks Slice 3 and resumes Slice 2.

Then S1–S6 (minus B7, promoted) before Slice 5; L1–L4 before Slice 7.

> **These are CP‑1's B numbers, not `backlog.md` bucket B.** Two independent B sequences
> collide: CP‑1 B4 is the log-chunk bug, backlog B4 is promoting `_now_iso`. Always write
> the prefix.

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
| `docs/CP-1-REVIEW.md` | **The live fix list.** B1–B7 block Slice 3 |
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
