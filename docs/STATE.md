# State

**Single entry point for any new session.** Read this plus `AGENTS.md` and you have the
project. Everything else is reference, read on demand.

**Update this at the end of every session.** A stale STATE.md is worse than none — it
gets trusted.

_Last updated: 2026-08-01_

---

## Where we are

Phases 0–2 complete. **CP‑1 ran and returned NOT TRUSTWORTHY.** The fix list is
[docs/CP-1-REVIEW.md](CP-1-REVIEW.md). Slice 2 is **halted**; Slice 3 stays **blocked**.

| | Status |
| --- | --- |
| Phases 0–2 — baseline, architecture, plan + review (R1–R10, D1–D4) | ✅ |
| Slice 0 — gate infrastructure | ✅ `bfbcaaf` |
| Bugfixes A6, A8, A9 | ✅ `75d1924`, `0e5af61`, `20ae054` — each with its proof hash |
| Slice 1 — contract test suite | ✅ `8b0116e` — but see CP‑1: green ≠ trustworthy |
| Slice 2 brief for Codex | ✅ `99218ce` → `tasks/slice-02.md` |
| **CP‑1 — oracle review** | 🔴 **RETURNED NOT TRUSTWORTHY.** CP‑1 B1–B7 outstanding |
| CP‑1 B4 — log-chunk UTF‑8 corruption | ✅ `a539f56`, test-first at `ea93e34`. Merged |
| Suite hermeticity — `.env.dev` leak | ✅ `bf88f64` |
| **Slice 2 — lint + packaging** | 🛑 **HALTED.** Briefed, not started |
| Slice 3 | 🛑 blocked by CP‑1 |
| Slice 2.5 spike · Slice 2.9 `app/domain/` | ⬜ in the plan (R4, R10) |
| Slices 4–10, Phase 5 | ⬜ |

CP‑1's verdict in one line: **seven places where a test's name and docstring claimed a
property the test could not fail to detect the absence of.** An incomplete oracle is
safe — the gap shows as low coverage. An oracle that asserts what it does not check is
not, because everything downstream is graded against it. Two of the seven were not test
defects at all but live bugs, each found by following the test that claimed to pin it.

Two items are already closed, both test-first:

- **CP‑1 B4** — `read_log_chunk` split UTF‑8 codepoints at chunk boundaries and decoded with
  `errors="replace"`, so the bytes were destroyed on both sides of the boundary and
  never recovered. Live corruption inside a must-never-break contract. Note the fix
  CP‑1 sketched has a starvation bug — see the contract note in
  [refactor-plan.md](refactor-plan.md) Slice 5.
- **Suite hermeticity** — `tests/conftest.py` used `os.environ.setdefault`, so a sourced
  `.env.dev` kept its real `mssql+pyodbc` URL and the "offline" suite opened a live
  connection to SQL Server during `TestClient`'s lifespan. This is why 208/0 could not
  be reproduced on another machine: the result depended on whether that machine's
  container was up. See backlog **A13** before anyone retries this as a fixture fix.

Working on `main` (deliberate). `origin/main` is at `f195ece`; **nothing since then has
been pushed** — the whole restructure exists on one machine only.

> Do not record an absolute commit count here. Writing it creates a commit, which makes
> the number wrong the instant it is written — `9b03785 "correct unpushed count to 22"`
> was itself the 23rd, and it stalled a Codex run on a STATE-versus-disk mismatch.
> Anything self-invalidating belongs in `git`, not in STATE.md.
Everything since `f195ece` exists on one machine only. `refactor/restructure` is a
separate worktree still parked at `660a6a0`.

## Gate (`ci/baseline.json`) — measured, not remembered

```
pytest    226 passed, 0 failed, 0 skipped, 1 deselected (external)
pyright   32 errors    (basic; off=0, standard=32, strict=1036)
ruff      747 findings (676 auto-fixable — that is Slice 2; expect 31 after)
imports   2 broken     (both from A7, one edge)
```

208 → 226: 8 from `tests/test_suite_hermeticity.py`, 10 from CP‑1 B4. Banked in `d9f4ce7` —
`pytest_passed` is the one `HIGHER_IS_BETTER` key, so it is the ratchet moving in the
permitted direction.

**The old 208 was not a comparable number.** It was measured with an ambient
environment leaking in, so it meant something different on every machine. 226 is
reproducible: verified byte-identical with and without `.env.dev` sourced.

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

**Read [CP-1-REVIEW.md](CP-1-REVIEW.md) first — it supersedes this list.** These are the
gaps that were *visible* before the review. CP‑1's finding is that the more dangerous
gaps were the invisible ones: tests that read as covering a property while being
structurally incapable of failing on it.

- **`tests/integration/test_pipeline_offline.py` does not exist.** The directory is
  empty and untracked. `pipeline/pipeline.py` is at **22%** — it *dropped* when the
  legacy end-to-end test left the gate and nothing replaced it. Branch
  `fix/pipeline-offline` was planned this session and **never created**; no work exists.
- `prefect_run.py` at 0% — Slice 8 rewrites it, verified differentially (D2/R7).
- 18 of 42 routes are unauthenticated (**A12**) — pinned as the current contract, not
  endorsed. On the CP‑3 agenda. Note CP‑1 **B5**: the 401 sweep is exhaustive over all 24
  protected routes, which makes the auth story *read* as complete, but "rejects a
  stranger" and "rejects a logged-in stranger" are different properties and only the
  first is checked. The A12 question is about the 18; B5 is about the other 24.
- `app/pipeline/parsers.py` has **zero** coverage in the new suite (CP‑1 **S6**): the
  `job_html` fixture and `tests/fixtures/jobs/job_stepstone_1.html` are read by nothing.
  Combined with the missing integration test, the parse → score → artifact path is
  covered at one of its three stages.

## Next four actions

In order. Everything here is CP‑1 remediation — **Slice 2 does not resume until CP‑1
comes back clean**, because Slice 2's own verification is graded against this oracle.

1. **CP‑1 B1, B2, B3, B6, B7** — one commit against `tests/`, plus two constant promotions in
   `app/`. B1–B3 are the `copy.deepcopy` fixes to the scoring invariants (shallow copies
   and shared set references make three assertions unfireable); B6 promotes the accept
   threshold and `LOG_CHUNK_MAX_BYTES` to constants the app owns, so the test asserts
   *about* the value instead of duplicating it. **B7** —
   `test_health_db_reports_reachability` — is promoted out of S1 into this batch: it
   asserts `in (200, 503)` under a docstring claiming SQLite, and that is the assertion
   that concealed the live DB connection above.
2. **CP‑1 B5** — `other_user` fixture plus a cross-tenant sweep over the DB-backed protected
   routes. Ownership is currently asserted for 3 of 24. **If any route returns 200, stop:
   that is a live authorisation bug and an escalation, not a test fix.**
3. **Write `tests/integration/test_pipeline_offline.py`** on `fix/pipeline-offline`, or
   explicitly accept 22% on the parse → score → artifact path. Slice 3 moves
   `stepstone/` into `sources/` and this is the only test that would notice if the
   parse → score handoff broke during the move.
4. **Re-run CP‑1** against the repaired suite. Only a clean verdict unblocks Slice 3 —
   and resumes Slice 2.

S1–S6 stay before Slice 5 (minus B7, promoted); L1–L4 before Slice 7.

> **These are CP‑1's B numbers, from [CP-1-REVIEW.md](CP-1-REVIEW.md) — not `backlog.md`
> bucket B.** Two independent B sequences are live at once and they collide: CP‑1 B4 is
> the log-chunk UTF‑8 bug, backlog B4 is promoting `_now_iso`. Always write the prefix.

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
