# State

**Single entry point for any new session.** Read this plus `AGENTS.md` and you have the
project. Everything else is reference, read on demand.

**Update this at the end of every session.** A stale STATE.md is worse than none — it
gets trusted.

_Last updated: 2026-08-07 (Slice 2.5 spike closed; **CP‑2 closed**; next action is S1's accept sets)_

---

## Where we are

**CP‑1 is CLOSED.** The oracle is trustworthy. CP1-1 … CP1-8 are all fixed; the review's
remaining items are **S** (should-fix) and **L** (latent), neither of which blocks.

**Slice 2, the Slice 2.5 spike and CP‑2 are all done.** The spike passed, so Slice 8 stays a
refactor — neither its estimate nor its risk rating changes — but its scope widened on three
counts recorded below and in `refactor-plan.md` §6.

**S1's accept sets are closed (`c1daf51`) and the liveness audit is done**
(`docs/liveness-report.md`, generated against `fd85028`). **CP‑3 is the next action — a new
Chat session, not this one.** **Slice 3 is blocked until CP‑3 closes**
(`CHAT-CHECKPOINTS.md` §CP‑3: "Slice 3 must not start").

| | Status |
| --- | --- |
| Phases 0–2 · Slice 0 · bugfixes A6, A8, A9 | ✅ |
| Slice 1 — contract test suite | ✅ `8b0116e` — but see CP‑1: green ≠ trustworthy |
| **CP‑1 — oracle review, both passes** | ✅ **CLOSED.** CP1-1 … CP1-8 all fixed |
| **Slice 2 — lint + packaging** | ✅ merged at `80e73c8`; report at `346664e` |
| **Slice 2.5 — single-process spike** | ✅ **PASSED** 2026-08-07 — verdict in `refactor-plan.md` §6 |
| **CP‑2 — spike verdict** | ✅ **CLOSED 2026-08-07.** Passed. Slice 8 stays a refactor; scope widened — see below |
| **S1's seven wide accept sets** | ✅ **CLOSED**, merged `c1daf51`. Gate re-run post-merge: all five metrics unmoved |
| **Liveness audit** | ✅ **DONE 2026-08-07** — `docs/liveness-report.md`, 1,174 lines. Corrected 4 bucket‑D premises, found A17/A18 |
| **CP‑3** | ⬜ **NEXT ACTION.** Chat, new session. Agenda in `CHAT-CHECKPOINTS.md` §CP‑3 |
| Slice 3 | ⬜ **BLOCKED by CP‑3** — not unblocked. `CHAT-CHECKPOINTS.md` §CP‑3: "Slice 3 must not start" |
| S2–S5, S7, S8, S9 | ⬜ before Slice 5 |
| L1–L4 | ⬜ before Slice 7 |
| Slice 2.9 `app/domain/` · Slices 4–10 · Phase 5 | ⬜ |

> The three rows above the accept-set row were rewritten on 2026-08-07. This file had said
> "Slice 2 is the next action" since `1944312`, but `git log` shows Slice 2 merged at `80e73c8`
> with its report at `346664e`. The spike was run against the merged tree.

### What landed this session (2026-08-07)

Documentation only — the Slice 2.5 spike merged no code, by design. `spike/inprocess-batch` was
branched from `main`, never committed to, and deleted. The verdict is in `refactor-plan.md` §6
and risk-register row 9 is closed. Three run directories from the spike are on disk under
`output/3aa9e552-.../junior_data_bi/` — two failed (`20260807T140117`, `20260807T140311`) and one
`completed` (`20260807T140914`); `output/` is gitignored, so they are local evidence for CP‑2 only.

Two defects were found and deliberately **not** fixed, since the spike's scope was to answer a
question: the in-process path never writes its exception's traceback to `run.log` (backlog **A3**
shape — a 25-byte log for a failed run), and it double-writes every log line by adding one
`FileHandler` to both the root and `prefect` loggers. Both are in `refactor-plan.md` §6 and both
land naturally inside Slice 8.

### CP‑2 verdict (closed, 2026-08-07)

Passed. Slice 8 remains a refactor — estimate and risk rating unchanged, risk-register
row 9 closed. **Its scope widens on three counts**, all from the spike:

1. **"In-process" is a misnomer.** Prefect spawns a second uvicorn on port 8912 as a
   child, and it survives `SIGTERM` — a process and port leak across restarts. Today's
   path removes the second *terminal*, not the second *process*. Single-process is still
   ahead, not behind. `LocalOrchestrator` must own worker lifecycle through `lifespan`,
   and the acceptance test is that no listener remains after shutdown.
2. **Prefect's config lives outside the repo.** `unset PREFECT_API_URL` does not clear
   it — the value comes from `~/.prefect/profiles.toml`, and ephemeral mode defaults to
   `False` in 3.1.15. So behaviour depends on files a fresh user machine does not have.
   This is the third instance of the same disease: `.env.dev` leaking into the test
   suite, self-invalidating counts in STATE.md, now the Prefect profile. It is a direct
   argument *for* `LocalOrchestrator` — removing Prefect removes the ambient surface
   entirely, which is what the one-click goal actually requires.
3. **Two observability defects** found and deliberately unfixed: the in-process path
   never writes its traceback to `run.log` (runs 1–2 left a 25-byte log with HTTP 200 and
   a correct `failed` status — an artifact that cannot explain its own failure, the A3
   shape), and every line is logged twice from one `FileHandler` on both the root and
   `prefect` loggers. Both land inside Slice 8.

### What landed the session before (`db3b908..HEAD` at that time)

CP1-8 — the StepStone route test stubbed nothing and reached the live network.

- `d130fa4` — **the network guard, installed before any fix.** `tests/net_guard.py`
  refuses outbound `connect`/`connect_ex`/`getaddrinfo` suite-wide;
  `tests/test_suite_hermeticity.py` §3 (7 tests) makes it executable. Run against the
  *unmodified* suite it reddened **exactly two** tests, both already known — so the leak
  was no wider than CP‑1 described. Deliberately committed red (2 failed / 296 passed).
- `2c87308` — **the fix.** Stub bound to `ss_search` (the real callee) with
  `raising=True`; asserts one status code, the exact body, **and the recorded call args**,
  so the binding is observable rather than inferred. Test-side only, no `app/` change.
- `9a4b4df` — `AGENTS.md`: `raising=False` is forbidden in a stub.
- `144a62f` — review closed, baseline banked 291 → 298.
- `1906754` — ADR 0009 → S7 pointer; AGENT-WORKFLOW §1 "a review's suggested code is a
  hypothesis, not a spec"; backlog note on the guard's process boundary.

Three results worth carrying, all measured while building it:

1. **Raising is not enough; the refusal must be recorded.** asyncio resolves DNS on an
   executor thread, so the leak raised where nothing was listening — and 78 broad handlers
   (A3) absorb what does reach them. `conftest` checks the guard's record per phase,
   outside every `except` in `app/`, and in the **call** phase so `pytest_passed` moves.
2. **`BaseException` was tried and rejected** — it kills the anyio portal `TestClient`
   runs on. Reasoning in `tests/net_guard.py`; do not re-derive it.
3. **Loopback is deliberately allowed.** Egress is what makes a gate machine-dependent.

## Gate (`ci/baseline.json`) — `make gate`, 2026-08-02, GATE PASSED

```
pytest    298 passed, 0 failed, 0 skipped, 1 deselected (external)
pyright   32 errors    (basic; off=0, standard=32, strict=1036)
ruff      747 findings (672 auto-fixable — that is Slice 2; expect 31 after)
imports   2 broken     (both from A7, one edge)
```

Banked four times: 208 → 226 → 248 → 291 → **298**. `pytest_passed` is the one
`HIGHER_IS_BETTER` key. The other four have not moved across any of the four, which is the
result to check rather than skim — every one of those batches was tests-and-docs only.

> **Read 291 with care; it is weaker evidence than it was recorded as.** It was written up
> as the first confirmation of this gate from a machine other than the one that produced
> it. Two of the 291 issued live HTTP requests, and their assertions accepted **both** the
> egress and the no-egress outcome — so a matching count could not distinguish "hermetic"
> from "reaching the network and reporting the same number either way". The invariance
> only held where the network fails *fast*: a machine that black-holes would have hung
> instead, which is exactly CP1-7's two presentations against SQL Server.
>
> **298 is the first count here that no network condition can change.** The lesson for
> CP‑3/CP‑4 is not "re-measure on a second machine" — that was done and it passed. It is
> that a matching number is only evidence of hermeticity once something in the suite
> **fails** when the environment differs.

Coverage is a **reported metric, never a gate** (R3), last measured at `db3b908`:
`pipeline.py` 92%, `parsers.py` 97%, `output.py` 87%, `potential_bucket.py` 91%,
`scoring.py` 79%, `run_manager.py` 82%, `fastapi_run.py` 50%, `prefect_run.py` 0%,
total 52%.

## Next three actions

0. ~~S1's seven wide accept sets~~ — **CLOSED**, merged to `main` at `c1daf51`, branch
   deleted, gate re-run clean afterwards. Kept here only for the record it leaves behind.

   Enumerated and measured by
   Claude Code (Opus 5 / high), executed by Codex in three commits, reviewed from the
   report. Outcome, for the record: 15 live accept-set assertions in `tests/contracts/`,
   **7 violations · 6 legitimate · 2 deferred.** The seven are narrowed to their measured
   code with the missing-field content the sets were hiding; the six legitimate ones were
   tightened anyway in a separate commit labelled as such; **the two deferred are the GUI
   page routes `health_and_pages:117,123` — backlog A12 wearing an accept set, and on the
   CP‑3 agenda.** None of the 15 contained 500. `pytest_passed` stayed at 298 across all
   three commits, which is the check that matters: no test was added or removed, only
   rewritten.

   `test_start_batch_run_…` (S10, below) came back stronger than briefed — it now asserts
   `calls == [("batch", "junior_data_bi", 7)]`, so **which backend was dispatched** is
   observable, not just that a stub fired. Slice 8 replaces that selection; a silent
   backend switch now fails a test.

   Two things it left behind, both recorded rather than fixed:

   - **`FocusProfileModel` is not on `app.pipeline`'s public surface**, so the test binds
     `fr.FocusProfileModel` and `fr.BatchSearchConfig` off `fastapi_run`. That is the
     correct call under the allowlist — importing from `app.pipeline.models` would have
     violated the submodule rule — and it is **direct evidence for Slice 2.9**, which moves
     that type to `app/domain/`. Both bindings break loudly (`AttributeError`) when
     Slice 2.9 or the Slice 6/7 router extraction moves them. That is intended; do not
     add a shim to soften it.
   - **The Codex sandbox stall is confirmed, not just suspected** — `TestClient` hangs in
     its AnyIO portal, so the gate ran outside the sandbox. `AGENT-WORKFLOW.md` §1b is
     updated: this is now a standing constraint, and Slice 6's brief must either accept
     out-of-sandbox verification explicitly or route verification to Claude Code.

0b. ~~The liveness audit~~ — **DONE**, `docs/liveness-report.md` (1,174 lines, five-section
   output contract, generated against `fd85028`). What it changed, all already written into
   the files that CP‑3 reads:

   - **Four bucket‑D premises were wrong**, and `backlog.md` §D is amended in place with the
     originals struck through. `smoke.py` is *not* the backend-dispatch façade D3 hedged
     about (it GETs a URL and returns a page `<title>`); `profile_store.py` is 85% covered,
     not 32%, and is the **seed source** for the DB store rather than a parallel one;
     `resume_parse.py` is 51%, not "12%, the lowest"; `n8n workflows/` is **tracked**, not
     untracked, and is the only first-party caller of `/job_details` and `/bundle`.
   - **Two new bugs, both A3-shaped**, filed as **A17** (`/search_stepstone` returns 500
     where it means 400 — its own `HTTPException` eaten by its own `except Exception`;
     `/bundle` has the same shape and is correct, so the fix is copyable) and **A18**
     (résumé upload cannot report a parse failure, which is *why* D5 is unanswerable).
   - **Three of the eight types `AGENTS.md` scheduled for `domain/` are dead** —
     `JobDetailsResponse` is shadowed by a live same-named class in `api/schemas.py`, and
     `FetchMeta`/`JobScoring` in `pipeline/models.py` are referenced only from inside the
     dead one. **Slice 2.9 deletes them rather than moving them**; `AGENTS.md` §Target
     architecture is corrected.
   - **A3 is 77, not 78** — the 78 was measured on `660a6a0`. Corrected, with a note not to
     quote the number.
   - **`architecture.md` is stale in three known places** (it predates `bc301e3`); banner
     added at the top of that file. It is on CP‑3's read list, so read the banner first.

1. **CP‑3 — the next action.** Chat, **new session**, agenda in `CHAT-CHECKPOINTS.md` §CP‑3.
   Read `liveness-report.md` §4 and §5 before `backlog.md`. Take **D1** first — it reshapes
   A6/A8/A9/A10 and the whole migration-proof exception, and the audit shrank its cost
   (one ~30-line mssql function in `app/`; `db/types.py` already dialect-neutral; the
   Dockerfile's `unixodbc` justification measurably false). Two sequencing constraints the
   audit produced: **decide D3 before fixing A17**, and **decide D6 before deciding the fate
   of `/job_details` and `/bundle`.**

   **Decide before the session whether to spend the hour on the manual `coverage run` pass**
   (`liveness-audit.md` §"Manual step the audit cannot do", report §5 Q8). It has not been
   done. Ten routes have no caller anywhere in the repo and nothing static can settle them.

2. **Slice 3**, once CP‑3 closes. Slice 3 moves `app/stepstone/` into `sources/`. Decide **D3**
   (`stepstone/smoke.py` is a deletion candidate) knowing it is the module
   `/search_stepstone` resolves to — and that `stub_stepstone_adapter` in
   `tests/conftest.py` binds `ss_search` with `raising=True`, so it will fail loudly rather
   than silently when that symbol moves. That is intended.

## Blocked / do not touch

- **A10** — PostgreSQL migration blockers. ⛔ Do not fix. Decide **D1** (drop SQL Server?)
  at CP‑3 first; the right fix depends on the answer. Nothing depends on it.
- **A14, A15, A16** — ⛔ pinned broken on purpose (TEST-STRATEGY §2.4); do not fix as part
  of anything else. Each is a behaviour change wanting its own commit, which rewrites its
  own pin in the same diff. A14's fix is normalisation to UTC in `_parse_iso8601`, **not**
  a `try/except` at the raising line — that would satisfy the pin and leave the second
  call site (S8) untouched.
- **A1** — `_LogSink`, `F821` at `fastapi_run.py:1586`. Not in Slice 2 — live behaviour
  bug, needs its own test-first commit.
- **A7** — `db/` imports `pipeline/`. Closed by Slice 2.9, not standalone.
- **A5** — `make_engine` rejects in-memory SQLite. Open; DB fixtures use file-based.
- **A13** — `check_db` unreachable from fixtures. Environment fix landed and is
  sufficient; the seam defect waits for the Slice 6 service layer.
- **The network guard stops at the process boundary.** Still true, and **Slice 2.5 has now
  crossed it deliberately** (2026-08-07): the spike drove a real batch and made live requests to
  stepstone.de from outside the suite. That was expected and is not a leak — the guard is a
  `tests/` fixture and the spike ran no tests. The item stays open for the reason it was always
  open: `fastapi_run.py:1301` spawns `python -m app.prefect_run crawl|process`, and that child
  gets an unpatched socket module, so a *test* that reaches the subprocess path would egress
  unnoticed. The spike adds one datum — the in-process path spawns a child too (Prefect's
  ephemeral server), so "the subprocess path" is no longer the only boundary crossing. See
  `backlog.md` §"The network guard stops at the process boundary".

## Known gaps in the oracle

[CP-1-REVIEW.md](CP-1-REVIEW.md) is the live list. Summary: **S7** (ADR 0009's ≤3-byte
bound and its "300 random bodies" are untested — widening the overshoot to 64 leaves the
gate green; now cross-referenced from the ADR itself), **S8** (A14 pinned at one of its two
call sites), **S9** (the heuristic language component has no invariant anywhere — deleting
the German penalty outright leaves `tests/unit` + `tests/integration` green).

**Standing recommendation, not yet done:** make mutation verification a committed,
re-runnable harness (`scripts/mutate.py`, mutation set as data, score reported in
`ci/baseline.json` but **never gated** — same status as coverage under R3). Every campaign
so far has worked and every one has left its evidence as prose in a commit message: not
enumerated, not re-runnable, stale the moment Slice 3 or 5 moves the code it points at.
Rationale in [CP-1-REVIEW.md §Second pass](CP-1-REVIEW.md#second-pass--2026-08-02).

**S11 — the one place the audit found coverage that would not catch a refactor.**
`/search_stepstone_list`, `/job_details`, `/bundle`, `/aggregate_report` and
`/api/run_single` have tests asserting schema rejection and nothing else: **0 of 90 handler
statements execute across the five.** `/job_details` (35) and `/api/run_single` (29) are the
heaviest and both run the full fetch → enrich → score path. **Slice 6 extracts services from
exactly those bodies.** Note carefully what `s1-accept-sets` did and did not do here: it made
the assertions precise at the *validation* layer, naming the missing fields — that is real,
and it is not handler coverage. Precision and coverage are easy to conflate and these five are
where the difference bites. Slice 6's acceptance criterion is therefore weaker than it reads
for these routes. **CP‑3 agenda item 4** decides whether behavioural tests land before Slice 6
or after; after means writing them against already-moved code. Report §5 **Q7**.

**S10 — found 2026-08-07 while measuring the accept sets. FIXED in `501b157`.**
`test_start_batch_run_returns_a_run_id_without_running_anything`
(`tests/contracts/test_runs_api.py:160`) asserts nothing it claims to. The `client`
fixture seeds a `User` but no `Profile`, so `get_focus_profile_model_for_user` returns
`None` and the handler raises 404 at `fastapi_run.py:1700`. Measured: as written → 404,
stub calls `[]`; with a `Profile` row → 200, stub calls `["batch"]`, `run_id` minted. So
the `if response.status_code == 200:` branch is dead, no run is minted, and **both
monkeypatched stubs never execute** — the same shape as CP1-8, caught by the accept set
rather than by a live request this time. Two adjacent defects in the same test: lines
170–171 use `raising=False` (an `AGENTS.md` violation, and Slice 8 renames exactly those
two symbols), and line 197's `!= 500` is dead under the accept set above it. All three
are fixed in `tasks/s1-accept-sets.md` commit 2 — narrowing to `== 404` was rejected
because it pins the misnomer.

Also open: `prefect_run.py` at 0% (Slice 8 rewrites it, verified differentially, D2/R7);
**A12**, 18 of 42 routes unauthenticated (pinned, not endorsed — CP‑3 agenda); and
`/api/profile/{key}` GET/POST/DELETE are **not tenant-scoped** (file-backed store, no
`/my/` — CP‑3 agenda beside A12).

## Repo

Working on `main` (deliberate). CP‑1's closure and Slice 2 have both left this machine: `main`
and `origin/main` were level immediately before this update, so the only unpushed work is this
session's documentation commit. `refactor/restructure` is a separate worktree at
`/home/saber/Asus/job-agent-refactor`, parked at `660a6a0`. No `slice/*` or `spike/*` branch
exists — `slice/02` was merged and `spike/inprocess-batch` was deleted per its brief.

> Do not record an absolute commit count here. Writing it creates a commit, which makes
> the number wrong the instant it is written — `9b03785 "correct unpushed count to 22"`
> was itself the 23rd, and it stalled a Codex run on a STATE-versus-disk mismatch.

## Friction worth knowing

- **`tasks/<ID>.report.md` is always committable even when the allowlist omits it.** Now
  in `AGENTS.md` §Conventions, because stating it only in `AGENT-WORKFLOW.md` §2 failed
  twice: Slice 2 stopped at its final step on the contradiction, and `s1-accept-sets`
  finished but left its report uncommitted. Both workers read the brief, which pointed
  elsewhere.
- The `ruff-check` hook lints whole staged **files**, so a commit touching most of `app/`
  is blocked by pre-existing Slice 2 findings. `--no-verify` with a stated reason is the
  expected escape. (CP1-8 needed none — all five commits passed every hook.)
- A stale `.git/index.lock` was found and removed this session: zero bytes, ~5 hours old,
  no `git` process running. Check both before removing one.
- `git add` leaves things staged across commands. Stage per commit; check
  `git diff --cached --stat` first.
- `ruff --exclude` *replaces* pyproject's exclude list — use `--extend-exclude`.
  `ruff format` also reformats Python inside Markdown. Both are in AGENTS.md.
- pytest's assertion rewriting prints every operand — never assert directly on a value
  that may hold a secret. In AGENTS.md §Conventions.
- `ruff` reports **672** auto-fixable where this file recorded 676 since Slice 0. Predates
  CP1-8 (which adds zero findings). `ruff_findings` is the gate key and has not moved;
  Slice 2 should measure rather than trust either number.

---

## Map

| File | What it is |
| --- | --- |
| `AGENTS.md` (= `CLAUDE.md`) | Conventions, layering, do-nots. Auto-loads. |
| `docs/CP-1-REVIEW.md` | Both verdicts. **CP1-1 … CP1-8 all closed**; §Second pass is the live S/L list |
| `docs/refactor-plan.md` | The slices. Files, symbols, shims, verify blocks. |
| `docs/PHASE-2-REVIEW.md` | Amendments R1–R10, decisions D1–D4 |
| `docs/TEST-STRATEGY.md` | Why the old suite rotted; §8 has the `_experience_delta` verdict |
| `docs/CHAT-CHECKPOINTS.md` | Where execution stops for a Chat decision |
| `docs/SESSION-HANDOFF.md` | OPEN/CLOSE prompts for every surface |
| `docs/AGENT-WORKFLOW.md` | Claude-plans / Codex-executes loop; §1 "a review's suggested code is a hypothesis"; §2 is the brief schema |
| `docs/backlog.md` | Parked items, buckets A–D, plus the engineering-practices table |
| `docs/architecture.md` | Module inventory, dependency graph |
| `docs/adr/` | Why each decision was made |
| `docs/adr/0009-…` | The soft-`max_bytes` contract, and **S7**, the missing test for it |
| `docs/liveness-report.md` | **The evidence CP‑3 runs on.** §4 ranked deletion candidates, §5 eight open questions. Read before `backlog.md` §D |
| `docs/liveness-audit.md` | The spec that produced it. §"Manual step" is the `coverage run` pass, **still not done** |
| `tests/net_guard.py` | The egress guard and the reasoning behind it. Read before changing how the suite reaches — or does not reach — the network |
| `tasks/` | Codex briefs. `slice-02.md` and `s1-accept-sets.md` are both done and merged |

## Restart protocol

- **Claude Code** — new session per slice. Prefer `/clear` over `/compact`.
- **Codex** — cold every task. `AGENTS.md` + `tasks/slice-NN.md` is the whole context.
- **Chat** — new session per checkpoint.

Never paste state into a new session. Point at files.

Any decision made in Chat and not written into a file is lost. Three have been caught
this way: A10's deferral, R10's slice, and A11/A12.
