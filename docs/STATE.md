# State

**Single entry point for any new session.** Read this plus `AGENTS.md` and you have the
project. Everything else is reference, read on demand.

**Update this at the end of every session.** A stale STATE.md is worse than none — it
gets trusted.

_Last updated: 2026-08-02 (CP1-8 closed — the suite is now offline by enforcement)_

---

## Where we are

**CP‑1 passed on the second pass, and its one blocking exception — CP1-8 — is now
closed.** The oracle is trustworthy; **Slice 3 is unblocked** (CP‑3 and the liveness audit
still come first), and **Slice 2 resumes**. Verdict and evidence:
[CP-1-REVIEW.md §Second pass](CP-1-REVIEW.md#second-pass--2026-08-02).

The verdict was reached by **re-measuring rather than reading** — the environment was
rebuilt from `requirements.lock.txt` on a clean Python 3.12 and the suite run from
scratch: 291 passed, 0 failed, 1 deselected, matching `ci/baseline.json` exactly. Read
that reproduction with the caveat recorded under §Gate below; it proves less than it was
written up as proving.

**CP1-8 was found after the verdict was first given, by asking which slice moves the code
each weak test covers.** `test_search_stepstone_returns_the_adapter_result` accepted `500`,
and Slice 3 is the slice that moves `app/stepstone/` — so the one test covering that route
accepted the runtime failure state of the relocation it was supposed to grade. Following
that up showed the accept set was the smaller half of the problem: **neither monkeypatch in
that test bound to anything the handler calls**, so it exercised the real adapter and
issued a live request to `https://www.stepstone.de/en/` inside the "offline" gate.
`tests/test_suite_hermeticity.py` covered env vars, the DB and the settings flags, and had
**no network assertion**. Same shape as CP1-7, one layer out.

**How it was closed, in the order that mattered: the guard went in first, before any fix.**
`tests/net_guard.py` — a session-scoped autouse fixture refusing every outbound connection
and DNS lookup — was installed against the *unmodified* suite, to find the offenders rather
than to confirm the two already known. It found **exactly two**, both known
(`test_search_stepstone_returns_the_adapter_result`, `test_public_routes_do_not_401`), so
the leak was as narrow as CP‑1 described and nothing else was hiding behind it. Fixing
first would have left that unmeasured.

> **Carry this into CP‑3 and CP‑4:** grade a weak test by *what is about to move
> underneath it*, not only by how weak it is. This review's S/L list is ordered by
> severity; it should also have been ordered by proximity to the next slice. That single
> question turned a deferred style item into a blocker.

| | Status |
| --- | --- |
| Phases 0–2 · Slice 0 · bugfixes A6, A8, A9 | ✅ |
| Slice 1 — contract test suite | ✅ `8b0116e` — but see CP‑1: green ≠ trustworthy |
| Slice 2 brief for Codex | ✅ `99218ce` → `tasks/slice-02.md` |
| CP1-4 — log-chunk UTF‑8 corruption | ✅ `ea93e34` red → `a539f56` green · [ADR 0009](adr/0009-log-chunks-are-codepoint-aligned.md) confirmed correct — but **only the ADR's reasoning**, not its guarantee: its ≤3-byte bound and its "300 random bodies" verification are still untested, which is **[S7](CP-1-REVIEW.md#s7--adr-0009s-soft-limit-is-a-decision-no-test-can-defend)**. Read that before Slice 5 moves `run_manager` |
| Suite hermeticity — `.env.dev` leak | ✅ `bf88f64` |
| **CP1-5 — cross-tenant isolation** | ✅ **probe clean, no auth bug.** 15 probes + 5 controls |
| CP1-1, CP1-2, CP1-3, CP1-6, CP1-7 | ✅ one commit, `tests/` + constant promotions |
| **L3 `test_pipeline_offline.py`** | ✅ 43 tests · closes CP‑1 S6 · found A14, A15, A16 |
| **CP‑1 re-run** | ✅ **TRUSTWORTHY** — 2026-08-02 against `db3b908` — except **CP1-8** |
| **CP1-8 — StepStone test stubs nothing, hits the live network** | ✅ **closed.** Guard first, then the fix · 291 → 298 · **no longer blocks Slice 3** |
| S1's remaining seven wide accept sets | ⬜ before Slice 3 |
| S7, S8, S9 — new from the second pass | ⬜ before Slice 5 |
| **Slice 2 — lint + packaging** | ▶️ **resumed** — `tasks/slice-02.md` is executable again |
| Slice 3 | ⬜ unblocked; **CP‑3 + the liveness audit still come first** |
| Slice 2.5 spike · Slice 2.9 `app/domain/` · Slices 4–10 · Phase 5 | ⬜ |

**CP1-8 was test-side only, as predicted** — no `app/` change, so it did not collide with
Slice 2's ruff pass on `slice/02`. What landed:

- `tests/net_guard.py` — the guard. Refuses outbound `connect`, `connect_ex` and
  `getaddrinfo`; **loopback is deliberately allowed** (a local DB is already pinned by the
  engine assertion, and `-m external` needs a loopback port for Playwright).
- **A refusal is recorded, not only raised, and the record is what fails the test.** The
  leaking route is `except Exception: raise HTTPException(500)` and 78 such handlers exist
  (A3), so a guard that only raises is absorbed by the code it polices. `conftest` checks
  the record at the end of each phase, where no `except` in `app/` can reach it. Measured
  while building it: asyncio resolves DNS on an executor thread, so the CP1-8 leak is
  caught by the *record*, not by the raise — the raise happened where nobody was looking.
- The check runs in the **call** phase, not teardown. A teardown failure leaves the test
  counted as passed, and `pytest_passed` is the one ratchet key that would then not move.
- `tests/test_suite_hermeticity.py` §3 — seven tests closing the *category*, not the
  instance. Including one that asserts the `conftest` hooks are still wired: without it,
  deleting them leaves every other network test green while leaks go back to invisible.
- The fix itself — `stub_stepstone_adapter` in `conftest`, bound to **`ss_search`** with
  `raising=True`, asserting `== 200`, the exact body, **and the recorded call args**, so
  the stub's *binding* is observable rather than inferred from a status code.

**Rejected, recorded so it is not retried:** making `NetworkAccessAttempted` a
`BaseException` to defeat `except Exception:`. It works, and it also escapes anyio's
blocking portal, killing the event-loop thread `TestClient` runs on — every leak became one
red test plus a teardown `RuntimeError: This portal is not running`, naming anyio instead
of the network. The record makes the stronger base class unnecessary.

**S1's remaining seven are scheduled ahead of Slice 3 but are not a correctness gate on
it.** They moved because `AGENTS.md` §Conventions now forbids the exact assertion shape
the gated suite still contains ("no assertion may accept both the success and the failure
state… never write an accept set containing 500") — the CP1-7 remediation wrote the rule
and left the instances. A conventions file that a cold Codex run reads before every task,
carrying a live counterexample in the same repo, teaches the counterexample.

CP‑1's **first**-pass verdict: **seven tests whose name and docstring claimed a property
the test could not fail to detect the absence of.** An incomplete oracle is safe — low
coverage is visible. One that asserts what it does not check is not, because every later
gate is graded against it. Two of the seven were live bugs, each found by following the
test that claimed to pin it. Both are now closed:

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
pytest    298 passed, 0 failed, 0 skipped, 1 deselected (external)
pyright   32 errors    (basic; off=0, standard=32, strict=1036)
ruff      747 findings (672 auto-fixable — that is Slice 2; expect 31 after)
imports   2 broken     (both from A7, one edge)
```

Banked four times. 208 → 226 in `d9f4ce7` (8 hermeticity tests, 10 from CP1-4); 226 →
248 with the CP‑1 batch (20 cross-tenant, 1 rescoring, 1 log-cap); 248 → 291 with the
L3 integration suite, all 43 in `tests/integration/test_pipeline_offline.py`; **291 → 298**
with CP1-8's network guard, all 7 in `tests/test_suite_hermeticity.py` §3.
`pytest_passed` is the one `HIGHER_IS_BETTER` key, so this is the ratchet moving in the
permitted direction.

**The other four have not moved across any of the four**, and that is the result to check
rather than skim. The CP‑1 batch only promoted constants; the L3 suite and CP1-8 add no
`app/` code at all. Any of them would have shown up in `ruff` or `imports` had it done more
than it claimed.

> `ruff` reads **672** auto-fixable, not the 676 recorded here since Slice 0 — a
> discrepancy that predates CP1-8 (this batch adds zero findings; the total is 747 before
> and after). Not chased, because `ruff_findings` is the gate key and it has not moved.
> Slice 2 should measure rather than trust either number.

**The old 208 was not a comparable number** — measured with an ambient environment
leaking in, it meant something different on every machine. **248 is verified identical
with and without `.env.dev` sourced**, re-checked on 2026-08-02. Re-run that both ways
whenever this number moves; it is the only thing that makes it mean anything.

**291 was independently reproduced at the CP‑1 second pass** — environment rebuilt from
`requirements.lock.txt` on a clean Python 3.12, suite run from scratch, 291 passed / 0
failed / 1 deselected. Coverage was not re-measured; the numbers below stand from
`db3b908`.

> **Amended 2026-08-02 when CP1-8 was closed: that reproduction is weaker evidence than
> it was recorded as.** It was written up as the first confirmation of this number from
> something other than the machine that produced it — what "no longer single-machine" was
> supposed to buy. It is not, quite. Two of the 291 issued live HTTP requests to
> `https://www.stepstone.de/en/`, and **their assertions accept both the egress and the
> no-egress outcome** — `in (200, 422, 500)` and `!= 401`. A matching count therefore
> could not distinguish "the suite is hermetic" from "the suite reaches the network and
> says so either way", which is the one thing the reproduction was meant to establish.
> The machine that produced the confirming run did have egress, so that run made the two
> requests.
>
> Worse, the invariance is not total: it holds where the network *fails fast*. A machine
> that black-holes rather than refuses would have hung on the fetch timeout instead of
> reproducing anything — the same two presentations, machine up or machine down, that
> CP1-7 produced against SQL Server. Same defect class, one layer out, and the count was
> just as blind to it.
>
> **298 is the first count here that no network condition can change**, because
> `tests/net_guard.py` now refuses egress outright. The lesson for CP‑3 and CP‑4 is not
> "re-measure on a second machine" — that was done and it passed. It is that a *matching
> number* is only evidence of hermeticity once something in the suite fails when the
> environment differs.

Coverage is a **reported metric, never a gate** (R3). Re-measured 2026-08-02:
`pipeline/pipeline.py` **22% → 92%**, `pipeline/parsers.py` **0% → 97%**,
`pipeline/output.py` 87%, `potential_bucket.py` 91%, `scoring.py` 79%,
`run_manager.py` 82%, `fastapi_run.py` 50%, `prefect_run.py` 0%, total **46% → 52%**.

The two that moved are the L3 suite's doing, and the number is the *least* interesting
part of it — 43 tests bought three filed bugs (**A14**, **A15**, **A16**) that 22%
coverage had given no hint of.

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

**Added by the second pass** (all **S**, none blocking — an incomplete oracle is the safe
kind):

- **S7** — ADR 0009's soft limit has no test. Widening the overshoot from ≤3 bytes to 64
  leaves all 291 green. The ADR's "do not tighten this to `assert len(chunk) <= max_bytes`"
  is prose where the repo's own escalation rule asks for an executable check, and its
  claimed "300 random bodies" verification is not in the suite. (Reproduced and it holds —
  400 bodies × sizes 1–20, max overshoot exactly 3 — but Slice 5 moves the module and
  inherits none of it.)
- **S8** — A14 is pinned at `pipeline.py:158` and not at `:69`. The second site swallows
  the `TypeError` into `"cache_get failed"` — a false statement, `cache_get` succeeded —
  and refetches. Measured: 1 fetch to warm, **2** after a cached call with an aware cutoff.
  A `try/except` at `:158` would satisfy the pin and leave this.
- **S9** — **the heuristic language component has no invariant anywhere.** Deleting the
  German penalty outright (`if penalty_key in _LANG_PENALTY:` → `if False:`) leaves
  `tests/unit` + `tests/integration` fully green, 107 passed; so do `english_bonus = 0`
  and dropping the customer-facing `-5`. A15's "all six levels are equal" would therefore
  keep passing after the behaviour it characterises had been removed. `TEST-STRATEGY` §5.1
  lists eight invariant families and language is not one of them.

**Standing recommendation from the second pass:** make mutation verification a committed,
re-runnable harness (`scripts/mutate.py`, mutation set as data, score reported in
`ci/baseline.json` but **never gated** — same status as coverage under R3). The `db3b908`
campaign worked and caught two can't-fail assertions in the file written to answer CP‑1,
but its evidence is prose in a commit message: not enumerated, not re-runnable, and stale
the moment Slice 3 or Slice 5 moves the code it points at. Rationale and the four-point
shape are in [CP-1-REVIEW.md §Second pass](CP-1-REVIEW.md#second-pass--2026-08-02). This
is AGENT-WORKFLOW/TEST-STRATEGY scope, not a CP‑1 blocker.

- ~~`tests/integration/test_pipeline_offline.py` does not exist~~ · ~~`parsers.py` at zero
  coverage (CP‑1 S6)~~ — ✅ **both closed 2026-08-02.** 42 tests; `job_html` and
  `job_stepstone_1.html` are wired at last. parse → score → artifact is now covered at all
  three stages instead of one. It surfaced three defects — backlog **A14**, **A15**,
  **A16** — all pinned as-is and none fixed.
- `prefect_run.py` at 0% — Slice 8 rewrites it, verified differentially (D2/R7).
- 18 of 42 routes unauthenticated (**A12**) — pinned, not endorsed; CP‑3 agenda. **CP1-5
  closed the other half**: the 24 protected routes are now tested for "rejects a
  *logged-in* stranger", not only "rejects a stranger", and all of them do.
- **`/api/profile/{key}` GET/POST/DELETE are not tenant-scoped** — the file-backed store,
  no `/my/`. Any authenticated user reads and writes the same global
  `config/focus_profiles.json`. Found while running the CP1-5 probe and deliberately left
  alone: it is not DB-backed, so it was outside the probe's scope, and D1 has already
  decided the DB surface supersedes it. **CP‑3 agenda, beside A12.**

## Next action

**CP1-8 is closed** (2026-08-02, test-side only, 291 → 298). The three prescribed steps all
landed: the stub binds to **`ss_search`** with `raising=True`, the assertion is one status
code plus the body plus the recorded call args, and the network guard is in — installed
*first*, against the unmodified suite, which found exactly the two known offenders and
nothing else. Detail under §Where we are; the rule it produced is in `AGENTS.md`
§Conventions (`raising=False` is forbidden in a stub).

Then, in order:

1. **Slice 2** — resume from `tasks/slice-02.md`. It was halted only pending the CP‑1
   verdict, and CP1-8 touched no `app/` code, so nothing collides on `slice/02`.
2. **S1's remaining seven** before Slice 3. One correction to carry into that fix so it
   does not over-claim: `/health/config` at `test_health_and_pages_api.py:28` is the
   mildest of the set and is **not** vacuous — `health_config()` can return 200, 500 **or**
   503 (`fastapi_run.py:209-213`), so `in (200, 503)` does still exclude one outcome.
   Unlike CP1-7's `/health/db`, which could return only the two it accepted.
3. **Slice 3** — after CP1-8. Run **CP‑3** first, as `CHAT-CHECKPOINTS.md` requires, and
   the liveness audit before that. Note **D3** (`stepstone/smoke.py`, a deletion candidate)
   is the module CP1-8's route resolves to — decide D3 knowing that.
4. **S2–S5, S7–S9** before Slice 5. **S5 is still the cheapest leftover** —
   `test_blockers_still_dominate_when_the_llm_is_mocked` asserts a magic `<= 35` two
   functions after the identical property is asserted relationally as
   `<= blocking.blocker_cap_hard`. Left alone deliberately, three times now: it is an S
   item and no batch has been scoped to it.
4. **L1–L4** before Slice 7.

**Do not fix A14, A15 or A16 as part of any of the above.** All three are pinned in their
broken state on purpose (TEST-STRATEGY §2.4) and the second pass confirmed the pins are
the right call. Each fix is a behaviour change wanting its own commit, which rewrites its
pin in the same diff. For A14 specifically, the fix is **normalisation to UTC in
`_parse_iso8601`**, not a `try/except` at the raising line — that would satisfy the pin,
go green, and leave the cache-path presentation at `pipeline.py:69` untouched (S8).

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
| `docs/CP-1-REVIEW.md` | Both verdicts. CP1-1 … **CP1-8 all closed**; §Second pass is the live list (S1's remaining seven → before Slice 3; S7–S9 before Slice 5) |
| `tests/net_guard.py` | The egress guard and the reasoning behind it: why the record and not just the raise, why `Exception` and not `BaseException`, why loopback is allowed. Read before changing anything about how the suite reaches — or does not reach — the network |
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
| `docs/adr/0009-log-chunks-are-codepoint-aligned.md` | The soft-`max_bytes` contract. Confirmed correct at the CP‑1 second pass; **S7** is the missing test for it |
| `tasks/slice-02.md` | The Codex brief — **executable again** (the halt lifted with CP‑1's clean verdict) |

## Restart protocol

- **Claude Code** — new session per slice. Prefer `/clear` over `/compact`.
- **Codex** — cold every task. `AGENTS.md` + `tasks/slice-NN.md` is the whole context.
- **Chat** — new session per checkpoint.

`.claude/commands/` does not exist yet (Phase 3, AGENT-WORKFLOW §8 step 7), so briefs are
written from an explicit prompt against §2's schema. `make report SLICE=… BASE=…` works.

Never paste state into a new session. Point at files.

Any decision made in Chat and not written into a file is lost. Three have been caught
this way: A10's deferral, R10's slice, and A11/A12.
