# State

**Single entry point for any new session.** Read this plus `AGENTS.md` and you have the
project. Everything else is reference, read on demand.

**Update this at the end of every session.** A stale STATE.md is worse than none — it
gets trusted.

_Last updated: 2026-08-02 (CP‑1 closed)_

---

## Where we are

**CP‑1 is CLOSED.** The oracle is trustworthy. CP1-1 … CP1-8 are all fixed; the review's
remaining items are **S** (should-fix) and **L** (latent), neither of which blocks.

**Slice 3 is unblocked. Slice 2 is the next action.**

| | Status |
| --- | --- |
| Phases 0–2 · Slice 0 · bugfixes A6, A8, A9 | ✅ |
| Slice 1 — contract test suite | ✅ `8b0116e` — but see CP‑1: green ≠ trustworthy |
| **CP‑1 — oracle review, both passes** | ✅ **CLOSED.** CP1-1 … CP1-8 all fixed |
| **Slice 2 — lint + packaging** | ▶️ **the next action** — `tasks/slice-02.md` |
| S1's remaining seven wide accept sets | ⬜ before Slice 3 (scheduling, not a gate) |
| Slice 3 | ⬜ unblocked — but **CP‑3 + the liveness audit run first** |
| S2–S5, S7, S8, S9 | ⬜ before Slice 5 |
| L1–L4 | ⬜ before Slice 7 |
| Slice 2.5 spike · Slice 2.9 `app/domain/` · Slices 4–10 · Phase 5 | ⬜ |

### What landed this session (`db3b908..HEAD`)

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

1. **Slice 2 — lint + packaging.** Resume from `tasks/slice-02.md`, on `slice/02`. It was
   halted pending the CP‑1 verdict and that verdict is now in. Nothing from CP1-8 collides
   with it: all five commits are `tests/` and `docs/`.
2. **S1's remaining seven wide accept sets**, before Slice 3. Scheduling, not a
   correctness gate: `AGENTS.md` §Conventions forbids the shape the gated suite still
   contains, and a conventions file carrying a live counterexample teaches the
   counterexample. One correction so the fix does not over-claim — `/health/config`
   (`test_health_and_pages_api.py:28`) is **not** vacuous; that route can return 200, 500
   **or** 503, so `in (200, 503)` does exclude an outcome.
3. **CP‑3**, then the liveness audit, then Slice 3. Slice 3 moves `app/stepstone/` into
   `sources/`. Decide **D3** (`stepstone/smoke.py` is a deletion candidate) knowing it is
   the module `/search_stepstone` resolves to — and that `stub_stepstone_adapter` in
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
- **The network guard stops at the process boundary.** Latent today, and it stops being
  latent at **Slice 2.5**, whose spike drives batch execution directly —
  `fastapi_run.py:1297` spawns `python -m app.prefect_run crawl|process`, and a child gets
  an unpatched socket module. See `backlog.md` §"The network guard stops at the process
  boundary"; put it in the Slice 2.5 brief's *Stop and ask*.

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

Also open: `prefect_run.py` at 0% (Slice 8 rewrites it, verified differentially, D2/R7);
**A12**, 18 of 42 routes unauthenticated (pinned, not endorsed — CP‑3 agenda); and
`/api/profile/{key}` GET/POST/DELETE are **not tenant-scoped** (file-backed store, no
`/my/` — CP‑3 agenda beside A12).

## Repo

Working on `main` (deliberate). **`origin/main` is at `f562202`; local `main` is ahead of
it and unpushed** — CP‑1's closure has not left this machine. `refactor/restructure` is a
separate worktree at `/home/saber/Asus/job-agent-refactor`, parked at `660a6a0`.
Branch `slice/02` does not exist yet.

> Do not record an absolute commit count here. Writing it creates a commit, which makes
> the number wrong the instant it is written — `9b03785 "correct unpushed count to 22"`
> was itself the 23rd, and it stalled a Codex run on a STATE-versus-disk mismatch.

## Friction worth knowing

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
| `docs/liveness-audit.md` | Prompt to run before CP‑3 |
| `tests/net_guard.py` | The egress guard and the reasoning behind it. Read before changing how the suite reaches — or does not reach — the network |
| `tasks/slice-02.md` | The Codex brief — the next action |

## Restart protocol

- **Claude Code** — new session per slice. Prefer `/clear` over `/compact`.
- **Codex** — cold every task. `AGENTS.md` + `tasks/slice-NN.md` is the whole context.
- **Chat** — new session per checkpoint.

Never paste state into a new session. Point at files.

Any decision made in Chat and not written into a file is lost. Three have been caught
this way: A10's deferral, R10's slice, and A11/A12.
