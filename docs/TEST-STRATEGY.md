# Test Strategy — Job Agent Restructure

Plan only. No code changes until this is agreed.

Baseline at time of writing: **18 passed, 6 failed**, commit `660a6a0`,
pytest 9.1.1 + pytest-asyncio 1.4.0.

---

## 1. Diagnosis — why the current suite failed

Six failures, but the interesting output is the *pattern*. Five distinct design
flaws, each evidenced by a specific failure.

### F1 — Tests bind to internals instead of contracts

```
test_run_logs_endpoint
  run_id = run_manager.create_run_dir()
  TypeError: missing 2 required positional arguments: 'user_id', 'profile_key'
```

The test reaches past the HTTP boundary and calls a helper directly. When runs
became user/profile-scoped, the test broke — even though the *endpoint* it claims
to test may still work fine. It was never testing the endpoint.

**Consequence for the restructure:** every test written this way breaks again when
`fastapi_run.py` is split, and each break is a false alarm that costs review time.

### F2 — Tests depend on global mutable defaults

```
test_experience_penalty_triggers
  components = score_job(job)["components"]      # no profile passed -> DEFAULT_FOCUS
  assert components["experience"] <= -15
  E   assert -1 <= -15
```

The expected value is a function of `DEFAULT_FOCUS`, which lives in another module.
Three explanations fit the evidence equally well — a bug in `_experience_delta`,
a deliberate softening of the penalty, or an edit to `DEFAULT_FOCUS` — and the test
cannot discriminate between them. **A test that fails without telling you why is
barely better than no test.**

This is the single most important flaw to fix. It is why the suite rotted silently.

### F3 — Tests were never updated when features landed

`app/auth/` is dated Dec 19–20 2025; `test_api_endpoints.py` is dated Dec 8. Auth
arrived, the test client stopped being authenticated, three tests went red, and
nothing forced the issue. No CI, so red is invisible.

### F4 — Tests that never ran, and looked like they did

`test_fetch_job_details_calls_enrichment_when_enrich_true` is `async def`.
`pytest-asyncio` was not installed, so pytest emitted a warning and **skipped** it.
It appeared in the suite, appeared in the project write-up's test inventory, and
executed zero times. Phantom coverage is worse than absent coverage, because it
gets counted as protection that isn't there.

### F5 — Magic-number assertions with no documented intent

`<= -15` records a number, not a reason. Nobody can now say whether 15 was the
contract or an artifact of whatever the code returned the day it was written. Any
scoring tuning invalidates it, so it will be "fixed" by editing the number, which
teaches you to ignore it.

---

## 2. Principles for the new suite

Derived directly from the five flaws above.

1. **Test contracts, not wiring.** Target the outermost stable surface that proves
   the behavior — HTTP endpoint, public function signature, on-disk artifact.
   Never a private helper, never a module path that the restructure will move.
2. **No implicit global state.** Every scoring test constructs and passes an
   explicit profile. `DEFAULT_FOCUS` is itself a thing under test, not a
   precondition of other tests.
3. **Assert invariants and relationships, not constants** — except where the
   constant genuinely is the contract. `penalty(gap=5) < penalty(gap=0)` survives
   tuning; `== -15` does not.
4. **Characterize first, fix second.** Pin what the code does *today*. If today's
   behavior is wrong, pin it, open an issue, fix in a separate commit. Otherwise
   you cannot distinguish a bug fix from a refactor regression in the diff.
5. **Fail loudly or don't exist.** `--strict-markers` and `--strict-config`;
   unknown marks become errors. Zero skips in a green run.
6. **Offline and deterministic.** No live network, no DB container, no Playwright
   in the default suite. Fixtures only.

---

## 3. Layers

| Layer | What it covers | Speed | Survives restructure? |
|---|---|---|---|
| **L1 Unit / invariant** | Scoring components, date parsing, URL dedup, resume parsing, config validation | <1s total | Yes — pure functions, stable signatures |
| **L2 Contract** | HTTP endpoints via `TestClient`; artifact shape on `tmp_path`; log-streaming offset protocol | ~2s | Yes — that's the point |
| **L3 Pipeline integration** | HTML fixture → normalized record → score → artifacts, fully offline | ~3s | Mostly |

Explicitly out of scope for the default suite: live StepStone calls, real SQL
Server, real Playwright, real LLM. Those belong in a separately-marked suite you
run deliberately (`-m external`), never in the gate.

---

## 4. Proposed layout

Organized to match the target architecture, so the files don't move later.

```
tests/
  conftest.py               # shared factories, no app-global mutation
  fixtures/
    jobs/                   # HTML + JSON job fixtures
    profiles/               # explicit focus profiles
  unit/
    test_scoring_components.py
    test_scoring_invariants.py
    test_dates.py
    test_url_pool.py
    test_resume_parse.py
    test_settings.py
  contracts/
    test_auth_api.py
    test_profiles_api.py
    test_runs_api.py
    test_resume_api.py
    test_run_artifacts.py
    test_log_streaming.py
  integration/
    test_pipeline_offline.py
  legacy/                   # the current suite, quarantined
```

---

## 5. What each area must cover

### 5.1 Scoring invariants — the highest-value file

These hold regardless of tuning, so they never need editing when you adjust
weights. This is the direct answer to F2 and F5.

- **Determinism.** Same job + same profile → identical components, twice.
- **No hidden state.** Scoring the same job before and after scoring a different
  job gives the same result.
- **Blocker dominance.** A hard blocker caps the total below the accept threshold
  regardless of every other component being maximal.
- **Monotonicity — skills.** Adding a matching skill never decreases the skill
  component.
- **Monotonicity — experience.** Increasing the required-years gap never increases
  the experience component. *(This is the property `test_experience_penalty_triggers`
  was reaching for. Stated this way it would have caught a broken
  `_experience_delta` without depending on `DEFAULT_FOCUS` or the number 15.)*
- **Boundedness.** Total stays within the documented range for all fixtures.
- **Profile sensitivity.** The same job scored against two different profiles
  produces different results in the expected direction.
- **LLM-off baseline.** With LLM scoring disabled, output equals the pure
  heuristic. With it enabled and mocked, blockers and caps still dominate.

Absolute-value assertions are permitted only for the accept/reject **threshold**,
because that number is a genuine product contract.

### 5.2 Artifact contract

The project's stated deliverable. Must not break.

- A completed run produces `status.json`, `run.log`, `run_metrics.json`,
  `analysis_summary.json`, `REPORT_SUMMARY.md`.
- Status polling succeeds when optional artifacts are **absent** — the Pydantic
  `None` bug from the write-up, pinned so it cannot recur.
- Run directory path is `output/<user_id>/<profile_key>/<run_id>/`.
- Status transitions are valid and terminal states are terminal.
- Writes are atomic: a partially-written status file is never observable.

### 5.3 Log streaming protocol

Cheap, entirely deterministic, and the GUI depends on it.

- Offset 0 returns from the start and a correct next-offset.
- Mid-file offset returns the right slice.
- Offset at EOF returns empty and an unchanged offset.
- Offset past EOF does not error.
- `max_bytes` is respected and never splits the response contract.
- Appends between two reads are picked up by the second.

### 5.4 HTTP contracts

One authenticated fixture client, dependency-overridden — never a real login
round-trip per test. Covers auth, profiles (including the plural-list vs singular-
upsert distinction and the `x-upsert-action` header), run start/status/logs/summary,
resume upload, and health. For each: unauthenticated returns 401, authenticated
returns the documented status and response shape.

These are written against `fastapi_run.py` today and must pass **unchanged** after
the routers are split. That is the acceptance criterion for the split.

### 5.5 Database tests — free pre-validation of a later slice

Run L2 DB-touching tests against **file-based SQLite on `tmp_path`**, one fresh file
per test.

> **Amended 2026-07-31.** This section originally specified *in-memory* SQLite. That
> is currently impossible, and finding out cost nothing — which was the point.
> `make_engine` ([app/db/engine.py:48](../app/db/engine.py#L48)) passes `pool_size`,
> `max_overflow`, and `pool_timeout` unconditionally. SQLAlchemy gives `sqlite://` a
> `SingletonThreadPool`, which rejects all three, so collection dies with a
> `TypeError` before a single test runs. Logged as **backlog A5**.
>
> File-based SQLite gets a `QueuePool` and works today, so Slice 1 is **not blocked** —
> it just uses a file. No application change required to start.

The double duty still holds: this surfaces every `mssql`-specific assumption in the
models and migrations before the SQLite slice starts. A5 is the first instalment, and
it arrived before a line of test code was written.

**Two follow-ups, neither blocking Slice 1:**

1. **Dialect dispatch in `make_engine`.** Pool arguments are meaningless for SQLite and
   fatal for `:memory:`. This is arguably a bug rather than a migration task — the
   engine factory cannot construct an engine for a dialect the project has chosen as
   its default. Treat it like `_LogSink`: a standalone commit, not folded into a slice.
2. **Then switch the fixture to in-memory.** Once dispatch exists, in-memory needs both
   of these, not just one:

   ```python
   create_engine(
       "sqlite://",
       connect_args={"check_same_thread": False},
       poolclass=StaticPool,      # without this, every connection gets a NEW empty
   )                              # database and your schema vanishes between calls
   ```

   `StaticPool` is the part people miss. `SingletonThreadPool` also breaks under
   `TestClient`, which runs handlers on a different thread than the fixture.

Until then, file-based on `tmp_path` is correct and costs a few milliseconds per test.

---

## 6. Fixture strategy

- `profile_factory(**overrides)` — builds an explicit focus profile. **No test may
  rely on `DEFAULT_FOCUS`.** `DEFAULT_FOCUS` gets its own dedicated test asserting
  its shape, so a change to it fails one obvious test instead of silently shifting
  a dozen unrelated ones.
- `job_factory(**overrides)` — builds a `UnifiedJobPosting` directly, no HTML
  parsing, for scoring tests.
- HTML fixtures only where parsing is what's under test.
- `tmp_path` for every artifact test. No writes outside it.
- SQLite session fixture, fresh **file on `tmp_path`** per test — not `:memory:`,
  until backlog A5 is fixed. See §5.5.
- Authenticated `TestClient` fixture via `dependency_overrides`.

---

## 7. Disposition of the existing 24 tests

- **Do not repair any of them.** Confirmed waste — they bind to internals that are
  about to move.
- Move the whole current suite to `tests/legacy/`, excluded from the gate but
  runnable on demand. It is a free second opinion during Phase 4.
- Delete each legacy file once the new suite covers its ground.
- The 6 failures need no fix, with one exception below.

---

## 8. ~~Open question~~ — RESOLVED 2026-08-01

`_experience_delta` returns **-1** where the old test expected **≤ -15**.

**Verdict: explanation 3 — the test was measuring config, not code. Not a live bug.**
Current behaviour is correct and is pinned as-is.

### The mechanism

`DEFAULT_FOCUS.max_required_experience_years = 3` and
`experience_penalty_strength = 1.0`. The fixture job says *"3+ years"* — exactly **at**
the cap, not beyond it. So `_experience_delta` takes the "within preferred cap" branch:

```
base = -5  ->  int(-5 * strength/3) = int(-1.67) = -1
```

A job asking for exactly the profile's stated maximum is not a blocker, so a light nudge
is the right answer. The penalty still bites past the cap:

| required | delta | |
| ---: | ---: | --- |
| 0–3 years | -1 | within cap |
| 4 years | -8 | beyond cap |
| 5+ years | -10 | beyond cap, saturates |

Verified monotonic non-increasing across 0–10 years, and both knobs work: strength
0→3 scales the 5-year penalty 0→-30; raising the cap 3→5 turns -30 into -5.

### The forensic evidence

`_YEARS_PENALTY = [(5, -25), (4, -20), (3, -15)]` still sits at
[scoring.py:97](../app/pipeline/scoring.py#L97) and is **never read** — pyright reports it
unaccessed. Its `(3, -15)` entry is exactly the `≤ -15` the old test asserted.

That is the whole story: the implementation was restructured from a flat years→penalty
lookup into a cap-aware, strength-scaled formula, the lookup table was orphaned, and the
test was never updated. The test encoded a constant derived from `DEFAULT_FOCUS` plus a
since-replaced table — flaws **F2** and **F5** in one line.

### Consequences

- Scoring behaviour is **pinned as-is**. No application change.
- Assertions are relational, never absolute — `penalty(beyond cap) < penalty(within cap)`
  survives tuning; `== -15` did not.
- The dead `_YEARS_PENALTY` is a cleanup for Slice 2, not for this slice (backlog **B8**).

### Noted, not fixed

The regex is `(\d+)\+?\s+(years|jahr|jahre)` — singular English *"year"* is missing, so
*"1 year of experience"* scores 0 rather than a small penalty. Real but tiny, and fixing
it is a behaviour change: it belongs in bucket C, not here. Characterised in
`tests/unit/test_scoring_invariants.py` so the gap is visible rather than latent.

## 9. Gate definition

A slice is done when:

```bash
pytest -q                          # 0 failed, 0 skipped
pyright --outputjson | jq .summary # error count not above baseline
ruff check .                       # finding count not above baseline
```

Coverage floors on the four modules that matter — `fastapi_run.py`,
`pipeline/scoring.py`, `pipeline/pipeline.py`, `gui_runs/run_manager.py` — set
after the first measurement, ratcheted upward, never downward.

Add CI (GitHub Actions) running the gate on every push, with the DB matrix as
SQLite + Postgres. This is what prevents a repeat of F3, where auth landed and the
suite stayed red for seven weeks unnoticed.

---

## 10. Sequencing — a deviation from the playbook

The playbook puts tests in Phase 0, before the architecture map. **Recommend
running Phase 1 first**, for a specific reason: contract tests target the public
surface, and Phase 1 is what tells you what the public surface actually is. Writing
them blind means guessing at the boundary and rewriting when the map contradicts
you.

Phase 1 is read-only, so it needs no test oracle to be safe.

```
Phase 0   finish baseline (pyright, coverage)          <- you are here
Phase 1   map the architecture (read-only)
Phase 1.5 write the new test suite, per this document  <- oracle established
Phase 2   refactor plan, reviewed in Chat
Phase 3   project skills
Phase 4   execute slices, gated by the suite from 1.5
Phase 5   UI track
```

No slice in Phase 4 begins before 1.5 is green. That is the whole point.
