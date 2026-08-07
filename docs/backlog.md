# Backlog

Parking lot for everything that is **not** an architectural slice. Its job is to stop detail-level
changes leaking opportunistically into structural slices — the top failure mode in a large
refactor is the orthogonal change buried in a 40-file diff that nobody reviews carefully.

Nothing here is scheduled. Items move out when a phase claims them.

## Buckets

| Bucket | When it ships | Why |
| --- | --- | --- |
| **A — Bugs** | Standalone commits, any time — **unless the row is marked BLOCKED** | A behavior change inside a structural diff makes failures unattributable |
| **B — Touched-anyway** | Inside the slice that moves the file, only if genuinely trivial | Otherwise: move first, verify, change second |
| **C — Behavior & features** | After Phase 4 | Same attributability argument, at feature scale |
| **D — Deletions** | **Before** the extraction slices | Refactoring code you are about to delete is pure waste — see [ADR 0008](adr/0008-deletions-precede-extraction.md) |

Bucket D is the exception that matters. The general "behavior changes wait" rule gets deletions
backwards: under it, a dead feature gets a service extracted in Slice 6 and a router in Slice 7,
and *then* gets deleted.

---

## A — Bugs

| # | Item | Evidence | Notes |
| --- | --- | --- | --- |
| A1 | `_LogSink` — URL-pool prune endpoint always fails silently | Reproduced: `"Prune failed: cannot access free variable '_LogSink'"`. `try` block indented into the class body at [app/fastapi_run.py:1567-1617](../app/fastapi_run.py#L1567) | **Already scheduled** as a standalone commit in [refactor-plan.md](refactor-plan.md). Write the failing contract test first. |
| A2 | `jwt_secret` defaults to `None` | `_env('JOBAGENT_JWT_SECRET', default=None)` at [app/config/settings.py:177](../app/config/settings.py#L177) | Config layer is specified as fail-fast validation. A missing signing secret should refuse to boot, not fail somewhere downstream. |
| A3 | **77** broad `except` handlers with no re-raise | AST scan across `app/`; 17 in `fastapi_run.py`, 8 in `stepstone/search_playwright.py`, 7 in `prefect_run.py` | A1 is one of these. Not a claim that 77 bugs exist — a claim that nobody knows which are load-bearing. **Count corrected 2026-08-07: 78 was measured at `660a6a0`, where `db/session.py` had 4; on `main` it has 3.** The number drifts with any commit and should be re-measured, never quoted — same disease as the self-invalidating counts STATE.md warns about. **Triaged in part by the liveness audit**, which promoted three to their own rows: A1, **A17** and **A18**. The rest are still unexamined; finish at CP‑3 per `CHAT-CHECKPOINTS.md` §CP‑3 agenda item 2. |
| A17 | `/search_stepstone` returns **500** where it means 400 | Measured, not inferred: `?backend=nonsense` raises the handler's own `HTTPException(400)`, which its enclosing `except Exception` catches and re-raises as 500. `/bundle` has the identical shape and is **correct**, because it carries `except HTTPException: raise` — those are the only two sites, so the fix is a one-line copy of the working one. [liveness-report.md](liveness-report.md) §1.2 | Textbook A3 shape: a broad handler swallowing a deliberate signal. **Do not fix before D3 is decided** — report §5 **Q4**: `smoke.py` is the module behind this route and is a live deletion candidate, so fixing first may be fixing code about to be removed. The route is also public and unauthenticated (A12). Whichever way D3 goes, this needs a test written first — an accept set containing 500 is exactly what `AGENTS.md` forbids, and no test covers it today. |
| A18 | Résumé upload cannot report a parse failure | `parse_resume_file` is wrapped in `except Exception: pass` at its single call site, and `ResumeUploadResponse` has no field that could carry a parse status. A failed parse is therefore indistinguishable from a success, to the user and to us. [liveness-report.md](liveness-report.md) §1.3 | Product-visible silent failure, not just an internal one. **Blocks D5:** the recorded question "which résumé formats are really used" cannot be answered from this code, because the code cannot tell you when a format failed. Fix this first and the telemetry it unblocks *is* D5's evidence. Needs a response-schema change, so it is a behaviour change — bucket **C** work, decided at CP‑3, not a drive-by fix. |
| A4 | `FetchMeta \| None` passed where `FetchMeta` required | pyright, [app/fastapi_run.py:495](../app/fastapi_run.py#L495) and [:1676](../app/fastapi_run.py#L1676) | Two of the 32 baseline pyright errors. Same shape as the Pydantic `None` bug already hit once. |
| A14 | Staleness comparison raises `TypeError` whenever exactly one side carries a timezone | `TypeError: can't compare offset-naive and offset-aware datetimes`, uncaught, at [app/pipeline/pipeline.py:158](../app/pipeline/pipeline.py#L158). `_parse_iso8601` preserves whatever offset it is handed, so a date-only `datePosted` is naive and a `Z`-suffixed `cutoff_iso` is aware. Full matrix measured: both-naive ✅, both-aware ✅, **mixed raises**. | Found writing `tests/integration/test_pipeline_offline.py`. **The broken pair is the normal one, not an exotic one:** date-only `datePosted` is what StepStone emits (our own fixture has `"2024-10-01"`), and [prefect_run.py:680](../app/prefect_run.py#L680) builds its cutoff as `.isoformat().replace("+00:00", "Z")` — aware. So the batch path supplies exactly the combination that raises, and it escapes `fetch_job_details` to the caller. The same comparison at [line 69](../app/pipeline/pipeline.py#L69) sits inside the cache short-circuit's broad `except Exception`, so on a cache hit it is swallowed into "cache_get failed" and then hit again uncaught below — one bug with two presentations. Fix is normalisation to UTC in `_parse_iso8601`, not a `try/except`. **Characterised as `pytest.raises`** by `test_staleness_raises_when_only_one_side_carries_a_timezone`; that test fails when this is fixed, which is intended — the fix rewrites it in the same commit. |
| A15 | `candidate_german_level` does not affect the heuristic score at all | Measured across `A1/B1/B2/C2/Native/Unknown` against the fixture posting (which requires German B2): identical score, identical `german_requirement` component, every time. `_penalize_language` keys the penalty on the *posting's* detected level scaled by confidence and never reads the profile field. The field is consulted in exactly one place, [scoring.py:146](../app/pipeline/scoring.py#L146) in `classify_blockers`, and only against `llm_part`. | With LLM scoring off — the default, and the gate's configuration — the field is **inert**: a native speaker is penalised −72 for a B2 requirement exactly as an A1 beginner is. Not filed as a straightforward bug because making it live is a scoring **behaviour change** that moves every German posting's score at once, which is bucket **C** and wants a deliberate decision about the intended semantics. Characterised by `test_the_candidate_german_level_does_not_move_the_heuristic_score`, which asserts the scores are all equal and therefore fails loudly the day someone wires it up. |
| A16 | `write_job_bundle` reads `os.environ` directly, and it outranks settings | `base_root = os.getenv("JOBAGENT_OUTPUT_ROOT")` at [app/pipeline/pipeline.py:185](../app/pipeline/pipeline.py#L185), taking precedence over `settings.output_dir`. | Straight violation of AGENTS.md §Conventions ("Configuration is read from settings in `app/config/`, never `os.environ` directly in feature code"). Consequence is that a stray environment variable silently relocates every artifact a run produces, and no settings-based test would observe it. Trivial to fix — but it is a live precedence change, so it gets its own commit rather than riding Slice 6. Pinned meanwhile by `test_the_output_root_env_var_overrides_settings`, because Slice 6 moves this code and an undocumented env override is exactly what a move drops silently. |
| A12 | 18 of 42 routes are unauthenticated, including pipeline and network endpoints | `/bundle`, `/job_details`, `/search_stepstone`, `/search_stepstone_list`, `/aggregate_report`, `/run_state` (GET+POST), `/playwright_check` have no `Depends(get_current_user)`. Enumerated from the live dependency graph in Slice 1. | Pinned as the *current* contract by `test_public_routes_do_not_401`, so a change is visible. Whether it *should* be the contract is a bucket-C question: `/playwright_check` launches a browser and `/search_stepstone` hits the network, both unauthenticated. Not a bug report — nobody has decided the intended surface. Take it at CP-3 alongside bucket C. |
| A13 | `check_db()` is unreachable from the test fixtures — `SessionLocal` is bound at import | [app/db/health.py:2](../app/db/health.py#L2) does `from app.db.session import SessionLocal`, which copies the binding at import time. `db_engine` monkeypatches `session_module.SessionLocal`, so it redirects `db_session()`, `run_db_with_retries()` and `get_db()` — all of which resolve the module global at *call* time — but **cannot** reach `check_db`. `check_db` is what `TestClient`'s lifespan calls, via `_startup_checks` at [fastapi_run.py:132](../app/fastapi_run.py#L132). | **Recorded so nobody retries the fixture fix.** This was the mechanism behind the `.env.dev` leak: `conftest.py` used `os.environ.setdefault`, so a sourced `.env.dev` kept its real `mssql+pyodbc` URL and every client fixture opened a live connection to the developer's SQL Server. Measured: `check_db()` returned `ok=True` in 0.31s against a running container, and all 208 tests passed — **against mssql, not SQLite**. With the container down it blocks on the ODBC login timeout instead, which is why 208/0 reproduced on one machine and not another. **Closed at the environment layer** (`bf88f64`: unconditional assignment + `tests/test_suite_hermeticity.py`), which is the correct fix and is sufficient. The seam defect itself is still open and no fixture can paper over it — a future `import app.db.health` inside a test, or any new import-time binding of `SessionLocal`, reintroduces the same unreachability. Real fix is the Slice 6 service layer / DI seam; see the `db_engine` docstring, which already argues the missing seam is an argument for it. |
| A11 | `score_job` mutates the caller's job dict | Adds `language_requirements: []` in place when the key is absent. Found by an invariant test in Slice 1. | Benign today, but it couples a caller's dict to scoring internals. Characterised, not fixed, by `test_scoring_mutates_the_job_dict_in_exactly_one_known_way`, which pins the exact surface so any *additional* mutation fails loudly. The profile is asserted un-mutated as an absolute — a shared `FocusConfig` is reused across every job in a run. |
| A10 ⛔ **BLOCKED — do not fix. Decide D1 first (checkpoint CP‑3).** | Two mssql-only constructs in the **first two** migrations block PostgreSQL entirely | `upgrade head` on real PostgreSQL 16 dies at `a932afee4b12` with `function sysdatetimeoffset() does not exist`; with that patched it dies at `3b4d3b5b3c1a` with `column "is_active" is of type boolean but default expression is of type integer` (`server_default=sa.text("0")`, [line 32](../alembic/versions/3b4d3b5b3c1a_add_resumes_table.py#L32)). | Found while verifying A9 on all three dialects. **Distinct from A8:** A8 dropped the `sysdatetimeoffset` defaults in a *later* revision, but PostgreSQL validates a default function at `CREATE TABLE` (SQLite defers it to first INSERT), so the chain never reaches A8's revision. **A fix is licensed by the proof-based exception** — a probe making the default dialect-conditional (`sysdatetimeoffset()` on mssql, `now()` elsewhere) kept the full-chain mssql SQL byte-identical at `16879270846c62d9…`, and cleared both `init_schema` and `df04761bd175` on Postgres. Probe reverted; not applied. `is_active` wants `sa.text("false")` or `server_default=sa.false()`. **Blocked deliberately:** the licensed fix means maintaining dialect-conditional defaults indefinitely, and that cost exists *only* because mssql must stay byte-identical. If **D1** drops SQL Server, the proof-based exception becomes moot, the history squashes to one clean SQLite/PostgreSQL baseline, and A6/A8/A9/A10 collapse into it. Fixing A10 now risks work that D1 deletes — so it is evidence *for* that decision, not a task. Nothing is blocked by leaving it: SQLite and mssql both run `upgrade head` clean today. See [CHAT-CHECKPOINTS.md](CHAT-CHECKPOINTS.md) CP‑3. |
| ~~A9~~ | ~~Migration `df04761bd175` uses bare `ALTER COLUMN` — fails on SQLite~~ | ~~`sqlite3.OperationalError: near "ALTER": syntax error`~~ | **DONE** — edited in place under the proof-based exception. `op.batch_alter_table` at default `recreate="auto"`; mssql SQL byte-identical, sha256 `05af4e9efffeaec21a8893b7d35a70395355548d3605b2e43bfb552a478f7fb1` before and after. `upgrade head` from empty now runs clean on **SQLite** and **real SQL Server 2022**; PostgreSQL is blocked by A10, not by this. |
| ~~A8~~ | ~~`server_default=sysdatetimeoffset()` is mssql-only — blocks INSERT on SQLite~~ | ~~`sqlite3.OperationalError: unknown function: sysdatetimeoffset()`~~ | **DONE** — new revision `38ce63bd88cf`, not an edit: dropping a DEFAULT changes SQL Server DDL, so the proof-based exception did not cover it. 8 columns moved to a Python-side `app.db.types.utcnow`. Added `UtcDateTime` so results are tz-aware on SQLite too, which `DateTime(timezone=True)` alone does not give. The `xfail(strict=True)` XPASSed and forced its own removal. |
| A7 | `db/` imports `pipeline/` — layering violation | `app.db.crud_profiles -> app.pipeline.models` at [app/db/crud_profiles.py:12](../app/db/crud_profiles.py#L12), importing `FocusProfileModel`. Caught by import-linter in Slice 0. | Target architecture: "`db/` must not import `services/` or `pipeline/`". Trips two contracts, so it is the whole of the seeded `importlinter_broken: 2`. Fixing it takes the ratchet to 0. Likely resolution: move `FocusProfileModel` down (it is a config/domain type, not a pipeline type) rather than duplicating it. Note the same file also imports `config.profile_store`, so it sits inside the D1 profile-store decision — sequence it with Slice 6b. |
| A6 | ORM models are not DDL-portable — `UNIQUEIDENTIFIER` on 12 columns | `Base.metadata.create_all(create_engine("sqlite://"))` raises `CompileError: SQLiteTypeCompiler can't render element of type UNIQUEIDENTIFIER`. All 6 tables affected (`users`, `resumes`, `profiles`, `runs`, `run_items`, `url_pool`). | Found while applying R2, which had asserted this property passes. Blocks **both** SQLite *and* the Slice 10 Postgres matrix — not SQLite-specific. Fix is a drop-in: SQLAlchemy 2.0's `sa.Uuid` emits `UNIQUEIDENTIFIER` on mssql (schema-identical, no migration for the existing deployment), `UUID` on Postgres, `CHAR(32)` on SQLite. Blocks the DB-touching half of Slice 1. |
| A5 | `make_engine` breaks on in-memory SQLite | `TypeError: Invalid argument(s) 'max_overflow','pool_timeout'`. [app/db/engine.py:48-57](../app/db/engine.py#L48) passes `pool_size`/`max_overflow`/`pool_timeout` unconditionally; `:memory:` gets a `SingletonThreadPool`, which rejects them. Collection dies before any test runs. | Found by pointing the container at SQLite — exactly the free pre-validation [ADR 0007](adr/0007-single-process-sqlite-default.md) predicted. File-based SQLite works (QueuePool), so CI and the image both use a file URL as a stopgap. **Real fix is dialect dispatch in Slice 10**, and it blocks the test strategy's in-memory-SQLite plan (§5.5). |

## B — Touched anyway

> **The numbering collision is resolved (2026-08-02).** [CP-1-REVIEW.md](CP-1-REVIEW.md)
> used to have its own `B1`–`B7` meaning "blocks Slice 3", overlapping this bucket for
> seven numbers — backlog B4 is promoting `_now_iso`, CP-1's B4 was the log-chunk UTF-8
> bug. The rule was "always write the prefix", which is a convention and therefore only as
> reliable as the next brief that forgets it; a bare `B4` in a cold Codex prompt points at
> the wrong task with nothing to signal it. CP-1's items are now **CP1-1 … CP1-7**.
> **`B1`–`B8` on this page are unambiguous and mean this bucket.**

| # | Item | Rides with |
| --- | --- | --- |
| B1 | Delete `get_db` from `app/db/session.py` — dead, one reference (its own definition) | Slice 2 |
| B2 | Add `app/config/__init__.py`, `app/gui_runs/__init__.py` — only packages lacking one | Slice 2 |
| B3 | `B904` raise-without-`from` (17 findings) | Own commit — touches error semantics, not style |
| B4 | Promote `run_manager._now_iso` to `common/utils.timestamp_iso` | Slice 5 — removes a cross-package private-symbol dependency |
| B5 | Deduplicate the two batch runners (~330 near-identical lines) | Commit *after* Slice 6a, never during |
| B6 | 15 of 47 env vars undocumented in `.env.example` (incl. `JOBAGENT_ENV`, `JOBAGENT_USE_LLM_ENRICH`, `JOBAGENT_OPENAI_MODEL_ENRICH`) | Any time — pure docs |
| B8 | Dead `_YEARS_PENALTY` lookup table at [scoring.py:97](../app/pipeline/scoring.py#L97) — never read (pyright: unaccessed). Orphaned when the experience penalty was restructured into the cap-aware formula; its `(3, -15)` entry is what the old `test_experience_penalty_triggers` was asserting. | Slice 2 |
| B7 | `requirements.txt` superseded by `pyproject.toml` + `requirements.lock.txt` | Delete once CI has run green a few times |

## C — Behavior & features

> **Not yet populated.** This is the long product session — walking every feature and deciding
> keep / change / drop. Deliberately deferred.
>
> Do not start it from memory. Run the [liveness audit](liveness-audit.md) first: it turns
> "think about every feature" into "confirm or overrule ~40 rows of evidence", which is a much
> shorter and much better-informed session.

| # | Item | Notes |
| --- | --- | --- |
| C1 | *(awaiting the feature session)* | |

## D — Deletions (decide before Slice 6)

Candidates only. Each needs evidence from the liveness audit before anything is removed.

> **The liveness audit ran on 2026-08-07 and corrected four of these premises.** Full evidence
> in [liveness-report.md](liveness-report.md) §4; the "Evidence needed" column below is amended
> in place, with the original question kept so it is clear what changed. **Read the report's §4
> and §5 before CP‑3, not this table alone** — a corrected premise still leaves the decision
> open, and three of the six now depend on a different question than the one first recorded.
>
> Do **not** treat "coverage is higher than we thought" as an argument to keep something. The
> audit's own rule: unreferenced and unused are different claims, and coverage proves neither.

| # | Candidate | Why it is a candidate | Evidence needed — **amended 2026-08-07** |
| --- | --- | --- | --- |
| D1 | SQL Server / `pyodbc` support | [ADR 0007](adr/0007-single-process-sqlite-default.md) makes SQLite the default. `pyodbc` still forces `unixodbc` into the container and special-cases `app/db/engine.py` | Is any real deployment on SQL Server? **Still unanswerable from the repo — it is a deployment question, report §5 Q3.** But the cost of keeping it is now measured and smaller than assumed: mssql-specific code in `app/` is **one ~30-line function** (`_ensure_connect_timeout`, `db/engine.py`), and `app/db/types.py` already makes the ORM dialect-neutral. **One premise is false:** `Dockerfile:54-56` justifies the `unixodbc` layer with "`import pyodbc` happens at import time via `app.db`" — measured, it does not; importing `app.fastapi_run` under a SQLite URL leaves `pyodbc` out of `sys.modules`. **Take this first at CP‑3 — it reshapes the other D items and blocks A10.** |
| D2 | File-backed `config/profile_store.py` | ~~Two parallel profile stores~~ — **premise wrong.** 85% covered, not 32% | ~~Which is canonical?~~ **The wrong question.** The file store is the **seed source** for the DB store (`crud_profiles.py:166` → `get_default_profiles_dict()`, called from signup at `auth_routes.py:44`) and the **read fallback** when a user has no row. It is not a parallel implementation. Real question, report §5 **Q2**: if it goes, do a new user's default profiles come from a checked-in JSON asset, a migration, or code? **Still blocks Slice 6b.** |
| D3 | `app/stepstone/smoke.py` | Reads like a dev scratch harness; 37% coverage | ~~Is it the backend-dispatch façade?~~ **No — measured.** It GETs a URL and returns a page `<title>`; it returns no job listings. The real search is `search_http` / `search_playwright` (imported as `crawl_http` / `crawl_pw`), used by a different route. So D3's hedge does not apply: it is exactly the dev scratch harness it was suspected of being. **Deleting it removes a module, a public unauthenticated route, and an open bug** — see A17 and report §5 **Q4**: fix the bug or delete the route, but not both. `stub_stepstone_adapter` binds `ss_search`, so deletion breaks that fixture loudly and deliberately. |
| D4 | `app/pipeline/url_pool_maintenance.py` | 14% coverage, and its only endpoint is the one broken by A1 — so it has never actually run in production | **Confirmed, and sequenced.** The only call site is `fastapi_run.py:1593`, inside the block A1 makes unreachable. The feature has never run through its endpoint. **Decide after A1 is fixed, not before** — deleting now deletes a feature nobody has been able to evaluate. |
| D5 | `app/pipeline/resume_parse.py` paths | ~~12% coverage — the lowest of any non-zero module~~ — **both halves wrong.** 51%, and five modules are now lower | ~~Which résumé formats are really used?~~ **Cannot be answered from this code.** The single call site wraps `parse_resume_file` in `except Exception: pass` and `ResumeUploadResponse` has no field that could report a parse failure — see **A18**. A failed parse is indistinguishable from a success, so there is no signal to answer D5 with. **Fix A18 first; the telemetry it unblocks is the answer.** Not a deletion candidate as it stands. |
| D6 | `n8n workflows/` | ~~Untracked prototype directory~~ — **premise wrong.** Tracked: `git ls-files` returns `n8n workflows/job_agent_l7.json`, committed in `5d9facb`, last modified 2025‑10‑27 | Superseded by the orchestrator work? **Nothing in `app/` or `tests/` depends on it — but it is the only first-party caller of `/job_details` and `/bundle`.** Report §5 **Q5**: whether those two routes have a real consumer depends entirely on whether this prototype still runs somewhere, which the repo cannot answer. **Sequence D6 before any decision about those two routes.** |
| D7 | Prefect orchestration | 0% coverage, 698 lines | **Not a deletion candidate.** Listed here to record that it was considered and rejected: it becomes an opt-in backend ([ADR 0006](adr/0006-orchestrator-protocol.md)). Audit confirms: 3 refs each for both flows, reachable from route #29. |

---

## Engineering practices — status

| Item | Status |
| --- | --- |
| Suite is offline, enforced | Done for **this** process — `tests/net_guard.py` + `tests/test_suite_hermeticity.py` §3 (CP1-8). **Does not cover child processes** — see the note below before relying on it |
| CI running the gate on every push | Done — `.github/workflows/ci.yml` + `ci/gate.py` ratchet (pytest, pyright, ruff, import-linter) |
| Architecture enforced as a contract | Done — `.importlinter`, seeded at 2 broken (A7). Slice 0 |
| `import-linter` pinned `<2.6` | **Constraint, not a preference** — 2.6+ needs `rich>=14.2`; prefect 3.1.15 needs `rich<14`. Revisit when prefect relaxes its pin |
| Dependency locking | Done — `requirements.lock.txt` (152 pins) + `pyproject.toml` ranges |
| Pre-commit hooks | Config written; needs `pip install pre-commit && pre-commit install` |
| ADRs | Done — [docs/adr/](adr/), 8 records |
| Docker image | `Dockerfile` + `.dockerignore` written |
| Tagged release | **Not cut.** Deliberately left to a human — see the note in the handover |
| Sentry / Dependabot / changelog automation | Skipped — solo, local-first, no consumers |

### The network guard stops at the process boundary

`tests/net_guard.py` monkeypatches `socket` **in the pytest process**. A child process
gets a fresh interpreter and an unpatched socket module, so anything the suite spawns can
reach the network freely and neither the raise nor the record will show it. The guard's
own tests cannot detect this, because they run in-process by construction.

Nothing crosses that boundary in the gate today — measured, not assumed: installing the
guard turned exactly two tests red, both in-process, and the batch path is not exercised
by any gated test. So this is a **latent** gap, recorded now because it stops being latent
on a specific, already-planned change.

Where it will bite, precisely:

- **[app/fastapi_run.py:1297](../app/fastapi_run.py#L1297)** is the spawn site. It runs
  `python -m app.prefect_run crawl` and then `... process` via `subprocess.run`, streaming
  both into the run log. `app/prefect_run.py` is what runs *inside* those children — it
  spawns nothing itself, so instrumenting it is not the fix and grepping it for
  `subprocess` finds nothing.
- **Slice 2.5's spike drives batch execution directly**, which is exactly the path above.
  The first gated test that exercises it re-opens the CP1-8 hole one level out: a live
  crawl from inside the "offline" suite, with the suite's own hermeticity file still
  reporting green.

Two options when that lands, neither costed yet: refuse the spawn in tests (patch the
subprocess boundary itself, so the *attempt* fails loudly and the child never starts), or
propagate the guard into the child through `env` — `fastapi_run.py` already builds an
`env` for the call, so a `JOBAGENT_NO_NETWORK` that a `sitecustomize` or conftest honours
is feasible. **The first is preferable for the gate**: a test that needs a real child
process to do real work is an `external` test, not a hermetic one, and refusing the spawn
says so at the point of the mistake.

Whoever writes the Slice 2.5 brief owns this. Note it in the brief's *Stop and ask* block
rather than discovering it from a green run.
