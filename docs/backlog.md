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
| A3 | 78 broad `except` handlers with no re-raise | AST scan across `app/`; 17 in `fastapi_run.py`, 8 in `stepstone/search_playwright.py`, 7 in `prefect_run.py` | A1 is one of these 78. The rest are unexamined. Not a claim that 78 bugs exist — a claim that nobody knows which are load-bearing. Triage during the liveness audit. |
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

| # | Candidate | Why it is a candidate | Evidence needed |
| --- | --- | --- | --- |
| D1 | SQL Server / `pyodbc` support | [ADR 0007](adr/0007-single-process-sqlite-default.md) makes SQLite the default. `pyodbc` still forces `unixodbc` into the container and special-cases `app/db/engine.py` | Is any real deployment on SQL Server? If not, dropping it simplifies engine, image and migrations at once. **Take this first at CP‑3 — it reshapes the other D items and blocks A10.** A10 prices it concretely: keeping mssql means dialect-conditional defaults maintained forever, because the byte-identical constraint is the only reason the migration history cannot simply be squashed. |
| D2 | File-backed `config/profile_store.py` | Two parallel profile stores, both routed — file-backed and DB-backed `db/crud_profiles.py`. 32% coverage | Which is canonical? **Blocks Slice 6b** — see [refactor-plan.md](refactor-plan.md). Architectural in disguise. |
| D3 | `app/stepstone/smoke.py` | Reads like a dev scratch harness; 37% coverage | Does anything but tests call it? If it is the backend-dispatch façade, it becomes `sources/stepstone/adapter.py` instead of being deleted |
| D4 | `app/pipeline/url_pool_maintenance.py` | 14% coverage, and its only endpoint is the one broken by A1 — so it has never actually run in production | Once A1 is fixed, does the feature earn its keep? |
| D5 | `app/pipeline/resume_parse.py` paths | 12% coverage — the lowest of any non-zero module | Which résumé formats are really used? |
| D6 | `n8n workflows/` | Untracked prototype directory | Superseded by the orchestrator work? |
| D7 | Prefect orchestration | 0% coverage, 698 lines | **Not a deletion candidate.** Listed here to record that it was considered and rejected: it becomes an opt-in backend ([ADR 0006](adr/0006-orchestrator-protocol.md)). |

---

## Engineering practices — status

| Item | Status |
| --- | --- |
| CI running the gate on every push | Done — `.github/workflows/ci.yml` + `ci/gate.py` ratchet (pytest, pyright, ruff, import-linter) |
| Architecture enforced as a contract | Done — `.importlinter`, seeded at 2 broken (A7). Slice 0 |
| `import-linter` pinned `<2.6` | **Constraint, not a preference** — 2.6+ needs `rich>=14.2`; prefect 3.1.15 needs `rich<14`. Revisit when prefect relaxes its pin |
| Dependency locking | Done — `requirements.lock.txt` (152 pins) + `pyproject.toml` ranges |
| Pre-commit hooks | Config written; needs `pip install pre-commit && pre-commit install` |
| ADRs | Done — [docs/adr/](adr/), 8 records |
| Docker image | `Dockerfile` + `.dockerignore` written |
| Tagged release | **Not cut.** Deliberately left to a human — see the note in the handover |
| Sentry / Dependabot / changelog automation | Skipped — solo, local-first, no consumers |
