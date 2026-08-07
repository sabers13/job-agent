# Job Agent

Local-first job-search triage pipeline. Crawls job listings (StepStone), normalizes
them into a stable schema, scores fit with explainable heuristics plus optional LLM
enrichment, and writes run-scoped artifacts to `output/<user_id>/<profile_key>/<run_id>/`.

FastAPI control plane, SQLAlchemy + Alembic persistence, Playwright-backed fetching
with a "polite fetch" policy.

**This repo is mid-restructure.** It was written quickly and is being re-engineered
toward a single-process, zero-config application that a non-technical user can launch
and operate. Read "Target architecture" and "Current state" below before proposing
any change — they differ, deliberately.

**Must never break:** run artifacts. `run_id`, `status.json`, `run.log`, and the
offset-based log streaming contract are the system's actual deliverable. An HTTP 200
with missing or malformed artifacts is a failure, not a success.

> **Before starting any slice, read [docs/CHAT-CHECKPOINTS.md](docs/CHAT-CHECKPOINTS.md).**
> If a checkpoint blocks it, stop, print the handoff prompt from §Handoff prompt with the
> placeholders filled in, and do not begin the slice. If a §Unplanned escalation trigger
> fires mid-slice, stop at that point and do the same.
>
> Chat has direct read access to this repo, so a handoff prompt is a **pointer, not a
> paste**. Name the files; do not copy their contents in.

**This file is `AGENTS.md`, at the repo root.** `CLAUDE.md` is a symlink to it, so Claude
Code and Codex read the same text and it cannot drift. Edit `AGENTS.md`; never replace the
symlink with a copy.

---

## Commands

<!-- VERIFY EACH OF THESE AGAINST YOUR REPO BEFORE COMMITTING THIS FILE.
     A wrong command here gets run every session and fails silently-ish. -->

- Test:       `pytest -q`
- Coverage:   `pytest --cov=app --cov-report=term-missing`
- Typecheck:  `pyright --outputjson`
- Lint:       `ruff check . && ruff format .`
- Migrate:    `alembic upgrade head`
- Run:        `uvicorn app.fastapi_run:app --host 127.0.0.1 --port 5001 --reload`

Environment: `source .venv/bin/activate && set -a; source .env.dev; set +a`

---

## Target architecture

The restructure moves toward this layering. Do not assume it already exists — check
before importing.

```
app/
  api/            FastAPI routers. HTTP concerns only: request/response models,
                  status codes, auth dependencies. No business logic.
  services/       Business logic. Every operation callable without an HTTP request.
                  This is what tests should target.
  orchestration/  Orchestrator protocol + backends. LocalOrchestrator is the
                  default; PrefectOrchestrator is opt-in.
  pipeline/       parse -> enrich -> score -> output. Source-agnostic.
  sources/        Job-board adapters (stepstone/, ...). Source-specific parsing,
                  search, and date handling live here and nowhere else.
  fetching/       Generic HTTP client, Playwright driver, polite-fetch policy.
                  Knows nothing about job boards.
  db/             ORM models, session management, CRUD, engine, health. No
                  business logic.
  config/         Settings, focus config, profile store. Fail-fast validation.
  domain/         Shared Pydantic types — the project's vocabulary.
                  UnifiedJobPosting, FocusProfileModel, LLMDetail,
                  BlockerCaps, Constraints.
                  Types only: no I/O, no business logic, no imports from any
                  other app package.
                  NOTE (2026-08-07): this list read eight types until the
                  liveness audit measured them. JobDetailsResponse, FetchMeta
                  and JobScoring in pipeline/models.py have zero LSP
                  references — JobDetailsResponse is shadowed by a live
                  same-named class in api/schemas.py that fastapi_run.py:42
                  actually imports, and the other two are referenced only from
                  inside the dead one. Slice 2.9 must delete them, not move
                  them. See liveness-report.md §4 rank 1.
  common/         Shared leaf utilities (utils, logging context). Imports nothing
                  from other app packages. Anything here must be genuinely generic;
                  if it knows about jobs, runs, or profiles, it belongs elsewhere.
```

`domain/` and `common/` are both leaves, and the distinction matters: `domain/` holds
types that *are* about jobs and profiles; `common/` holds helpers that are not. If a
utility knows what a job is, it belongs in neither — it belongs in a service.

`app/gui_runs/` is folded into `app/services/` (run lifecycle) during the restructure.
The name is a holdover from when runs were a GUI concern; they are now a core one.

### Import rules

Allowed direction is strictly downward:

```
api  ->  services  ->  {orchestration, pipeline, db, config}
                       pipeline -> {sources, fetching, db, config}
                       sources  -> {fetching, config}
                       db, config, fetching -> {domain, common}
                       domain, common -> nothing
```

- `domain/` and `common/` are leaves: neither may import from any other `app/`
  package. Everything may import them.
- **Shared types live in `domain/`, never in `pipeline/`.** A service, router, or CRUD
  function returning a job or a profile imports that type from `app.domain`. If you
  find yourself importing a Pydantic model from `app.pipeline`, that model is in the
  wrong package — stop and report it.
- Nothing may import from `api/`.
- `pipeline/` must not import from `sources/stepstone/` directly — go through the
  source-adapter interface. Adding a second job board must not require editing
  `pipeline/`.
- `db/` must not import `services/` or `pipeline/`.
- If a move would create an import cycle, **stop and report it** rather than
  working around it with a local import or a `TYPE_CHECKING` guard.

### Public API boundary

Each package's public surface is what `__init__.py` re-exports. Anything else is
internal and may be moved without notice. Cross-package imports must target the
package, not a submodule: `from app.pipeline import score_job`, not
`from app.pipeline.scoring import score_job`.

---

## Current state (pre-refactor reality)

Accurate as of the start of the restructure. Update as slices land.

47 modules under `app/`, 16 test files under `tests/`.

- `app/api/` exists but is partial — it holds `auth_routes.py` and `schemas.py`
  only. The router extraction has started; it is not finished.
- `app/fastapi_run.py` is still a god-module: run APIs, resume endpoints, health
  routes, GUI page routes, and the orchestration trigger. Finishing the split is
  the first planned slice.
- There is no service layer. Business logic sits in route handlers, so most of it
  is only reachable through HTTP.
- `app/gui_runs/run_manager.py` owns the run directory, status, and log-chunk
  contract. It is the most behavior-critical module in the repo.
- `app/stepstone/` is source-specific but sits beside generic `app/fetching/`, and
  `app/pipeline/` reaches into it. No source-adapter interface exists yet.
- Orchestration is Prefect-only (`app/prefect_run.py`) and assumes a separately
  started `prefect server`.
- Persistence targets SQL Server via `pyodbc`. Migrating to a dialect-agnostic
  default (SQLite local) is planned.

### Churn zones (file mtimes as of Jan 2026)

- **Unsettled**, last touched Jan 16–17: `fastapi_run.py`, `api/auth_routes.py`,
  `db/*`, `auth/deps.py`, `gui_runs/run_manager.py`, `prefect_run.py`,
  `pipeline/output.py`, `pipeline/potential_bucket.py`, `config/settings.py`.
  Expect inconsistency and half-finished ideas here.
- **Stable**, untouched since Oct–Dec 2025: `fetching/*`, `stepstone/*`,
  `pipeline/parsers.py`, and the scoring tests. Treat as settled; moving this code
  behind an interface is low-risk because it is not otherwise in motion.

---

## Conventions

- Python 3.12. Type hints required on every public function.
- **No silent failure.** Catch the narrowest exception that can actually occur. A
  broad `except Exception:` must either re-raise or log at ERROR with the exception
  attached — never swallow into a generic "failed" status. That pattern is how
  `_LogSink` kept an endpoint permanently broken while returning HTTP 200. When
  re-raising inside `except`, use `raise ... from err` so the original traceback
  survives. Roughly 77 such handlers exist (backlog A3, measured on `main` at
  `fd85028`); do not add another. **Re-measure rather than quoting that number** — it
  drifts with any commit, and this file previously carried 78, measured on a branch.

  **A broad handler swallows deliberate signals too, not just accidents.**
  `except Exception` catches the `HTTPException` your own handler raised, so a 400
  becomes a 500. Measured on `/search_stepstone` (backlog A17). If a handler body can
  raise `HTTPException`, the enclosing broad `except` needs `except HTTPException: raise`
  above it — `/bundle` does this and is correct; those two routes are the whole
  population, so the pattern is copyable.
- **Never introduce `Any` to silence pyright.** Use `Protocol`, `TypedDict`, or a
  proper generic. If the correct type is genuinely unknowable, use `object` and
  narrow, and leave a comment explaining why.
- Migrations must run on **both SQLite and PostgreSQL**. SQLite has no
  `ALTER COLUMN` and limited constraint support, so use
  `with op.batch_alter_table(...)` for any column modification. Never write raw
  `mssql`-dialect SQL in a migration.
- Database access goes through SQLAlchemy. No raw connection strings or raw SQL
  outside `app/db/`.
- Configuration is read from settings in `app/config/`, never `os.environ` directly
  in feature code.
- Fetching goes through the polite-fetch layer. Never call `httpx`/`requests`/
  Playwright directly from `pipeline/` or `sources/`.
- Tests live in `tests/`, mirror the package path of what they test, and must not
  make live network calls. Use fixtures. This is enforced, not merely asked for:
  `tests/net_guard.py` installs a session-wide autouse guard that refuses any outbound
  connection or DNS lookup, and `tests/test_suite_hermeticity.py` §3 asserts the guard
  is installed and that a refusal swallowed by a broad `except Exception:` still fails
  the test. Loopback is deliberately allowed; the reasoning is in `net_guard.py`.
- **`monkeypatch.setattr(..., raising=False)` is forbidden in a stub.** The default is
  `raising=True` — keep it. `raising=False` converts "this name does not exist" from an
  `AttributeError` into a freshly created attribute that nothing reads, so the stub
  binds to nothing, the test exercises the real dependency, and it reports that it
  stubbed it.

  Measured, not theorised: `monkeypatch.setattr(fr, "search_stepstone_http", …,
  raising=False)` named an attribute that has never existed on `app.fastapi_run` —
  `fastapi_run.py:79` imports that function *as* `crawl_http`. The swallowed
  `AttributeError` is precisely how a live HTTP request to `https://www.stepstone.de/en/`
  survived inside the gated "offline" suite, on the one route whose only test claimed to
  stub the adapter. See **CP1-8**.

  The single legitimate use is **deliberately creating** an attribute that is supposed to
  be absent. Then the creation is the point, so assert it: the test must fail if the
  attribute starts existing on its own, or if the code under test never reads it. A stub
  whose whole job is to bind to a real name is never that case.

  Binding to an existing name is necessary but not sufficient — bind to the name the
  code **calls**. Rebinding a module attribute that FastAPI captured at decoration time
  (the route handler itself) changes nothing on the request path. That was the second
  half of the same defect, and it was the half that `raising=True` would not have caught.
- **No assertion may accept both the success and the failure state.**
  `assert response.status_code in (200, 503)` is not an assertion — it is a comment
  that costs a test run. If both outcomes pass, the test reports "covered" while
  checking nothing, and it spends a reviewer's attention to discover that.

  This is not hypothetical here. Three tests in this repo could not fail:

  | Test | Why it could not fail |
  | --- | --- |
  | `test_scoring_is_deterministic` | `score_job` mutates its input, so call 2 gets a different object. It asserted `f(x) == f(mutate(x))`, not `f(x) == f(x)`. |
  | `test_scoring_mutates_the_job_dict_in_exactly_one_known_way` | The "profile was not mutated" check compared a `set` against a **reference** to itself — `s == s`, unconditionally true. |
  | `test_health_db_reports_reachability` | `in (200, 503)` under a docstring reading "SQLite is reachable in tests". |

  The third is why this is a rule and not a style note. It **concealed a live SQL
  Server connection inside a green suite** — the suite was reaching a real database,
  and the assertion was wide enough to accept that as success. See backlog **A13**.

  When a route genuinely cannot be pinned to one code offline, assert the negative
  that still has content — `!= 404 and != 500` — rather than enumerating an accept
  set broad enough to swallow the failure. Never write an accept set containing 500:
  that permits an unhandled server error.
- **Config and hermeticity tests must not assert directly on values that may hold
  secrets.** pytest rewrites assertions to print every operand, so
  `assert settings.database_url.startswith("sqlite:")` dumps the full URL —
  credentials included — into the failure output and therefore into CI logs. Compare
  into a local first and assert on that: a dialect name, a scheme, a boolean, a
  length. Measured, not theorised: the first draft of
  `tests/test_suite_hermeticity.py` printed the dev SQL Server password on failure.
  Note SQLAlchemy's `URL.__repr__` masks the password *component* but not query
  parameters, and `mssql+pyodbc` carries its credentials inside `odbc_connect`.
- Logging: structured and run-scoped. Anything happening inside a run must be
  traceable in that run's `run.log`.
- **`tasks/<ID>.report.md` is always writable and always committable, whether or not the
  brief's allowlist names it.** The report is the deliverable of the task, not a scope
  violation, and `scripts/slice_report.sh` creates it as the final step of every brief. An
  allowlist that omits it is incomplete, not restrictive — commit the report anyway and
  note it.

  This rule lives here, in the always-loaded file, because stating it only in
  `AGENT-WORKFLOW.md` §2 has now failed twice. Slice 2 stopped at its final step on the
  contradiction; `s1-accept-sets` completed but left its report uncommitted. Both workers
  were reading the brief, which pointed at a different section. A rule that only works when
  someone remembers to link it is not written down.

---

## Do not

- **Do not modify files outside the scope you were asked about.** If you notice an
  unrelated problem, report it in your summary; do not fix it.
- Do not add dependencies without asking. This project is being packaged for
  non-technical users; every dependency is an install-time risk.
- **Never `pip install` directly into the venv.** Add the pin to `pyproject.toml`,
  regenerate `requirements.lock.txt`, then install from the lock, and run `pip check`.
  Direct installs have silently broken this environment twice — `pytest` 8.3.5 → 9.1.1
  mid-baseline, and `import-linter` 2.13 dragging `rich` past the ceiling `prefect`
  requires. Both were transitive upgrades nobody asked for.
- Do not delete a compatibility shim until the full test suite passes with it
  removed — not just the affected module's tests.
- Do not delete the Prefect orchestration code. It becomes an optional backend
  behind the `Orchestrator` protocol, not dead code to remove.
- Do not edit existing files in `alembic/versions/`. Schema changes get a new
  revision. Rewriting migration history is a separate, explicitly requested task.
  - **One exception, defined by proof rather than by filename.** An existing migration
    may be edited *only* when the edit provably emits **byte-identical SQL on mssql** —
    the only deployed dialect, already at head. Demonstrate it: dump the compiled DDL
    before and after and compare hashes. Record the hash in the commit message.
    Anything that changes the mssql SQL is a new revision, not an edit, no matter how
    small.

    This licenses making a migration portable to SQLite/PostgreSQL when the mssql
    output is unchanged — e.g. `sa.Uuid` in place of `UNIQUEIDENTIFIER` (backlog A6,
    proven identical), or `op.batch_alter_table` in place of `op.alter_column`, which
    at the default `recreate="auto"` only recreates on SQLite and passes through to a
    plain `ALTER` elsewhere (backlog A9 — same proof still required).

    It does **not** license changes that alter mssql DDL, such as dropping a
    `server_default` (backlog A8). Those get a new revision.
- Do not weaken a test to make it pass. If a test is wrong, say so and explain why.
- **`--no-verify` is expected when a commit touches a file with pre-existing debt.**
  The `ruff-check` hook lints whole staged *files*, not your diff, so any commit
  touching `app/db/models.py` (12 `UP037`), `app/fastapi_run.py` or most of `app/` is
  blocked by findings that belong to Slice 2's pyupgrade pass. Do not "just fix them" —
  that pulls Slice 2 into an unrelated diff, which has already happened once. Use
  `--no-verify` and say why in the commit message. `ci/gate.py` is the authoritative
  gate; the hook is a fast local signal, not the contract.
- **Do not run `pre-commit run --all-files`.** It has already rewritten 55 files in one
  pass and buried an 8-column bugfix inside the pyupgrade diff that `refactor-plan.md`
  reserves for Slice 2. Hooks run on staged files at commit time; running them across
  the repo is a mass rewrite wearing a lint hook's clothes.
- **Never pass `--exclude` to `ruff`. Use `--extend-exclude`.** `--exclude` *replaces*
  the `exclude` list in `pyproject.toml` rather than adding to it, which silently
  re-enables `alembic/versions/`. A dry run of Slice 2 with `--exclude tests/legacy`
  rewrote three existing migrations that way — straight through the prohibition above,
  with nothing in the diff to signal it.
- **Exclude `docs/` from `ruff format`.** It reformats Python code blocks *inside
  Markdown*, so a plain `ruff format .` edits `docs/refactor-plan.md` and
  `docs/TEST-STRATEGY.md`. The working invocation is:
  `ruff check . --fix --extend-exclude tests/legacy,docs` then
  `ruff format . --extend-exclude tests/legacy,docs`.
- **A commit listed in `.git-blame-ignore-revs` must be purely mechanical.** Reproducible
  from its parent by re-running the formatter and nothing else. Hand edits inside a
  blame-ignored commit are invisible to `git blame` forever — ship them as a separate
  commit. Verify reproducibility by re-running the formatter against the parent in a
  scratch worktree and comparing `git write-tree` to the commit's tree hash.
- Do not reintroduce a second long-running process. Eliminating the two-terminal
  startup is a core goal; anything requiring the user to open another terminal
  defeats it.
- Do not commit `.env`, `.env.dev`, `jar.txt`, or anything under `output/`.

---

## Verification gates

A slice is not done until both pass:

```bash
pytest -q
pyright --outputjson | jq '.summary'
```

Type-checking clean is necessary, not sufficient. Behavioral correctness is proven
by tests, and for anything touching the run lifecycle, by inspecting the artifacts
an actual run produces.
