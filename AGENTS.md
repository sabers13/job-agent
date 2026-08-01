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
                  UnifiedJobPosting, FocusProfileModel, JobScoring, FetchMeta,
                  LLMDetail, JobDetailsResponse, BlockerCaps, Constraints.
                  Types only: no I/O, no business logic, no imports from any
                  other app package.
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
  survives. 78 such handlers exist today (backlog A3); do not add a 79th.
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
  make live network calls. Use fixtures.
- Logging: structured and run-scoped. Anything happening inside a run must be
  traceable in that run's `run.log`.

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
- **Do not run `pre-commit run --all-files`.** It has already rewritten 55 files in one
  pass and buried an 8-column bugfix inside the pyupgrade diff that `refactor-plan.md`
  reserves for Slice 2. Hooks run on staged files at commit time; running them across
  the repo is a mass rewrite wearing a lint hook's clothes.
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
