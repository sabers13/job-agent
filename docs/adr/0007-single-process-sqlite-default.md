# 0007 — Single process, SQLite by default

**Status:** Proposed · **Date:** 2026-07-31

## Context

Running the app currently requires SQL Server reachable over ODBC (`pyodbc`, ODBC Driver 18) and
a separately started `prefect server`. For a local-first tool intended to be cloned and run, that
is two hard infrastructure dependencies before anything happens.

`app/db/engine.py` encodes this: `_ensure_connect_timeout` special-cases `mssql+pyodbc` and
manipulates `odbc_connect` strings.

## Decision

Default to SQLite on a local file. Keep PostgreSQL as the supported multi-user option. Combined
with ADR 0006, the result is one process and one file.

Migrations must run on **both SQLite and PostgreSQL**:

- `with op.batch_alter_table(...)` for any column modification — SQLite has no `ALTER COLUMN`.
- No raw `mssql`-dialect SQL in any migration.
- Existing files in `alembic/versions/` are never edited; schema changes get a new revision.

## Consequences

- `git clone && docker run` becomes sufficient. The Dockerfile already defaults to
  `sqlite:////data/job-agent.db`.
- The CI Postgres/SQLite migration matrix becomes meaningful and lands with this slice. It is
  deliberately absent from CI until then, because `alembic upgrade head` currently targets SQL
  Server and a matrix job that cannot pass is theater.

**Cost:** SQLite's concurrency model is single-writer. Acceptable for a local single-user tool;
the escape hatch is pointing `JOBAGENT_DATABASE_URL` at Postgres.

**Free pre-validation:** running the contract tests against in-memory SQLite (test strategy §5.5)
surfaces every `mssql`-specific assumption in the models *before* this slice starts. Anything that
fails there is work this slice was going to hit anyway — which is why it is sequenced last, after
the test suite exists, rather than first.

**Unresolved:** `pyodbc` stays a dependency for the SQL Server path, which means the container
still installs `unixodbc` to satisfy the import. Whether SQL Server support is worth that weight
is a keep/drop question for the liveness audit (ADR 0008), not a decision made here.
