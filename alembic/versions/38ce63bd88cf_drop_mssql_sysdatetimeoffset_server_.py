"""drop mssql sysdatetimeoffset server defaults (A8)

Revision ID: 38ce63bd88cf
Revises: 3b4d3b5b3c1a
Create Date: 2026-07-31 20:17:13.425537

Backlog A8. Eight timestamp columns carried
``server_default=sa.text("sysdatetimeoffset()")``. That function exists only on SQL
Server, so every INSERT failed on SQLite with ``unknown function:
sysdatetimeoffset()`` -- the schema could be created but never written to.

The defaults move to the application: ``app.db.types.utcnow`` is now the Python-side
``default=`` on those columns, which is portable by construction.

This is a NEW revision rather than an edit to the two migrations that introduced the
defaults. The recorded exception in docs/CLAUDE.md is proof-based -- it licenses edits
that provably produce byte-identical SQL Server DDL. This change *does* alter SQL
Server DDL (it drops eight DEFAULT clauses), so the exception does not cover it and a
real migration is required.

``batch_alter_table`` is used throughout: SQLite cannot ``ALTER COLUMN`` in place, so
Alembic rebuilds the table via copy-and-swap. On SQL Server and PostgreSQL it emits a
plain ALTER.

**Behaviour change worth stating plainly:** rows inserted by anything other than this
application -- a manual ``INSERT``, a bulk load, a fixture that bypasses the ORM -- no
longer receive a timestamp automatically. The columns remain NOT NULL, so such a write
now fails loudly instead of silently recording the wrong time. Existing rows are
untouched.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "38ce63bd88cf"
down_revision: str | Sequence[str] | None = "3b4d3b5b3c1a"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# (table, column) for every column that carried server_default=sysdatetimeoffset().
_TIMESTAMP_COLUMNS: list[tuple[str, str]] = [
    ("users", "created_at"),
    ("profiles", "created_at"),
    ("runs", "created_at"),
    ("run_items", "created_at"),
    ("url_pool", "first_seen_at"),
    ("url_pool", "last_seen_at"),
    ("resumes", "created_at"),
    ("resumes", "updated_at"),
]


def upgrade() -> None:
    """Drop the SQL-Server-only server defaults."""
    for table, column in _TIMESTAMP_COLUMNS:
        with op.batch_alter_table(table) as batch:
            batch.alter_column(
                column,
                existing_type=sa.DateTime(timezone=True),
                existing_nullable=False,
                server_default=None,
            )


def downgrade() -> None:
    """Restore them.

    ``sysdatetimeoffset()`` is SQL-Server-only, so this downgrade is only meaningful
    against SQL Server -- which is the only deployment that ever had these defaults.
    On SQLite or PostgreSQL it would write a default referencing a function the engine
    does not have; that is inherent to reversing an mssql-specific construct, not an
    oversight.
    """
    for table, column in _TIMESTAMP_COLUMNS:
        with op.batch_alter_table(table) as batch:
            batch.alter_column(
                column,
                existing_type=sa.DateTime(timezone=True),
                existing_nullable=False,
                server_default=sa.text("sysdatetimeoffset()"),
            )
