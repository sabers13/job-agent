"""add profile_name and description to profiles

Revision ID: df04761bd175
Revises: a932afee4b12
Create Date: 2025-12-20 04:26:41.952381

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'df04761bd175'
down_revision: Union[str, Sequence[str], None] = 'a932afee4b12'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "profiles",
        sa.Column("profile_name", sa.String(length=128), nullable=False, server_default=""),
    )
    op.add_column(
        "profiles",
        sa.Column("description", sa.String(length=512), nullable=True),
    )
    # Remove server default for future inserts.
    #
    # Backlog A9. A bare op.alter_column emits `ALTER TABLE ... ALTER COLUMN`, which
    # SQLite has no syntax for -- `alembic upgrade head` died here with
    # `near "ALTER": syntax error` on any fresh SQLite database.
    #
    # batch_alter_table at the default recreate="auto" recreates the table only on
    # SQLite and passes straight through to a plain ALTER everywhere else, so the
    # mssql SQL is unchanged. Proven byte-identical before/after -- see the commit
    # message for the hash. That proof is what licenses editing this file in place
    # rather than adding a revision (AGENTS.md, "Do not edit existing files in
    # alembic/versions/", proof-based exception).
    with op.batch_alter_table("profiles") as batch_op:
        batch_op.alter_column("profile_name", server_default=None)


def downgrade() -> None:
    op.drop_column("profiles", "description")
    op.drop_column("profiles", "profile_name")
