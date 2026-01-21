"""add resumes table

Revision ID: 3b4d3b5b3c1a
Revises: df04761bd175
Create Date: 2025-12-21 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.mssql import UNIQUEIDENTIFIER


revision: str = "3b4d3b5b3c1a"
down_revision: Union[str, Sequence[str], None] = "df04761bd175"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "resumes",
        sa.Column("id", UNIQUEIDENTIFIER(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("user_id", UNIQUEIDENTIFIER(as_uuid=True), nullable=False),
        sa.Column("filename", sa.String(length=255), nullable=False),
        sa.Column("mime_type", sa.String(length=128), nullable=False),
        sa.Column("sha256", sa.String(length=64), nullable=False),
        sa.Column("storage_path", sa.String(length=512), nullable=False),
        sa.Column("text_content", sa.UnicodeText(), nullable=True),
        sa.Column("parsed_json", sa.UnicodeText(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("0")),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("sysdatetimeoffset()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("sysdatetimeoffset()"),
        ),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"]),
    )
    op.create_index("ix_resumes_user_sha256", "resumes", ["user_id", "sha256"])
    op.create_index("ix_resumes_user_active", "resumes", ["user_id", "is_active"])


def downgrade() -> None:
    op.drop_index("ix_resumes_user_active", table_name="resumes")
    op.drop_index("ix_resumes_user_sha256", table_name="resumes")
    op.drop_table("resumes")
