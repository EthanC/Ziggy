"""Add durable HTTP archive submissions."""

from __future__ import annotations

from typing import TYPE_CHECKING

import sqlalchemy as sa
from alembic import op

from ziggy.models import UtcDateTime

if TYPE_CHECKING:
    from collections.abc import Sequence

revision: str = "f31a6b9c2d84"
down_revision: str | None = "4c2f9a8e1d76"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("pages", schema=None) as batch_op:
        batch_op.alter_column("domain_id", existing_type=sa.Integer(), nullable=True)
    op.add_column(
        "archive_jobs",
        sa.Column(
            "archive_only",
            sa.Boolean(),
            server_default=sa.false(),
            nullable=False,
        ),
    )
    op.create_table(
        "archive_submissions",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("page_id", sa.Integer(), nullable=False),
        sa.Column("identifier", sa.String(length=128), nullable=False),
        sa.Column("priority", sa.Integer(), nullable=False),
        sa.Column("accepted_at", UtcDateTime(), nullable=False),
        sa.Column("archive_job_id", sa.String(length=36), nullable=True),
        sa.CheckConstraint(
            "length(identifier) BETWEEN 1 AND 128",
            name="ck_archive_submissions_identifier_length",
        ),
        sa.CheckConstraint(
            "priority BETWEEN -100 AND 100",
            name="ck_archive_submissions_priority",
        ),
        sa.ForeignKeyConstraint(
            ["archive_job_id"], ["archive_jobs.id"], ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(["page_id"], ["pages.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_archive_submissions_pending",
        "archive_submissions",
        ["page_id", sa.text("priority DESC"), "accepted_at", "id"],
        unique=False,
        sqlite_where=sa.text("archive_job_id IS NULL"),
    )
    op.create_index(
        "ix_archive_submissions_job",
        "archive_submissions",
        ["archive_job_id", sa.text("priority DESC")],
        unique=False,
    )
    op.create_index(
        "ix_archive_submissions_page",
        "archive_submissions",
        ["page_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_archive_submissions_page", table_name="archive_submissions")
    op.drop_index("ix_archive_submissions_job", table_name="archive_submissions")
    op.drop_index("ix_archive_submissions_pending", table_name="archive_submissions")
    op.drop_table("archive_submissions")
    with op.batch_alter_table("archive_jobs", schema=None) as batch_op:
        batch_op.drop_column("archive_only")
    op.execute(
        "DELETE FROM captures WHERE page_id IN "
        "(SELECT id FROM pages WHERE domain_id IS NULL)"
    )
    op.execute(
        "DELETE FROM archive_jobs WHERE page_id IN "
        "(SELECT id FROM pages WHERE domain_id IS NULL)"
    )
    op.execute("DELETE FROM pages WHERE domain_id IS NULL")
    with op.batch_alter_table("pages", schema=None) as batch_op:
        batch_op.alter_column("domain_id", existing_type=sa.Integer(), nullable=False)
