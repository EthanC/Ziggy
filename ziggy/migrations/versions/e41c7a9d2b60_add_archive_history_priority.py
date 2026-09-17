"""Add Internet Archive history priority state."""

from __future__ import annotations

from typing import TYPE_CHECKING

import sqlalchemy as sa
from alembic import op

import ziggy.models

if TYPE_CHECKING:
    from collections.abc import Sequence


revision: str = "e41c7a9d2b60"
down_revision: str | None = "d92e7a4c1f63"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("pages", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "archive_history_checked_at",
                ziggy.models.UtcDateTime(length=32),
                nullable=True,
            )
        )
        batch_op.add_column(
            sa.Column(
                "latest_archive_at",
                ziggy.models.UtcDateTime(length=32),
                nullable=True,
            )
        )
        batch_op.add_column(
            sa.Column(
                "next_archive_history_check_at",
                ziggy.models.UtcDateTime(length=32),
                nullable=True,
            )
        )
        batch_op.add_column(
            sa.Column(
                "archive_history_check_attempts",
                sa.Integer(),
                server_default="0",
                nullable=False,
            )
        )
        batch_op.add_column(
            sa.Column("archive_history_check_error", sa.Text(), nullable=True)
        )
        batch_op.add_column(
            sa.Column(
                "archive_history_lease_owner", sa.String(length=36), nullable=True
            )
        )
        batch_op.add_column(
            sa.Column(
                "archive_history_lease_expires_at",
                ziggy.models.UtcDateTime(length=32),
                nullable=True,
            )
        )

    op.execute(
        """
        UPDATE pages SET
            latest_archive_at = (
                SELECT max(captures.captured_at) FROM captures
                WHERE captures.page_id = pages.id
            ),
            archive_history_checked_at = (
                SELECT max(captures.completed_at) FROM captures
                WHERE captures.page_id = pages.id
            ),
            next_archive_history_check_at = CASE
                WHEN EXISTS (
                    SELECT 1 FROM captures WHERE captures.page_id = pages.id
                ) THEN NULL
                ELSE discovered_at
            END
        """
    )
    op.create_index(
        "ix_pages_due_archive_history",
        "pages",
        ["next_archive_history_check_at", "archive_history_lease_expires_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_pages_due_archive_history", table_name="pages")
    with op.batch_alter_table("pages", schema=None) as batch_op:
        batch_op.drop_column("archive_history_lease_expires_at")
        batch_op.drop_column("archive_history_lease_owner")
        batch_op.drop_column("archive_history_check_error")
        batch_op.drop_column("archive_history_check_attempts")
        batch_op.drop_column("next_archive_history_check_at")
        batch_op.drop_column("latest_archive_at")
        batch_op.drop_column("archive_history_checked_at")
