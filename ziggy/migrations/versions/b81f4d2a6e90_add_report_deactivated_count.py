"""Add page deactivation reporting state."""

from __future__ import annotations

from typing import TYPE_CHECKING

import sqlalchemy as sa
from alembic import op

import ziggy.models

if TYPE_CHECKING:
    from collections.abc import Sequence

revision: str = "b81f4d2a6e90"
down_revision: str | None = "a7426f0d1c3b"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("pages", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "deactivated_at", ziggy.models.UtcDateTime(length=32), nullable=True
            )
        )
    op.execute(
        "UPDATE pages SET deactivated_at = last_crawled_at "
        "WHERE active = 0 AND last_crawled_at IS NOT NULL"
    )
    with op.batch_alter_table("reports", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "deactivated_count",
                sa.Integer(),
                server_default="0",
                nullable=False,
            )
        )


def downgrade() -> None:
    with op.batch_alter_table("reports", schema=None) as batch_op:
        batch_op.drop_column("deactivated_count")
    with op.batch_alter_table("pages", schema=None) as batch_op:
        batch_op.drop_column("deactivated_at")
