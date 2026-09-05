"""Add page active state."""

from __future__ import annotations

from typing import TYPE_CHECKING

import sqlalchemy as sa
from alembic import op

if TYPE_CHECKING:
    from collections.abc import Sequence

revision: str = "a7426f0d1c3b"
down_revision: str | None = "2f4a8c1d9e70"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("pages", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "active",
                sa.Boolean(),
                server_default=sa.true(),
                nullable=False,
            )
        )


def downgrade() -> None:
    with op.batch_alter_table("pages", schema=None) as batch_op:
        batch_op.drop_column("active")
