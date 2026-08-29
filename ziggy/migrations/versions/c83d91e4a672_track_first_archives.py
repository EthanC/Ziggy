"""Track captures that created a page's first Internet Archive snapshot."""

from __future__ import annotations

from typing import TYPE_CHECKING

import sqlalchemy as sa
from alembic import op

if TYPE_CHECKING:
    from collections.abc import Sequence


revision: str = "c83d91e4a672"
down_revision: str | None = "9d14f3a7c2e1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("captures", schema=None) as batch_op:
        batch_op.add_column(sa.Column("first_archive", sa.Boolean(), nullable=True))
    with op.batch_alter_table("reports", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "first_archive_count",
                sa.Integer(),
                server_default="0",
                nullable=False,
            )
        )


def downgrade() -> None:
    with op.batch_alter_table("reports", schema=None) as batch_op:
        batch_op.drop_column("first_archive_count")
    with op.batch_alter_table("captures", schema=None) as batch_op:
        batch_op.drop_column("first_archive")
