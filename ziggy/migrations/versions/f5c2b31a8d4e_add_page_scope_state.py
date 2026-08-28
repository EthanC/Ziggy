"""add page scope state."""

from __future__ import annotations

from typing import TYPE_CHECKING

import sqlalchemy as sa
from alembic import op

if TYPE_CHECKING:
    from collections.abc import Sequence

revision: str = "f5c2b31a8d4e"
down_revision: str | None = "6b519c405276"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("pages", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "in_scope",
                sa.Boolean(),
                server_default=sa.true(),
                nullable=False,
            )
        )


def downgrade() -> None:
    with op.batch_alter_table("pages", schema=None) as batch_op:
        batch_op.drop_column("in_scope")
