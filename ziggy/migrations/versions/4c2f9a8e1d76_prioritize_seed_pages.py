"""Prioritize configured seed pages."""

from __future__ import annotations

from typing import TYPE_CHECKING

import sqlalchemy as sa
from alembic import op

if TYPE_CHECKING:
    from collections.abc import Sequence


revision: str = "4c2f9a8e1d76"
down_revision: str | None = "7a3e9c1d4b20"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "pages",
        sa.Column(
            "is_seed",
            sa.Boolean(),
            server_default=sa.false(),
            nullable=False,
        ),
    )
    op.create_index(
        "ix_pages_due_seed_crawl",
        "pages",
        ["next_crawl_at", "crawl_lease_expires_at"],
        unique=False,
        sqlite_where=sa.text("is_seed = 1"),
    )


def downgrade() -> None:
    op.drop_index("ix_pages_due_seed_crawl", table_name="pages")
    with op.batch_alter_table("pages", schema=None) as batch_op:
        batch_op.drop_column("is_seed")
