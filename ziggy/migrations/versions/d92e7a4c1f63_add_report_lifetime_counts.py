"""Add lifetime page counts to reports."""

from __future__ import annotations

from typing import TYPE_CHECKING

import sqlalchemy as sa
from alembic import op

if TYPE_CHECKING:
    from collections.abc import Sequence


revision: str = "d92e7a4c1f63"
down_revision: str | None = "b81f4d2a6e90"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    columns = (
        "lifetime_discovered_count",
        "lifetime_archived_count",
        "lifetime_first_archive_count",
        "lifetime_deactivated_count",
    )
    with op.batch_alter_table("reports", schema=None) as batch_op:
        for name in columns:
            batch_op.add_column(
                sa.Column(name, sa.Integer(), server_default="0", nullable=False)
            )

    op.execute(
        """
        UPDATE reports SET
            lifetime_discovered_count = (
                SELECT count(*) FROM pages
                WHERE pages.discovered_at < reports.window_end
            ),
            lifetime_archived_count = (
                SELECT count(DISTINCT captures.page_id) FROM captures
                WHERE captures.completed_at < reports.window_end
            ),
            lifetime_first_archive_count = (
                SELECT count(*) FROM captures
                WHERE captures.completed_at < reports.window_end
                  AND captures.first_archive = 1
            ),
            lifetime_deactivated_count = (
                SELECT count(*) FROM pages
                WHERE pages.deactivated_at < reports.window_end
            )
        """
    )


def downgrade() -> None:
    with op.batch_alter_table("reports", schema=None) as batch_op:
        batch_op.drop_column("lifetime_deactivated_count")
        batch_op.drop_column("lifetime_first_archive_count")
        batch_op.drop_column("lifetime_archived_count")
        batch_op.drop_column("lifetime_discovered_count")
