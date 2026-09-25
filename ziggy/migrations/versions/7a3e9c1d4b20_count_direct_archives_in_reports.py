"""Count direct archive jobs in reports."""

from __future__ import annotations

from typing import TYPE_CHECKING

import sqlalchemy as sa
from alembic import op

if TYPE_CHECKING:
    from collections.abc import Sequence


revision: str = "7a3e9c1d4b20"
down_revision: str | None = "e41c7a9d2b60"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    _recalculate_reports(direct_only=True)


def downgrade() -> None:
    _recalculate_reports(direct_only=False)


def _recalculate_reports(*, direct_only: bool) -> None:
    op.execute(
        sa.text(
            """
        UPDATE reports SET
            archived_count = (
                SELECT count(DISTINCT captures.page_id)
                FROM captures
                JOIN archive_jobs ON archive_jobs.id = captures.archive_job_id
                WHERE captures.completed_at >= reports.window_start
                  AND captures.completed_at < reports.window_end
                  AND (:direct_only = 0 OR archive_jobs.kind = 'DIRECT')
            ),
            lifetime_archived_count = (
                SELECT count(DISTINCT captures.page_id)
                FROM captures
                JOIN archive_jobs ON archive_jobs.id = captures.archive_job_id
                WHERE captures.completed_at < reports.window_end
                  AND (:direct_only = 0 OR archive_jobs.kind = 'DIRECT')
            ),
            first_archive_count = (
                SELECT count(*)
                FROM captures
                JOIN archive_jobs ON archive_jobs.id = captures.archive_job_id
                WHERE captures.completed_at >= reports.window_start
                  AND captures.completed_at < reports.window_end
                  AND captures.first_archive = 1
                  AND (:direct_only = 0 OR archive_jobs.kind = 'DIRECT')
            ),
            lifetime_first_archive_count = (
                SELECT count(*)
                FROM captures
                JOIN archive_jobs ON archive_jobs.id = captures.archive_job_id
                WHERE captures.completed_at < reports.window_end
                  AND captures.first_archive = 1
                  AND (:direct_only = 0 OR archive_jobs.kind = 'DIRECT')
            )
        """
        ).bindparams(direct_only=direct_only)
    )
