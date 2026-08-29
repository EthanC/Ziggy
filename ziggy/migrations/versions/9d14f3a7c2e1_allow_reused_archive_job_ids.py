"""Allow Archive.org to reuse deduplicated capture job IDs."""

from __future__ import annotations

from typing import TYPE_CHECKING

from alembic import op

if TYPE_CHECKING:
    from collections.abc import Sequence

revision: str = "9d14f3a7c2e1"
down_revision: str | None = "f5c2b31a8d4e"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_NAMING_CONVENTION = {"uq": "uq_%(table_name)s_%(column_0_name)s"}


def upgrade() -> None:
    with op.batch_alter_table(
        "archive_jobs", schema=None, naming_convention=_NAMING_CONVENTION
    ) as batch_op:
        batch_op.drop_constraint("uq_archive_jobs_external_job_id", type_="unique")


def downgrade() -> None:
    with op.batch_alter_table("archive_jobs", schema=None) as batch_op:
        batch_op.create_unique_constraint(
            "uq_archive_jobs_external_job_id", ["external_job_id"]
        )
