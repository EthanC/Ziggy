"""Index scheduling candidates and checkpoint scope reconciliation."""

import sqlalchemy as sa
from alembic import op

revision = "08c6a1f94d27"
down_revision = "f31a6b9c2d84"
branch_labels = None
depends_on = None

_SCOPED = "in_scope IS 1 AND blocked_reason IS NULL"
_ARCHIVE = f"{_SCOPED} AND active IS 1"
_INDEXES = (
    ("ix_pages_schedule_seed", ["next_crawl_at", "id"], f"{_SCOPED} AND is_seed IS 1"),
    (
        "ix_pages_schedule_crawl",
        ["active", "next_crawl_at", "id"],
        f"{_SCOPED} AND is_seed IS 0",
    ),
    (
        "ix_pages_schedule_archive_unknown",
        ["next_archive_at", "id"],
        f"{_ARCHIVE} AND archive_history_checked_at IS NULL",
    ),
    (
        "ix_pages_schedule_archive_known",
        ["next_archive_at", "id"],
        f"{_ARCHIVE} AND archive_history_checked_at IS NOT NULL",
    ),
    (
        "ix_pages_schedule_history",
        ["next_archive_history_check_at", "id"],
        (
            f"{_ARCHIVE} AND archive_history_checked_at IS NULL "
            "AND error IS NULL AND status_code BETWEEN 200 AND 299"
        ),
    ),
    (
        "ix_pages_schedule_archive_age",
        ["latest_archive_at", "next_archive_at", "id"],
        f"{_ARCHIVE} AND archive_history_checked_at IS NOT NULL",
    ),
)


def upgrade() -> None:
    for name, columns, predicate in _INDEXES:
        op.create_index(name, "pages", columns, sqlite_where=sa.text(predicate))
    op.create_index(
        "ix_archive_jobs_work",
        "archive_jobs",
        ["next_attempt_at", "intent_at", "id"],
        sqlite_where=sa.text(
            "((external_job_id IS NOT NULL "
            "AND state IN ('SUBMITTED', 'PENDING', 'RATE_LIMITED')) "
            "OR (external_job_id IS NULL "
            "AND state IN ('INTENT', 'UNCERTAIN', 'RATE_LIMITED')) "
            "OR (state = 'SUCCEEDED' "
            "AND (saved_to_my_archive IS 0 OR outlinks_processed IS 0)))"
        ),
    )
    op.create_table(
        "scope_checkpoint",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("fingerprint", sa.String(64), nullable=False),
        sa.Column("last_page_id", sa.Integer(), nullable=False),
        sa.Column("completed", sa.Boolean(), nullable=False),
    )


def downgrade() -> None:
    op.drop_index("ix_archive_jobs_work", table_name="archive_jobs")
    op.drop_table("scope_checkpoint")
    for name, _columns, _predicate in reversed(_INDEXES):
        op.drop_index(name, table_name="pages")
