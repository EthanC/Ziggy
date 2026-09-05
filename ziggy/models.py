"""SQLAlchemy persistence models."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING
from uuid import uuid4

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    true,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.types import TypeDecorator

if TYPE_CHECKING:
    from sqlalchemy.engine.interfaces import Dialect


def utc_now() -> datetime:
    """Return the current aware UTC timestamp."""
    return datetime.now(UTC)


class UtcDateTime(TypeDecorator[datetime]):
    """Store aware UTC datetimes as sortable SQLite-safe text."""

    impl = String(32)
    cache_ok = True

    def process_bind_param(
        self, value: datetime | None, dialect: Dialect
    ) -> str | None:
        """Convert an aware datetime to canonical UTC text."""
        del dialect
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("naive datetimes are not allowed")
        return value.astimezone(UTC).isoformat(timespec="microseconds")

    def process_result_value(
        self, value: str | None, dialect: Dialect
    ) -> datetime | None:
        """Restore canonical UTC text as an aware datetime."""
        del dialect
        if value is None:
            return None
        return datetime.fromisoformat(value).astimezone(UTC)


class Base(DeclarativeBase):
    """Declarative model base."""


class ArchiveJobKind(StrEnum):
    """Origins of persisted archive work."""

    DIRECT = "direct"
    OUTLINK = "outlink"


class ArchiveJobState(StrEnum):
    """Persisted archive submission and polling states."""

    INTENT = "intent"
    UNCERTAIN = "uncertain"
    SUBMITTED = "submitted"
    PENDING = "pending"
    RATE_LIMITED = "rate_limited"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class ReportState(StrEnum):
    """Persisted report delivery states."""

    PENDING = "pending"
    DELIVERED = "delivered"
    LOGGED = "logged"
    FAILED = "failed"


class Domain(Base):
    """Stable configured host scope."""

    __tablename__ = "domains"

    id: Mapped[int] = mapped_column(primary_key=True)
    host: Mapped[str] = mapped_column(String(253), unique=True)
    scheme: Mapped[str] = mapped_column(String(5))
    include_subdomains: Mapped[bool] = mapped_column(Boolean)
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(UtcDateTime(), default=utc_now)
    configured_at: Mapped[datetime] = mapped_column(UtcDateTime(), default=utc_now)
    deactivated_at: Mapped[datetime | None] = mapped_column(UtcDateTime())

    __table_args__ = (
        CheckConstraint("scheme IN ('http', 'https')", name="ck_domains_scheme"),
        Index("ix_domains_active", "active"),
    )


class Page(Base):
    """One normalized page URL in the crawl frontier."""

    __tablename__ = "pages"

    id: Mapped[int] = mapped_column(primary_key=True)
    domain_id: Mapped[int] = mapped_column(ForeignKey("domains.id", ondelete="CASCADE"))
    url: Mapped[str] = mapped_column(Text, unique=True)
    active: Mapped[bool] = mapped_column(Boolean, default=True, server_default=true())
    deactivated_at: Mapped[datetime | None] = mapped_column(UtcDateTime())
    in_scope: Mapped[bool] = mapped_column(Boolean, default=True, server_default=true())
    discovered_at: Mapped[datetime] = mapped_column(UtcDateTime(), default=utc_now)
    discovered_from_id: Mapped[int | None] = mapped_column(
        ForeignKey("pages.id", ondelete="SET NULL")
    )
    first_crawled_at: Mapped[datetime | None] = mapped_column(UtcDateTime())
    last_crawled_at: Mapped[datetime | None] = mapped_column(UtcDateTime())
    next_crawl_at: Mapped[datetime] = mapped_column(UtcDateTime(), default=utc_now)
    next_archive_at: Mapped[datetime] = mapped_column(UtcDateTime(), default=utc_now)
    status_code: Mapped[int | None] = mapped_column(Integer)
    final_url: Mapped[str | None] = mapped_column(Text)
    content_type: Mapped[str | None] = mapped_column(String(255))
    etag: Mapped[str | None] = mapped_column(Text)
    last_modified: Mapped[str | None] = mapped_column(Text)
    error: Mapped[str | None] = mapped_column(Text)
    sitemap_depth: Mapped[int] = mapped_column(Integer, default=0)
    crawl_attempts: Mapped[int] = mapped_column(Integer, default=0)
    crawl_lease_owner: Mapped[str | None] = mapped_column(String(36))
    crawl_lease_expires_at: Mapped[datetime | None] = mapped_column(UtcDateTime())
    archive_lease_owner: Mapped[str | None] = mapped_column(String(36))
    archive_lease_expires_at: Mapped[datetime | None] = mapped_column(UtcDateTime())
    query_base_url: Mapped[str | None] = mapped_column(Text)
    query_variant_slot: Mapped[int | None] = mapped_column(Integer)
    blocked_reason: Mapped[str | None] = mapped_column(String(32))

    __table_args__ = (
        Index("ix_pages_due_crawl", "next_crawl_at", "crawl_lease_expires_at"),
        Index("ix_pages_due_archive", "next_archive_at", "archive_lease_expires_at"),
        Index("ix_pages_domain", "domain_id"),
        Index(
            "uq_pages_query_variant_slot",
            "query_base_url",
            "query_variant_slot",
            unique=True,
            sqlite_where=query_variant_slot.is_not(None),
        ),
    )


class ArchiveJob(Base):
    """Persisted Save Page Now intent and remote job state."""

    __tablename__ = "archive_jobs"

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid4())
    )
    page_id: Mapped[int] = mapped_column(ForeignKey("pages.id", ondelete="CASCADE"))
    parent_job_id: Mapped[str | None] = mapped_column(
        ForeignKey("archive_jobs.id", ondelete="SET NULL")
    )
    kind: Mapped[ArchiveJobKind] = mapped_column(
        Enum(ArchiveJobKind, native_enum=False, validate_strings=True)
    )
    state: Mapped[ArchiveJobState] = mapped_column(
        Enum(ArchiveJobState, native_enum=False, validate_strings=True)
    )
    cycle_key: Mapped[str] = mapped_column(String(255), unique=True)
    external_job_id: Mapped[str | None] = mapped_column(String(255))
    intent_at: Mapped[datetime] = mapped_column(UtcDateTime(), default=utc_now)
    submitted_at: Mapped[datetime | None] = mapped_column(UtcDateTime())
    completed_at: Mapped[datetime | None] = mapped_column(UtcDateTime())
    next_attempt_at: Mapped[datetime] = mapped_column(UtcDateTime(), default=utc_now)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    error: Mapped[str | None] = mapped_column(Text)
    service_code: Mapped[str | None] = mapped_column(String(255))
    saved_to_my_archive: Mapped[bool] = mapped_column(Boolean, default=False)
    outlinks_processed: Mapped[bool] = mapped_column(Boolean, default=False)
    lease_owner: Mapped[str | None] = mapped_column(String(36))
    lease_expires_at: Mapped[datetime | None] = mapped_column(UtcDateTime())

    __table_args__ = (
        Index("ix_archive_jobs_due", "state", "next_attempt_at", "lease_expires_at"),
        Index("ix_archive_jobs_external_active", "external_job_id", "state"),
        Index("ix_archive_jobs_page", "page_id"),
    )


Index(
    "uq_archive_jobs_active_direct_page",
    ArchiveJob.page_id,
    unique=True,
    sqlite_where=(ArchiveJob.kind == ArchiveJobKind.DIRECT)
    & ArchiveJob.state.in_(
        (
            ArchiveJobState.INTENT,
            ArchiveJobState.UNCERTAIN,
            ArchiveJobState.SUBMITTED,
            ArchiveJobState.PENDING,
            ArchiveJobState.RATE_LIMITED,
        )
    ),
)


class Capture(Base):
    """Immutable successful Internet Archive capture."""

    __tablename__ = "captures"

    id: Mapped[int] = mapped_column(primary_key=True)
    page_id: Mapped[int] = mapped_column(ForeignKey("pages.id", ondelete="CASCADE"))
    archive_job_id: Mapped[str] = mapped_column(
        ForeignKey("archive_jobs.id", ondelete="RESTRICT"), unique=True
    )
    captured_at: Mapped[datetime] = mapped_column(UtcDateTime())
    wayback_url: Mapped[str] = mapped_column(Text)
    screenshot: Mapped[str | None] = mapped_column(Text)
    first_archive: Mapped[bool | None] = mapped_column(Boolean)
    completed_at: Mapped[datetime] = mapped_column(UtcDateTime(), default=utc_now)

    __table_args__ = (
        UniqueConstraint("page_id", "captured_at", name="uq_captures_page_timestamp"),
        Index("ix_captures_page_captured", "page_id", "captured_at"),
    )


class Report(Base):
    """A fixed reporting window and its persisted counts."""

    __tablename__ = "reports"

    id: Mapped[int] = mapped_column(primary_key=True)
    window_start: Mapped[datetime] = mapped_column(UtcDateTime())
    window_end: Mapped[datetime] = mapped_column(UtcDateTime())
    generated_at: Mapped[datetime] = mapped_column(UtcDateTime(), default=utc_now)
    discovered_count: Mapped[int] = mapped_column(Integer)
    archived_count: Mapped[int] = mapped_column(Integer)
    outstanding_count: Mapped[int] = mapped_column(Integer)
    lifetime_discovered_count: Mapped[int] = mapped_column(
        Integer, default=0, server_default="0"
    )
    lifetime_archived_count: Mapped[int] = mapped_column(
        Integer, default=0, server_default="0"
    )
    first_archive_count: Mapped[int] = mapped_column(
        Integer, default=0, server_default="0"
    )
    lifetime_first_archive_count: Mapped[int] = mapped_column(
        Integer, default=0, server_default="0"
    )
    deactivated_count: Mapped[int] = mapped_column(
        Integer, default=0, server_default="0"
    )
    lifetime_deactivated_count: Mapped[int] = mapped_column(
        Integer, default=0, server_default="0"
    )
    active_domain_count: Mapped[int] = mapped_column(Integer)
    state: Mapped[ReportState] = mapped_column(
        Enum(ReportState, native_enum=False, validate_strings=True)
    )
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    next_attempt_at: Mapped[datetime] = mapped_column(UtcDateTime(), default=utc_now)
    delivered_at: Mapped[datetime | None] = mapped_column(UtcDateTime())
    error: Mapped[str | None] = mapped_column(Text)
    discord_message_id: Mapped[str | None] = mapped_column(String(32))
    discord_channel_id: Mapped[str | None] = mapped_column(String(32))
    discord_webhook_id: Mapped[str | None] = mapped_column(String(32))
    lease_owner: Mapped[str | None] = mapped_column(String(36))
    lease_expires_at: Mapped[datetime | None] = mapped_column(UtcDateTime())

    __table_args__ = (
        UniqueConstraint("window_start", "window_end", name="uq_reports_window"),
        Index("ix_reports_pending", "state", "next_attempt_at", "lease_expires_at"),
    )


class ServiceState(Base):
    """Process heartbeat and scheduler checkpoints."""

    __tablename__ = "service_state"

    instance_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    started_at: Mapped[datetime] = mapped_column(UtcDateTime())
    heartbeat_at: Mapped[datetime] = mapped_column(UtcDateTime())
    last_report_window_end: Mapped[datetime | None] = mapped_column(UtcDateTime())
