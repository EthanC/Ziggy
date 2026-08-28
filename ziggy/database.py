"""Async SQLite setup, migrations, reconciliation, and work leases."""

from __future__ import annotations

import asyncio
from importlib import resources
from typing import TYPE_CHECKING, Literal, Protocol

from alembic import command
from alembic.config import Config as AlembicConfig
from sqlalchemy import event, exists, or_, select, text, update
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from ziggy.models import ArchiveJob, ArchiveJobState, Domain, Page
from ziggy.urls import url_in_scope

if TYPE_CHECKING:
    from datetime import datetime, timedelta
    from pathlib import Path

    from sqlalchemy.engine import Engine

    from ziggy.config import Config, DomainSettings

WorkKind = Literal["crawl", "archive"]


class _Cursor(Protocol):
    def execute(self, statement: str) -> object: ...

    def close(self) -> None: ...


class _DbapiConnection(Protocol):
    def cursor(self) -> _Cursor: ...


def database_url(path: Path) -> str:
    """Build an aiosqlite URL for an absolute database path."""
    return f"sqlite+aiosqlite:///{path.as_posix()}"


def create_engine(path: Path) -> AsyncEngine:
    """Create an async engine with required SQLite connection pragmas."""
    path.parent.mkdir(parents=True, exist_ok=True)
    engine = create_async_engine(database_url(path))

    @event.listens_for(engine.sync_engine, "connect")
    def configure_sqlite(
        dbapi_connection: _DbapiConnection, connection_record: object
    ) -> None:
        del connection_record
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA busy_timeout=5000")
            cursor.execute("PRAGMA synchronous=NORMAL")
        finally:
            cursor.close()

    return engine


def session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Create sessions that retain loaded attributes after commit."""
    return async_sessionmaker(engine, expire_on_commit=False)


async def run_migrations(path: Path) -> None:
    """Upgrade the configured database without blocking the event loop."""
    await asyncio.to_thread(path.parent.mkdir, parents=True, exist_ok=True)
    await asyncio.to_thread(_run_migrations_sync, path)


def _run_migrations_sync(path: Path) -> None:
    migration_resource = resources.files("ziggy.migrations")
    with resources.as_file(migration_resource) as script_directory:
        config = AlembicConfig()
        config.set_main_option("script_location", str(script_directory))
        config.set_main_option("sqlalchemy.url", f"sqlite:///{path.as_posix()}")
        command.upgrade(config, "head")


async def reconcile_domains(
    session: AsyncSession, config: Config, now: datetime
) -> None:
    """Apply a complete domain replacement and reconcile retained pages."""
    await session.execute(text("BEGIN IMMEDIATE"))
    configured_hosts = {domain.host for domain in config.domains}
    existing = {domain.host: domain for domain in await session.scalars(select(Domain))}
    configured: list[tuple[DomainSettings, Domain]] = []
    for settings in config.domains:
        domain = existing.get(settings.host)
        if domain is None:
            domain = Domain(
                host=settings.host,
                scheme=settings.scheme,
                include_subdomains=settings.include_subdomains,
                active=True,
                created_at=now,
                configured_at=now,
            )
            session.add(domain)
            await session.flush()
        else:
            domain.scheme = settings.scheme
            domain.include_subdomains = settings.include_subdomains
            domain.active = True
            domain.configured_at = now
            domain.deactivated_at = None
        configured.append((settings, domain))
    for host, domain in existing.items():
        if host not in configured_hosts and domain.active:
            domain.active = False
            domain.deactivated_at = now

    for page in await session.scalars(select(Page)):
        owner = next(
            (
                domain
                for settings, domain in configured
                if url_in_scope(
                    page.url,
                    settings.host,
                    include_subdomains=settings.include_subdomains,
                )
            ),
            None,
        )
        page.in_scope = owner is not None
        if owner is not None:
            page.domain_id = owner.id

    for settings, domain in configured:
        await session.execute(
            insert(Page)
            .values(
                [
                    {
                        "domain_id": domain.id,
                        "url": url,
                        "in_scope": True,
                        "discovered_at": now,
                        "next_crawl_at": now,
                        "next_archive_at": now,
                    }
                    for url in settings.seed_urls
                ]
            )
            .on_conflict_do_update(
                index_elements=[Page.url],
                set_={"domain_id": domain.id, "in_scope": True},
            )
        )
    await session.commit()


async def insert_discovered_pages(
    session: AsyncSession,
    domain_id: int,
    urls: tuple[str, ...],
    now: datetime,
    discovered_from_id: int | None,
) -> None:
    """Bulk-add normalized discoveries while preserving URL uniqueness."""
    if not urls:
        return
    await session.execute(
        insert(Page)
        .values(
            [
                {
                    "domain_id": domain_id,
                    "url": url,
                    "in_scope": True,
                    "discovered_at": now,
                    "discovered_from_id": discovered_from_id,
                    "next_crawl_at": now,
                    "next_archive_at": now,
                }
                for url in urls
            ]
        )
        .on_conflict_do_update(
            index_elements=[Page.url],
            set_={"domain_id": domain_id, "in_scope": True},
        )
    )


async def claim_due_page(
    session: AsyncSession,
    kind: WorkKind,
    owner: str,
    now: datetime,
    lease_duration: timedelta,
) -> Page | None:
    """Atomically claim the oldest due page belonging to an active domain."""
    due_column = Page.next_crawl_at if kind == "crawl" else Page.next_archive_at
    owner_column = (
        Page.crawl_lease_owner if kind == "crawl" else Page.archive_lease_owner
    )
    expires_column = (
        Page.crawl_lease_expires_at
        if kind == "crawl"
        else Page.archive_lease_expires_at
    )
    conditions = [
        Domain.active.is_(True),
        Page.in_scope.is_(True),
        due_column <= now,
        or_(expires_column.is_(None), expires_column <= now),
    ]
    if kind == "archive":
        conditions.append(
            ~exists(
                select(ArchiveJob.id).where(
                    ArchiveJob.page_id == Page.id,
                    ArchiveJob.state.in_(
                        (
                            ArchiveJobState.INTENT,
                            ArchiveJobState.UNCERTAIN,
                            ArchiveJobState.SUBMITTED,
                            ArchiveJobState.PENDING,
                            ArchiveJobState.RATE_LIMITED,
                        )
                    ),
                )
            )
        )
    candidate = (
        select(Page.id)
        .join(Domain, Page.domain_id == Domain.id)
        .where(*conditions)
        .order_by(due_column, Page.id)
        .limit(1)
        .scalar_subquery()
    )
    statement = (
        update(Page)
        .where(
            Page.id == candidate,
            or_(expires_column.is_(None), expires_column <= now),
        )
        .values(
            {
                owner_column: owner,
                expires_column: now + lease_duration,
            }
        )
        .returning(Page)
    )
    page = (await session.scalars(statement)).one_or_none()
    await session.commit()
    return page


async def release_leases(session: AsyncSession, owner: str) -> None:
    """Release page and archive-job leases held by one service instance."""
    await session.execute(
        update(Page)
        .where(Page.crawl_lease_owner == owner)
        .values(
            crawl_lease_owner=None,
            crawl_lease_expires_at=None,
        )
    )
    await session.execute(
        update(Page)
        .where(Page.archive_lease_owner == owner)
        .values(
            archive_lease_owner=None,
            archive_lease_expires_at=None,
        )
    )
    await session.execute(
        update(ArchiveJob)
        .where(ArchiveJob.lease_owner == owner)
        .values(lease_owner=None, lease_expires_at=None)
    )
    await session.commit()


def sync_engine(async_engine: AsyncEngine) -> Engine:
    """Return the proxied synchronous engine for event inspection."""
    return async_engine.sync_engine
