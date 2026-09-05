"""Async SQLite setup, migrations, reconciliation, and work leases."""

from __future__ import annotations

import asyncio
import sqlite3
from contextlib import closing
from importlib import resources
from typing import TYPE_CHECKING, Literal, Protocol, cast

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

from ziggy.models import ArchiveJob, ArchiveJobState, Capture, Domain, Page
from ziggy.urls import (
    DEFAULT_MAX_QUERY_VARIANTS_PER_BASE,
    query_base_url,
    sensitive_query_key,
    url_in_scope,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from datetime import datetime, timedelta
    from pathlib import Path

    from sqlalchemy.engine import Engine

    from ziggy.config import Config, DomainSettings

WorkKind = Literal["crawl", "archive"]
_PRE_QUERY_FRONTIER_REVISIONS = {
    None,
    "6b519c405276",
    "f5c2b31a8d4e",
    "9d14f3a7c2e1",
    "c83d91e4a672",
}


class _Cursor(Protocol):
    def execute(self, statement: str) -> object: ...

    def close(self) -> None: ...


class _DbapiConnection(Protocol):
    def cursor(self) -> _Cursor: ...


async def insert_page_candidates(
    session: AsyncSession,
    candidates: Sequence[Mapping[str, object]],
    max_query_variants_per_base: int,
) -> tuple[str, ...]:
    """Insert safe page candidates without exceeding per-base query slots."""
    unique = {
        str(candidate["url"]): dict(candidate)
        for candidate in candidates
        if sensitive_query_key(str(candidate["url"])) is None
    }
    if not unique:
        return ()

    existing = {
        page.url: page
        for page in await session.scalars(
            select(Page).where(Page.url.in_(tuple(unique)))
        )
    }
    for url, page in existing.items():
        if page.blocked_reason is None:
            page.domain_id = cast("int", unique[url]["domain_id"])
            page.in_scope = True

    pending = [values for url, values in unique.items() if url not in existing]
    bases = {
        base
        for values in pending
        if (base := query_base_url(str(values["url"]))) is not None
    }
    occupied: dict[str, set[int]] = {base: set() for base in bases}
    if bases:
        rows = await session.execute(
            select(Page.query_base_url, Page.query_variant_slot).where(
                Page.query_base_url.in_(bases),
                Page.query_variant_slot.is_not(None),
            )
        )
        for base, slot in rows:
            occupied[str(base)].add(int(slot))

    admitted: list[dict[str, object]] = []
    for values in pending:
        base = query_base_url(str(values["url"]))
        if base is None:
            values["query_base_url"] = None
            values["query_variant_slot"] = None
            admitted.append(values)
            continue
        free_slot = next(
            (
                slot
                for slot in range(1, max_query_variants_per_base + 1)
                if slot not in occupied[base]
            ),
            None,
        )
        if free_slot is None:
            continue
        occupied[base].add(free_slot)
        values["query_base_url"] = base
        values["query_variant_slot"] = free_slot
        admitted.append(values)

    if not admitted:
        return ()
    result = await session.execute(
        insert(Page).values(admitted).on_conflict_do_nothing().returning(Page.url)
    )
    return tuple(result.scalars())


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


async def run_migrations(
    path: Path,
    query_variant_cap: int = DEFAULT_MAX_QUERY_VARIANTS_PER_BASE,
) -> None:
    """Upgrade the configured database without blocking the event loop."""
    await asyncio.to_thread(path.parent.mkdir, parents=True, exist_ok=True)
    await asyncio.to_thread(_run_migrations_sync, path, query_variant_cap)


def _run_migrations_sync(path: Path, query_variant_cap: int) -> None:
    previous_revision = _database_revision(path)
    migration_resource = resources.files("ziggy.migrations")
    with resources.as_file(migration_resource) as script_directory:
        config = AlembicConfig()
        config.set_main_option("script_location", str(script_directory))
        config.set_main_option("sqlalchemy.url", f"sqlite:///{path.as_posix()}")
        config.attributes["query_variant_cap"] = query_variant_cap
        command.upgrade(config, "head")
    if previous_revision in _PRE_QUERY_FRONTIER_REVISIONS:
        with closing(sqlite3.connect(path)) as connection:
            connection.execute("VACUUM")


def _database_revision(path: Path) -> str | None:
    if not path.exists():
        return None
    with closing(sqlite3.connect(path)) as connection:
        has_version_table = connection.execute(
            "SELECT 1 FROM sqlite_master "
            "WHERE type = 'table' AND name = 'alembic_version'"
        ).fetchone()
        if has_version_table is None:
            return None
        row = connection.execute("SELECT version_num FROM alembic_version").fetchone()
        return None if row is None else str(row[0])


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
        if page.blocked_reason is not None or sensitive_query_key(page.url) is not None:
            page.blocked_reason = page.blocked_reason or "sensitive_query"
            page.in_scope = False
            continue
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
        query_variant_cap = getattr(
            getattr(config, "crawl", None),
            "max_query_variants_per_base",
            DEFAULT_MAX_QUERY_VARIANTS_PER_BASE,
        )
        await insert_page_candidates(
            session,
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
            ],
            query_variant_cap,
        )
    await session.commit()


async def insert_discovered_pages(  # noqa: PLR0913, PLR0917
    session: AsyncSession,
    domain_id: int,
    urls: tuple[str, ...],
    now: datetime,
    discovered_from_id: int | None,
    max_query_variants_per_base: int = DEFAULT_MAX_QUERY_VARIANTS_PER_BASE,
) -> None:
    """Bulk-add normalized discoveries while preserving URL uniqueness."""
    if not urls:
        return
    await insert_page_candidates(
        session,
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
        ],
        max_query_variants_per_base,
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
        Page.active.is_(True),
        Page.in_scope.is_(True),
        Page.blocked_reason.is_(None),
        due_column <= now,
        or_(expires_column.is_(None), expires_column <= now),
    ]
    order_by = [due_column, Page.id]
    if kind == "archive":
        has_capture = exists(select(Capture.id).where(Capture.page_id == Page.id))
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
        order_by.insert(0, has_capture)
    candidate = (
        select(Page.id)
        .join(Domain, Page.domain_id == Domain.id)
        .where(*conditions)
        .order_by(*order_by)
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
