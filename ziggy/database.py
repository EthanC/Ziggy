"""Async SQLite setup, migrations, reconciliation, and work leases."""

from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from importlib import resources
from typing import TYPE_CHECKING, Literal, Protocol, cast
from urllib.parse import urlsplit
from uuid import uuid4

from alembic import command
from alembic.config import Config as AlembicConfig
from sqlalchemy import (
    and_,
    case,
    event,
    exists,
    func,
    literal_column,
    or_,
    select,
    text,
    update,
)
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from ziggy.models import (
    ArchiveJob,
    ArchiveJobKind,
    ArchiveJobState,
    ArchiveSubmission,
    Domain,
    Page,
    ScopeCheckpoint,
)
from ziggy.transactions import write_transaction
from ziggy.urls import (
    DEFAULT_MAX_QUERY_VARIANTS_PER_BASE,
    host_in_scope,
    query_base_url,
    sensitive_query_key,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping, Sequence
    from datetime import datetime, timedelta
    from pathlib import Path

    from sqlalchemy.engine import Engine
    from sqlalchemy.orm import InstrumentedAttribute
    from sqlalchemy.sql import ColumnElement

    from ziggy.config import Config, DomainSettings

WorkKind = Literal["crawl", "archive"]
_RECONCILE_BATCH_SIZE = 1_000
ACTIVE_ARCHIVE_STATES = (
    ArchiveJobState.INTENT,
    ArchiveJobState.UNCERTAIN,
    ArchiveJobState.SUBMITTED,
    ArchiveJobState.PENDING,
    ArchiveJobState.RATE_LIMITED,
)
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


@dataclass(frozen=True, slots=True)
class SubmissionReceipt:
    """Durable acknowledgement for one normalized URL."""

    id: str
    url: str


class AdmissionCapacityError(RuntimeError):
    """The configured outstanding-submission capacity is exhausted."""


async def insert_page_candidates(  # noqa: C901, PLR0912, PLR0915
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
    promote: list[tuple[Page, dict[str, object]]] = []
    for url, page in existing.items():
        values = unique[url]
        is_seed = values.get("is_seed") is True
        if getattr(page, "domain_id", values["domain_id"]) is None and not is_seed:
            promote.append((page, values))
            continue
        if page.blocked_reason is None or is_seed:
            page.domain_id = cast("int", values["domain_id"])
            page.in_scope = True
            if is_seed:
                if not page.is_seed:
                    page.next_crawl_at = min(
                        page.next_crawl_at,
                        cast("datetime", values["next_crawl_at"]),
                    )
                page.is_seed = True
                page.blocked_reason = None

    pending = [values for url, values in unique.items() if url not in existing]
    bases = {
        base
        for values in [*pending, *(values for _page, values in promote)]
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
            occupied[str(base)].add(cast("int", slot))

    admitted: list[dict[str, object]] = []
    for page, values in promote:
        base = query_base_url(str(values["url"]))
        if base is not None:
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
            page.query_base_url = base
            page.query_variant_slot = free_slot
        page.domain_id = cast("int", values["domain_id"])
        page.in_scope = True
        page.discovered_from_id = cast("int | None", values.get("discovered_from_id"))
        page.sitemap_depth = cast("int", values.get("sitemap_depth", 0))
        page.next_crawl_at = cast("datetime", values["next_crawl_at"])

    for values in pending:
        if values.get("is_seed") is True:
            values["query_base_url"] = None
            values["query_variant_slot"] = None
            admitted.append(values)
            continue
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


async def admit_archive_submissions(  # noqa: PLR0913
    session: AsyncSession,
    identifier: str,
    urls: Sequence[str],
    priority: int,
    now: datetime,
    *,
    outstanding_limit: int,
) -> tuple[SubmissionReceipt, ...]:
    """Persist a normalized batch and its attribution in one short transaction."""
    await session.execute(text("BEGIN IMMEDIATE"))
    pending = await session.scalar(
        select(func.count())
        .select_from(ArchiveSubmission)
        .where(ArchiveSubmission.archive_job_id.is_(None))
    )
    active = await session.scalar(
        select(func.count())
        .select_from(ArchiveJob)
        .join(ArchiveSubmission, ArchiveSubmission.archive_job_id == ArchiveJob.id)
        .where(ArchiveJob.state.in_(ACTIVE_ARCHIVE_STATES))
    )
    if (pending or 0) + (active or 0) + len(urls) > outstanding_limit:
        await session.rollback()
        raise AdmissionCapacityError("outstanding submission capacity is exhausted")

    existing = {
        page.url: page
        for page in await session.scalars(select(Page).where(Page.url.in_(urls)))
    }
    for url in urls:
        if url not in existing:
            page = Page(
                domain_id=None,
                url=url,
                active=True,
                in_scope=False,
                discovered_at=now,
                next_crawl_at=now,
                next_archive_at=now,
                next_archive_history_check_at=None,
            )
            session.add(page)
            existing[url] = page
    await session.flush()

    active_jobs = {
        job.page_id: job.id
        for job in await session.scalars(
            select(ArchiveJob).where(
                ArchiveJob.page_id.in_(page.id for page in existing.values()),
                ArchiveJob.kind == ArchiveJobKind.DIRECT,
                ArchiveJob.state.in_(ACTIVE_ARCHIVE_STATES),
            )
        )
    }
    receipts = tuple(SubmissionReceipt(str(uuid4()), url) for url in urls)
    await session.execute(
        insert(ArchiveSubmission),
        [
            {
                "id": receipt.id,
                "page_id": existing[receipt.url].id,
                "identifier": identifier,
                "priority": priority,
                "accepted_at": now,
                "archive_job_id": active_jobs.get(existing[receipt.url].id),
            }
            for receipt in receipts
        ],
    )
    await session.commit()
    return receipts


def database_url(path: Path) -> str:
    """Build an aiosqlite URL for an absolute database path."""
    return f"sqlite+aiosqlite:///{path.as_posix()}"


def create_engine(path: Path, *, busy_timeout_ms: int = 5_000) -> AsyncEngine:
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
            cursor.execute(f"PRAGMA busy_timeout={busy_timeout_ms}")
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


async def reconcile_domains(  # noqa: C901, PLR0915
    session: AsyncSession, config: Config, now: datetime
) -> None:
    """Resume changed scope in bounded chunks; skip completed unchanged scans."""
    fingerprint = hashlib.sha256(
        json.dumps(
            sorted(
                (settings.host, settings.include_subdomains)
                for settings in config.domains
            )
        ).encode()
    ).hexdigest()
    await session.commit()

    async def setup() -> tuple[list[tuple[DomainSettings, int]], ScopeCheckpoint]:
        configured = await _configure_domains(session, config, now)
        checkpoint = await session.get(ScopeCheckpoint, 1, populate_existing=True)
        if checkpoint is None:
            checkpoint = ScopeCheckpoint(
                id=1, fingerprint=fingerprint, last_page_id=0, completed=False
            )
            session.add(checkpoint)
        elif checkpoint.fingerprint != fingerprint:
            checkpoint.fingerprint = fingerprint
            checkpoint.last_page_id = 0
            checkpoint.completed = False
        return configured, checkpoint

    configured, checkpoint = await write_transaction(session, setup)
    configured_by_host = {
        settings.host: (settings.include_subdomains, domain_id)
        for settings, domain_id in configured
    }
    while not checkpoint.completed:
        pages = (
            await session.execute(
                select(Page.id, Page.url, Page.blocked_reason)
                .where(Page.id > checkpoint.last_page_id, Page.domain_id.is_not(None))
                .order_by(Page.id)
                .limit(_RECONCILE_BATCH_SIZE)
            )
        ).all()
        changes: list[dict[str, object]] = []
        for page_id, url, blocked_reason in pages:
            values: dict[str, object] = {"id": page_id}
            if blocked_reason is not None or sensitive_query_key(url) is not None:
                values.update(
                    blocked_reason=blocked_reason or "sensitive_query", in_scope=False
                )
            else:
                host = urlsplit(url).hostname
                owner = configured_by_host.get(host or "")
                if owner is None and host is not None:
                    owner = next(
                        (
                            candidate
                            for configured_host, candidate in configured_by_host.items()
                            if host_in_scope(
                                host, configured_host, include_subdomains=candidate[0]
                            )
                        ),
                        None,
                    )
                values["in_scope"] = owner is not None
                if owner is not None:
                    values["domain_id"] = owner[1]
            changes.append(values)
        await session.commit()

        last_page_id = pages[-1][0] if pages else 0

        async def persist_batch(
            changes: list[dict[str, object]] = changes,
            last_page_id: int = last_page_id,
        ) -> bool:
            await session.refresh(checkpoint)
            if checkpoint.fingerprint != fingerprint:
                return False
            if changes:
                await session.execute(update(Page), changes)
                checkpoint.last_page_id = max(checkpoint.last_page_id, last_page_id)
            else:
                checkpoint.completed = True
            return True

        if not await write_transaction(session, persist_batch):
            return

    seed_urls = tuple(
        url for settings, _domain_id in configured for url in settings.seed_urls
    )

    async def clear_seeds() -> None:
        stale_seed = Page.is_seed.is_(True)
        if seed_urls:
            stale_seed &= Page.url.not_in(seed_urls)
        await session.execute(update(Page).where(stale_seed).values(is_seed=False))

    await write_transaction(session, clear_seeds)
    query_variant_cap = getattr(
        getattr(config, "crawl", None),
        "max_query_variants_per_base",
        DEFAULT_MAX_QUERY_VARIANTS_PER_BASE,
    )
    candidates = [
        {
            "domain_id": domain_id,
            "url": url,
            "is_seed": True,
            "in_scope": True,
            "discovered_at": now,
            "next_crawl_at": now,
            "next_archive_at": now,
        }
        for settings, domain_id in configured
        for url in settings.seed_urls
    ]
    for offset in range(0, len(candidates), 200):

        async def insert_seeds(offset: int = offset) -> None:
            await insert_page_candidates(
                session, candidates[offset : offset + 200], query_variant_cap
            )

        await write_transaction(session, insert_seeds)


async def _configure_domains(
    session: AsyncSession,
    config: Config,
    now: datetime,
) -> list[tuple[DomainSettings, int]]:
    configured_hosts = {domain.host for domain in config.domains}
    existing = {domain.host: domain for domain in await session.scalars(select(Domain))}
    configured: list[tuple[DomainSettings, int]] = []
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
        configured.append((settings, domain.id))
    for host, domain in existing.items():
        if host not in configured_hosts and domain.active:
            domain.active = False
            domain.deactivated_at = now
    return configured


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
    await session.commit()
    for offset in range(0, len(urls), 200):
        candidates: list[dict[str, object]] = [
            {
                "domain_id": domain_id,
                "url": url,
                "in_scope": True,
                "discovered_at": now,
                "discovered_from_id": discovered_from_id,
                "next_crawl_at": now,
                "next_archive_at": now,
            }
            for url in urls[offset : offset + 200]
        ]

        async def persist(candidates: list[dict[str, object]] = candidates) -> None:
            await insert_page_candidates(
                session, candidates, max_query_variants_per_base
            )

        await write_transaction(session, persist)


async def claim_due_page(  # noqa: PLR0913
    session: AsyncSession,
    kind: WorkKind,
    owner: str,
    now: datetime,
    lease_duration: timedelta,
    *,
    archive_interval: timedelta | None = None,
) -> Page | None:
    """Select outside the writer transaction, then recheck and claim one page."""
    if kind == "archive":
        if archive_interval is None:
            raise ValueError("archive_interval is required for archive work")
        eligible = and_(
            _available(Page.archive_lease_expires_at, now),
            _available(Page.archive_history_lease_expires_at, now),
            ~_active_archive_job(),
            or_(_ordinary_archive(now), _pending_submission()),
        )
        return await _claim_page(
            session,
            lambda: _archive_candidate(session, now, archive_interval),
            eligible,
            {
                "archive_lease_owner": owner,
                "archive_lease_expires_at": now + lease_duration,
            },
        )

    eligible = and_(
        _scoped_page(),
        Page.next_crawl_at <= now,
        _available(Page.crawl_lease_expires_at, now),
    )

    async def candidate() -> int | None:
        for category in (
            Page.is_seed.is_(True),
            and_(Page.is_seed.is_(False), Page.active.is_(True)),
            and_(Page.is_seed.is_(False), Page.active.is_(False)),
        ):
            page_id = await session.scalar(
                select(Page.id)
                .where(eligible, category)
                .order_by(Page.next_crawl_at, Page.id)
                .limit(1)
            )
            if page_id is not None:
                return page_id
        return None

    return await _claim_page(
        session,
        candidate,
        eligible,
        {"crawl_lease_owner": owner, "crawl_lease_expires_at": now + lease_duration},
    )


def _available(
    column: InstrumentedAttribute[datetime | None], now: datetime
) -> ColumnElement[bool]:
    return or_(column.is_(None), column <= now)


def _scoped_page() -> ColumnElement[bool]:
    return and_(
        Page.in_scope.is_(True),
        Page.blocked_reason.is_(None),
        exists(
            select(Domain.id).where(
                Domain.id == Page.domain_id, Domain.active.is_(True)
            )
        ),
    )


def _ordinary_archive(now: datetime) -> ColumnElement[bool]:
    return and_(_scoped_page(), Page.active.is_(True), Page.next_archive_at <= now)


def _active_archive_job() -> ColumnElement[bool]:
    return exists(
        select(ArchiveJob.id).where(
            ArchiveJob.page_id == Page.id,
            ArchiveJob.kind == ArchiveJobKind.DIRECT,
            ArchiveJob.state.in_(ACTIVE_ARCHIVE_STATES),
        )
    )


def _pending_submission() -> ColumnElement[bool]:
    return exists(
        select(ArchiveSubmission.id).where(
            ArchiveSubmission.page_id == Page.id,
            ArchiveSubmission.archive_job_id.is_(None),
        )
    )


async def _archive_candidate(
    session: AsyncSession,
    now: datetime,
    interval: timedelta,
) -> int | None:
    available = and_(
        _available(Page.archive_lease_expires_at, now),
        _available(Page.archive_history_lease_expires_at, now),
        ~_active_archive_job(),
    )
    ordinary = _ordinary_archive(now)
    cutoff = now - interval
    history = case(
        (
            and_(
                Page.archive_history_checked_at.is_not(None),
                or_(Page.latest_archive_at.is_(None), Page.latest_archive_at < cutoff),
            ),
            0,
        ),
        (Page.archive_history_checked_at.is_(None), 1),
        else_=2,
    )
    pending = (
        select(
            ArchiveSubmission.page_id,
            func.max(ArchiveSubmission.priority).label("priority"),
            func.min(ArchiveSubmission.accepted_at).label("accepted"),
        )
        .where(ArchiveSubmission.archive_job_id.is_(None))
        .group_by(ArchiveSubmission.page_id)
        .subquery()
    )
    priority = case(
        (ordinary, func.max(pending.c.priority, 0)), else_=pending.c.priority
    )
    due = case((ordinary, Page.next_archive_at), else_=pending.c.accepted)
    submitted = (
        await session.execute(
            select(Page.id, priority, history, due)
            .join(pending, pending.c.page_id == Page.id)
            .where(available)
            .order_by(priority.desc(), history, due, Page.id)
            .limit(1)
        )
    ).first()
    if submitted is not None and submitted[1] > 0:
        return submitted[0]

    candidate = await _ordinary_archive_candidate(session, ordinary & available, cutoff)
    if candidate is not None:
        _history_rank, _due_at, page_id = candidate
        if (
            submitted is not None
            and submitted[1] == 0
            and (submitted[2], submitted[3], submitted[0]) < candidate
        ):
            return submitted[0]
        return page_id
    return None if submitted is None else submitted[0]


async def _ordinary_archive_candidate(
    session: AsyncSession,
    eligible: ColumnElement[bool],
    cutoff: datetime,
) -> tuple[int, datetime, int] | None:
    best: tuple[int, datetime, int] | None = None
    for rank, category in (
        (
            0,
            and_(
                Page.archive_history_checked_at.is_not(None),
                Page.latest_archive_at.is_(None),
            ),
        ),
        (
            0,
            and_(
                Page.archive_history_checked_at.is_not(None),
                Page.latest_archive_at < cutoff,
            ),
        ),
        (1, Page.archive_history_checked_at.is_(None)),
        (
            2,
            and_(
                Page.archive_history_checked_at.is_not(None),
                Page.latest_archive_at >= cutoff,
            ),
        ),
    ):
        if best is not None and rank > best[0]:
            break
        # An age-index probe avoids walking the due index when all known captures
        # are recent. The second probe preserves due-time ordering within a band.
        if (
            rank == 0
            and await session.scalar(
                select(Page.id)
                .where(eligible, category)
                .order_by(Page.latest_archive_at)
                .limit(1)
            )
            is None
        ):
            continue
        row = (
            await session.execute(
                select(Page.next_archive_at, Page.id)
                .where(eligible, category)
                .order_by(Page.next_archive_at, Page.id)
                .limit(1)
            )
        ).first()
        if row is not None:
            candidate = (rank, row[0], row[1])
            best = candidate if best is None else min(best, candidate)
    return best


async def _claim_page(
    session: AsyncSession,
    candidate: Callable[[], Awaitable[int | None]],
    eligible: ColumnElement[bool],
    values: Mapping[str, object],
) -> Page | None:
    for _ in range(16):
        page_id = await candidate()
        await session.commit()
        if page_id is None:
            return None

        async def claim(page_id: int = page_id) -> Page | None:
            return (
                await session.scalars(
                    update(Page)
                    .where(Page.id == page_id, eligible)
                    .values(**values)
                    .returning(Page)
                    .execution_options(populate_existing=True)
                )
            ).one_or_none()

        page = await write_transaction(session, claim)
        if page is not None:
            return page
    return None


async def claim_due_archive_history_check(
    session: AsyncSession,
    owner: str,
    now: datetime,
    lease_duration: timedelta,
) -> Page | None:
    """Select validated history work before acquiring the writer lock."""
    eligible = and_(
        _scoped_page(),
        Page.active.is_(True),
        Page.status_code.between(literal_column("200"), literal_column("299")),
        Page.error.is_(None),
        Page.archive_history_checked_at.is_(None),
        Page.next_archive_history_check_at.is_not(None),
        Page.next_archive_history_check_at <= now,
        _available(Page.archive_history_lease_expires_at, now),
        _available(Page.archive_lease_expires_at, now),
        ~exists(
            select(ArchiveJob.id).where(
                ArchiveJob.page_id == Page.id,
                ArchiveJob.state.in_(ACTIVE_ARCHIVE_STATES),
            )
        ),
    )

    async def candidate() -> int | None:
        return await session.scalar(
            select(Page.id)
            .where(eligible)
            .order_by(Page.next_archive_history_check_at, Page.id)
            .limit(1)
        )

    return await _claim_page(
        session,
        candidate,
        eligible,
        {
            "archive_history_lease_owner": owner,
            "archive_history_lease_expires_at": now + lease_duration,
        },
    )


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
        .where(Page.archive_history_lease_owner == owner)
        .values(
            archive_history_lease_owner=None,
            archive_history_lease_expires_at=None,
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
