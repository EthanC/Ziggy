from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import select, text
from sqlalchemy.dialects import sqlite
from sqlalchemy.exc import StatementError

from ziggy import database as database_module
from ziggy.config import DomainSettings
from ziggy.database import (
    claim_due_page,
    create_engine,
    database_url,
    insert_discovered_pages,
    insert_page_candidates,
    reconcile_domains,
    release_leases,
    run_migrations,
    session_factory,
    sync_engine,
)
from ziggy.models import (
    ArchiveJob,
    ArchiveJobKind,
    ArchiveJobState,
    Domain,
    Page,
    UtcDateTime,
)


@pytest.fixture
async def database(tmp_path):
    path = tmp_path / "ziggy.sqlite3"
    await run_migrations(path)
    engine = create_engine(path)
    sessions = session_factory(engine)
    try:
        yield engine, sessions
    finally:
        await engine.dispose()


async def _add_page(sessions, now, *, active=True, suffix=""):
    async with sessions() as session:
        domain = Domain(
            host=f"example{suffix}.com",
            scheme="https",
            include_subdomains=False,
            active=active,
            created_at=now,
            configured_at=now,
        )
        session.add(domain)
        await session.flush()
        page = Page(
            domain_id=domain.id,
            url=f"https://example{suffix}.com/",
            discovered_at=now,
            next_crawl_at=now,
            next_archive_at=now,
        )
        session.add(page)
        await session.commit()
        return page


def test_database_revision_handles_database_without_migration_table(tmp_path):
    path = tmp_path / "unmigrated.sqlite3"
    path.touch()

    assert database_module._database_revision(path) is None  # noqa: SLF001


async def test_insert_page_candidates_rejects_sensitive_and_caps_query_variants(
    database,
):
    _, sessions = database
    now = datetime(2026, 8, 28, 9, tzinfo=UTC)
    async with sessions() as session:
        domain = Domain(
            host="example.com",
            scheme="https",
            include_subdomains=False,
            created_at=now,
            configured_at=now,
        )
        session.add(domain)
        await session.flush()
        values = [
            {
                "domain_id": domain.id,
                "url": url,
                "discovered_at": now,
                "next_crawl_at": now,
                "next_archive_at": now,
            }
            for url in (
                "https://example.com/page?a=1",
                "https://example.com/page?a=2",
                "https://example.com/page?a=3",
                "https://example.com/page?loginToken=secret",
                "https://example.com/other?a=1",
            )
        ]

        inserted = await insert_page_candidates(session, values, 2)
        await session.commit()

        assert inserted == (
            "https://example.com/page?a=1",
            "https://example.com/page?a=2",
            "https://example.com/other?a=1",
        )
        pages = (await session.scalars(select(Page).order_by(Page.url))).all()
        assert [page.url for page in pages] == sorted(inserted)
        assert {(page.query_base_url, page.query_variant_slot) for page in pages} == {
            ("https://example.com/page", 1),
            ("https://example.com/page", 2),
            ("https://example.com/other", 1),
        }

        overflow = dict(values[2])
        overflow["url"] = "https://example.com/page?a=4"
        assert await insert_page_candidates(session, [overflow], 2) == ()
        sensitive = dict(values[0])
        sensitive["url"] = "https://example.com/?token=secret"
        assert await insert_page_candidates(session, [sensitive], 2) == ()
        assert await insert_page_candidates(session, [values[0]], 2) == ()

        pages[0].blocked_reason = "query_variant_cap"
        pages[0].in_scope = False
        await session.commit()
        assert await insert_page_candidates(session, [values[4]], 2) == ()
        assert pages[0].in_scope is False

    config = SimpleNamespace(
        domains=(DomainSettings("example.com"),),
        crawl=SimpleNamespace(max_query_variants_per_base=2),
    )
    async with sessions() as session:
        await reconcile_domains(session, config, now)
        blocked = await session.scalar(
            select(Page).where(Page.blocked_reason.is_not(None))
        )
        assert blocked is not None
        assert blocked.in_scope is False


async def test_insert_page_candidates_zero_cap_still_allows_queryless_pages(database):
    _, sessions = database
    now = datetime(2026, 8, 28, 9, tzinfo=UTC)
    async with sessions() as session:
        domain = Domain(
            host="example.com",
            scheme="https",
            include_subdomains=False,
            created_at=now,
            configured_at=now,
        )
        session.add(domain)
        await session.flush()
        candidates = [
            {
                "domain_id": domain.id,
                "url": url,
                "discovered_at": now,
                "next_crawl_at": now,
                "next_archive_at": now,
            }
            for url in ("https://example.com/page", "https://example.com/page?a=1")
        ]

        assert await insert_page_candidates(session, candidates, 0) == (
            "https://example.com/page",
        )


def test_utc_datetime_converts_bind_and_result_values_to_utc():
    value_type = UtcDateTime()
    dialect = sqlite.dialect()
    source = datetime(
        2026, 8, 28, 15, 4, 5, 123456, timezone(timedelta(hours=5, minutes=30))
    )

    bound = value_type.process_bind_param(source, dialect)
    restored = value_type.process_result_value(bound, dialect)

    assert bound == "2026-08-28T09:34:05.123456+00:00"
    assert restored == datetime(2026, 8, 28, 9, 34, 5, 123456, UTC)
    assert restored is not None
    assert restored.tzinfo is UTC
    assert value_type.process_bind_param(None, dialect) is None
    assert value_type.process_result_value(None, dialect) is None


def test_utc_datetime_rejects_naive_bind_values():
    with pytest.raises(ValueError, match="naive datetimes are not allowed"):
        UtcDateTime().process_bind_param(
            datetime(2026, 8, 28),  # noqa: DTZ001 - deliberately naive input
            sqlite.dialect(),
        )


async def test_utc_datetime_round_trips_through_sqlite_and_rejects_naive_values(
    database,
):
    engine, sessions = database
    source = datetime(2026, 8, 28, 15, 4, 5, 123456, timezone(timedelta(hours=-4)))
    expected = datetime(2026, 8, 28, 19, 4, 5, 123456, UTC)

    async with sessions() as session:
        domain = Domain(
            host="aware.example",
            scheme="https",
            include_subdomains=False,
            created_at=source,
            configured_at=source,
        )
        session.add(domain)
        await session.commit()
        assert domain.created_at == expected

    async with sessions() as session:
        loaded = await session.get(Domain, domain.id)
        assert loaded is not None
        assert loaded.created_at == expected
        assert loaded.created_at.tzinfo is UTC

    async with engine.connect() as connection:
        stored = await connection.scalar(
            text("SELECT created_at FROM domains WHERE host = 'aware.example'")
        )
    assert stored == "2026-08-28T19:04:05.123456+00:00"

    async with sessions() as session:
        session.add(
            Domain(
                host="naive.example",
                scheme="https",
                include_subdomains=False,
                created_at=datetime(2026, 8, 28),  # noqa: DTZ001
                configured_at=expected,
            )
        )
        with pytest.raises(StatementError, match="naive datetimes are not allowed"):
            await session.commit()


async def test_create_engine_creates_parent_and_sets_every_connection_pragma(tmp_path):
    path = tmp_path / "missing" / "nested" / "ziggy.sqlite3"
    assert database_url(path) == f"sqlite+aiosqlite:///{path.as_posix()}"

    engine = create_engine(path)
    try:
        assert path.parent.is_dir()
        assert sync_engine(engine) is engine.sync_engine
        async with engine.connect() as connection:
            pragmas = {
                name: await connection.scalar(text(f"PRAGMA {name}"))
                for name in (
                    "foreign_keys",
                    "journal_mode",
                    "busy_timeout",
                    "synchronous",
                )
            }
        assert pragmas == {
            "foreign_keys": 1,
            "journal_mode": "wal",
            "busy_timeout": 5000,
            "synchronous": 1,
        }
    finally:
        await engine.dispose()


async def test_reconcile_domains_adds_removes_readds_and_deduplicates_seeds(database):
    _, sessions = database
    first = datetime(2026, 8, 28, 9, tzinfo=UTC)
    second = first + timedelta(hours=1)
    third = second + timedelta(hours=1)
    initial = SimpleNamespace(
        domains=(
            DomainSettings("alpha.example", seeds=("/", "/about")),
            DomainSettings("beta.example", seeds=("/",)),
        )
    )
    replacement = SimpleNamespace(
        domains=(
            DomainSettings(
                "alpha.example",
                scheme="http",
                include_subdomains=True,
                seeds=("/", "/new"),
            ),
        )
    )
    restored = SimpleNamespace(
        domains=(
            replacement.domains[0],
            DomainSettings(
                "beta.example", include_subdomains=True, seeds=("/", "/again")
            ),
        )
    )

    async with sessions() as session:
        await reconcile_domains(session, initial, first)
        original_domains = {
            domain.host: domain for domain in await session.scalars(select(Domain))
        }
        beta_id = original_domains["beta.example"].id
        beta_created_at = original_domains["beta.example"].created_at

        await reconcile_domains(session, replacement, second)
        alpha = await session.scalar(
            select(Domain).where(Domain.host == "alpha.example")
        )
        beta = await session.scalar(select(Domain).where(Domain.host == "beta.example"))
        assert alpha is not None
        assert beta is not None
        assert (alpha.scheme, alpha.include_subdomains) == ("http", True)
        assert alpha.configured_at == second
        assert beta.active is False
        assert beta.deactivated_at == second

        await reconcile_domains(session, restored, third)
        await reconcile_domains(session, restored, third)
        beta = await session.scalar(select(Domain).where(Domain.host == "beta.example"))
        assert beta is not None
        assert beta.id == beta_id
        assert beta.created_at == beta_created_at
        assert beta.configured_at == third
        assert beta.include_subdomains is True
        assert beta.active is True
        assert beta.deactivated_at is None

        urls = set(await session.scalars(select(Page.url)))
        assert urls == {
            "https://alpha.example/",
            "https://alpha.example/about",
            "http://alpha.example/",
            "http://alpha.example/new",
            "https://beta.example/",
            "https://beta.example/again",
        }


async def test_reconcile_domains_transfers_and_deactivates_retained_pages(database):
    _, sessions = database
    now = datetime(2026, 8, 28, 9, tzinfo=UTC)
    broad = SimpleNamespace(
        domains=(DomainSettings("example.com", include_subdomains=True),)
    )
    partitioned = SimpleNamespace(
        domains=(DomainSettings("example.com"), DomainSettings("child.example.com"))
    )
    narrowed = SimpleNamespace(domains=(DomainSettings("example.com"),))

    async with sessions() as session:
        await reconcile_domains(session, broad, now)
        parent = await session.scalar(
            select(Domain).where(Domain.host == "example.com")
        )
        assert parent is not None
        await insert_discovered_pages(
            session,
            parent.id,
            ("https://child.example.com/page",),
            now,
            None,
        )
        await session.commit()
        child_page = await session.scalar(
            select(Page).where(Page.url == "https://child.example.com/page")
        )
        assert child_page is not None
        child_page.status_code = 204
        page_id = child_page.id
        await session.commit()

        await reconcile_domains(session, partitioned, now + timedelta(hours=1))
        child_domain = await session.scalar(
            select(Domain).where(Domain.host == "child.example.com")
        )
        child_page = await session.get(Page, page_id)
        assert child_domain is not None
        assert child_page is not None
        assert child_page.domain_id == child_domain.id
        assert child_page.in_scope is True
        assert child_page.status_code == 204

        await reconcile_domains(session, narrowed, now + timedelta(hours=2))
        child_page = await session.get(Page, page_id)
        assert child_page is not None
        assert child_page.in_scope is False
        assert child_domain.active is False

        await reconcile_domains(session, broad, now + timedelta(hours=3))
        child_page = await session.get(Page, page_id)
        assert child_page is not None
        assert child_page.domain_id == parent.id
        assert child_page.in_scope is True


@pytest.mark.parametrize("kind", ["crawl", "archive"])
async def test_claim_due_page_skips_out_of_scope_pages(database, kind):
    _, sessions = database
    now = datetime(2026, 8, 28, 9, tzinfo=UTC)
    page = await _add_page(sessions, now)

    async with sessions() as session:
        persisted = await session.get(Page, page.id)
        assert persisted is not None
        persisted.in_scope = False
        await session.commit()

        assert (
            await claim_due_page(session, kind, "worker", now, timedelta(minutes=10))
            is None
        )


async def test_insert_discovered_pages_ignores_existing_and_repeated_urls(database):
    _, sessions = database
    now = datetime(2026, 8, 28, 9, tzinfo=UTC)
    original = await _add_page(sessions, now)
    later = now + timedelta(minutes=5)
    urls = (
        original.url,
        "https://example.com/new",
        "https://example.com/new",
        "https://example.com/other",
    )

    async with sessions() as session:
        await insert_discovered_pages(
            session, original.domain_id, urls, later, original.id
        )
        await insert_discovered_pages(
            session, original.domain_id, urls, later, original.id
        )
        await insert_discovered_pages(
            session, original.domain_id, (), later, original.id
        )
        await session.commit()

        pages = (await session.scalars(select(Page).order_by(Page.url))).all()
        assert [page.url for page in pages] == [
            "https://example.com/",
            "https://example.com/new",
            "https://example.com/other",
        ]
        persisted_original = next(page for page in pages if page.id == original.id)
        assert persisted_original.discovered_at == now
        assert persisted_original.discovered_from_id is None
        assert all(
            page.discovered_from_id == original.id
            for page in pages
            if page.id != original.id
        )


@pytest.mark.parametrize("kind", ["crawl", "archive"])
async def test_claim_due_page_is_atomic_and_respects_lease_expiry(database, kind):
    _, sessions = database
    now = datetime(2026, 8, 28, 9, tzinfo=UTC)
    page = await _add_page(sessions, now)
    duration = timedelta(minutes=10)

    async def claim(owner, claim_time):
        async with sessions() as session:
            return await claim_due_page(session, kind, owner, claim_time, duration)

    first_attempts = await asyncio.gather(
        claim("worker-a", now), claim("worker-b", now)
    )
    claims = [result for result in first_attempts if result is not None]
    assert len(claims) == 1
    assert claims[0].id == page.id
    winner = (
        claims[0].crawl_lease_owner
        if kind == "crawl"
        else claims[0].archive_lease_owner
    )
    assert winner in {"worker-a", "worker-b"}

    assert await claim("worker-c", now + duration - timedelta(microseconds=1)) is None
    reclaimed = await claim("worker-c", now + duration)
    assert reclaimed is not None
    assert reclaimed.id == page.id
    assert (
        reclaimed.crawl_lease_owner
        if kind == "crawl"
        else reclaimed.archive_lease_owner
    ) == "worker-c"


@pytest.mark.parametrize("kind", ["crawl", "archive"])
async def test_claim_due_page_skips_pages_from_inactive_domains(database, kind):
    _, sessions = database
    now = datetime(2026, 8, 28, 9, tzinfo=UTC)
    await _add_page(sessions, now, active=False)

    async with sessions() as session:
        assert (
            await claim_due_page(session, kind, "worker", now, timedelta(minutes=10))
            is None
        )


@pytest.mark.parametrize("kind", ["crawl", "archive"])
async def test_claim_due_page_skips_inactive_pages(database, kind):
    _, sessions = database
    now = datetime(2026, 8, 28, 9, tzinfo=UTC)
    page = await _add_page(sessions, now)

    async with sessions() as session:
        persisted = await session.get(Page, page.id)
        persisted.active = False
        await session.commit()

        assert (
            await claim_due_page(session, kind, "worker", now, timedelta(minutes=10))
            is None
        )


async def test_archive_claim_is_blocked_by_every_active_direct_job_state(database):
    _, sessions = database
    now = datetime(2026, 8, 28, 9, tzinfo=UTC)
    page = await _add_page(sessions, now)

    async with sessions() as session:
        job = ArchiveJob(
            id="job-1",
            page_id=page.id,
            kind=ArchiveJobKind.DIRECT,
            state=ArchiveJobState.INTENT,
            cycle_key="cycle-1",
            intent_at=now,
            next_attempt_at=now,
        )
        session.add(job)
        await session.commit()

        for state in (
            ArchiveJobState.INTENT,
            ArchiveJobState.UNCERTAIN,
            ArchiveJobState.SUBMITTED,
            ArchiveJobState.PENDING,
            ArchiveJobState.RATE_LIMITED,
        ):
            job.state = state
            await session.commit()
            assert (
                await claim_due_page(
                    session, "archive", "worker", now, timedelta(minutes=10)
                )
                is None
            )

        job.state = ArchiveJobState.SUCCEEDED
        await session.commit()
        claimed = await claim_due_page(
            session, "archive", "worker", now, timedelta(minutes=10)
        )
        assert claimed is not None
        assert claimed.id == page.id


async def test_release_leases_only_releases_leases_owned_by_the_instance(database):
    _, sessions = database
    now = datetime(2026, 8, 28, 9, tzinfo=UTC)
    expires = now + timedelta(minutes=10)

    async with sessions() as session:
        domain = Domain(
            host="leases.example",
            scheme="https",
            include_subdomains=False,
            created_at=now,
            configured_at=now,
        )
        session.add(domain)
        await session.flush()
        crawl_owned = Page(
            domain_id=domain.id,
            url="https://leases.example/crawl",
            discovered_at=now,
            next_crawl_at=now,
            next_archive_at=now,
            crawl_lease_owner="target",
            crawl_lease_expires_at=expires,
            archive_lease_owner="other",
            archive_lease_expires_at=expires,
        )
        archive_owned = Page(
            domain_id=domain.id,
            url="https://leases.example/archive",
            discovered_at=now,
            next_crawl_at=now,
            next_archive_at=now,
            crawl_lease_owner="other",
            crawl_lease_expires_at=expires,
            archive_lease_owner="target",
            archive_lease_expires_at=expires,
        )
        session.add_all((crawl_owned, archive_owned))
        await session.flush()
        target_job = ArchiveJob(
            id="target-job",
            page_id=crawl_owned.id,
            kind=ArchiveJobKind.OUTLINK,
            state=ArchiveJobState.PENDING,
            cycle_key="target-cycle",
            intent_at=now,
            next_attempt_at=now,
            lease_owner="target",
            lease_expires_at=expires,
        )
        other_job = ArchiveJob(
            id="other-job",
            page_id=archive_owned.id,
            kind=ArchiveJobKind.OUTLINK,
            state=ArchiveJobState.PENDING,
            cycle_key="other-cycle",
            intent_at=now,
            next_attempt_at=now,
            lease_owner="other",
            lease_expires_at=expires,
        )
        session.add_all((target_job, other_job))
        await session.commit()

        await release_leases(session, "target")
        await session.refresh(crawl_owned)
        await session.refresh(archive_owned)
        await session.refresh(target_job)
        await session.refresh(other_job)

        assert (crawl_owned.crawl_lease_owner, crawl_owned.crawl_lease_expires_at) == (
            None,
            None,
        )
        assert (
            crawl_owned.archive_lease_owner,
            crawl_owned.archive_lease_expires_at,
        ) == (
            "other",
            expires,
        )
        assert (
            archive_owned.archive_lease_owner,
            archive_owned.archive_lease_expires_at,
        ) == (
            None,
            None,
        )
        assert (
            archive_owned.crawl_lease_owner,
            archive_owned.crawl_lease_expires_at,
        ) == (
            "other",
            expires,
        )
        assert (target_job.lease_owner, target_job.lease_expires_at) == (None, None)
        assert (other_job.lease_owner, other_job.lease_expires_at) == (
            "other",
            expires,
        )
