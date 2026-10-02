from __future__ import annotations

import asyncio
import os
import sqlite3
import subprocess
import sys
from contextlib import closing
from datetime import UTC, datetime, timedelta
from time import perf_counter
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import event, insert, select, update

from ziggy import crawler
from ziggy.archive import check_archive_history, claim_archive_job
from ziggy.config import CrawlSettings
from ziggy.crawler import FetchResult, crawl_page
from ziggy.database import (
    claim_due_archive_history_check,
    claim_due_page,
    create_engine,
    run_migrations,
    session_factory,
)
from ziggy.models import Domain, Page, ServiceState
from ziggy.transactions import write_transaction


def test_cold_healthcheck_is_lightweight_and_read_only(tmp_path):
    path = tmp_path / "health #1.sqlite3"
    now = datetime.now(UTC).isoformat()
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("CREATE TABLE service_state (heartbeat_at TEXT)")
        connection.execute("INSERT INTO service_state VALUES (?)", (now,))
        connection.commit()
    config = tmp_path / "ziggy.toml"
    config.write_text(f'[ziggy]\ndatabase = "{path.name}"\n', encoding="utf-8")
    code = (
        "import sys; from ziggy.cli import main; "
        "result = main(sys.argv[1:]); "
        "assert not any(name in sys.modules for name in "
        "('ziggy.service', 'ziggy.crawler', 'ziggy.archive', "
        "'sqlalchemy', 'archivist')); "
        "raise SystemExit(result)"
    )
    started = perf_counter()
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-c", code, "healthcheck", "--config", str(config)],
        capture_output=True,
        text=True,
        timeout=5,
        env={**os.environ, "ZIGGY_HTTP_ENABLED": "false"},
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert perf_counter() - started < 4
    assert not path.with_name(f"{path.name}-journal").exists()


@pytest.fixture(scope="module")
def large_database(tmp_path_factory):
    path = tmp_path_factory.mktemp("scheduling") / "large.sqlite3"
    asyncio.run(run_migrations(path))
    now = datetime(2026, 10, 2, tzinfo=UTC).isoformat(timespec="microseconds")
    with closing(sqlite3.connect(path)) as connection:
        connection.execute(
            "INSERT INTO domains VALUES (1, 'example.com', 'https', 0, 1, ?, ?, NULL)",
            (now, now),
        )
        connection.execute(
            "WITH RECURSIVE numbers(n) AS (VALUES(1) UNION ALL "
            "SELECT n+1 FROM numbers WHERE n < 1100000) "
            "INSERT INTO pages (id, domain_id, url, is_seed, active, in_scope, "
            "discovered_at, next_crawl_at, next_archive_at, sitemap_depth, "
            "crawl_attempts, archive_history_check_attempts, status_code, "
            "next_archive_history_check_at) "
            "SELECT n, CASE WHEN n <= 1000000 THEN 1 END, "
            "'https://example.com/' || n, n = 1, 1, n <= 1000000, "
            ":now, :now, :now, 0, 0, 0, 200, :now FROM numbers",
            {"now": now},
        )
        connection.execute(
            "INSERT INTO archive_jobs (id, page_id, kind, state, cycle_key, "
            "intent_at, next_attempt_at, attempts, saved_to_my_archive, "
            "outlinks_processed, archive_only) SELECT 'job-' || id, id, 'DIRECT', "
            "'SUCCEEDED', 'cycle-' || id, :now, :now, 0, 1, 1, 1 "
            "FROM pages WHERE domain_id IS NULL",
            {"now": now},
        )
        connection.execute(
            "INSERT INTO archive_submissions (id, page_id, identifier, priority, "
            "accepted_at, archive_job_id) SELECT 'receipt-' || id, id, 'fixture', "
            "0, :now, 'job-' || id FROM pages WHERE domain_id IS NULL",
            {"now": now},
        )
        connection.commit()
    return path


@pytest.mark.parametrize("analyze", [False, True])
@pytest.mark.parametrize("queue_state", ["due", "idle"])
async def test_large_scheduling_selection_does_not_hold_writer_lock(  # noqa: C901, PLR0915
    large_database,
    analyze,
    queue_state,
):
    engine = create_engine(large_database)
    sessions = session_factory(engine)
    now = datetime(2026, 10, 2, tzinfo=UTC)
    statements = []
    timings = []
    writer_started = None
    writer_durations = []

    def before(connection, cursor, statement, parameters, context, executemany):  # noqa: PLR0913, PLR0917
        statements.append((statement, parameters))
        context.started = perf_counter()

    def after(connection, cursor, statement, parameters, context, executemany):  # noqa: PLR0913, PLR0917
        nonlocal writer_started
        timings.append((statement, perf_counter() - context.started))
        if statement == "BEGIN IMMEDIATE":
            writer_started = perf_counter()

    def committed(session):
        nonlocal writer_started
        if writer_started is not None:
            writer_durations.append(perf_counter() - writer_started)
            writer_started = None

    try:
        async with engine.begin() as connection:
            if queue_state == "idle":
                await connection.execute(
                    update(Page)
                    .where(Page.domain_id.is_not(None), Page.next_archive_at <= now)
                    .values(
                        next_crawl_at=now + timedelta(days=1),
                        next_archive_at=now + timedelta(days=1),
                        next_archive_history_check_at=now + timedelta(days=1),
                    )
                )
            if analyze:
                await connection.exec_driver_sql("ANALYZE")
            elif (
                await connection.exec_driver_sql(
                    "SELECT 1 FROM sqlite_master WHERE name='sqlite_stat1'"
                )
            ).scalar():
                await connection.exec_driver_sql("DELETE FROM sqlite_stat1")
                await connection.exec_driver_sql("ANALYZE sqlite_schema")
        event.listen(engine.sync_engine, "before_cursor_execute", before)
        event.listen(engine.sync_engine, "after_cursor_execute", after)
        async with sessions() as session:
            event.listen(session.sync_session, "after_commit", committed)
            for kind in ("crawl", "archive"):
                claimed = await claim_due_page(
                    session,
                    kind,
                    "benchmark",
                    now,
                    timedelta(minutes=5),
                    archive_interval=timedelta(days=30),
                )
                assert (claimed is not None) == (queue_state == "due")
            claimed = await claim_due_archive_history_check(
                session, "history", now, timedelta(minutes=5)
            )
            assert (claimed is not None) == (queue_state == "due")
            assert (
                await claim_archive_job(session, "poller", now, timedelta(minutes=5))
                is None
            )
        event.remove(engine.sync_engine, "before_cursor_execute", before)
        event.remove(engine.sync_engine, "after_cursor_execute", after)
        measured = [(sql.split()[0], round(elapsed, 4)) for sql, elapsed in timings]
        print(f"queue={queue_state}, analyze={analyze}: {measured}")  # noqa: T201
        print(f"writer transactions: {writer_durations}")  # noqa: T201
        writes = [sql for sql, _ in statements if sql.startswith("UPDATE")]
        assert all("LIMIT" not in sql for sql in writes), writes
        assert sum(sql.startswith("SELECT") for sql, _ in statements) >= 3
        async with engine.connect() as connection:
            for sql, parameters in statements:
                if sql.startswith("SELECT"):
                    plan = await connection.exec_driver_sql(
                        f"EXPLAIN QUERY PLAN {sql}", parameters
                    )
                    details = [row[3] for row in plan]
                    assert "SCAN pages" not in details, details
                    if "ORDER BY pages.next_archive_history_check_at" in sql:
                        assert any(
                            "ix_pages_schedule_history" in detail for detail in details
                        ), details
                        assert not any("TEMP B-TREE" in detail for detail in details), (
                            details
                        )
                    if "ix_archive_jobs_work" in sql:
                        assert any(
                            "ix_archive_jobs_work" in detail for detail in details
                        ), details
        for sql, elapsed in timings:
            if sql.startswith("UPDATE"):
                assert elapsed < 0.5, (sql, elapsed)
        assert sum(elapsed for _, elapsed in timings) < 0.5
        assert all(elapsed < 0.5 for elapsed in writer_durations)
    finally:
        await engine.dispose()


async def test_discovery_chunks_resume_after_interruption(tmp_path, monkeypatch):
    path = tmp_path / "chunks.sqlite3"
    await run_migrations(path)
    engine = create_engine(path)
    sessions = session_factory(engine)
    now = datetime.now(UTC)
    monkeypatch.setattr(crawler, "_DISCOVERY_BATCH_SIZE", 2)
    body = "".join(f'<a href="/{i}">child</a>' for i in range(5)).encode()
    client = SimpleNamespace(
        fetch=AsyncMock(
            return_value=FetchResult(
                200,
                "https://example.com/",
                {"Content-Type": "text/html"},
                body,
                None,
                (),
            )
        )
    )
    batches = 0

    def interrupt(connection, cursor, statement, parameters, *args):
        nonlocal batches
        if statement.startswith("INSERT INTO pages"):
            batches += 1
            if batches == 2:
                raise asyncio.CancelledError

    try:
        async with sessions() as session:
            session.add(
                Domain(
                    id=1, host="example.com", scheme="https", include_subdomains=False
                )
            )
            await session.flush()
            page = Page(
                domain_id=1,
                url="https://example.com/",
                is_seed=True,
                next_crawl_at=now,
                crawl_lease_owner="worker",
                crawl_lease_expires_at=now + timedelta(minutes=1),
            )
            session.add(page)
            await session.commit()
            page_id = page.id
            event.listen(engine.sync_engine, "before_cursor_execute", interrupt)
            try:
                with pytest.raises(asyncio.CancelledError):
                    await crawl_page(
                        session,
                        page,
                        configured_host="example.com",
                        include_subdomains=False,
                        client=client,
                        settings=CrawlSettings(),
                        now=now,
                    )
            finally:
                event.remove(engine.sync_engine, "before_cursor_execute", interrupt)
        async with sessions() as session:
            assert len((await session.scalars(select(Page))).all()) == 3
            page = await claim_due_page(
                session,
                "crawl",
                "restart",
                now + timedelta(minutes=1),
                timedelta(minutes=5),
            )
            assert page.id == page_id
            assert page.last_crawled_at is None
            await crawl_page(
                session,
                page,
                configured_host="example.com",
                include_subdomains=False,
                client=client,
                settings=CrawlSettings(),
                now=now + timedelta(minutes=1),
            )
            assert len((await session.scalars(select(Page))).all()) == 6
            assert page.crawl_lease_owner is None
    finally:
        await engine.dispose()


async def test_eight_crawlers_history_and_heartbeat_progress_together(tmp_path):
    path = tmp_path / "concurrent.sqlite3"
    await run_migrations(path)
    engine = create_engine(path, busy_timeout_ms=25)
    sessions = session_factory(engine)
    now = datetime.now(UTC)
    arrivals = 0
    ready = asyncio.Event()

    class Client:
        async def fetch(self, url, *args, **kwargs):
            nonlocal arrivals
            arrivals += 1
            if arrivals == 8:
                ready.set()
            await ready.wait()
            return FetchResult(200, url, {}, b"", None, ())

        async def latest_capture_at(self, url):
            await ready.wait()

    try:
        async with sessions() as session:
            session.add(
                Domain(
                    id=1, host="example.com", scheme="https", include_subdomains=False
                )
            )
            session.add(
                ServiceState(instance_id="live", started_at=now, heartbeat_at=now)
            )
            await session.flush()
            await session.execute(
                insert(Page),
                [
                    {
                        "domain_id": 1,
                        "url": f"https://example.com/{i}",
                        "status_code": 200,
                    }
                    for i in range(9)
                ],
            )
            await session.commit()

        async def crawl(page_id):
            async with sessions() as session:
                page = await session.get(Page, page_id)
                await crawl_page(
                    session,
                    page,
                    configured_host="example.com",
                    include_subdomains=False,
                    client=Client(),
                    settings=CrawlSettings(concurrency=8),
                    now=now,
                )

        async def history():
            async with sessions() as session:
                page = await session.get(Page, 9)
                await check_archive_history(session, page, Client(), now)

        async def heartbeat():
            await ready.wait()
            async with sessions() as session:

                async def beat():
                    await session.execute(
                        update(ServiceState).values(
                            heartbeat_at=now + timedelta(seconds=1)
                        )
                    )

                await write_transaction(session, beat)

        await asyncio.wait_for(
            asyncio.gather(*(crawl(i) for i in range(1, 9)), history(), heartbeat()), 10
        )
        async with sessions() as session:
            pages = (await session.scalars(select(Page).order_by(Page.id))).all()
            assert all(page.last_crawled_at == now for page in pages[:8])
            assert pages[8].archive_history_checked_at == now
    finally:
        await engine.dispose()
