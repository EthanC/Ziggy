from __future__ import annotations

import asyncio
import logging.config
import runpy
from datetime import UTC, datetime
from importlib import resources
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from alembic import command
from alembic import context as alembic_context
from alembic.autogenerate import compare_metadata
from alembic.config import Config as AlembicConfig
from alembic.migration import MigrationContext
from sqlalchemy import inspect, select, text
from sqlalchemy.exc import IntegrityError

from ziggy.database import create_engine, run_migrations, session_factory
from ziggy.models import (
    ArchiveJob,
    ArchiveJobKind,
    ArchiveJobState,
    Base,
    Domain,
    Page,
)

ROOT = Path(__file__).parents[1]
HEAD_REVISION = "2f4a8c1d9e70"
APPLICATION_TABLES = set(Base.metadata.tables)


def test_migration_environment_runs_offline_without_config_file(monkeypatch):
    config = MagicMock(config_file_name=None)
    config.get_main_option.return_value = "sqlite:///offline.sqlite3"
    configure = MagicMock()
    transaction = MagicMock()
    run_migrations_offline = MagicMock()
    file_config = MagicMock()
    monkeypatch.setattr(alembic_context, "config", config, raising=False)
    monkeypatch.setattr(alembic_context, "is_offline_mode", lambda: True)
    monkeypatch.setattr(alembic_context, "configure", configure)
    monkeypatch.setattr(
        alembic_context, "begin_transaction", MagicMock(return_value=transaction)
    )
    monkeypatch.setattr(alembic_context, "run_migrations", run_migrations_offline)
    monkeypatch.setattr(logging.config, "fileConfig", file_config)

    with resources.as_file(
        resources.files("ziggy.migrations").joinpath("env.py")
    ) as env:
        runpy.run_path(env)

    file_config.assert_not_called()
    configure.assert_called_once_with(
        url="sqlite:///offline.sqlite3",
        target_metadata=Base.metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        render_as_batch=True,
    )
    transaction.__enter__.assert_called_once_with()
    transaction.__exit__.assert_called_once()
    run_migrations_offline.assert_called_once_with()


def _downgrade_to_base(path):
    config = AlembicConfig(ROOT / "alembic.ini")
    config.set_main_option("sqlalchemy.url", f"sqlite:///{path.as_posix()}")
    command.downgrade(config, "base")


def _upgrade_to(path, revision):
    config = AlembicConfig(ROOT / "alembic.ini")
    config.set_main_option("sqlalchemy.url", f"sqlite:///{path.as_posix()}")
    command.upgrade(config, revision)


async def _inspect_database(path):
    engine = create_engine(path)
    try:
        async with engine.connect() as connection:
            tables = set(
                await connection.run_sync(
                    lambda sync_connection: inspect(sync_connection).get_table_names()
                )
            )
            revision = None
            if "alembic_version" in tables:
                revision = await connection.scalar(
                    text("SELECT version_num FROM alembic_version")
                )
            return tables, revision
    finally:
        await engine.dispose()


async def test_fresh_migration_is_at_head_complete_and_matches_model_metadata(tmp_path):
    path = tmp_path / "fresh.sqlite3"

    await run_migrations(path)
    await run_migrations(path)
    tables, revision = await _inspect_database(path)

    assert tables == APPLICATION_TABLES | {"alembic_version"}
    assert revision == HEAD_REVISION

    engine = create_engine(path)
    try:
        async with engine.connect() as connection:
            differences = await connection.run_sync(
                lambda sync_connection: compare_metadata(
                    MigrationContext.configure(
                        sync_connection,
                        opts={"compare_type": True},
                    ),
                    Base.metadata,
                )
            )
        assert differences == []
    finally:
        await engine.dispose()


def test_migration_resources_are_packaged_with_ziggy():
    migrations = resources.files("ziggy.migrations")

    assert migrations.joinpath("env.py").is_file()
    assert migrations.joinpath("script.py.mako").is_file()
    assert migrations.joinpath("versions", "6b519c405276_initial_schema.py").is_file()
    assert migrations.joinpath(
        "versions", "f5c2b31a8d4e_add_page_scope_state.py"
    ).is_file()
    assert migrations.joinpath(
        "versions", "9d14f3a7c2e1_allow_reused_archive_job_ids.py"
    ).is_file()
    assert migrations.joinpath(
        "versions", "c83d91e4a672_track_first_archives.py"
    ).is_file()
    assert migrations.joinpath(
        "versions", "2f4a8c1d9e70_secure_query_frontier.py"
    ).is_file()


async def test_scope_migration_preserves_existing_pages_and_defaults_them_in_scope(
    tmp_path,
):
    path = tmp_path / "upgrade.sqlite3"
    await asyncio.to_thread(_upgrade_to, path, "6b519c405276")
    engine = create_engine(path)
    timestamp = "2026-08-28T09:00:00.000000+00:00"
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO domains "
                    "(host, scheme, include_subdomains, active, created_at, "
                    "configured_at) "
                    "VALUES ('example.com', 'https', 0, 1, :now, :now)"
                ),
                {"now": timestamp},
            )
            await connection.execute(
                text(
                    "INSERT INTO pages "
                    "(domain_id, url, discovered_at, next_crawl_at, next_archive_at, "
                    "sitemap_depth, crawl_attempts) "
                    "VALUES (1, 'https://example.com/', :now, :now, :now, 0, 0)"
                ),
                {"now": timestamp},
            )
    finally:
        await engine.dispose()

    await run_migrations(path)
    engine = create_engine(path)
    try:
        async with engine.connect() as connection:
            assert await connection.scalar(text("SELECT in_scope FROM pages")) == 1
    finally:
        await engine.dispose()


async def test_remote_job_id_migration_preserves_and_allows_reused_ids(tmp_path):
    path = tmp_path / "reused-job-id.sqlite3"
    await asyncio.to_thread(_upgrade_to, path, "f5c2b31a8d4e")
    engine = create_engine(path)
    timestamp = "2026-08-28T09:00:00.000000+00:00"
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO domains "
                    "(host, scheme, include_subdomains, active, created_at, "
                    "configured_at) VALUES ('example.com', 'https', 0, 1, :now, :now)"
                ),
                {"now": timestamp},
            )
            await connection.execute(
                text(
                    "INSERT INTO pages "
                    "(domain_id, url, in_scope, discovered_at, next_crawl_at, "
                    "next_archive_at, sitemap_depth, crawl_attempts) "
                    "VALUES (1, 'https://example.com/', 1, :now, :now, :now, 0, 0)"
                ),
                {"now": timestamp},
            )
            await connection.execute(
                text(
                    "INSERT INTO archive_jobs "
                    "(id, page_id, kind, state, cycle_key, external_job_id, "
                    "intent_at, next_attempt_at, attempts, saved_to_my_archive, "
                    "outlinks_processed) VALUES "
                    "('first', 1, 'DIRECT', 'SUCCEEDED', 'first-cycle', "
                    "'shared-job', :now, :now, 0, 1, 1)"
                ),
                {"now": timestamp},
            )
            await connection.execute(
                text(
                    "INSERT INTO captures "
                    "(page_id, archive_job_id, captured_at, wayback_url, completed_at) "
                    "VALUES (1, 'first', :now, 'https://web.archive.org/old', :now)"
                ),
                {"now": timestamp},
            )
            await connection.execute(
                text(
                    "INSERT INTO reports "
                    "(window_start, window_end, generated_at, discovered_count, "
                    "archived_count, outstanding_count, active_domain_count, state, "
                    "attempts, next_attempt_at) VALUES "
                    "(:now, :now, :now, 1, 1, 0, 1, 'PENDING', 0, :now)"
                ),
                {"now": timestamp},
            )
    finally:
        await engine.dispose()

    await run_migrations(path)
    engine = create_engine(path)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO archive_jobs "
                    "(id, page_id, kind, state, cycle_key, external_job_id, "
                    "intent_at, next_attempt_at, attempts, saved_to_my_archive, "
                    "outlinks_processed) VALUES "
                    "('second', 1, 'DIRECT', 'SUCCEEDED', 'second-cycle', "
                    "'shared-job', :now, :now, 0, 1, 1)"
                ),
                {"now": timestamp},
            )
            assert (
                await connection.scalar(
                    text(
                        "SELECT COUNT(*) FROM archive_jobs "
                        "WHERE external_job_id = 'shared-job'"
                    )
                )
                == 2
            )
            assert (
                await connection.scalar(text("SELECT first_archive FROM captures"))
                is None
            )
            assert (
                await connection.scalar(text("SELECT first_archive_count FROM reports"))
                == 0
            )
    finally:
        await engine.dispose()


async def test_query_frontier_migration_cleans_and_quarantines_existing_rows(
    tmp_path,
):
    path = tmp_path / "query-cleanup.sqlite3"
    await asyncio.to_thread(_upgrade_to, path, "c83d91e4a672")
    engine = create_engine(path)
    timestamp = "2026-08-28T09:00:00.000000+00:00"
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO domains "
                    "(id, host, scheme, include_subdomains, active, created_at, "
                    "configured_at) VALUES "
                    "(1, 'example.com', 'https', 0, 1, :now, :now)"
                ),
                {"now": timestamp},
            )
            page_rows = [
                (1, "https://example.com/", None, timestamp),
                (2, "https://example.com/page?a=1", None, timestamp),
                (3, "https://example.com/page?a=2", None, "2026-08-28T09:01:00+00:00"),
                (4, "https://example.com/page?a=3", None, "2026-08-28T09:02:00+00:00"),
                (5, "https://example.com/page?a=4", None, "2026-08-28T09:03:00+00:00"),
                (6, "https://example.com/login?loginToken=secret", None, timestamp),
                (7, "https://example.com/child", 5, timestamp),
            ]
            for page_id, url, source_id, discovered_at in page_rows:
                await connection.execute(
                    text(
                        "INSERT INTO pages "
                        "(id, domain_id, url, in_scope, discovered_at, "
                        "discovered_from_id, next_crawl_at, next_archive_at, "
                        "sitemap_depth, crawl_attempts) VALUES "
                        "(:id, 1, :url, 1, :discovered, :source, :now, :now, 0, 0)"
                    ),
                    {
                        "id": page_id,
                        "url": url,
                        "source": source_id,
                        "discovered": discovered_at,
                        "now": timestamp,
                    },
                )
            for page_id in (2, 3, 4, 5, 6):
                await connection.execute(
                    text(
                        "INSERT INTO archive_jobs "
                        "(id, page_id, kind, state, cycle_key, intent_at, "
                        "next_attempt_at, attempts, saved_to_my_archive, "
                        "outlinks_processed) VALUES "
                        "(:job, :page, 'DIRECT', 'SUCCEEDED', :cycle, :now, "
                        ":now, 0, 1, 1)"
                    ),
                    {
                        "job": f"job-{page_id}",
                        "page": page_id,
                        "cycle": f"cycle-{page_id}",
                        "now": timestamp,
                    },
                )
            for page_id in (2, 3, 4, 6):
                await connection.execute(
                    text(
                        "INSERT INTO captures "
                        "(page_id, archive_job_id, captured_at, wayback_url, "
                        "completed_at, first_archive) VALUES "
                        "(:page, :job, :now, :wayback, :now, 1)"
                    ),
                    {
                        "page": page_id,
                        "job": f"job-{page_id}",
                        "now": timestamp,
                        "wayback": f"https://web.archive.invalid/{page_id}",
                    },
                )
    finally:
        await engine.dispose()

    await run_migrations(path, query_variant_cap=2)
    engine = create_engine(path)
    try:
        async with engine.begin() as connection:
            pages = (
                (
                    await connection.execute(
                        text(
                            "SELECT id, query_base_url, query_variant_slot, "
                            "blocked_reason, in_scope, discovered_from_id "
                            "FROM pages ORDER BY id"
                        )
                    )
                )
                .mappings()
                .all()
            )
            assert [page["id"] for page in pages] == [1, 2, 3, 4, 7]
            assert (pages[1]["query_base_url"], pages[1]["query_variant_slot"]) == (
                "https://example.com/page",
                1,
            )
            assert (pages[2]["query_base_url"], pages[2]["query_variant_slot"]) == (
                "https://example.com/page",
                2,
            )
            assert (pages[3]["blocked_reason"], pages[3]["in_scope"]) == (
                "query_variant_cap",
                0,
            )
            assert pages[4]["discovered_from_id"] is None
            assert await connection.scalar(text("SELECT count(*) FROM captures")) == 3
            assert (
                await connection.scalar(text("SELECT count(*) FROM archive_jobs")) == 3
            )
            assert (
                await connection.execute(text("PRAGMA foreign_key_check"))
            ).all() == []
            with pytest.raises(IntegrityError):
                await connection.execute(
                    text(
                        "INSERT INTO pages "
                        "(domain_id, url, in_scope, discovered_at, next_crawl_at, "
                        "next_archive_at, sitemap_depth, crawl_attempts, "
                        "query_base_url, query_variant_slot) VALUES "
                        "(1, 'https://example.com/page?a=9', 1, :now, :now, :now, "
                        "0, 0, 'https://example.com/page', 1)"
                    ),
                    {"now": timestamp},
                )
    finally:
        await engine.dispose()


async def test_migration_can_create_a_fresh_database_below_missing_directories(
    tmp_path,
):
    path = tmp_path / "missing" / "nested" / "ziggy.sqlite3"

    await run_migrations(path)

    tables, revision = await _inspect_database(path)
    assert tables == APPLICATION_TABLES | {"alembic_version"}
    assert revision == HEAD_REVISION


async def test_migration_downgrades_to_empty_base_and_upgrades_again(tmp_path):
    path = tmp_path / "round-trip.sqlite3"
    await run_migrations(path)

    await asyncio.to_thread(_downgrade_to_base, path)
    downgraded_tables, downgraded_revision = await _inspect_database(path)
    assert downgraded_tables == {"alembic_version"}
    assert downgraded_revision is None

    await run_migrations(path)
    upgraded_tables, upgraded_revision = await _inspect_database(path)
    assert upgraded_tables == APPLICATION_TABLES | {"alembic_version"}
    assert upgraded_revision == HEAD_REVISION


async def test_migrated_partial_index_rejects_duplicate_active_direct_jobs(tmp_path):
    path = tmp_path / "constraints.sqlite3"
    await run_migrations(path)
    engine = create_engine(path)
    sessions = session_factory(engine)
    now = datetime(2026, 8, 28, 9, tzinfo=UTC)

    try:
        async with sessions() as session:
            domain = Domain(
                host="index.example",
                scheme="https",
                include_subdomains=False,
                created_at=now,
                configured_at=now,
            )
            session.add(domain)
            await session.flush()
            page = Page(
                domain_id=domain.id,
                url="https://index.example/",
                discovered_at=now,
                next_crawl_at=now,
                next_archive_at=now,
            )
            session.add(page)
            await session.flush()
            page_id = page.id
            first = ArchiveJob(
                id="first",
                page_id=page_id,
                kind=ArchiveJobKind.DIRECT,
                state=ArchiveJobState.PENDING,
                cycle_key="first-cycle",
                intent_at=now,
                next_attempt_at=now,
            )
            session.add(first)
            await session.commit()

            session.add(
                ArchiveJob(
                    id="duplicate",
                    page_id=page_id,
                    kind=ArchiveJobKind.DIRECT,
                    state=ArchiveJobState.INTENT,
                    cycle_key="duplicate-cycle",
                    intent_at=now,
                    next_attempt_at=now,
                )
            )
            with pytest.raises(IntegrityError):
                await session.commit()
            await session.rollback()

            first = await session.get(ArchiveJob, "first")
            assert first is not None
            first.state = ArchiveJobState.SUCCEEDED
            await session.commit()
            session.add(
                ArchiveJob(
                    id="replacement",
                    page_id=page_id,
                    kind=ArchiveJobKind.DIRECT,
                    state=ArchiveJobState.INTENT,
                    cycle_key="replacement-cycle",
                    intent_at=now,
                    next_attempt_at=now,
                )
            )
            await session.commit()

            jobs = (await session.scalars(select(ArchiveJob))).all()
            assert {job.id for job in jobs} == {"first", "replacement"}
    finally:
        await engine.dispose()
