"""Long-lived asyncio service orchestration."""

from __future__ import annotations

import asyncio
import signal
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from uuid import uuid4

from loguru import logger
from sqlalchemy import delete, func, select, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from ziggy.archive import (
    ArchiveAuthenticationError,
    ArchiveError,
    ArchiveJobState,
    ArchiveRateLimitError,
    ArchivistClient,
    available_archive_submission_slots,
    claim_archive_job,
    create_archive_intent,
    poll_archive_job,
    submit_archive_job,
)
from ziggy.config import (
    Config,
    ConfigError,
    Secrets,
    database_change_requires_restart,
    load_config,
    resolve_secrets,
)
from ziggy.crawler import CrawlerClient, crawl_page
from ziggy.database import (
    claim_due_page,
    create_engine,
    database_url,
    reconcile_domains,
    release_leases,
    run_migrations,
    session_factory,
)
from ziggy.logging import LoggingController
from ziggy.models import ArchiveJob, Domain, Page, Report, ServiceState
from ziggy.reporting import (
    claim_report,
    create_report,
    deliver_report,
    next_report_window,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

_IDLE_DELAY = 0.5
_LEASE_DURATION = timedelta(minutes=5)
_HEARTBEAT_INTERVAL = 30.0
_HEALTH_MAX_AGE = timedelta(seconds=90)
_ARCHIVE_CAPACITY_RECHECK_DELAY = 30.0


@dataclass(slots=True)
class RuntimeState:
    """Hot-reloadable service dependencies and configuration."""

    config: Config
    secrets: Secrets
    archive_client: ArchivistClient
    crawler: CrawlerClient
    logging: LoggingController
    archive_paused: bool = False
    retired_archive_clients: list[ArchivistClient] = field(default_factory=list)
    retired_crawlers: list[CrawlerClient] = field(default_factory=list)

    async def close(self) -> None:
        """Close every current and retired archive client."""
        await self.archive_client.close()
        for client in self.retired_archive_clients:
            await client.close()
        await self.crawler.close()
        for crawler in self.retired_crawlers:
            await crawler.close()


async def run_service(config_path: Path) -> None:  # noqa: PLR0915
    """Start Ziggy, own all resources, and stop cleanly on a signal."""
    config = load_config(config_path)
    secrets = resolve_secrets()
    logging_controller = LoggingController()
    logging_controller.configure(config.logging, secrets)
    engine: AsyncEngine | None = None
    archive_client: ArchivistClient | None = None
    try:
        config.ziggy.database.parent.mkdir(parents=True, exist_ok=True)
        await run_migrations(config.ziggy.database)
        engine = create_engine(config.ziggy.database)
        archive_client = ArchivistClient(
            secrets.archive_email,
            secrets.archive_password,
            timeout=config.crawl.request_timeout,
            request_delay=config.archive.request_delay,
        )
        await archive_client.login()
    except BaseException:
        if archive_client is not None:
            with suppress(Exception):
                await archive_client.close()
        if engine is not None:
            with suppress(Exception):
                await engine.dispose()
        with suppress(Exception):
            await logging_controller.close()
        raise
    sessions = session_factory(engine)
    crawler = CrawlerClient(config.crawl)
    state = RuntimeState(config, secrets, archive_client, crawler, logging_controller)
    instance_id = str(uuid4())
    stop = asyncio.Event()
    remove_signals = _install_signal_handlers(stop)
    try:
        now = datetime.now(UTC)
        async with sessions() as session:
            await reconcile_domains(session, config, now)
            session.add(
                ServiceState(instance_id=instance_id, started_at=now, heartbeat_at=now)
            )
            await session.commit()
        logger.info("Ziggy service started")
        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(
                _config_watcher(config_path, state, sessions, stop),
                name="config-watcher",
            )
            tasks.create_task(
                _crawl_scheduler(state, sessions, instance_id, stop),
                name="crawl-scheduler",
            )
            tasks.create_task(
                _archive_submission_scheduler(state, sessions, instance_id, stop),
                name="archive-submission-scheduler",
            )
            tasks.create_task(
                _archive_poll_scheduler(state, sessions, instance_id, stop),
                name="archive-poll-scheduler",
            )
            tasks.create_task(
                _report_scheduler(state, sessions, instance_id, stop),
                name="report-scheduler",
            )
            tasks.create_task(_heartbeat(sessions, instance_id, stop), name="heartbeat")
    finally:
        stop.set()
        remove_signals()
        async with sessions() as session:
            await release_leases(session, instance_id)
            await session.execute(
                update(Report)
                .where(Report.lease_owner == instance_id)
                .values(lease_owner=None, lease_expires_at=None)
            )
            await session.execute(
                delete(ServiceState).where(ServiceState.instance_id == instance_id)
            )
            await session.commit()
        await state.close()
        await engine.dispose()
        logger.info("Ziggy service stopped")
        await logging_controller.close()


async def _config_watcher(
    config_path: Path,
    state: RuntimeState,
    sessions: async_sessionmaker[AsyncSession],
    stop: asyncio.Event,
) -> None:
    while not stop.is_set():
        await _wait(stop, state.config.ziggy.config_reload_interval.total_seconds())
        if stop.is_set():
            return
        try:
            replacement = load_config(config_path)
            secrets = resolve_secrets()
        except ConfigError as error:
            logger.error("Configuration reload rejected: {}", error)
            continue
        if database_change_requires_restart(state.config, replacement):
            logger.error("Database path change requires a service restart")
            continue
        if (
            replacement == state.config
            and secrets == state.secrets
            and not state.archive_paused
        ):
            continue
        if (
            secrets.archive_email != state.secrets.archive_email
            or secrets.archive_password != state.secrets.archive_password
            or replacement.crawl.request_timeout != state.config.crawl.request_timeout
            or replacement.archive.request_delay != state.config.archive.request_delay
            or state.archive_paused
        ):
            candidate = ArchivistClient(
                secrets.archive_email,
                secrets.archive_password,
                timeout=replacement.crawl.request_timeout,
                request_delay=replacement.archive.request_delay,
            )
            candidate_ready = False
            try:
                await candidate.login()
                candidate_ready = True
            except ArchiveError as error:
                logger.error(
                    "Internet Archive credential reload rejected ({}); "
                    "keeping previous state",
                    type(error).__name__,
                )
                continue
            finally:
                if not candidate_ready:
                    with suppress(Exception):
                        await candidate.close()
            state.retired_archive_clients.append(state.archive_client)
            state.archive_client = candidate
            state.archive_paused = False
        if replacement.crawl != state.config.crawl:
            state.retired_crawlers.append(state.crawler)
            state.crawler = CrawlerClient(replacement.crawl)
        async with sessions() as session:
            await reconcile_domains(session, replacement, datetime.now(UTC))
        state.logging.configure(replacement.logging, secrets)
        state.config = replacement
        state.secrets = secrets
        logger.info("Configuration reload applied")


async def _crawl_scheduler(
    state: RuntimeState,
    sessions: async_sessionmaker[AsyncSession],
    instance_id: str,
    stop: asyncio.Event,
) -> None:
    while not stop.is_set():
        claimed: list[int] = []
        settings = state.config.crawl
        for _ in range(settings.concurrency):
            async with sessions() as session:
                page = await claim_due_page(
                    session, "crawl", instance_id, datetime.now(UTC), _LEASE_DURATION
                )
            if page is None:
                break
            claimed.append(page.id)
        if not claimed:
            await _wait(stop, _IDLE_DELAY)
            continue
        async with asyncio.TaskGroup() as tasks:
            for page_id in claimed:
                tasks.create_task(
                    _crawl_one(state, sessions, page_id),
                    name=f"crawl-{page_id}",
                )


async def _crawl_one(
    state: RuntimeState,
    sessions: async_sessionmaker[AsyncSession],
    page_id: int,
) -> None:
    async with sessions() as session:
        page = await session.get(Page, page_id)
        if page is None:
            return
        domain = await session.get(Domain, page.domain_id)
        if domain is None or not domain.active or not page.in_scope:
            page.crawl_lease_owner = None
            page.crawl_lease_expires_at = None
            await session.commit()
            return
        try:
            await crawl_page(
                session,
                page,
                configured_host=domain.host,
                include_subdomains=domain.include_subdomains,
                client=state.crawler,
                settings=state.config.crawl,
                now=datetime.now(UTC),
            )
        except Exception as error:  # noqa: BLE001 - isolate one leased page.
            await _worker_failure(session, page, "crawl", error)


async def _archive_submission_scheduler(
    state: RuntimeState,
    sessions: async_sessionmaker[AsyncSession],
    instance_id: str,
    stop: asyncio.Event,
) -> None:
    while not stop.is_set():
        if state.archive_paused:
            await _wait(stop, _IDLE_DELAY)
            continue
        available_slots = await _archive_submission_slots(state, sessions, stop)
        if available_slots == 0:
            continue
        claimed: list[int] = []
        for _ in range(available_slots):
            async with sessions() as session:
                page = await claim_due_page(
                    session, "archive", instance_id, datetime.now(UTC), _LEASE_DURATION
                )
            if page is None:
                break
            claimed.append(page.id)
        if not claimed:
            await _wait(stop, _IDLE_DELAY)
            continue
        async with asyncio.TaskGroup() as tasks:
            for page_id in claimed:
                tasks.create_task(
                    _submit_one(state, sessions, page_id),
                    name=f"archive-submit-{page_id}",
                )


async def _archive_submission_slots(
    state: RuntimeState,
    sessions: async_sessionmaker[AsyncSession],
    stop: asyncio.Event,
) -> int:
    async with sessions() as session:
        local_slots = await available_archive_submission_slots(
            session, state.config.archive.max_pending_jobs
        )
    if local_slots == 0:
        await _wait(stop, _IDLE_DELAY)
        return 0
    try:
        remote_slots = await state.archive_client.submission_capacity()
    except ArchiveAuthenticationError:
        state.archive_paused = True
        logger.error("Internet Archive authentication paused new submissions")
        return 0
    except ArchiveRateLimitError as error:
        retry_delay = (
            (error.retry_at - datetime.now(UTC)).total_seconds()
            if error.retry_at is not None
            else _ARCHIVE_CAPACITY_RECHECK_DELAY
        )
        await _wait(stop, max(_IDLE_DELAY, retry_delay))
        return 0
    except ArchiveError:
        await _wait(stop, _ARCHIVE_CAPACITY_RECHECK_DELAY)
        return 0
    if remote_slots == 0:
        await _wait(stop, _ARCHIVE_CAPACITY_RECHECK_DELAY)
        return 0
    return min(
        state.config.archive.concurrency,
        local_slots,
        remote_slots if remote_slots is not None else local_slots,
    )


async def _submit_one(
    state: RuntimeState,
    sessions: async_sessionmaker[AsyncSession],
    page_id: int,
) -> None:
    async with sessions() as session:
        page = await session.get(Page, page_id)
        if page is None:
            return
        domain = await session.get(Domain, page.domain_id)
        if domain is None or not domain.active or not page.in_scope:
            page.archive_lease_owner = None
            page.archive_lease_expires_at = None
            await session.commit()
            return
        job = await create_archive_intent(session, page, datetime.now(UTC))
        try:
            await submit_archive_job(
                session,
                job,
                page=page,
                client=state.archive_client,
                settings=state.config.archive,
                now=datetime.now(UTC),
            )
        except ArchiveAuthenticationError:
            state.archive_paused = True
            logger.error("Internet Archive authentication paused new submissions")
        except Exception as error:  # noqa: BLE001 - preserve scheduler liveness.
            await _archive_worker_failure(session, job, error)


async def _archive_poll_scheduler(
    state: RuntimeState,
    sessions: async_sessionmaker[AsyncSession],
    instance_id: str,
    stop: asyncio.Event,
) -> None:
    while not stop.is_set():
        claimed: list[str] = []
        for _ in range(state.config.archive.concurrency):
            async with sessions() as session:
                job = await claim_archive_job(
                    session, instance_id, datetime.now(UTC), _LEASE_DURATION
                )
            if job is None:
                break
            claimed.append(job.id)
        if not claimed:
            await _wait(stop, _IDLE_DELAY)
            continue
        async with asyncio.TaskGroup() as tasks:
            for job_id in claimed:
                tasks.create_task(
                    _poll_one(state, sessions, job_id),
                    name=f"archive-poll-{job_id}",
                )


async def _poll_one(
    state: RuntimeState,
    sessions: async_sessionmaker[AsyncSession],
    job_id: str,
) -> None:
    async with sessions() as session:
        job = await session.get(ArchiveJob, job_id)
        if job is None:
            return
        page = await session.get(Page, job.page_id)
        if page is None:
            return
        domain = await session.get(Domain, page.domain_id)
        if domain is None:
            return
        try:
            if job.state == ArchiveJobState.INTENT or (
                job.state == ArchiveJobState.RATE_LIMITED
                and job.external_job_id is None
            ):
                job.state = ArchiveJobState.UNCERTAIN
                await session.commit()
            if job.external_job_id is None and state.archive_paused:
                job.lease_owner = None
                job.lease_expires_at = None
                job.next_attempt_at = datetime.now(UTC) + timedelta(minutes=5)
                await session.commit()
                return
            if job.state in {
                ArchiveJobState.INTENT,
                ArchiveJobState.UNCERTAIN,
            }:
                await submit_archive_job(
                    session,
                    job,
                    page=page,
                    client=state.archive_client,
                    settings=state.config.archive,
                    now=datetime.now(UTC),
                    allow_submission=domain.active and page.in_scope,
                )
            else:
                await poll_archive_job(
                    session,
                    job,
                    page=page,
                    domain=domain,
                    client=state.archive_client,
                    settings=state.config.archive,
                    now=datetime.now(UTC),
                )
        except ArchiveAuthenticationError:
            state.archive_paused = True
            logger.error("Internet Archive authentication failed during persisted work")
        except Exception as error:  # noqa: BLE001 - preserve scheduler liveness.
            await _archive_worker_failure(session, job, error)


async def _report_scheduler(
    state: RuntimeState,
    sessions: async_sessionmaker[AsyncSession],
    instance_id: str,
    stop: asyncio.Event,
) -> None:
    while not stop.is_set():
        now = datetime.now(UTC)
        async with sessions() as session:
            last_end = await session.scalar(select(func.max(Report.window_end)))
            window = next_report_window(last_end, now, state.config.reporting.interval)
            if window is not None:
                await create_report(session, window, now)
            report = await claim_report(session, instance_id, now, _LEASE_DURATION)
            if report is not None:
                await deliver_report(
                    session, report, state.secrets.reporting_webhook_url, now
                )
        await _wait(stop, _IDLE_DELAY)


async def _heartbeat(
    sessions: async_sessionmaker[AsyncSession],
    instance_id: str,
    stop: asyncio.Event,
) -> None:
    while not stop.is_set():
        async with sessions() as session:
            await session.execute(
                update(ServiceState)
                .where(ServiceState.instance_id == instance_id)
                .values(heartbeat_at=datetime.now(UTC))
            )
            await session.commit()
        await _wait(stop, _HEARTBEAT_INTERVAL)


async def check_health(path: Path, now: datetime | None = None) -> bool:
    """Return whether any running instance has a recent database heartbeat."""
    if not await asyncio.to_thread(path.exists):
        return False
    engine = create_async_engine(database_url(path))
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as session:
            heartbeat = await session.scalar(
                select(func.max(ServiceState.heartbeat_at))
            )
        current = now or datetime.now(UTC)
        return heartbeat is not None and current - heartbeat <= _HEALTH_MAX_AGE
    except OSError, SQLAlchemyError, ValueError:
        return False
    finally:
        await engine.dispose()


async def _worker_failure(
    session: AsyncSession, page: Page, kind: str, error: Exception
) -> None:
    page.error = type(error).__name__
    page.crawl_lease_owner = None
    page.crawl_lease_expires_at = None
    page.next_crawl_at = datetime.now(UTC) + timedelta(minutes=1)
    await session.commit()
    logger.exception("Unexpected {} worker failure", kind)


async def _archive_worker_failure(
    session: AsyncSession, job: ArchiveJob, error: Exception
) -> None:
    job.error = type(error).__name__
    job.lease_owner = None
    job.lease_expires_at = None
    job.next_attempt_at = datetime.now(UTC) + timedelta(minutes=1)
    await session.commit()
    logger.exception("Unexpected archive worker failure")


async def _wait(stop: asyncio.Event, seconds: float) -> None:
    with suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout=seconds)


def _install_signal_handlers(stop: asyncio.Event) -> Callable[[], None]:
    loop = asyncio.get_running_loop()
    installed: list[signal.Signals] = []

    def request_stop() -> None:
        stop.set()

    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, request_stop)
            installed.append(signum)
        except NotImplementedError:
            signal.signal(
                signum, lambda _signum, _frame: loop.call_soon_threadsafe(request_stop)
            )

    def remove() -> None:
        for signum in installed:
            loop.remove_signal_handler(signum)

    return remove
