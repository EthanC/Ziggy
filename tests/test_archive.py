from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import pytest
from archivist import (
    ArchivistError,
    AuthenticationError,
    InternetArchiveFailedStatus,
    InternetArchivePendingStatus,
    InternetArchiveSuccessStatus,
    RateLimitError,
)
from sqlalchemy import select

from ziggy import archive
from ziggy.archive import (
    ArchiveAuthenticationError,
    ArchiveError,
    ArchiveRateLimitError,
    ArchiveStatus,
    FailedStatus,
    PendingStatus,
    SuccessStatus,
    claim_archive_job,
    create_archive_intent,
    poll_archive_job,
    submit_archive_job,
)
from ziggy.config import ArchiveSettings
from ziggy.database import create_engine, run_migrations, session_factory
from ziggy.models import (
    ArchiveJob,
    ArchiveJobKind,
    ArchiveJobState,
    Base,
    Capture,
    Domain,
    Page,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence
    from pathlib import Path

    from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker


NOW = datetime(2026, 3, 4, 5, 6, 7, tzinfo=UTC)
CAPTURED_AT = NOW + timedelta(seconds=30)
LEASE = timedelta(minutes=2)
SETTINGS = ArchiveSettings(
    interval=timedelta(days=30),
    dedupe_window=timedelta(hours=12),
    max_attempts=3,
)


@dataclass(slots=True)
class Database:
    engine: AsyncEngine
    sessions: async_sessionmaker[AsyncSession]


@dataclass(slots=True)
class FakeArchiveClient:
    submit_result: str = "remote-1"
    status_result: ArchiveStatus = field(
        default_factory=lambda: PendingStatus("remote-1", None)
    )
    outlink_results: Sequence[SuccessStatus] = ()
    capture_results: Sequence[SuccessStatus] = ()
    submit_error: ArchiveError | None = None
    status_error: ArchiveError | None = None
    outlinks_error: ArchiveError | None = None
    add_error: ArchiveError | None = None
    captures_error: ArchiveError | None = None
    submissions: list[tuple[str, timedelta]] = field(default_factory=list)
    status_calls: list[str] = field(default_factory=list)
    outlink_calls: list[str] = field(default_factory=list)
    add_calls: list[str] = field(default_factory=list)
    capture_calls: list[tuple[str, datetime]] = field(default_factory=list)
    close_calls: int = 0

    async def submit(self, url: str, dedupe_window: timedelta) -> str:
        self.submissions.append((url, dedupe_window))
        if self.submit_error is not None:
            raise self.submit_error
        return self.submit_result

    async def status(self, job_id: str) -> ArchiveStatus:
        self.status_calls.append(job_id)
        if self.status_error is not None:
            raise self.status_error
        return self.status_result

    async def outlinks(self, job_id: str) -> Sequence[SuccessStatus]:
        self.outlink_calls.append(job_id)
        if self.outlinks_error is not None:
            raise self.outlinks_error
        return self.outlink_results

    async def add_to_my_archive(self, job_id: str) -> None:
        self.add_calls.append(job_id)
        if self.add_error is not None:
            raise self.add_error

    async def captures_since(
        self, url: str, since: datetime
    ) -> Sequence[SuccessStatus]:
        self.capture_calls.append((url, since))
        if self.captures_error is not None:
            raise self.captures_error
        return self.capture_results

    async def close(self) -> None:
        self.close_calls += 1


@pytest.fixture(params=("metadata", "migrated"))
async def database(
    tmp_path: Path, request: pytest.FixtureRequest
) -> AsyncIterator[Database]:
    path = tmp_path / f"archive-{request.param}.sqlite3"
    if request.param == "migrated":
        await run_migrations(path)
    engine = create_engine(path)
    if request.param == "metadata":
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
    try:
        yield Database(engine, session_factory(engine))
    finally:
        await engine.dispose()


async def add_page(
    session: AsyncSession,
    *,
    host: str = "example.com",
    url: str = "https://example.com/page",
    active: bool = True,
    include_subdomains: bool = False,
) -> tuple[Domain, Page]:
    domain = Domain(
        host=host,
        scheme="https",
        include_subdomains=include_subdomains,
        active=active,
        created_at=NOW,
        configured_at=NOW,
    )
    session.add(domain)
    await session.flush()
    page = Page(
        domain_id=domain.id,
        url=url,
        discovered_at=NOW,
        next_crawl_at=NOW,
        next_archive_at=NOW,
    )
    session.add(page)
    await session.flush()
    return domain, page


async def add_job(  # noqa: PLR0913
    session: AsyncSession,
    page: Page,
    *,
    state: ArchiveJobState = ArchiveJobState.INTENT,
    external_job_id: str | None = None,
    job_id: str = "local-1",
    intent_at: datetime = NOW,
    next_attempt_at: datetime = NOW,
    attempts: int = 0,
    saved: bool = False,
    outlinks_processed: bool = False,
) -> ArchiveJob:
    job = ArchiveJob(
        id=job_id,
        page_id=page.id,
        kind=ArchiveJobKind.DIRECT,
        state=state,
        cycle_key=f"cycle-{job_id}",
        external_job_id=external_job_id,
        intent_at=intent_at,
        next_attempt_at=next_attempt_at,
        attempts=attempts,
        saved_to_my_archive=saved,
        outlinks_processed=outlinks_processed,
        lease_owner="old-worker",
        lease_expires_at=NOW + LEASE,
    )
    session.add(job)
    await session.flush()
    return job


def success(
    job_id: str = "remote-1",
    url: str = "https://example.com/page",
    captured_at: datetime = CAPTURED_AT,
) -> SuccessStatus:
    return SuccessStatus(
        job_id=job_id,
        original_url=url,
        captured_at=captured_at,
        wayback_url=f"https://web.archive.org/web/{job_id}",
        screenshot=f"https://web.archive.org/screenshot/{job_id}",
    )


async def test_create_intent_commits_before_remote_work(database: Database):
    async with database.sessions() as session:
        _, page = await add_page(session)
        page.archive_lease_owner = "submitter"
        page.archive_lease_expires_at = NOW + LEASE
        await session.commit()

        job = await create_archive_intent(session, page, NOW)
        job_id = job.id

        assert job.state is ArchiveJobState.INTENT
        assert job.kind is ArchiveJobKind.DIRECT
        assert job.intent_at == NOW
        assert job.next_attempt_at == NOW
        assert job.lease_owner == "submitter"
        assert job.lease_expires_at == NOW + LEASE
        assert page.archive_lease_owner is None
        assert page.archive_lease_expires_at is None

    async with database.sessions() as observer:
        persisted_job = await observer.get(ArchiveJob, job_id)
        persisted_page = await observer.get(Page, page.id)
        assert persisted_job is not None
        assert persisted_job.state is ArchiveJobState.INTENT
        assert persisted_page is not None
        assert persisted_page.archive_lease_owner is None


async def test_submit_success_persists_remote_acceptance(database: Database):
    client = FakeArchiveClient(submit_result="accepted-42")
    async with database.sessions() as session:
        _, page = await add_page(session)
        job = await add_job(session, page, attempts=2)
        job.error = "old error"
        await session.commit()

        await submit_archive_job(
            session, job, page=page, client=client, settings=SETTINGS, now=NOW
        )

        assert client.submissions == [(page.url, SETTINGS.dedupe_window)]
        assert job.external_job_id == "accepted-42"
        assert job.state is ArchiveJobState.SUBMITTED
        assert job.submitted_at == NOW
        assert job.next_attempt_at == NOW
        assert job.attempts == 0
        assert job.error is None
        assert job.lease_owner is None
        assert job.lease_expires_at is None


async def test_submit_authentication_failure_is_persisted_and_raised(
    database: Database,
):
    client = FakeArchiveClient(
        submit_error=ArchiveAuthenticationError("bad credentials")
    )
    async with database.sessions() as session:
        _, page = await add_page(session)
        job = await add_job(session, page)
        await session.commit()

        with pytest.raises(ArchiveAuthenticationError, match="bad credentials"):
            await submit_archive_job(
                session, job, page=page, client=client, settings=SETTINGS, now=NOW
            )

        assert job.state is ArchiveJobState.UNCERTAIN
        assert job.error == "authentication failed"
        assert job.next_attempt_at == NOW + timedelta(minutes=5)
        assert job.external_job_id is None
        assert job.lease_owner is None


@pytest.mark.parametrize(
    ("error", "expected_state", "expected_retry", "expected_error"),
    [
        (
            ArchiveRateLimitError(NOW + timedelta(minutes=7)),
            ArchiveJobState.UNCERTAIN,
            NOW + timedelta(minutes=7),
            "ArchiveRateLimitError",
        ),
        (
            ArchiveError("service unavailable"),
            ArchiveJobState.UNCERTAIN,
            NOW + timedelta(seconds=2),
            "ArchiveError",
        ),
    ],
)
async def test_submit_retryable_failures_preserve_uncertainty(
    database: Database,
    error: ArchiveError,
    expected_state: ArchiveJobState,
    expected_retry: datetime,
    expected_error: str,
):
    client = FakeArchiveClient(submit_error=error)
    async with database.sessions() as session:
        _, page = await add_page(session)
        job = await add_job(session, page)
        await session.commit()

        await submit_archive_job(
            session, job, page=page, client=client, settings=SETTINGS, now=NOW
        )

        assert job.state is expected_state
        assert job.attempts == 1
        assert job.next_attempt_at == expected_retry
        assert job.error == expected_error
        assert job.external_job_id is None
        assert job.lease_owner is None


async def test_uncertain_intent_recovers_from_cdx_without_resubmitting(
    database: Database,
):
    earlier = success("history:early", captured_at=NOW + timedelta(seconds=10))
    latest = success("history:latest", captured_at=CAPTURED_AT)
    client = FakeArchiveClient(capture_results=(earlier, latest))
    async with database.sessions() as session:
        _, page = await add_page(session)
        job = await add_job(session, page, state=ArchiveJobState.UNCERTAIN)
        await session.commit()

        await submit_archive_job(
            session, job, page=page, client=client, settings=SETTINGS, now=NOW
        )

        capture = await session.scalar(
            select(Capture).where(Capture.archive_job_id == job.id)
        )
        assert client.capture_calls == [(page.url, job.intent_at)]
        assert client.submissions == []
        assert job.state is ArchiveJobState.SUCCEEDED
        assert job.saved_to_my_archive is True
        assert job.outlinks_processed is True
        assert job.completed_at == NOW
        assert capture is not None
        assert capture.captured_at == latest.captured_at
        assert capture.wayback_url == latest.wayback_url
        assert page.next_archive_at == latest.captured_at + SETTINGS.interval


async def test_uncertain_intent_resubmits_when_cdx_has_no_capture(database: Database):
    client = FakeArchiveClient(submit_result="replacement", capture_results=())
    async with database.sessions() as session:
        _, page = await add_page(session)
        job = await add_job(session, page, state=ArchiveJobState.UNCERTAIN)
        await session.commit()

        await submit_archive_job(
            session, job, page=page, client=client, settings=SETTINGS, now=NOW
        )

        assert client.capture_calls == [(page.url, NOW)]
        assert client.submissions == [(page.url, SETTINGS.dedupe_window)]
        assert job.state is ArchiveJobState.SUBMITTED
        assert job.external_job_id == "replacement"


async def test_submit_commits_uncertain_before_crossing_remote_boundary(
    database: Database,
):
    async with database.sessions() as session:
        _, page = await add_page(session)
        job = await add_job(session, page)
        await session.commit()

        class ObservingClient(FakeArchiveClient):
            async def submit(self, url: str, dedupe_window: timedelta) -> str:
                async with database.sessions() as observer:
                    persisted = await observer.get(ArchiveJob, job.id)
                    assert persisted is not None
                    assert persisted.state is ArchiveJobState.UNCERTAIN
                    assert persisted.external_job_id is None
                raise asyncio.CancelledError

        with pytest.raises(asyncio.CancelledError):
            await submit_archive_job(
                session,
                job,
                page=page,
                client=ObservingClient(),
                settings=SETTINGS,
                now=NOW,
            )

    async with database.sessions() as observer:
        persisted = await observer.get(ArchiveJob, job.id)
        assert persisted is not None
        assert persisted.state is ArchiveJobState.UNCERTAIN


@pytest.mark.parametrize(
    ("retry_at", "expected"),
    [
        (NOW + timedelta(seconds=20), NOW + timedelta(seconds=20)),
        (None, NOW + timedelta(seconds=2)),
    ],
)
async def test_poll_pending_schedules_remote_retry(
    database: Database, retry_at: datetime | None, expected: datetime
):
    client = FakeArchiveClient(status_result=PendingStatus("remote-1", retry_at))
    async with database.sessions() as session:
        domain, page = await add_page(session)
        job = await add_job(
            session,
            page,
            state=ArchiveJobState.SUBMITTED,
            external_job_id="remote-1",
        )
        job.error = "old"
        await session.commit()

        await poll_archive_job(
            session,
            job,
            page=page,
            domain=domain,
            client=client,
            settings=SETTINGS,
            now=NOW,
        )

        assert job.state is ArchiveJobState.PENDING
        assert job.next_attempt_at == expected
        assert job.error is None
        assert job.lease_owner is None
        assert job.completed_at is None


async def test_poll_terminal_failure_records_service_code(database: Database):
    client = FakeArchiveClient(status_result=FailedStatus("remote-1", "robots-denied"))
    async with database.sessions() as session:
        domain, page = await add_page(session)
        job = await add_job(
            session,
            page,
            state=ArchiveJobState.PENDING,
            external_job_id="remote-1",
        )
        await session.commit()

        await poll_archive_job(
            session,
            job,
            page=page,
            domain=domain,
            client=client,
            settings=SETTINGS,
            now=NOW,
        )

        assert job.state is ArchiveJobState.FAILED
        assert job.service_code == "robots-denied"
        assert job.completed_at == NOW
        assert page.next_archive_at == NOW + SETTINGS.interval
        assert job.lease_owner is None
        assert await session.scalar(select(Capture)) is None


async def test_poll_success_persists_capture_and_finishes_post_processing(
    database: Database,
):
    remote_success = success()
    client = FakeArchiveClient(status_result=remote_success)
    async with database.sessions() as session:
        domain, page = await add_page(session)
        job = await add_job(
            session,
            page,
            state=ArchiveJobState.SUBMITTED,
            external_job_id="remote-1",
        )
        await session.commit()

        await poll_archive_job(
            session,
            job,
            page=page,
            domain=domain,
            client=client,
            settings=SETTINGS,
            now=NOW,
        )

        capture = await session.scalar(select(Capture))
        assert job.state is ArchiveJobState.SUCCEEDED
        assert job.completed_at == NOW
        assert job.saved_to_my_archive is True
        assert job.outlinks_processed is True
        assert job.error is None
        assert page.next_archive_at == CAPTURED_AT + SETTINGS.interval
        assert capture is not None
        assert capture.page_id == page.id
        assert capture.archive_job_id == job.id
        assert capture.captured_at == CAPTURED_AT
        assert client.status_calls == ["remote-1"]
        assert client.add_calls == ["remote-1"]
        assert client.outlink_calls == ["remote-1"]


async def test_post_processing_retry_resumes_after_saved_checkpoint(
    database: Database,
):
    client = FakeArchiveClient(
        status_result=success(), outlinks_error=ArchiveError("temporary")
    )
    async with database.sessions() as session:
        domain, page = await add_page(session)
        job = await add_job(
            session,
            page,
            state=ArchiveJobState.SUBMITTED,
            external_job_id="remote-1",
        )
        await session.commit()

        await poll_archive_job(
            session,
            job,
            page=page,
            domain=domain,
            client=client,
            settings=SETTINGS,
            now=NOW,
        )

        assert job.state is ArchiveJobState.SUCCEEDED
        assert job.saved_to_my_archive is True
        assert job.outlinks_processed is False
        assert job.attempts == 1
        assert job.error == "ArchiveError"
        assert job.next_attempt_at == NOW + timedelta(seconds=2)

        client.outlinks_error = None
        await poll_archive_job(
            session,
            job,
            page=page,
            domain=domain,
            client=client,
            settings=SETTINGS,
            now=NOW + timedelta(seconds=2),
        )

        assert client.status_calls == ["remote-1"]
        assert client.add_calls == ["remote-1"]
        assert client.outlink_calls == ["remote-1", "remote-1"]
        assert job.outlinks_processed is True
        assert job.error is None


async def test_post_processing_rate_limit_uses_service_retry_time(database: Database):
    retry_at = NOW + timedelta(minutes=9)
    client = FakeArchiveClient(
        status_result=success(), add_error=ArchiveRateLimitError(retry_at)
    )
    async with database.sessions() as session:
        domain, page = await add_page(session)
        job = await add_job(
            session,
            page,
            state=ArchiveJobState.SUBMITTED,
            external_job_id="remote-1",
        )
        await session.commit()

        await poll_archive_job(
            session,
            job,
            page=page,
            domain=domain,
            client=client,
            settings=SETTINGS,
            now=NOW,
        )

        assert job.state is ArchiveJobState.SUCCEEDED
        assert job.saved_to_my_archive is False
        assert job.outlinks_processed is False
        assert job.next_attempt_at == retry_at
        assert job.error == "Internet Archive rate limit"
        assert job.attempts == 0


async def test_outlinks_use_exact_subdomain_scope_and_persist_child_captures(
    database: Database,
):
    children = (
        success("child-exact", "https://NEWS.example.com/a/../child#fragment"),
        success("child-parent", "https://example.com/parent-host"),
        success("child-sibling", "https://shop.example.com/sibling"),
        success("child-nested", "https://deep.news.example.com/nested"),
        success("child-invalid", "mailto:test@example.com"),
    )
    client = FakeArchiveClient(status_result=success(), outlink_results=children)
    async with database.sessions() as session:
        broad_domain, _ = await add_page(
            session, host="example.com", url="https://example.com/root", active=True
        )
        exact_domain, parent_page = await add_page(
            session,
            host="news.example.com",
            url="https://news.example.com/article",
            active=True,
            include_subdomains=False,
        )
        parent_job = await add_job(
            session,
            parent_page,
            state=ArchiveJobState.SUBMITTED,
            external_job_id="remote-1",
        )
        await session.commit()

        await poll_archive_job(
            session,
            parent_job,
            page=parent_page,
            domain=exact_domain,
            client=client,
            settings=SETTINGS,
            now=NOW,
        )

        pages = list(await session.scalars(select(Page).order_by(Page.url)))
        urls = {page.url for page in pages}
        assert urls == {
            "https://example.com/root",
            "https://news.example.com/article",
            "https://news.example.com/child",
        }
        child_page = next(page for page in pages if page.url.endswith("/child"))
        assert child_page.domain_id == exact_domain.id
        assert child_page.domain_id != broad_domain.id
        assert child_page.discovered_from_id == parent_page.id
        assert child_page.next_archive_at == CAPTURED_AT + SETTINGS.interval

        child_job = await session.scalar(
            select(ArchiveJob).where(ArchiveJob.external_job_id == "child-exact")
        )
        assert child_job is not None
        assert child_job.page_id == child_page.id
        assert child_job.parent_job_id == parent_job.id
        assert child_job.kind is ArchiveJobKind.OUTLINK
        assert child_job.state is ArchiveJobState.SUCCEEDED
        assert child_job.saved_to_my_archive is True
        assert child_job.outlinks_processed is True

        child_capture = await session.scalar(
            select(Capture).where(Capture.archive_job_id == child_job.id)
        )
        assert child_capture is not None
        assert child_capture.page_id == child_page.id
        assert child_capture.captured_at == CAPTURED_AT
        assert child_capture.wayback_url.endswith("child-exact")


async def test_claims_persisted_intent_for_recovery_even_when_domain_is_inactive(
    database: Database,
):
    async with database.sessions() as session:
        _, inactive_page = await add_page(
            session,
            host="inactive.example",
            url="https://inactive.example/",
            active=False,
        )
        inactive_job = await add_job(
            session,
            inactive_page,
            job_id="inactive-intent",
            intent_at=NOW - timedelta(minutes=2),
        )
        inactive_job.lease_owner = None
        inactive_job.lease_expires_at = None
        await session.commit()

        claimed = await claim_archive_job(session, "worker", NOW, LEASE)
        assert claimed is not None
        assert claimed.id == inactive_job.id

        _, active_page = await add_page(
            session,
            host="active.example",
            url="https://active.example/",
            active=True,
        )
        active_job = await add_job(session, active_page, job_id="active-intent")
        active_job.lease_owner = None
        active_job.lease_expires_at = None
        await session.commit()

        claimed = await claim_archive_job(session, "worker", NOW, LEASE)
        assert claimed is not None
        assert claimed.id == active_job.id
        assert claimed.lease_owner == "worker"
        assert claimed.lease_expires_at == NOW + LEASE
        assert inactive_job.lease_owner == "worker"


@pytest.mark.parametrize(
    "state",
    [
        ArchiveJobState.SUBMITTED,
        ArchiveJobState.PENDING,
        ArchiveJobState.RATE_LIMITED,
    ],
)
async def test_claims_externally_accepted_jobs_for_inactive_domains(
    database: Database, state: ArchiveJobState
):
    async with database.sessions() as session:
        _, page = await add_page(session, active=False)
        job = await add_job(
            session,
            page,
            state=state,
            external_job_id=f"remote-{state.value}",
        )
        job.lease_owner = None
        job.lease_expires_at = None
        await session.commit()

        claimed = await claim_archive_job(session, "worker", NOW, LEASE)

        assert claimed is not None
        assert claimed.id == job.id
        assert claimed.lease_owner == "worker"


async def test_claims_incomplete_post_processing_for_inactive_domain(
    database: Database,
):
    async with database.sessions() as session:
        _, page = await add_page(session, active=False)
        job = await add_job(
            session,
            page,
            state=ArchiveJobState.SUCCEEDED,
            external_job_id="remote-success",
            saved=True,
            outlinks_processed=False,
        )
        job.lease_owner = None
        job.lease_expires_at = None
        await session.commit()

        claimed = await claim_archive_job(session, "worker", NOW, LEASE)

        assert claimed is not None
        assert claimed.id == job.id


async def test_poll_retry_exhaustion_fails_job_and_reschedules_page(
    database: Database,
):
    client = FakeArchiveClient(status_error=ArchiveError("poll unavailable"))
    async with database.sessions() as session:
        domain, page = await add_page(session)
        job = await add_job(
            session,
            page,
            state=ArchiveJobState.SUBMITTED,
            external_job_id="remote-1",
            attempts=SETTINGS.max_attempts - 1,
        )
        await session.commit()

        await poll_archive_job(
            session,
            job,
            page=page,
            domain=domain,
            client=client,
            settings=SETTINGS,
            now=NOW,
        )

        assert job.state is ArchiveJobState.FAILED
        assert job.attempts == SETTINGS.max_attempts
        assert job.completed_at == NOW
        assert job.error == "ArchiveError"
        assert job.lease_owner is None
        assert page.next_archive_at == NOW + SETTINGS.interval


async def test_last_submission_error_waits_for_final_history_check(
    database: Database,
):
    client = FakeArchiveClient(submit_error=ArchiveError("submit uncertain"))
    async with database.sessions() as session:
        _, page = await add_page(session)
        job = await add_job(session, page, attempts=SETTINGS.max_attempts - 1)
        await session.commit()

        await submit_archive_job(
            session, job, page=page, client=client, settings=SETTINGS, now=NOW
        )

        assert job.state is ArchiveJobState.UNCERTAIN
        assert job.attempts == SETTINGS.max_attempts
        assert job.completed_at is None
        assert job.next_attempt_at == NOW + timedelta(seconds=8)


@pytest.mark.parametrize(
    ("captures", "expected_state"),
    [
        ((), ArchiveJobState.FAILED),
        ((success("history:late"),), ArchiveJobState.SUCCEEDED),
    ],
)
async def test_exhausted_uncertain_job_checks_history_before_finishing(
    database: Database,
    captures: tuple[SuccessStatus, ...],
    expected_state: ArchiveJobState,
):
    client = FakeArchiveClient(capture_results=captures)
    async with database.sessions() as session:
        _, page = await add_page(session)
        job = await add_job(
            session,
            page,
            state=ArchiveJobState.UNCERTAIN,
            attempts=SETTINGS.max_attempts,
        )
        await session.commit()

        await submit_archive_job(
            session, job, page=page, client=client, settings=SETTINGS, now=NOW
        )

        assert client.capture_calls == [(page.url, job.intent_at)]
        assert client.submissions == []
        assert job.state is expected_state
        assert job.completed_at == NOW


@pytest.mark.parametrize("state", [ArchiveJobState.INTENT, ArchiveJobState.UNCERTAIN])
async def test_out_of_scope_job_finishes_without_new_submission(
    database: Database, state: ArchiveJobState
):
    client = FakeArchiveClient()
    async with database.sessions() as session:
        _, page = await add_page(session)
        job = await add_job(session, page, state=state)
        await session.commit()

        await submit_archive_job(
            session,
            job,
            page=page,
            client=client,
            settings=SETTINGS,
            now=NOW,
            allow_submission=False,
        )

        assert client.submissions == []
        assert job.state is ArchiveJobState.FAILED


def test_retry_at_translation():
    exact = NOW + timedelta(minutes=3)
    assert archive._retry_at(exact) is exact  # noqa: SLF001
    assert archive._retry_at(None) is None  # noqa: SLF001
    assert archive._retry_at(1) is None  # noqa: SLF001

    before = datetime.now(UTC)
    translated = archive._retry_at(2.5)  # noqa: SLF001
    after = datetime.now(UTC)
    assert translated is not None
    assert before + timedelta(seconds=2.5) <= translated
    assert translated <= after + timedelta(seconds=2.5)


def test_native_status_translation_helpers():
    retry_at = NOW + timedelta(seconds=15)
    pending = InternetArchivePendingStatus("pending-1", retry_after=retry_at)
    native_success = InternetArchiveSuccessStatus(
        "success-1",
        "https://example.com/original",
        CAPTURED_AT,
        screenshot="https://web.archive.org/screenshot.png",
    )
    failed = InternetArchiveFailedStatus(
        "failed-1", message="failed", service_code="blocked"
    )

    assert archive._status(pending) == PendingStatus("pending-1", retry_at)  # noqa: SLF001
    translated = archive._status(native_success)  # noqa: SLF001
    assert translated == SuccessStatus(
        job_id="success-1",
        original_url="https://example.com/original",
        captured_at=CAPTURED_AT,
        wayback_url=(
            "https://web.archive.org/web/20260304050637/https://example.com/original"
        ),
        screenshot="https://web.archive.org/screenshot.png",
    )
    assert archive._success(native_success) == translated  # noqa: SLF001
    assert archive._status(failed) == FailedStatus("failed-1", "blocked")  # noqa: SLF001


class NativeSubmitClient:
    def __init__(self, result: Any) -> None:
        self.result = result
        self.calls: list[tuple[str, Any]] = []

    async def submit(self, url: str, options: Any) -> Any:
        self.calls.append((url, options))
        if isinstance(self.result, BaseException):
            raise self.result
        return self.result


@dataclass(slots=True)
class NativeJob:
    job_id: str


async def test_adapter_submit_builds_required_options_without_network():
    native = NativeSubmitClient(NativeJob("native-job"))
    adapter = object.__new__(archive.ArchivistClient)
    adapter._has_account = True  # noqa: SLF001
    adapter._client = native  # noqa: SLF001

    result = await adapter.submit("https://example.com/", SETTINGS.dedupe_window)

    assert result == "native-job"
    assert len(native.calls) == 1
    url, options = native.calls[0]
    assert url == "https://example.com/"
    assert options.capture_outlinks is True
    assert options.capture_screenshot is True
    assert options.save_to_archive is True
    assert options.if_not_archived_within == SETTINGS.dedupe_window


class NativeClient:
    def __init__(self) -> None:
        self.login_error: Exception | None = None
        self.submit_result: Any = NativeJob("native-job")
        self.status_result: Any = InternetArchivePendingStatus("native-job")
        self.outlink_results: Sequence[Any] = ()
        self.search_results: list[Any] = []
        self.add_error: Exception | None = None
        self.submit_calls: list[tuple[str, Any]] = []
        self.status_calls: list[str] = []
        self.search_calls: list[tuple[str, dict[str, Any]]] = []
        self.add_calls: list[tuple[Any, tuple[str, ...]]] = []
        self.closed = False

    async def login(self) -> None:
        if self.login_error is not None:
            raise self.login_error

    async def submit(self, url: str, options: Any) -> Any:
        self.submit_calls.append((url, options))
        if isinstance(self.submit_result, BaseException):
            raise self.submit_result
        return self.submit_result

    async def status(self, job_id: str) -> Any:
        self.status_calls.append(job_id)
        if isinstance(self.status_result, BaseException):
            raise self.status_result
        return self.status_result

    async def status_outlinks(self, job_id: str) -> Sequence[Any]:
        self.status_calls.append(f"outlinks:{job_id}")
        if isinstance(self.outlink_results, BaseException):
            raise self.outlink_results
        return self.outlink_results

    async def add_to_my_web_archive(
        self, status: Any, *, tags: tuple[str, ...]
    ) -> None:
        self.add_calls.append((status, tags))
        if self.add_error is not None:
            raise self.add_error

    async def search(self, url: str, **kwargs: Any) -> Any:
        self.search_calls.append((url, kwargs))
        result = self.search_results.pop(0)
        if isinstance(result, BaseException):
            raise result
        return result

    async def close(self) -> None:
        self.closed = True


def adapter_with(native: NativeClient) -> archive.ArchivistClient:
    adapter = object.__new__(archive.ArchivistClient)
    adapter._has_account = True  # noqa: SLF001
    adapter._client = native  # noqa: SLF001
    return adapter


@dataclass(slots=True)
class NativeRecord:
    original_url: str
    timestamp: datetime

    def archive_url(self) -> str:
        return f"https://web.archive.org/history/{self.timestamp:%Y%m%d%H%M%S}"


class NativeSearchPage(list[NativeRecord]):
    def __init__(self, *records: NativeRecord, resume_key: str | None) -> None:
        super().__init__(records)
        self.resume_key = resume_key


def test_adapter_constructor_wires_account_and_timeout(monkeypatch: pytest.MonkeyPatch):
    captured: dict[str, Any] = {}
    native = NativeClient()

    def make_native(*, account: Any, timeout: float) -> NativeClient:
        captured.update(account=account, timeout=timeout)
        return native

    monkeypatch.setattr(archive, "AsyncInternetArchiveClient", make_native)

    adapter = archive.ArchivistClient("archive-user", "archive-password", timeout=4.5)

    assert adapter._client is native  # noqa: SLF001
    assert captured["account"].username == "archive-user"
    assert captured["account"].remember is True
    assert captured["timeout"] == 4.5


def test_adapter_constructor_supports_anonymous_client(
    monkeypatch: pytest.MonkeyPatch,
):
    captured: dict[str, Any] = {}
    native = NativeClient()

    def make_native(*, account: Any, timeout: float) -> NativeClient:
        captured.update(account=account, timeout=timeout)
        return native

    monkeypatch.setattr(archive, "AsyncInternetArchiveClient", make_native)

    adapter = archive.ArchivistClient(timeout=6.0)

    assert adapter._client is native  # noqa: SLF001
    assert captured == {"account": None, "timeout": 6.0}


@pytest.mark.parametrize(
    ("username", "password"),
    [("archive-user", None), (None, "archive-password")],
)
def test_adapter_constructor_rejects_partial_credentials(username, password):
    with pytest.raises(ValueError, match="must be provided together"):
        archive.ArchivistClient(username, password)


async def test_anonymous_adapter_skips_login_and_authenticated_options(
    monkeypatch: pytest.MonkeyPatch,
):
    native = NativeClient()
    native.login_error = AuthenticationError("login should not be attempted")
    captured: dict[str, Any] = {}

    def make_native(*, account: Any, timeout: float) -> NativeClient:
        captured.update(account=account, timeout=timeout)
        return native

    monkeypatch.setattr(archive, "AsyncInternetArchiveClient", make_native)
    adapter = archive.ArchivistClient()

    await adapter.login()
    result = await adapter.submit("https://example.com/", SETTINGS.dedupe_window)
    await adapter.add_to_my_archive(result)

    assert result == "native-job"
    assert native.status_calls == []
    _, options = native.submit_calls[0]
    assert options.capture_outlinks is True
    assert options.capture_screenshot is False
    assert options.save_to_archive is False
    assert options.if_not_archived_within == SETTINGS.dedupe_window


async def test_adapter_context_logs_in_and_closes():
    native = NativeClient()
    adapter = adapter_with(native)

    async with adapter as entered:
        assert entered is adapter
        assert native.closed is False

    assert native.closed is True


@pytest.mark.parametrize(
    ("native_error", "local_error", "message"),
    [
        (AuthenticationError("denied"), ArchiveAuthenticationError, "login failed"),
        (ArchivistError("offline"), ArchiveError, "ArchivistError"),
    ],
)
async def test_adapter_login_translates_errors(
    native_error: Exception, local_error: type[ArchiveError], message: str
):
    native = NativeClient()
    native.login_error = native_error

    with pytest.raises(local_error, match=message):
        await adapter_with(native).login()


@pytest.mark.parametrize(
    ("native_error", "local_error"),
    [
        (AuthenticationError("denied"), ArchiveAuthenticationError),
        (RateLimitError("slow down", retry_after=2.5), ArchiveRateLimitError),
        (ArchivistError("offline"), ArchiveError),
    ],
)
async def test_adapter_submit_translates_errors(
    native_error: Exception, local_error: type[ArchiveError]
):
    native = NativeClient()
    native.submit_result = native_error
    before = datetime.now(UTC)

    with pytest.raises(local_error) as caught:
        await adapter_with(native).submit("https://example.com/", timedelta(hours=1))

    if isinstance(caught.value, ArchiveRateLimitError):
        assert caught.value.retry_at is not None
        assert before + timedelta(seconds=2.5) <= caught.value.retry_at
        assert caught.value.retry_at <= datetime.now(UTC) + timedelta(seconds=2.5)


async def test_adapter_status_translates_success():
    native = NativeClient()
    native.status_result = InternetArchiveSuccessStatus(
        "native-job", "https://example.com/", CAPTURED_AT
    )

    translated = await adapter_with(native).status("native-job")

    assert isinstance(translated, SuccessStatus)
    assert translated.job_id == "native-job"


@pytest.mark.parametrize(
    ("native_error", "local_error"),
    [
        (AuthenticationError("denied"), ArchiveAuthenticationError),
        (RateLimitError("slow down", retry_after=NOW), ArchiveRateLimitError),
        (ArchivistError("offline"), ArchiveError),
    ],
)
async def test_adapter_status_translates_errors(
    native_error: Exception, local_error: type[ArchiveError]
):
    native = NativeClient()
    native.status_result = native_error

    with pytest.raises(local_error) as caught:
        await adapter_with(native).status("native-job")

    if isinstance(caught.value, ArchiveRateLimitError):
        assert caught.value.retry_at == NOW


async def test_adapter_outlinks_filters_and_translates_successes():
    native = NativeClient()
    native.outlink_results = (
        InternetArchivePendingStatus("pending-child"),
        InternetArchiveSuccessStatus(
            "successful-child", "https://example.com/child", CAPTURED_AT
        ),
        InternetArchiveFailedStatus("failed-child"),
    )

    translated = await adapter_with(native).outlinks("native-job")

    assert translated == (
        SuccessStatus(
            job_id="successful-child",
            original_url="https://example.com/child",
            captured_at=CAPTURED_AT,
            wayback_url=(
                "https://web.archive.org/web/20260304050637/https://example.com/child"
            ),
            screenshot=None,
        ),
    )


@pytest.mark.parametrize(
    ("native_error", "local_error"),
    [
        (AuthenticationError("denied"), ArchiveAuthenticationError),
        (RateLimitError("slow down", retry_after=NOW), ArchiveRateLimitError),
        (ArchivistError("offline"), ArchiveError),
    ],
)
async def test_adapter_outlinks_translates_errors(
    native_error: Exception, local_error: type[ArchiveError]
):
    native = NativeClient()
    native.outlink_results = native_error

    with pytest.raises(local_error):
        await adapter_with(native).outlinks("native-job")


async def test_adapter_adds_success_to_my_archive():
    native = NativeClient()
    native.status_result = InternetArchiveSuccessStatus(
        "native-job", "https://example.com/", CAPTURED_AT
    )

    await adapter_with(native).add_to_my_archive("native-job")

    assert native.add_calls == [(native.status_result, ("ziggy",))]


async def test_adapter_rejects_adding_non_success_to_my_archive():
    native = NativeClient()

    with pytest.raises(ArchiveError, match="capture is not successful"):
        await adapter_with(native).add_to_my_archive("native-job")


@pytest.mark.parametrize(
    ("native_error", "local_error"),
    [
        (AuthenticationError("denied"), ArchiveAuthenticationError),
        (RateLimitError("slow down", retry_after=NOW), ArchiveRateLimitError),
        (ArchivistError("offline"), ArchiveError),
    ],
)
async def test_adapter_add_to_my_archive_translates_errors(
    native_error: Exception, local_error: type[ArchiveError]
):
    native = NativeClient()
    native.status_result = InternetArchiveSuccessStatus(
        "native-job", "https://example.com/", CAPTURED_AT
    )
    if isinstance(native_error, AuthenticationError):
        native.status_result = native_error
    else:
        native.add_error = native_error

    with pytest.raises(local_error):
        await adapter_with(native).add_to_my_archive("native-job")


async def test_adapter_capture_history_paginates_and_translates_records():
    native = NativeClient()
    first = NativeRecord("https://example.com/", NOW)
    second = NativeRecord("https://example.com/", CAPTURED_AT)
    native.search_results = [
        NativeSearchPage(first, resume_key="next-page"),
        NativeSearchPage(second, resume_key=None),
    ]

    captures = await adapter_with(native).captures_since(first.original_url, NOW)

    assert [capture.job_id for capture in captures] == [
        "history:20260304050607",
        "history:20260304050637",
    ]
    assert [capture.wayback_url for capture in captures] == [
        first.archive_url(),
        second.archive_url(),
    ]
    assert native.search_calls == [
        (
            first.original_url,
            {
                "match_type": "exact",
                "from_timestamp": NOW,
                "show_resume_key": True,
                "resume_key": None,
            },
        ),
        (
            first.original_url,
            {
                "match_type": "exact",
                "from_timestamp": NOW,
                "show_resume_key": True,
                "resume_key": "next-page",
            },
        ),
    ]


@pytest.mark.parametrize(
    ("native_error", "local_error"),
    [
        (AuthenticationError("denied"), ArchiveAuthenticationError),
        (RateLimitError("slow down", retry_after=NOW), ArchiveRateLimitError),
        (ArchivistError("offline"), ArchiveError),
    ],
)
async def test_adapter_capture_history_translates_errors(
    native_error: Exception, local_error: type[ArchiveError]
):
    native = NativeClient()
    native.search_results = [native_error]

    with pytest.raises(local_error):
        await adapter_with(native).captures_since("https://example.com/", NOW)


@pytest.mark.parametrize(
    ("error", "expected_state", "expected_retry"),
    [
        (
            ArchiveRateLimitError(NOW + timedelta(minutes=4)),
            ArchiveJobState.UNCERTAIN,
            NOW + timedelta(minutes=4),
        ),
        (
            ArchiveError("cdx unavailable"),
            ArchiveJobState.UNCERTAIN,
            NOW + timedelta(seconds=2),
        ),
    ],
)
async def test_uncertain_cdx_failures_are_retried_without_submission(
    database: Database,
    error: ArchiveError,
    expected_state: ArchiveJobState,
    expected_retry: datetime,
):
    client = FakeArchiveClient(captures_error=error)
    async with database.sessions() as session:
        _, page = await add_page(session)
        job = await add_job(session, page, state=ArchiveJobState.UNCERTAIN)
        await session.commit()

        await submit_archive_job(
            session, job, page=page, client=client, settings=SETTINGS, now=NOW
        )

        assert client.submissions == []
        assert job.state is expected_state
        assert job.next_attempt_at == expected_retry
        assert job.attempts == 1


async def test_uncertain_cdx_authentication_is_persisted_released_and_raised(
    database: Database,
):
    client = FakeArchiveClient(
        captures_error=ArchiveAuthenticationError("credentials expired")
    )
    async with database.sessions() as session:
        _, page = await add_page(session)
        job = await add_job(session, page, state=ArchiveJobState.UNCERTAIN)
        await session.commit()

        with pytest.raises(ArchiveAuthenticationError, match="credentials expired"):
            await submit_archive_job(
                session, job, page=page, client=client, settings=SETTINGS, now=NOW
            )

        assert client.submissions == []
        assert job.state is ArchiveJobState.UNCERTAIN
        assert job.error == "authentication failed"
        assert job.next_attempt_at == NOW + timedelta(minutes=5)
        assert job.attempts == 0
        assert job.lease_owner is None


async def test_poll_rejects_persisted_job_without_remote_id(database: Database):
    async with database.sessions() as session:
        domain, page = await add_page(session)
        job = await add_job(session, page, state=ArchiveJobState.SUBMITTED)
        await session.commit()

        with pytest.raises(ArchiveError, match="no remote ID"):
            await poll_archive_job(
                session,
                job,
                page=page,
                domain=domain,
                client=FakeArchiveClient(),
                settings=SETTINGS,
                now=NOW,
            )


async def test_poll_authentication_failure_is_persisted_released_and_raised(
    database: Database,
):
    client = FakeArchiveClient(status_error=ArchiveAuthenticationError("denied"))
    async with database.sessions() as session:
        domain, page = await add_page(session)
        job = await add_job(
            session,
            page,
            state=ArchiveJobState.SUBMITTED,
            external_job_id="remote-1",
        )
        await session.commit()

        with pytest.raises(ArchiveAuthenticationError, match="denied"):
            await poll_archive_job(
                session,
                job,
                page=page,
                domain=domain,
                client=client,
                settings=SETTINGS,
                now=NOW,
            )

        assert job.error == "authentication failed"
        assert job.next_attempt_at == NOW + timedelta(minutes=5)
        assert job.lease_owner is None
        assert job.lease_expires_at is None


async def test_poll_rate_limit_without_metadata_uses_default_retry(database: Database):
    client = FakeArchiveClient(status_error=ArchiveRateLimitError(None))
    async with database.sessions() as session:
        domain, page = await add_page(session)
        job = await add_job(
            session,
            page,
            state=ArchiveJobState.PENDING,
            external_job_id="remote-1",
        )
        await session.commit()

        await poll_archive_job(
            session,
            job,
            page=page,
            domain=domain,
            client=client,
            settings=SETTINGS,
            now=NOW,
        )

        assert job.state is ArchiveJobState.RATE_LIMITED
        assert job.next_attempt_at == NOW + timedelta(minutes=1)
        assert job.attempts == 1
        assert job.lease_owner is None


async def test_completed_post_processing_performs_no_remote_calls(database: Database):
    client = FakeArchiveClient()
    async with database.sessions() as session:
        domain, page = await add_page(session)
        job = await add_job(
            session,
            page,
            state=ArchiveJobState.SUCCEEDED,
            external_job_id="remote-1",
            saved=True,
            outlinks_processed=True,
        )
        await session.commit()

        await poll_archive_job(
            session,
            job,
            page=page,
            domain=domain,
            client=client,
            settings=SETTINGS,
            now=NOW,
        )

        assert client.status_calls == []
        assert client.add_calls == []
        assert client.outlink_calls == []


async def test_post_processing_authentication_failure_is_persisted_and_raised(
    database: Database,
):
    client = FakeArchiveClient(
        status_result=success(), add_error=ArchiveAuthenticationError("denied")
    )
    async with database.sessions() as session:
        domain, page = await add_page(session)
        job = await add_job(
            session,
            page,
            state=ArchiveJobState.SUBMITTED,
            external_job_id="remote-1",
        )
        await session.commit()

        with pytest.raises(ArchiveAuthenticationError, match="denied"):
            await poll_archive_job(
                session,
                job,
                page=page,
                domain=domain,
                client=client,
                settings=SETTINGS,
                now=NOW,
            )

        assert job.state is ArchiveJobState.SUCCEEDED
        assert job.error == "authentication failed"
        assert job.next_attempt_at == NOW + timedelta(minutes=5)


class OutlinkSession:
    def __init__(self, scalar_results: list[Any]) -> None:
        self.scalar_results = scalar_results
        self.execute_calls = 0

    async def execute(self, statement: Any) -> None:
        del statement
        self.execute_calls += 1

    async def scalar(self, statement: Any) -> Any:
        del statement
        return self.scalar_results.pop(0)


def detached_page(page_id: int = 99) -> Page:
    return Page(
        id=page_id,
        domain_id=1,
        url="https://example.com/child",
        discovered_at=NOW,
        next_crawl_at=NOW,
        next_archive_at=NOW,
    )


def detached_parent() -> tuple[ArchiveJob, Page, Domain]:
    page = detached_page(1)
    job = ArchiveJob(
        id="parent",
        page_id=page.id,
        kind=ArchiveJobKind.DIRECT,
        state=ArchiveJobState.SUCCEEDED,
        cycle_key="parent-cycle",
        intent_at=NOW,
        next_attempt_at=NOW,
    )
    domain = Domain(
        id=1,
        host="example.com",
        scheme="https",
        include_subdomains=False,
        active=True,
        created_at=NOW,
        configured_at=NOW,
    )
    return job, page, domain


async def test_record_outlinks_tolerates_missing_page_after_insert():
    session = OutlinkSession([None])
    parent, parent_page, domain = detached_parent()

    await archive._record_outlinks(  # noqa: SLF001
        session, parent, parent_page, domain, [success("child")], SETTINGS, NOW
    )

    assert session.execute_calls == 1


async def test_record_outlinks_tolerates_missing_child_job_after_insert():
    session = OutlinkSession([detached_page(), None])
    parent, parent_page, domain = detached_parent()

    await archive._record_outlinks(  # noqa: SLF001
        session, parent, parent_page, domain, [success("child")], SETTINGS, NOW
    )

    assert session.execute_calls == 2
