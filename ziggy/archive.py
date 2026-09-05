"""Persisted Internet Archive submission, polling, and outlink handling."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from http import HTTPStatus
from typing import TYPE_CHECKING, Protocol, Self
from uuid import uuid4

from archivist import (
    ArchivistError,
    AsyncInternetArchiveClient,
    AuthenticationError,
    InternetArchiveAccount,
    InternetArchiveFailedStatus,
    InternetArchivePendingStatus,
    InternetArchiveSaveOptions,
    InternetArchiveSuccessStatus,
    InternetArchiveUserStatus,
    RateLimitError,
    ServiceError,
)
from loguru import logger
from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.dialects.sqlite import insert

from ziggy.database import insert_page_candidates
from ziggy.models import (
    ArchiveJob,
    ArchiveJobKind,
    ArchiveJobState,
    Capture,
    Domain,
    Page,
)
from ziggy.urls import (
    DEFAULT_MAX_QUERY_VARIANTS_PER_BASE,
    UrlError,
    normalize_url,
    sensitive_query_key,
    url_in_scope,
)

_SERVER_ERROR_MIN = 500
_SERVER_ERROR_MAX = 599
_DEFAULT_SERVER_ERROR_RECOVERY_PERIOD = timedelta(minutes=15)
_MIN_ADAPTIVE_REQUEST_DELAY = 1.0
_MAX_ADAPTIVE_REQUEST_DELAY = 60.0
_INITIAL_STATUS_DELAY = timedelta(seconds=2)
_SERVICE_FAILURE_RECOVERY_DELAY = timedelta(minutes=15)
_RETRYABLE_SERVICE_CODES = frozenset({"error:no-captures", "error:not-found"})

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence

    from sqlalchemy.ext.asyncio import AsyncSession

    from ziggy.config import ArchiveSettings


class ArchiveError(RuntimeError):
    """A safe local representation of an Archivist failure."""


class ArchiveAuthenticationError(ArchiveError):
    """Authentication failed and new submissions must pause."""


class ArchiveRateLimitError(ArchiveError):
    """Internet Archive requested a later retry."""

    def __init__(self, retry_at: datetime | None) -> None:
        """Store the Internet Archive's parsed retry time when available."""
        super().__init__("Internet Archive rate limit")
        self.retry_at = retry_at


class ArchiveJobNotFoundError(ArchiveError):
    """A submitted job is not yet available from the status endpoint."""


@dataclass(frozen=True, slots=True)
class PendingStatus:
    """A remote job that has not reached a terminal state."""

    job_id: str
    retry_at: datetime | None


@dataclass(frozen=True, slots=True)
class SuccessStatus:
    """A successful remote capture."""

    job_id: str
    original_url: str
    captured_at: datetime
    wayback_url: str
    screenshot: str | None
    first_archive: bool | None


@dataclass(frozen=True, slots=True)
class FailedStatus:
    """A terminal remote capture failure."""

    job_id: str
    service_code: str | None


ArchiveStatus = PendingStatus | SuccessStatus | FailedStatus


class ArchiveClient(Protocol):
    """Narrow archive boundary used by workflows and deterministic tests."""

    async def submit(self, url: str, dedupe_window: timedelta) -> str:
        """Submit a direct Save Page Now job and return its remote ID."""

    async def status(self, job_id: str) -> ArchiveStatus:
        """Return the current state of a persisted remote job."""

    async def outlinks(self, job_id: str) -> Sequence[SuccessStatus]:
        """Return successful child captures for a direct job."""

    async def add_to_my_archive(self, job_id: str) -> None:
        """Add one successful capture to My Web Archive."""

    async def my_web_archive_url(self) -> str | None:
        """Return the authenticated account's My Web Archive URL."""

    async def captures_since(
        self, url: str, since: datetime
    ) -> Sequence[SuccessStatus]:
        """Return capture history at or after an uncertain intent."""

    async def close(self) -> None:
        """Close network resources."""


class ArchivistClient:
    """Adapter from Archivist models and exceptions to Ziggy's boundary."""

    def __init__(
        self,
        email: str | None = None,
        password: str | None = None,
        timeout: float = 30.0,
        request_delay: float = 1.0,
        server_error_recovery_period: timedelta = _DEFAULT_SERVER_ERROR_RECOVERY_PERIOD,
    ) -> None:
        """Create an anonymous or account-backed Archivist client."""
        if bool(email) != bool(password):
            raise ValueError(
                "Internet Archive email and password must be provided together"
            )
        account = (
            InternetArchiveAccount(email, password, remember=True)
            if email and password
            else None
        )
        self._has_account = account is not None
        self._client = AsyncInternetArchiveClient(account=account, timeout=timeout)
        self._request_lock = asyncio.Lock()
        self._request_delay = request_delay
        self._adaptive_request_delay = request_delay
        self._server_error_recovery_period = server_error_recovery_period
        self._last_request_at: float | None = None
        self._rate_limit_until: datetime | None = None
        self._rate_limit_recovery_started_at: datetime | None = None
        self._server_error_failures = 0
        self._last_server_error_at: datetime | None = None
        self._server_error_until: datetime | None = None

    async def __aenter__(self) -> Self:
        """Verify configured account credentials before accepting work."""
        await self.login()
        return self

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: object,
    ) -> None:
        """Close the underlying network session."""
        del exception_type, exception, traceback
        await self.close()

    async def login(self) -> None:
        """Authenticate eagerly and expose a safe local exception."""
        if not self._has_account:
            return
        try:
            await self._request(self._client.login)
        except AuthenticationError as error:
            raise ArchiveAuthenticationError("Internet Archive login failed") from error
        except ArchivistError as error:
            raise ArchiveError(type(error).__name__) from error

    async def submission_capacity(self) -> int | None:
        """Return authenticated SPN queue capacity when it is available."""
        if not self._has_account:
            return None
        try:
            status: InternetArchiveUserStatus = await self._request(
                self._client.user_status
            )
        except AuthenticationError as error:
            raise ArchiveAuthenticationError("Internet Archive login failed") from error
        except ArchivistError as error:
            raise ArchiveError(type(error).__name__) from error
        return max(0, status.available)

    async def my_web_archive_url(self) -> str | None:
        """Return My Web Archive URL when account authentication is active."""
        if not self._has_account:
            return None
        try:
            return await self._request(self._client.my_web_archive_url)
        except AuthenticationError as error:
            raise ArchiveAuthenticationError("Internet Archive login failed") from error
        except ArchivistError as error:
            raise ArchiveError(type(error).__name__) from error

    async def submit(self, url: str, dedupe_window: timedelta) -> str:
        """Submit with account-only options when credentials are configured."""
        options = InternetArchiveSaveOptions(
            capture_outlinks=True,
            capture_screenshot=self._has_account,
            save_to_archive=self._has_account,
            if_not_archived_within=dedupe_window,
        )
        try:
            job = await self._request(lambda: self._client.submit(url, options))
        except AuthenticationError as error:
            raise ArchiveAuthenticationError("Internet Archive login failed") from error
        except ArchivistError as error:
            raise ArchiveError(type(error).__name__) from error
        return job.job_id

    async def status(self, job_id: str) -> ArchiveStatus:
        """Translate one remote status model."""
        try:
            status = await self._request(lambda: self._client.status(job_id))
        except AuthenticationError as error:
            raise ArchiveAuthenticationError("Internet Archive login failed") from error
        except ServiceError as error:
            if error.status_code == HTTPStatus.NOT_FOUND:
                raise ArchiveJobNotFoundError(
                    "Internet Archive job not found"
                ) from error
            raise ArchiveError(type(error).__name__) from error
        except ArchivistError as error:
            raise ArchiveError(type(error).__name__) from error
        return _status(status)

    async def outlinks(self, job_id: str) -> tuple[SuccessStatus, ...]:
        """Translate successful outlink statuses with known original URLs."""
        try:
            statuses = await self._request(lambda: self._client.status_outlinks(job_id))
        except AuthenticationError as error:
            raise ArchiveAuthenticationError("Internet Archive login failed") from error
        except ArchivistError as error:
            raise ArchiveError(type(error).__name__) from error
        return tuple(
            translated
            for status in statuses
            if isinstance(status, InternetArchiveSuccessStatus)
            for translated in (_success(status),)
        )

    async def add_to_my_archive(self, job_id: str) -> None:
        """Add the successful status represented by a persisted remote ID."""
        if not self._has_account:
            return
        try:
            status = await self._request(lambda: self._client.status(job_id))
            if not isinstance(status, InternetArchiveSuccessStatus):
                raise ArchiveError("capture is not successful")
            await self._request(
                lambda: self._client.add_to_my_web_archive(status, tags=("ziggy",))
            )
        except AuthenticationError as error:
            raise ArchiveAuthenticationError("Internet Archive login failed") from error
        except ArchivistError as error:
            raise ArchiveError(type(error).__name__) from error

    async def captures_since(
        self, url: str, since: datetime
    ) -> tuple[SuccessStatus, ...]:
        """Search all CDX pages from an uncertain intent timestamp."""
        found: list[SuccessStatus] = []
        resume_key: str | None = None
        try:
            while True:
                page = await self._request(
                    lambda resume_key=resume_key: self._client.search(
                        url,
                        match_type="exact",
                        from_timestamp=since,
                        show_resume_key=True,
                        resume_key=resume_key,
                    )
                )
                found.extend(
                    SuccessStatus(
                        job_id=f"history:{record.timestamp:%Y%m%d%H%M%S}",
                        original_url=record.original_url,
                        captured_at=record.timestamp,
                        wayback_url=record.archive_url(),
                        screenshot=None,
                        first_archive=None,
                    )
                    for record in page
                )
                resume_key = page.resume_key
                if resume_key is None:
                    return tuple(found)
        except AuthenticationError as error:
            raise ArchiveAuthenticationError("Internet Archive login failed") from error
        except ArchivistError as error:
            raise ArchiveError(type(error).__name__) from error

    async def _request[T](self, request: Callable[[], Awaitable[T]]) -> T:
        """Serialize requests and apply service cooldowns to later work."""
        async with self._request_lock:
            now = datetime.now(UTC)
            delay = max(
                (
                    (cooldown - now).total_seconds()
                    for cooldown in (
                        self._rate_limit_until,
                        self._server_error_until,
                    )
                    if cooldown is not None
                ),
                default=0.0,
            )
            if self._last_request_at is not None:
                delay = max(
                    delay,
                    self._adaptive_request_delay
                    - (asyncio.get_running_loop().time() - self._last_request_at),
                )
            if delay > 0:
                await asyncio.sleep(delay)
            self._rate_limit_until = None
            self._server_error_until = None
            self._last_request_at = asyncio.get_running_loop().time()
            try:
                result = await request()
            except RateLimitError as error:
                occurred_at = datetime.now(UTC)
                retry_at = _retry_at(error.retry_after) or occurred_at + timedelta(
                    minutes=1
                )
                self._rate_limit_until = retry_at
                self._adaptive_request_delay = min(
                    _MAX_ADAPTIVE_REQUEST_DELAY,
                    max(
                        _MIN_ADAPTIVE_REQUEST_DELAY,
                        self._adaptive_request_delay * 2,
                    ),
                )
                self._rate_limit_recovery_started_at = None
                raise ArchiveRateLimitError(retry_at) from error
            except ServiceError as error:
                if error.status_code is not None and (
                    _SERVER_ERROR_MIN <= error.status_code <= _SERVER_ERROR_MAX
                ):
                    occurred_at = datetime.now(UTC)
                    self._server_error_failures += 1
                    exponent = min(self._server_error_failures - 1, 6)
                    minutes = min(60, 2**exponent)
                    self._last_server_error_at = occurred_at
                    self._server_error_until = occurred_at + timedelta(minutes=minutes)
                raise
            else:
                self._recover_request_pacing(datetime.now(UTC))
                return result

    def _recover_request_pacing(self, succeeded_at: datetime) -> None:
        """Cautiously restore configured pacing after sustained success."""
        if self._last_server_error_at is None or (
            succeeded_at - self._last_server_error_at
            >= self._server_error_recovery_period
        ):
            self._server_error_failures = 0
            self._last_server_error_at = None
        self._server_error_until = None
        if self._adaptive_request_delay <= self._request_delay:
            self._rate_limit_recovery_started_at = None
            return
        if self._rate_limit_recovery_started_at is None:
            self._rate_limit_recovery_started_at = succeeded_at
            return
        if (
            succeeded_at - self._rate_limit_recovery_started_at
            < self._server_error_recovery_period
        ):
            return
        self._adaptive_request_delay = max(
            self._request_delay,
            self._adaptive_request_delay
            - max(_MIN_ADAPTIVE_REQUEST_DELAY, self._request_delay),
        )
        self._rate_limit_recovery_started_at = succeeded_at

    async def close(self) -> None:
        """Close Archivist's owned Niquests session."""
        await self._client.close()


def _retry_at(value: float | datetime | None) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, float):
        return datetime.now(UTC) + timedelta(seconds=value)
    return None


def _status(
    status: InternetArchivePendingStatus
    | InternetArchiveSuccessStatus
    | InternetArchiveFailedStatus,
) -> ArchiveStatus:
    if isinstance(status, InternetArchivePendingStatus):
        return PendingStatus(status.job_id, _retry_at(status.retry_after))
    if isinstance(status, InternetArchiveSuccessStatus):
        return _success(status)
    return FailedStatus(status.job_id, status.service_code)


def _success(status: InternetArchiveSuccessStatus) -> SuccessStatus:
    return SuccessStatus(
        job_id=status.job_id,
        original_url=status.original_url,
        captured_at=status.timestamp,
        wayback_url=status.archive_url(),
        screenshot=status.screenshot,
        first_archive=status.first_archive,
    )


async def create_archive_intent(
    session: AsyncSession, page: Page, now: datetime
) -> ArchiveJob:
    """Commit remote submission intent before crossing the external boundary."""
    job = ArchiveJob(
        page_id=page.id,
        kind=ArchiveJobKind.DIRECT,
        state=ArchiveJobState.INTENT,
        cycle_key=str(uuid4()),
        intent_at=now,
        next_attempt_at=now,
        lease_owner=page.archive_lease_owner,
        lease_expires_at=page.archive_lease_expires_at,
    )
    session.add(job)
    page.archive_lease_owner = None
    page.archive_lease_expires_at = None
    await session.commit()
    return job


async def submit_archive_job(  # noqa: PLR0913
    session: AsyncSession,
    job: ArchiveJob,
    *,
    page: Page,
    client: ArchiveClient,
    settings: ArchiveSettings,
    now: datetime,
    allow_submission: bool = True,
) -> None:
    """Submit or recover one persisted direct intent."""
    if job.state == ArchiveJobState.UNCERTAIN and not await _recover_uncertain(
        session,
        job,
        page=page,
        client=client,
        settings=settings,
        now=now,
        allow_submission=allow_submission,
    ):
        return
    if not allow_submission:
        _fail_job(job, page, settings, now)
        await session.commit()
        return

    job.state = ArchiveJobState.UNCERTAIN
    job.error = None
    await session.commit()
    try:
        external_job_id = await client.submit(page.url, settings.dedupe_window)
    except ArchiveAuthenticationError:
        job.error = "authentication failed"
        job.next_attempt_at = now + timedelta(minutes=5)
        _release_job(job)
        await session.commit()
        raise
    except ArchiveRateLimitError as error:
        _retry_uncertain(job, now, error, retry_at=error.retry_at)
        await session.commit()
        logger.warning("Archive submission rate limited for job {}", job.id)
        return
    except ArchiveError as error:
        _retry_uncertain(job, now, error)
        await session.commit()
        logger.warning(
            "Archive submission failed for job {}: {}",
            job.id,
            type(error).__name__,
        )
        return
    job.external_job_id = external_job_id
    job.state = ArchiveJobState.SUBMITTED
    job.submitted_at = now
    job.next_attempt_at = now + _INITIAL_STATUS_DELAY
    job.error = None
    job.service_code = None
    job.lease_owner = None
    job.lease_expires_at = None
    await session.commit()


async def _recover_uncertain(  # noqa: PLR0913
    session: AsyncSession,
    job: ArchiveJob,
    *,
    page: Page,
    client: ArchiveClient,
    settings: ArchiveSettings,
    now: datetime,
    allow_submission: bool,
) -> bool:
    try:
        captures = await client.captures_since(page.url, job.intent_at)
    except ArchiveAuthenticationError:
        job.error = "authentication failed"
        job.next_attempt_at = now + timedelta(minutes=5)
        _release_job(job)
        await session.commit()
        raise
    except ArchiveRateLimitError as error:
        _retry_uncertain(job, now, error, retry_at=error.retry_at)
        await session.commit()
        logger.warning("Archive recovery rate limited for job {}", job.id)
        return False
    except ArchiveError as error:
        _retry_uncertain(job, now, error)
        await session.commit()
        logger.warning(
            "Archive recovery failed for job {}: {}",
            job.id,
            type(error).__name__,
        )
        return False
    if captures:
        job.saved_to_my_archive = True
        job.outlinks_processed = True
        await _record_success(session, job, page, captures[-1], settings, now)
        return False
    if job.attempts >= settings.max_attempts or not allow_submission:
        _fail_job(job, page, settings, now)
        await session.commit()
        return False
    return True


async def poll_archive_job(  # noqa: PLR0911, PLR0913
    session: AsyncSession,
    job: ArchiveJob,
    *,
    page: Page,
    domain: Domain,
    client: ArchiveClient,
    settings: ArchiveSettings,
    now: datetime,
    max_query_variants_per_base: int = DEFAULT_MAX_QUERY_VARIANTS_PER_BASE,
) -> None:
    """Poll one persisted remote ID and finish resumable post-processing."""
    if job.external_job_id is None:
        raise ArchiveError("persisted polling job has no remote ID")
    if job.state == ArchiveJobState.SUCCEEDED:
        await _post_process(
            session,
            job,
            page,
            domain,
            client,
            settings,
            now,
            max_query_variants_per_base,
        )
        return
    try:
        status = await client.status(job.external_job_id)
    except ArchiveAuthenticationError:
        job.error = "authentication failed"
        job.next_attempt_at = now + timedelta(minutes=5)
        _release_job(job)
        await session.commit()
        raise
    except ArchiveRateLimitError as error:
        _rate_limit(job, now, error.retry_at)
        await session.commit()
        logger.warning("Archive polling rate limited for job {}", job.id)
        return
    except ArchiveJobNotFoundError as error:
        _retry_missing_job(job, now, error)
        await session.commit()
        logger.info("Archive job {} is not yet available for polling", job.id)
        return
    except ArchiveError as error:
        _retry_job(job, page, settings, now, error)
        await session.commit()
        logger.warning(
            "Archive polling failed for job {}: {}",
            job.id,
            type(error).__name__,
        )
        return
    if isinstance(status, PendingStatus):
        if now >= (job.submitted_at or job.intent_at) + settings.pending_timeout:
            job.state = ArchiveJobState.FAILED
            job.completed_at = now
            job.error = "remote job exceeded pending timeout"
            job.service_code = "pending_timeout"
            page.next_archive_at = now
            _release_job(job)
            await session.commit()
            logger.warning("Archive job {} exceeded pending timeout", job.id)
            return
        job.state = ArchiveJobState.PENDING
        job.next_attempt_at = status.retry_at or now + timedelta(seconds=2)
        job.error = None
        _release_job(job)
        await session.commit()
        return
    if isinstance(status, FailedStatus):
        await _record_failure(session, job, page, status, settings, now)
        return
    await _record_success(session, job, page, status, settings, now)
    await _post_process(
        session,
        job,
        page,
        domain,
        client,
        settings,
        now,
        max_query_variants_per_base,
    )


async def _record_failure(  # noqa: PLR0913, PLR0917
    session: AsyncSession,
    job: ArchiveJob,
    page: Page,
    status: FailedStatus,
    settings: ArchiveSettings,
    now: datetime,
) -> None:
    retryable = status.service_code in _RETRYABLE_SERVICE_CODES
    if retryable:
        job.attempts += 1
    job.service_code = status.service_code
    if retryable and job.attempts < settings.max_attempts:
        job.state = ArchiveJobState.UNCERTAIN
        job.external_job_id = None
        job.error = status.service_code
        job.next_attempt_at = now + _service_failure_recovery_delay(job.attempts)
        log = logger.info
        message = "Archive job {} will retry after service code {}"
    else:
        job.state = ArchiveJobState.FAILED
        job.completed_at = now
        page.next_archive_at = now + settings.interval
        log = logger.warning
        message = "Archive job {} failed with service code {}"
    _release_job(job)
    await session.commit()
    log(message, job.id, status.service_code or "unknown")


async def _record_success(  # noqa: PLR0913, PLR0917
    session: AsyncSession,
    job: ArchiveJob,
    page: Page,
    status: SuccessStatus,
    settings: ArchiveSettings,
    now: datetime,
) -> None:
    job.state = ArchiveJobState.SUCCEEDED
    job.completed_at = now
    job.error = None
    _release_job(job)
    await session.execute(
        insert(Capture)
        .values(
            page_id=page.id,
            archive_job_id=job.id,
            captured_at=status.captured_at,
            wayback_url=status.wayback_url,
            screenshot=status.screenshot,
            first_archive=status.first_archive,
            completed_at=now,
        )
        .on_conflict_do_nothing()
    )
    page.next_archive_at = status.captured_at + settings.interval
    await session.commit()


async def _post_process(  # noqa: PLR0913, PLR0917
    session: AsyncSession,
    job: ArchiveJob,
    page: Page,
    domain: Domain,
    client: ArchiveClient,
    settings: ArchiveSettings,
    now: datetime,
    max_query_variants_per_base: int = DEFAULT_MAX_QUERY_VARIANTS_PER_BASE,
) -> None:
    try:
        if not job.saved_to_my_archive:
            await client.add_to_my_archive(job.external_job_id or "")
            job.saved_to_my_archive = True
            await session.commit()
        if not job.outlinks_processed:
            children = await client.outlinks(job.external_job_id or "")
            await _record_outlinks(
                session,
                job,
                page,
                domain,
                children,
                settings,
                now,
                max_query_variants_per_base,
            )
            job.outlinks_processed = True
            job.error = None
            await session.commit()
    except ArchiveAuthenticationError:
        job.error = "authentication failed"
        job.next_attempt_at = now + timedelta(minutes=5)
        await session.commit()
        raise
    except ArchiveRateLimitError as error:
        job.next_attempt_at = error.retry_at or now + timedelta(minutes=1)
        job.error = str(error)
        await session.commit()
    except ArchiveError as error:
        _retry_job(job, page, settings, now, error)
        await session.commit()


async def _record_outlinks(  # noqa: PLR0913, PLR0917
    session: AsyncSession,
    parent: ArchiveJob,
    parent_page: Page,
    domain: Domain,
    children: Sequence[SuccessStatus],
    settings: ArchiveSettings,
    now: datetime,
    max_query_variants_per_base: int = DEFAULT_MAX_QUERY_VARIANTS_PER_BASE,
) -> None:
    for child in children:
        try:
            url = normalize_url(child.original_url)
        except UrlError:
            continue
        if sensitive_query_key(url) is not None:
            continue
        if not url_in_scope(
            url, domain.host, include_subdomains=domain.include_subdomains
        ):
            continue
        await insert_page_candidates(
            session,
            [
                {
                    "domain_id": domain.id,
                    "url": url,
                    "in_scope": True,
                    "discovered_at": now,
                    "discovered_from_id": parent_page.id,
                    "next_crawl_at": now,
                    "next_archive_at": child.captured_at + settings.interval,
                }
            ],
            max_query_variants_per_base,
        )
        child_page = await session.scalar(select(Page).where(Page.url == url))
        if child_page is None or child_page.blocked_reason is not None:
            continue
        child_page.next_archive_at = max(
            child_page.next_archive_at, child.captured_at + settings.interval
        )
        child_job_id = str(uuid4())
        child_cycle_key = f"outlink:{parent.id}:{child.job_id}"
        await session.execute(
            insert(ArchiveJob)
            .values(
                id=child_job_id,
                page_id=child_page.id,
                parent_job_id=parent.id,
                kind=ArchiveJobKind.OUTLINK,
                state=ArchiveJobState.SUCCEEDED,
                cycle_key=child_cycle_key,
                external_job_id=child.job_id,
                intent_at=parent.intent_at,
                submitted_at=parent.submitted_at,
                completed_at=now,
                next_attempt_at=now,
                saved_to_my_archive=True,
                outlinks_processed=True,
            )
            .on_conflict_do_nothing()
        )
        child_job = await session.scalar(
            select(ArchiveJob).where(ArchiveJob.cycle_key == child_cycle_key)
        )
        if child_job is not None:
            await session.execute(
                insert(Capture)
                .values(
                    page_id=child_page.id,
                    archive_job_id=child_job.id,
                    captured_at=child.captured_at,
                    wayback_url=child.wayback_url,
                    screenshot=child.screenshot,
                    first_archive=child.first_archive,
                    completed_at=now,
                )
                .on_conflict_do_nothing()
            )


async def claim_archive_job(
    session: AsyncSession,
    owner: str,
    now: datetime,
    lease_duration: timedelta,
) -> ArchiveJob | None:
    """Atomically claim due submission, polling, or post-processing work."""
    accepted = and_(
        ArchiveJob.external_job_id.is_not(None),
        ArchiveJob.state.in_(
            (
                ArchiveJobState.SUBMITTED,
                ArchiveJobState.PENDING,
                ArchiveJobState.RATE_LIMITED,
            )
        ),
    )
    post_processing = and_(
        ArchiveJob.state == ArchiveJobState.SUCCEEDED,
        or_(
            ArchiveJob.saved_to_my_archive.is_(False),
            ArchiveJob.outlinks_processed.is_(False),
        ),
    )
    requires_recovery = and_(
        ArchiveJob.state.in_(
            (
                ArchiveJobState.INTENT,
                ArchiveJobState.UNCERTAIN,
                ArchiveJobState.RATE_LIMITED,
            )
        ),
        ArchiveJob.external_job_id.is_(None),
    )
    candidate = (
        select(ArchiveJob.id)
        .where(
            ArchiveJob.next_attempt_at <= now,
            or_(
                ArchiveJob.lease_expires_at.is_(None),
                ArchiveJob.lease_expires_at <= now,
            ),
            or_(accepted, post_processing, requires_recovery),
        )
        .order_by(ArchiveJob.next_attempt_at, ArchiveJob.intent_at)
        .limit(1)
        .scalar_subquery()
    )
    statement = (
        update(ArchiveJob)
        .where(
            ArchiveJob.id == candidate,
            or_(
                ArchiveJob.lease_expires_at.is_(None),
                ArchiveJob.lease_expires_at <= now,
            ),
        )
        .values(lease_owner=owner, lease_expires_at=now + lease_duration)
        .returning(ArchiveJob)
    )
    job = (await session.scalars(statement)).one_or_none()
    await session.commit()
    return job


async def available_archive_submission_slots(
    session: AsyncSession, max_pending_jobs: int
) -> int:
    """Return local capacity after counting unfinished remote capture jobs."""
    pending = await session.scalar(
        select(func.count())
        .select_from(ArchiveJob)
        .where(
            ArchiveJob.kind == ArchiveJobKind.DIRECT,
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
    return max(0, max_pending_jobs - (pending or 0))


def _rate_limit(job: ArchiveJob, now: datetime, retry_at: datetime | None) -> None:
    job.state = ArchiveJobState.RATE_LIMITED
    job.next_attempt_at = retry_at or now + timedelta(minutes=1)
    job.error = "Internet Archive rate limit"
    job.attempts += 1
    _release_job(job)


def _retry_missing_job(
    job: ArchiveJob, now: datetime, error: ArchiveJobNotFoundError
) -> None:
    """Delay CDX recovery so a newly accepted capture has time to become visible."""
    job.state = ArchiveJobState.UNCERTAIN
    job.external_job_id = None
    job.attempts += 1
    job.error = type(error).__name__
    job.service_code = "error:not-found"
    job.next_attempt_at = now + _service_failure_recovery_delay(job.attempts)
    _release_job(job)


def _service_failure_recovery_delay(attempts: int) -> timedelta:
    exponent = min(max(0, attempts - 1), 2)
    return _SERVICE_FAILURE_RECOVERY_DELAY * (2**exponent)


def _retry_job(
    job: ArchiveJob,
    page: Page,
    settings: ArchiveSettings,
    now: datetime,
    error: ArchiveError,
) -> None:
    job.attempts += 1
    job.error = type(error).__name__
    if job.attempts >= settings.max_attempts:
        job.state = ArchiveJobState.FAILED
        job.completed_at = now
        page.next_archive_at = now + settings.interval
    else:
        job.next_attempt_at = now + timedelta(seconds=min(3600, 2**job.attempts))
    _release_job(job)


def _retry_uncertain(
    job: ArchiveJob,
    now: datetime,
    error: ArchiveError,
    *,
    retry_at: datetime | None = None,
) -> None:
    job.state = ArchiveJobState.UNCERTAIN
    job.attempts += 1
    job.error = type(error).__name__
    job.next_attempt_at = retry_at or now + timedelta(
        seconds=min(3600, 2**job.attempts)
    )
    _release_job(job)


def _fail_job(
    job: ArchiveJob,
    page: Page,
    settings: ArchiveSettings,
    now: datetime,
) -> None:
    job.state = ArchiveJobState.FAILED
    job.completed_at = now
    page.next_archive_at = now + settings.interval
    _release_job(job)


def _release_job(job: ArchiveJob) -> None:
    job.lease_owner = None
    job.lease_expires_at = None
