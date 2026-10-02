"""Persisted Internet Archive submission, polling, and outlink handling."""

from __future__ import annotations

import asyncio
from contextlib import suppress
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
from sqlalchemy import String, and_, case, func, or_, select, text, update
from sqlalchemy.dialects.sqlite import insert

from ziggy.database import insert_page_candidates
from ziggy.models import (
    ARCHIVE_WORK_PREDICATE,
    ArchiveJob,
    ArchiveJobKind,
    ArchiveJobState,
    ArchiveSubmission,
    Capture,
    Domain,
    Page,
)
from ziggy.transactions import Lease, LeaseLostError, write_transaction
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
_RETRYABLE_SERVICE_CODES = frozenset(
    {
        "error:no-captures",
        "error:not-found",
        "error:no-request",
        "error:gateway-timeout",
        "error:service-unavailable",
        "error:bad-gateway",
        "error:internal-server-error",
    }
)
_DAILY_LIMIT_CODES = frozenset(
    {"error:too-many-captures", "error:daily-limit", "error:daily-capture-limit"}
)
_OUTLINK_BATCH_SIZE = 50

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence

    from sqlalchemy.ext.asyncio import AsyncSession

    from ziggy.config import ArchiveSettings


class ArchiveError(RuntimeError):
    """A safe local representation of an Archivist failure."""


class ArchiveAuthenticationError(ArchiveError):
    """Authentication failed and new submissions must pause."""


class ArchiveServiceError(ArchiveError):
    """An HTTP server failure that needs delayed recovery."""


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
    """A failed remote attempt, including any supplied recovery deadline."""

    job_id: str
    service_code: str | None
    retry_at: datetime | None = None
    daily_limit: bool = False


ArchiveStatus = PendingStatus | SuccessStatus | FailedStatus


class ArchiveClient(Protocol):
    """Narrow archive boundary used by workflows and deterministic tests."""

    async def submit(
        self,
        url: str,
        dedupe_window: timedelta,
        *,
        capture_outlinks: bool = True,
    ) -> str:
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

    async def latest_capture_at(self, url: str) -> datetime | None:
        """Return the latest available Wayback capture timestamp."""

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

    async def submit(
        self,
        url: str,
        dedupe_window: timedelta,
        *,
        capture_outlinks: bool = True,
    ) -> str:
        """Submit with account-only options when credentials are configured."""
        options = InternetArchiveSaveOptions(
            capture_outlinks=capture_outlinks,
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

    async def latest_capture_at(self, url: str) -> datetime | None:
        """Return the latest available capture from the Availability API."""
        try:
            availability = await self._request(lambda: self._client.availability(url))
        except AuthenticationError as error:
            raise ArchiveAuthenticationError("Internet Archive login failed") from error
        except ArchivistError as error:
            raise ArchiveError(type(error).__name__) from error
        snapshot = availability.closest
        return (
            snapshot.timestamp if snapshot is not None and snapshot.available else None
        )

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
                retry_at = _retry_at(error.retry_after)
                message = str(error).casefold()
                daily_limit = _daily_capture_limit(None, message)
                if daily_limit:
                    retry_at = max(
                        retry_at or occurred_at, occurred_at + timedelta(days=1)
                    )
                else:
                    retry_at = retry_at or occurred_at + timedelta(minutes=1)
                per_url = daily_limit and any(
                    marker in message
                    for marker in ("this url", "per-url", "per url", "url has been")
                )
                if not per_url:
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
                    raise ArchiveServiceError(type(error).__name__) from error
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
    return FailedStatus(
        status.job_id,
        status.service_code,
        daily_limit=_daily_capture_limit(status.service_code, status.message),
    )


def _daily_capture_limit(code: str | None, message: str | None) -> bool:
    if code in _DAILY_LIMIT_CODES:
        return True
    return code in (None, "error:too-many-requests") and any(
        phrase in (message or "").casefold()
        for phrase in ("daily capture limit", "captures per day", "times today")
    )


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
    session: AsyncSession,
    page: Page,
    now: datetime,
    *,
    archive_only: bool = False,
) -> ArchiveJob:
    """Commit remote submission intent before crossing the external boundary."""
    lease = Lease.capture(page, "archive_lease_owner", "archive_lease_expires_at")
    await session.commit()

    async def create() -> ArchiveJob:
        return await _create_archive_intent(
            session, page, now, archive_only=archive_only
        )

    return await lease.persist(session, create)


async def _create_archive_intent(
    session: AsyncSession,
    page: Page,
    now: datetime,
    *,
    archive_only: bool,
) -> ArchiveJob:
    job = ArchiveJob(
        page_id=page.id,
        kind=ArchiveJobKind.DIRECT,
        state=ArchiveJobState.INTENT,
        cycle_key=str(uuid4()),
        intent_at=now,
        next_attempt_at=now,
        archive_only=archive_only,
        outlinks_processed=archive_only,
        lease_owner=page.archive_lease_owner,
        lease_expires_at=page.archive_lease_expires_at,
    )
    session.add(job)
    await session.flush()
    await session.execute(
        update(ArchiveSubmission)
        .where(
            ArchiveSubmission.page_id == page.id,
            ArchiveSubmission.archive_job_id.is_(None),
        )
        .values(archive_job_id=job.id)
    )
    page.archive_lease_owner = None
    page.archive_lease_expires_at = None
    return job


async def submit_archive_job(  # noqa: C901, PLR0913
    session: AsyncSession,
    job: ArchiveJob,
    *,
    page: Page,
    client: ArchiveClient,
    settings: ArchiveSettings,
    now: datetime,
    allow_submission: bool = True,
    preflight: Callable[[], Awaitable[bool]] | None = None,
) -> None:
    """Submit or recover one persisted direct intent."""
    url = page.url
    lease = Lease.capture(job, "lease_owner", "lease_expires_at")
    await session.commit()
    if job.state == ArchiveJobState.UNCERTAIN and not await _recover_uncertain(
        session,
        job,
        page=page,
        client=client,
        settings=settings,
        now=now,
        allow_submission=allow_submission,
        lease=lease,
    ):
        return
    if not allow_submission:

        async def fail() -> None:
            _fail_job(job, page, settings, now)

        await lease.persist(session, fail, (page,))
        return
    if preflight is not None and not await preflight():
        return

    await _checkpoint_job(session, lease, state=ArchiveJobState.UNCERTAIN, error=None)
    error: ArchiveError | None = None
    external_job_id: str | None = None
    try:
        if job.archive_only:
            external_job_id = await client.submit(
                url,
                settings.dedupe_window,
                capture_outlinks=False,
            )
        else:
            external_job_id = await client.submit(url, settings.dedupe_window)
    except ArchiveError as caught:
        error = caught

    async def record() -> None:
        if error is not None:
            _submission_error(job, now, error)
        else:
            job.external_job_id = external_job_id
            job.state = ArchiveJobState.SUBMITTED
            job.submitted_at = now
            job.next_attempt_at = now + _INITIAL_STATUS_DELAY
            job.error = None
            job.service_code = None
            _release_job(job)

    await lease.persist(session, record)
    if isinstance(error, ArchiveAuthenticationError):
        raise error
    if error is not None:
        logger.warning(
            "Archive submission failed for job {} ({}): {}",
            job.id,
            type(error).__name__,
            url,
        )


def _submission_error(job: ArchiveJob, now: datetime, error: ArchiveError) -> None:
    if isinstance(error, ArchiveAuthenticationError):
        job.error = "authentication failed"
        job.next_attempt_at = now + timedelta(minutes=5)
        _release_job(job)
    else:
        _retry_uncertain(
            job,
            now,
            error,
            retry_at=error.retry_at
            if isinstance(error, ArchiveRateLimitError)
            else None,
        )


async def _checkpoint_job(
    session: AsyncSession, lease: Lease, **values: object
) -> None:
    async def record() -> None:
        for name, value in values.items():
            setattr(lease.record, name, value)

    await lease.persist(session, record)


async def _recover_uncertain(  # noqa: PLR0913
    session: AsyncSession,
    job: ArchiveJob,
    *,
    page: Page,
    client: ArchiveClient,
    settings: ArchiveSettings,
    now: datetime,
    allow_submission: bool,
    lease: Lease,
) -> bool:
    captures: Sequence[SuccessStatus] = ()
    error: ArchiveError | None = None
    try:
        captures = await client.captures_since(page.url, job.intent_at)
    except ArchiveError as caught:
        error = caught

    async def recover() -> bool:
        if error is not None:
            _submission_error(job, now, error)
            return False
        if captures:
            job.saved_to_my_archive = True
            job.outlinks_processed = True
            await _record_success(
                session,
                job,
                page,
                max(captures, key=lambda capture: capture.captured_at),
                settings,
                now,
            )
            _release_job(job)
            return False
        if job.attempts >= settings.max_attempts or not allow_submission:
            _fail_job(job, page, settings, now)
            return False
        return True

    recovered = await lease.persist(session, recover, (page,))
    if isinstance(error, ArchiveAuthenticationError):
        raise error
    return recovered


async def poll_archive_job(  # noqa: PLR0913
    session: AsyncSession,
    job: ArchiveJob,
    *,
    page: Page,
    domain: Domain | None,
    client: ArchiveClient,
    settings: ArchiveSettings,
    now: datetime,
    max_query_variants_per_base: int = DEFAULT_MAX_QUERY_VARIANTS_PER_BASE,
) -> None:
    """Poll one persisted remote ID and finish resumable post-processing."""
    if job.external_job_id is None:
        raise ArchiveError("persisted polling job has no remote ID")
    lease = Lease.capture(job, "lease_owner", "lease_expires_at")
    await session.commit()
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
    status: ArchiveStatus | ArchiveError
    try:
        status = await client.status(job.external_job_id)
    except ArchiveError as caught:
        status = caught

    async def record() -> None:
        await _record_poll(session, job, page, status, settings, now)

    await lease.persist(session, record, (page,))
    if isinstance(status, ArchiveAuthenticationError):
        raise status
    if isinstance(status, SuccessStatus):
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


async def _record_poll(  # noqa: PLR0911, PLR0913, PLR0917
    session: AsyncSession,
    job: ArchiveJob,
    page: Page,
    status: ArchiveStatus | ArchiveError,
    settings: ArchiveSettings,
    now: datetime,
) -> None:
    if isinstance(status, ArchiveAuthenticationError):
        job.error = "authentication failed"
        job.next_attempt_at = now + timedelta(minutes=5)
        _release_job(job)
        return
    if isinstance(status, ArchiveRateLimitError):
        _rate_limit(job, now, status.retry_at)
        logger.warning("Archive polling rate limited for job {}: {}", job.id, page.url)
        return
    if isinstance(status, ArchiveJobNotFoundError):
        _retry_missing_job(job, now, status)
        logger.info(
            "Archive job {} is not yet available for polling: {}", job.id, page.url
        )
        return
    if isinstance(status, ArchiveError):
        _retry_job(job, page, settings, now, status)
        logger.warning(
            "Archive polling failed for job {} ({}): {}",
            job.id,
            page.url,
            type(status).__name__,
        )
        return
    if isinstance(status, PendingStatus):
        if now >= (job.submitted_at or job.intent_at) + settings.pending_timeout:
            job.state = ArchiveJobState.FAILED
            job.completed_at = now
            job.error = "remote job exceeded pending timeout"
            job.service_code = "pending_timeout"
            if not job.archive_only:
                page.next_archive_at = now
            _release_job(job)
            logger.warning(
                "Archive job {} exceeded pending timeout: {}", job.id, page.url
            )
            return
        job.state = ArchiveJobState.PENDING
        job.next_attempt_at = status.retry_at or now + timedelta(seconds=2)
        job.error = None
        _release_job(job)
        return
    if isinstance(status, FailedStatus):
        _record_failure(job, page, status, settings, now)
        return
    await _record_success(session, job, page, status, settings, now)


def _record_failure(
    job: ArchiveJob,
    page: Page,
    status: FailedStatus,
    settings: ArchiveSettings,
    now: datetime,
) -> None:
    daily_limit = status.daily_limit or status.service_code in _DAILY_LIMIT_CODES
    retryable = status.service_code in _RETRYABLE_SERVICE_CODES or daily_limit
    if retryable:
        job.attempts += 1
    job.service_code = status.service_code
    if retryable and job.attempts < settings.max_attempts:
        job.state = ArchiveJobState.UNCERTAIN
        job.external_job_id = None
        job.error = status.service_code
        delay = (
            timedelta(days=1)
            if daily_limit
            else _service_failure_recovery_delay(job.attempts)
        )
        job.next_attempt_at = max(now + delay, status.retry_at or now)
        log = logger.info
        message = "Archive job {} will retry after service code {}: {}"
    else:
        job.state = ArchiveJobState.FAILED
        job.completed_at = now
        if not job.archive_only:
            page.next_archive_at = now + settings.interval
        log = logger.warning
        message = "Archive job {} failed with service code {}: {}"
    _release_job(job)
    log(message, job.id, status.service_code or "unknown", page.url)


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
    if not job.archive_only:
        page.next_archive_at = status.captured_at + settings.interval
    _record_archive_history(page, status.captured_at, now)


async def _post_process(  # noqa: PLR0913, PLR0917
    session: AsyncSession,
    job: ArchiveJob,
    page: Page,
    domain: Domain | None,
    client: ArchiveClient,
    settings: ArchiveSettings,
    now: datetime,
    max_query_variants_per_base: int = DEFAULT_MAX_QUERY_VARIANTS_PER_BASE,
) -> None:
    lease = Lease.capture(job, "lease_owner", "lease_expires_at")
    await session.commit()
    try:
        if not job.saved_to_my_archive:
            await client.add_to_my_archive(job.external_job_id or "")
            await _checkpoint_job(session, lease, saved_to_my_archive=True)
        if job.archive_only and not job.outlinks_processed:
            await _checkpoint_job(session, lease, outlinks_processed=True)
        if not job.outlinks_processed and domain is not None:
            children = await client.outlinks(job.external_job_id or "")
            candidates = await asyncio.to_thread(_outlink_candidates, children)
            for offset in range(0, len(candidates), _OUTLINK_BATCH_SIZE):

                async def record_batch(offset: int = offset) -> None:
                    await _record_outlinks(
                        session,
                        job,
                        page,
                        domain,
                        candidates[offset : offset + _OUTLINK_BATCH_SIZE],
                        settings,
                        now,
                        max_query_variants_per_base,
                    )

                await lease.persist(session, record_batch, (page, domain))
            await _checkpoint_job(session, lease, outlinks_processed=True, error=None)
    except ArchiveAuthenticationError:
        await _checkpoint_job(
            session,
            lease,
            error="authentication failed",
            next_attempt_at=now + timedelta(minutes=5),
            lease_owner=None,
            lease_expires_at=None,
        )
        raise
    except ArchiveRateLimitError as error:
        await _checkpoint_job(
            session,
            lease,
            next_attempt_at=error.retry_at or now + timedelta(minutes=1),
            error=str(error),
        )
    except ArchiveError as error:

        async def retry(failure: ArchiveError = error) -> None:
            _retry_job(job, page, settings, now, failure)

        await lease.persist(session, retry, (page,))
    await _checkpoint_job(session, lease, lease_owner=None, lease_expires_at=None)


def _outlink_candidates(
    children: Sequence[SuccessStatus],
) -> list[tuple[SuccessStatus, str]]:
    candidates: list[tuple[SuccessStatus, str]] = []
    for child in children:
        try:
            url = normalize_url(child.original_url)
        except UrlError:
            continue
        if sensitive_query_key(url) is not None:
            continue
        candidates.append((child, url))
    return candidates


async def _record_outlinks(  # noqa: PLR0913, PLR0917
    session: AsyncSession,
    parent: ArchiveJob,
    parent_page: Page,
    domain: Domain,
    candidates: Sequence[tuple[SuccessStatus, str]],
    settings: ArchiveSettings,
    now: datetime,
    max_query_variants_per_base: int = DEFAULT_MAX_QUERY_VARIANTS_PER_BASE,
) -> None:
    if not domain.active:
        return
    for child, url in candidates:
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
        _record_archive_history(child_page, child.captured_at, now)
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


async def check_archive_history(
    session: AsyncSession,
    page: Page,
    client: ArchiveClient,
    now: datetime,
) -> None:
    """Classify one validated page from its latest Wayback capture."""
    lease = Lease.capture(
        page,
        "archive_history_lease_owner",
        "archive_history_lease_expires_at",
        "archive_lease_owner",
        "archive_lease_expires_at",
    )
    await session.commit()
    captured_at: datetime | None = None
    error: ArchiveError | None = None
    try:
        captured_at = await client.latest_capture_at(page.url)
    except ArchiveError as caught:
        error = caught

    async def record() -> None:
        _record_history_result(page, captured_at, error, now)

    with suppress(LeaseLostError):
        await lease.persist(session, record)


def _record_history_result(
    page: Page,
    captured_at: datetime | None,
    error: ArchiveError | None,
    now: datetime,
) -> None:
    if isinstance(error, ArchiveRateLimitError):
        page.archive_history_check_attempts += 1
        page.archive_history_check_error = type(error).__name__
        page.next_archive_history_check_at = error.retry_at or now + timedelta(
            minutes=1
        )
    elif error is not None:
        page.archive_history_check_attempts += 1
        page.archive_history_check_error = type(error).__name__
        page.next_archive_history_check_at = now + timedelta(
            seconds=min(3600, 2**page.archive_history_check_attempts)
        )
    else:
        page.archive_history_checked_at = now
        page.latest_archive_at = captured_at
        page.next_archive_history_check_at = None
        page.archive_history_check_attempts = 0
        page.archive_history_check_error = None
    page.archive_history_lease_owner = None
    page.archive_history_lease_expires_at = None


def _record_archive_history(
    page: Page, captured_at: datetime, checked_at: datetime
) -> None:
    """Record capture knowledge without replacing a newer known timestamp."""
    page.latest_archive_at = max(
        value for value in (page.latest_archive_at, captured_at) if value is not None
    )
    page.archive_history_checked_at = checked_at
    page.next_archive_history_check_at = None
    page.archive_history_check_attempts = 0
    page.archive_history_check_error = None
    page.archive_history_lease_owner = None
    page.archive_history_lease_expires_at = None


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
    submission_priority = (
        select(func.max(ArchiveSubmission.priority))
        .where(ArchiveSubmission.archive_job_id == ArchiveJob.id)
        .correlate(ArchiveJob)
        .scalar_subquery()
    )
    effective_priority = case(
        (
            ArchiveJob.archive_only.is_(True),
            func.coalesce(submission_priority, 0),
        ),
        else_=func.max(func.coalesce(submission_priority, -101), 0),
    )
    # Pin the partial work index even before ANALYZE. Otherwise SQLite can scan
    # completed SUCCEEDED jobs through its state index to find unfinished work.
    work = (
        text(
            "SELECT id FROM archive_jobs INDEXED BY ix_archive_jobs_work WHERE "  # noqa: S608 - fixed model predicate, no input values.
            + str(ARCHIVE_WORK_PREDICATE)
        )
        .columns(id=String())
        .subquery()
    )
    candidate = (
        select(ArchiveJob.id)
        .join(work, work.c.id == ArchiveJob.id)
        .where(
            ArchiveJob.next_attempt_at <= now,
            or_(
                ArchiveJob.lease_expires_at.is_(None),
                ArchiveJob.lease_expires_at <= now,
            ),
        )
        .order_by(
            effective_priority.desc(),
            ArchiveJob.next_attempt_at,
            ArchiveJob.intent_at,
            ArchiveJob.id,
        )
        .limit(1)
    )
    for _ in range(16):
        job_id = await session.scalar(candidate)
        await session.commit()
        if job_id is None:
            return None

        async def claim(job_id: str = job_id) -> ArchiveJob | None:
            return (
                await session.scalars(
                    update(ArchiveJob)
                    .where(
                        ArchiveJob.id == job_id,
                        ArchiveJob.next_attempt_at <= now,
                        or_(
                            ArchiveJob.lease_expires_at.is_(None),
                            ArchiveJob.lease_expires_at <= now,
                        ),
                        or_(accepted, post_processing, requires_recovery),
                    )
                    .values(lease_owner=owner, lease_expires_at=now + lease_duration)
                    .returning(ArchiveJob)
                    .execution_options(populate_existing=True)
                )
            ).one_or_none()

        job = await write_transaction(session, claim)
        if job is not None:
            return job
    return None


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
        if not job.archive_only:
            page.next_archive_at = now + settings.interval
    else:
        job.next_attempt_at = now + (
            _service_failure_recovery_delay(job.attempts)
            if isinstance(error, ArchiveServiceError)
            else timedelta(seconds=min(3600, 2**job.attempts))
        )
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
    job.next_attempt_at = retry_at or now + (
        _service_failure_recovery_delay(job.attempts)
        if isinstance(error, ArchiveServiceError)
        else timedelta(seconds=min(3600, 2**job.attempts))
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
    if not job.archive_only:
        page.next_archive_at = now + settings.interval
    _release_job(job)


def _release_job(job: ArchiveJob) -> None:
    job.lease_owner = None
    job.lease_expires_at = None
