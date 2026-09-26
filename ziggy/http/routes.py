"""HTTP route handlers and admission validation."""

from __future__ import annotations

import asyncio
import json
import unicodedata
from collections import deque
from datetime import UTC, datetime
from http import HTTPStatus
from time import monotonic
from typing import TYPE_CHECKING

from sqlalchemy.exc import OperationalError
from starlette.requests import ClientDisconnect, Request
from starlette.responses import JSONResponse

from ziggy.database import AdmissionCapacityError, admit_archive_submissions
from ziggy.urls import UrlError, normalize_url, sensitive_query_key

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

MAX_BODY_BYTES = 256 * 1024
MAX_BATCH_URLS = 100
MAX_IDENTIFIER_LENGTH = 128
MAX_URL_LENGTH = 8_192
MIN_PRIORITY = -100
MAX_PRIORITY = 100
OUTSTANDING_SUBMISSION_LIMIT = 10_000
ADMISSION_REQUESTS_PER_MINUTE = 60
REQUEST_DEADLINE_SECONDS = 10.0


class BodyTooLargeError(ValueError):
    """The streamed request body exceeded the configured limit."""


class RequestValidationError(ValueError):
    """The queue payload does not satisfy the public contract."""


class AdmissionRateLimiter:
    """A process-wide fixed-window admission limiter."""

    def __init__(self, limit: int = ADMISSION_REQUESTS_PER_MINUTE) -> None:
        """Create a limiter with the given per-minute request count."""
        self._limit = limit
        self._accepted: deque[float] = deque()

    def allow(self, now: float | None = None) -> bool:
        """Consume one request slot when the current minute has capacity."""
        current = monotonic() if now is None else now
        cutoff = current - 60.0
        while self._accepted and self._accepted[0] <= cutoff:
            self._accepted.popleft()
        if len(self._accepted) >= self._limit:
            return False
        self._accepted.append(current)
        return True


def error_response(
    status: HTTPStatus, code: str, message: str, *, retry_after: int | None = None
) -> JSONResponse:
    """Return a stable JSON error envelope."""
    headers = {"Retry-After": str(retry_after)} if retry_after is not None else None
    return JSONResponse(
        {"error": {"code": code, "message": message}},
        status_code=status,
        headers=headers,
    )


async def read_limited_body(request: Request) -> bytes:
    """Read an ASGI body incrementally and stop at the byte limit."""
    body = bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body) > MAX_BODY_BYTES:
            raise BodyTooLargeError("request body exceeds 256 KiB")
    return bytes(body)


def validate_payload(  # noqa: C901, PLR0912
    value: object,
) -> tuple[str, tuple[str, ...], int]:
    """Validate, normalize, and request-deduplicate one queue payload."""
    if not isinstance(value, dict):
        raise RequestValidationError("request body must be a JSON object")
    unknown = set(value) - {"identifier", "urls", "priority"}
    if unknown:
        raise RequestValidationError(
            f"unknown field(s): {', '.join(sorted(str(item) for item in unknown))}"
        )
    identifier = value.get("identifier")
    if not isinstance(identifier, str) or not identifier.strip():
        raise RequestValidationError("identifier must be a nonempty string")
    identifier = identifier.strip()
    if "\x00" in identifier:
        raise RequestValidationError("identifier must not contain NUL characters")
    if any(unicodedata.category(character) == "Cs" for character in identifier):
        raise RequestValidationError("identifier contains an invalid Unicode character")
    if len(identifier) > MAX_IDENTIFIER_LENGTH:
        raise RequestValidationError("identifier must be at most 128 characters")
    raw_urls = value.get("urls")
    if not isinstance(raw_urls, list) or not raw_urls:
        raise RequestValidationError("urls must be a nonempty array")
    if len(raw_urls) > MAX_BATCH_URLS:
        raise RequestValidationError("urls must contain at most 100 entries")
    priority = value.get("priority", 0)
    if isinstance(priority, bool) or not isinstance(priority, int):
        raise RequestValidationError("priority must be an integer")
    if not MIN_PRIORITY <= priority <= MAX_PRIORITY:
        raise RequestValidationError("priority must be between -100 and 100")

    normalized: dict[str, None] = {}
    for index, raw_url in enumerate(raw_urls):
        if not isinstance(raw_url, str):
            raise RequestValidationError(f"urls[{index}] must be a string")
        if len(raw_url) > MAX_URL_LENGTH:
            raise RequestValidationError(f"urls[{index}] exceeds 8192 characters")
        try:
            url = normalize_url(raw_url)
        except UrlError as error:
            raise RequestValidationError(
                f"urls[{index}] is invalid: {error}"
            ) from error
        if sensitive_query_key(url) is not None:
            raise RequestValidationError(
                f"urls[{index}] contains a sensitive query parameter"
            )
        normalized[url] = None
    return identifier, tuple(normalized), priority


async def queue_submission(request: Request) -> JSONResponse:  # noqa: C901, PLR0911
    """Durably enqueue one validated batch for the archive scheduler."""
    if request.headers.get("content-type", "").split(";", 1)[0].strip().lower() != (
        "application/json"
    ):
        return error_response(
            HTTPStatus.UNSUPPORTED_MEDIA_TYPE,
            "unsupported_media_type",
            "Content-Type must be application/json",
        )
    limiter: AdmissionRateLimiter = request.app.state.rate_limiter
    if not limiter.allow():
        return error_response(
            HTTPStatus.TOO_MANY_REQUESTS,
            "rate_limited",
            "admission rate limit exceeded",
            retry_after=60,
        )
    lock: asyncio.Lock = request.app.state.admission_lock
    if lock.locked():
        return error_response(
            HTTPStatus.SERVICE_UNAVAILABLE,
            "overloaded",
            "another admission transaction is in progress",
            retry_after=1,
        )
    try:
        async with asyncio.timeout(REQUEST_DEADLINE_SECONDS):
            body = await read_limited_body(request)
            try:
                payload = json.loads(body)
            except ValueError as error:
                raise RequestValidationError(
                    "request body must be valid JSON"
                ) from error
            identifier, urls, priority = validate_payload(payload)
            if lock.locked():
                return error_response(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    "overloaded",
                    "another admission transaction is in progress",
                    retry_after=1,
                )
            async with lock:
                sessions: async_sessionmaker[AsyncSession] = request.app.state.sessions
                async with sessions() as session:  # pragma: no branch
                    receipts = await admit_archive_submissions(
                        session,
                        identifier,
                        urls,
                        priority,
                        datetime.now(UTC),
                        outstanding_limit=OUTSTANDING_SUBMISSION_LIMIT,
                    )
    except BodyTooLargeError as error:
        return error_response(
            HTTPStatus.REQUEST_ENTITY_TOO_LARGE, "body_too_large", str(error)
        )
    except RequestValidationError as error:
        return error_response(
            HTTPStatus.UNPROCESSABLE_ENTITY, "invalid_request", str(error)
        )
    except AdmissionCapacityError as error:
        return error_response(
            HTTPStatus.SERVICE_UNAVAILABLE,
            "overloaded",
            str(error),
            retry_after=30,
        )
    except OperationalError, TimeoutError:
        return error_response(
            HTTPStatus.SERVICE_UNAVAILABLE,
            "temporarily_unavailable",
            "admission storage is temporarily unavailable",
            retry_after=1,
        )
    except ClientDisconnect:
        return error_response(
            HTTPStatus.BAD_REQUEST,
            "client_disconnected",
            "request body was interrupted",
        )
    return JSONResponse(
        {
            "submissions": [
                {"receipt_id": receipt.id, "url": receipt.url} for receipt in receipts
            ]
        },
        status_code=HTTPStatus.ACCEPTED,
    )
