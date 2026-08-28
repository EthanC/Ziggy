"""Bounded HTTP fetching, discovery parsing, and crawl state transitions."""

from __future__ import annotations

import asyncio
import email.utils
import gzip
import io
import zlib
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from random import SystemRandom
from typing import TYPE_CHECKING, Protocol, Self, cast
from urllib.parse import urlsplit

import niquests
from loguru import logger
from sqlalchemy.dialects.sqlite import insert

from ziggy.models import Page
from ziggy.urls import (
    SitemapContents,
    UrlError,
    extract_html_urls,
    extract_http_link_urls,
    extract_sitemap,
    normalize_url,
    url_in_scope,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from sqlalchemy.ext.asyncio import AsyncSession

    from ziggy.config import CrawlSettings

_TRANSIENT_STATUSES = {408, 425, 429}
_REDIRECT_STATUSES = {301, 302, 303, 307, 308}
_SITEMAP_TYPES = {"application/xml", "text/xml", "application/rss+xml"}
_MAX_SITEMAP_DEPTH = 8
_CHUNK_SIZE = 64 * 1024
_NOT_MODIFIED = 304
_SUCCESS_MIN = 200
_SUCCESS_MAX = 299
_SERVER_ERROR_MIN = 500
_SERVER_ERROR_MAX = 599
_GZIP_MAGIC = b"\x1f\x8b"


class RandomSource(Protocol):
    """Random behavior needed by retry scheduling."""

    def uniform(self, lower: float, upper: float) -> float:
        """Return a value within the inclusive range."""


class _SystemRandomSource:
    """Production jitter backed by the operating system random source."""

    def __init__(self) -> None:
        """Create the random source."""
        self._source = SystemRandom()

    def uniform(self, lower: float, upper: float) -> float:
        """Return a random value in the requested range."""
        return self._source.uniform(lower, upper)


_RANDOM: RandomSource = _SystemRandomSource()


class FetchError(RuntimeError):
    """A classified failure from one bounded fetch attempt."""

    def __init__(self, message: str, *, transient: bool) -> None:
        """Record a safe error message and retry classification."""
        super().__init__(message)
        self.transient = transient


class ResponseTooLargeError(FetchError):
    """A response exceeded the configured decoded-body limit."""

    def __init__(self, limit: int) -> None:
        """Build a permanent failure that does not include response content."""
        super().__init__(f"response exceeds {limit} bytes", transient=False)


@dataclass(frozen=True, slots=True)
class FetchResult:
    """A bounded HTTP result, including redirect boundary information."""

    status_code: int
    final_url: str
    headers: Mapping[str, str]
    body: bytes
    encoding: str | None
    redirect_urls: tuple[str, ...]
    blocked_redirect: str | None = None


@dataclass(slots=True)
class _HostLimit:
    semaphore: asyncio.Semaphore
    delay_lock: asyncio.Lock
    next_request_at: float = 0.0


class CrawlerClient:
    """A pooled Niquests client with global and per-host controls."""

    def __init__(self, settings: CrawlSettings) -> None:
        """Create an unopened pooled client with configured bounds."""
        self.settings = settings
        self._session = niquests.AsyncSession(
            retries=0,
            headers={"User-Agent": "Ziggy/0.1 (+https://github.com/EthanC/Ziggy)"},
        )
        self._global = asyncio.Semaphore(settings.concurrency)
        self._hosts: dict[str, _HostLimit] = {}
        self._closed = False

    async def __aenter__(self) -> Self:
        """Return this client for use in an async context."""
        return self

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: object,
    ) -> None:
        """Close the client when leaving an async context."""
        del exception_type, exception, traceback
        await self.close()

    async def close(self) -> None:
        """Close pooled connections; repeated calls are harmless."""
        if not self._closed:
            self._closed = True
            await self._session.close()

    def _host_limit(self, host: str) -> _HostLimit:
        limit = self._hosts.get(host)
        if limit is None:
            limit = _HostLimit(
                asyncio.Semaphore(self.settings.per_host_concurrency), asyncio.Lock()
            )
            self._hosts[host] = limit
        return limit

    async def _wait_for_host(self, limit: _HostLimit) -> None:
        async with limit.delay_lock:
            loop = asyncio.get_running_loop()
            delay = limit.next_request_at - loop.time()
            if delay > 0:
                await asyncio.sleep(delay)
            limit.next_request_at = loop.time() + self.settings.request_delay

    async def _request(
        self, url: str, headers: Mapping[str, str]
    ) -> tuple[niquests.AsyncResponse, _HostLimit]:
        if self._closed:
            raise RuntimeError("crawler client is closed")
        host = cast("str", urlsplit(url).hostname)
        limit = self._host_limit(host)
        await self._global.acquire()
        try:
            await limit.semaphore.acquire()
        except BaseException:
            self._global.release()
            raise
        try:
            await self._wait_for_host(limit)
            response = await self._session.get(
                url,
                headers=dict(headers),
                timeout=self.settings.request_timeout,
                allow_redirects=False,
                stream=True,
            )
        except niquests.exceptions.RequestException as error:
            limit.semaphore.release()
            self._global.release()
            raise FetchError(type(error).__name__, transient=True) from error
        except BaseException:
            limit.semaphore.release()
            self._global.release()
            raise
        return response, limit

    async def fetch(
        self,
        url: str,
        configured_host: str,
        *,
        include_subdomains: bool,
        etag: str | None = None,
        last_modified: str | None = None,
    ) -> FetchResult:
        """Fetch one page while enforcing scope at every redirect hop."""
        current = normalize_url(url)
        request_headers: dict[str, str] = {}
        if etag:
            request_headers["If-None-Match"] = etag
        if last_modified:
            request_headers["If-Modified-Since"] = last_modified
        redirects: list[str] = []
        for redirect_count in range(self.settings.max_redirects + 1):
            response, limit = await self._request(current, request_headers)
            try:
                async with response:
                    status_code = response.status_code
                    if status_code is None:
                        raise FetchError("response has no HTTP status", transient=True)
                    headers = {
                        str(key): str(value) for key, value in response.headers.items()
                    }
                    if status_code in _REDIRECT_STATUSES:
                        location = response.headers.get("Location")
                        if not location:
                            return FetchResult(
                                status_code,
                                current,
                                headers,
                                b"",
                                response.encoding,
                                tuple(redirects),
                            )
                        try:
                            target = normalize_url(location, base=current)
                        except UrlError as error:
                            raise FetchError(str(error), transient=False) from error
                        redirects.append(target)
                        if not url_in_scope(
                            target,
                            configured_host,
                            include_subdomains=include_subdomains,
                        ):
                            return FetchResult(
                                status_code,
                                current,
                                headers,
                                b"",
                                response.encoding,
                                tuple(redirects),
                                blocked_redirect=target,
                            )
                        if redirect_count == self.settings.max_redirects:
                            raise FetchError("redirect limit exceeded", transient=False)
                        current = target
                        request_headers = {}
                        continue
                    body = (
                        b""
                        if status_code == _NOT_MODIFIED
                        else await _read_bounded(
                            response, self.settings.max_response_bytes
                        )
                    )
                    body = _decode_gzip_sitemap(
                        current, body, self.settings.max_response_bytes
                    )
                    return FetchResult(
                        status_code,
                        current,
                        headers,
                        body,
                        response.encoding,
                        tuple(redirects),
                    )
            finally:
                limit.semaphore.release()
                self._global.release()
        raise AssertionError("redirect loop did not return")


async def _read_bounded(response: niquests.AsyncResponse, limit: int) -> bytes:
    content_length = response.headers.get("Content-Length")
    if content_length is not None:
        try:
            if int(content_length) > limit:
                raise ResponseTooLargeError(limit)
        except ValueError:
            pass
    body = bytearray()
    try:
        chunks = await response.iter_content(chunk_size=min(_CHUNK_SIZE, limit + 1))
        async for chunk in chunks:
            if len(body) + len(chunk) > limit:
                raise ResponseTooLargeError(limit)
            body.extend(chunk)
    except niquests.exceptions.RequestException as error:
        raise FetchError(type(error).__name__, transient=True) from error
    return bytes(body)


def _is_gzip_sitemap(url: str) -> bool:
    return urlsplit(url).path.lower().endswith(".xml.gz")


def _decode_gzip_sitemap(url: str, body: bytes, limit: int) -> bytes:
    if _is_gzip_sitemap(url) and body.startswith(_GZIP_MAGIC):
        return _decompress_gzip_bounded(body, limit)
    return body


def _decompress_gzip_bounded(body: bytes, limit: int) -> bytes:
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(body)) as stream:
            decoded = stream.read(limit + 1)
    except (OSError, EOFError, zlib.error) as error:
        raise FetchError("invalid gzip sitemap", transient=False) from error
    if len(decoded) > limit:
        raise ResponseTooLargeError(limit)
    return decoded


def is_transient_status(status_code: int) -> bool:
    """Return whether an HTTP response qualifies for short retry backoff."""
    return status_code in _TRANSIENT_STATUSES or (
        _SERVER_ERROR_MIN <= status_code <= _SERVER_ERROR_MAX
    )


def parse_retry_after(value: str | None, now: datetime) -> datetime | None:
    """Parse Retry-After seconds or an HTTP date into aware UTC."""
    if value is None:
        return None
    stripped = value.strip()
    if stripped.isdigit():
        try:
            return now + timedelta(seconds=int(stripped))
        except OverflowError:
            return None
    try:
        parsed = email.utils.parsedate_to_datetime(stripped)
    except TypeError, ValueError, OverflowError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def retry_time(
    now: datetime,
    attempt: int,
    retry_after: datetime | None,
    random_source: RandomSource = _RANDOM,
) -> datetime:
    """Calculate bounded exponential backoff with full jitter."""
    maximum = min(3600.0, float(2 ** max(0, attempt - 1)))
    calculated = now + timedelta(seconds=random_source.uniform(0.0, maximum))
    return max(calculated, retry_after) if retry_after is not None else calculated


def _content_type(headers: Mapping[str, str]) -> str | None:
    value = _header(headers, "Content-Type")
    return value.split(";", 1)[0].strip().lower() if value else None


def _header(headers: Mapping[str, str], name: str) -> str | None:
    lowered = name.lower()
    return next(
        (value for key, value in headers.items() if key.lower() == lowered), None
    )


def discoveries(
    result: FetchResult, sitemap_depth: int
) -> tuple[tuple[str, ...], bool]:
    """Parse static links from a successful bounded response."""
    content_type = _content_type(result.headers)
    found: dict[str, None] = dict.fromkeys(
        extract_http_link_urls(
            [value for key, value in result.headers.items() if key.lower() == "link"],
            result.final_url,
        )
    )
    path = urlsplit(result.final_url).path.lower()
    is_sitemap = content_type in _SITEMAP_TYPES or path.endswith((".xml", ".xml.gz"))
    if content_type == "text/html" or (
        content_type is None
        and result.body.lstrip().lower().startswith(b"<!doctype html")
    ):
        for value in extract_html_urls(
            _decode_html(result.body, result.encoding), result.final_url
        ):
            found[value] = None
    elif is_sitemap and sitemap_depth < _MAX_SITEMAP_DEPTH:
        contents: SitemapContents = extract_sitemap(result.body, result.final_url)
        for value in (*contents.pages, *contents.sitemaps):
            found[value] = None
    return tuple(found), is_sitemap


def _decode_html(body: bytes, encoding: str | None) -> str:
    try:
        return body.decode(encoding or "utf-8", errors="replace")
    except LookupError:
        return body.decode("utf-8", errors="replace")


async def crawl_page(  # noqa: PLR0913
    session: AsyncSession,
    page: Page,
    *,
    configured_host: str,
    include_subdomains: bool,
    client: CrawlerClient,
    settings: CrawlSettings,
    now: datetime,
    random_source: RandomSource = _RANDOM,
) -> None:
    """Fetch one leased page and persist its result and discoveries."""
    try:
        result = await client.fetch(
            page.url,
            configured_host,
            include_subdomains=include_subdomains,
            etag=page.etag,
            last_modified=page.last_modified,
        )
    except FetchError as error:
        page.error = str(error)
        page.crawl_attempts += 1
        if error.transient and page.crawl_attempts < settings.max_attempts:
            page.next_crawl_at = retry_time(
                now, page.crawl_attempts, None, random_source
            )
        else:
            page.next_crawl_at = now + settings.interval
            page.crawl_attempts = 0
        _release_crawl(page)
        await session.commit()
        return

    page.status_code = result.status_code
    page.final_url = result.final_url
    page.content_type = _content_type(result.headers)
    page.last_crawled_at = now
    page.first_crawled_at = page.first_crawled_at or now
    page.etag = _header(result.headers, "ETag") or page.etag
    page.last_modified = _header(result.headers, "Last-Modified") or page.last_modified
    page.error = (
        f"redirect left scope: {result.blocked_redirect}"
        if result.blocked_redirect
        else None
    )
    if is_transient_status(result.status_code):
        page.crawl_attempts += 1
        if page.crawl_attempts < settings.max_attempts:
            retry_after = parse_retry_after(_header(result.headers, "Retry-After"), now)
            page.next_crawl_at = retry_time(
                now, page.crawl_attempts, retry_after, random_source
            )
        else:
            page.next_crawl_at = now + settings.interval
            page.crawl_attempts = 0
    else:
        page.next_crawl_at = now + settings.interval
        page.crawl_attempts = 0

    if result.status_code == _NOT_MODIFIED:
        _release_crawl(page)
        await session.commit()
        return
    found: tuple[str, ...] = ()
    is_sitemap = False
    if _SUCCESS_MIN <= result.status_code <= _SUCCESS_MAX:
        try:
            found, is_sitemap = discoveries(result, page.sitemap_depth)
        except UrlError as error:
            page.error = str(error)
    scoped = tuple(
        value
        for value in (*result.redirect_urls, *found)
        if url_in_scope(
            value,
            configured_host,
            include_subdomains=include_subdomains,
        )
    )
    discovered = tuple(dict.fromkeys(scoped))
    for value in discovered:
        logger.info("Page found: {}", value)
    if discovered:
        await session.execute(
            insert(Page)
            .values(
                [
                    {
                        "domain_id": page.domain_id,
                        "url": value,
                        "in_scope": True,
                        "discovered_at": now,
                        "discovered_from_id": page.id,
                        "next_crawl_at": now,
                        "next_archive_at": now,
                        "sitemap_depth": page.sitemap_depth + 1
                        if is_sitemap and value in found
                        else 0,
                    }
                    for value in discovered
                ]
            )
            .on_conflict_do_update(
                index_elements=[Page.url],
                set_={"domain_id": page.domain_id, "in_scope": True},
            )
        )
    _release_crawl(page)
    await session.commit()


def _release_crawl(page: Page) -> None:
    page.crawl_lease_owner = None
    page.crawl_lease_expires_at = None
