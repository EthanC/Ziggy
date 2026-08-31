from __future__ import annotations

import asyncio
import gzip
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import niquests
import pytest

from ziggy import crawler
from ziggy.config import CrawlSettings
from ziggy.models import Page
from ziggy.urls import UrlError

NOW = datetime(2026, 8, 28, 12, 0, tzinfo=UTC)


class FakeResponse:
    def __init__(  # noqa: PLR0913
        self,
        status_code=200,
        *,
        headers=None,
        body=b"",
        chunks=None,
        encoding="utf-8",
        iter_error=None,
        enter_error=None,
    ):
        self.status_code = status_code
        self.headers = {} if headers is None else headers
        self.encoding = encoding
        self.chunks = [body] if chunks is None else chunks
        self.iter_error = iter_error
        self.enter_error = enter_error
        self.chunk_sizes = []
        self.entered = 0
        self.exited = 0

    async def __aenter__(self):
        self.entered += 1
        if self.enter_error is not None:
            raise self.enter_error
        return self

    async def __aexit__(self, *args):
        self.exited += 1

    async def iter_content(self, *, chunk_size):
        self.chunk_sizes.append(chunk_size)
        if self.iter_error is not None:
            raise self.iter_error

        async def generate():
            for item in self.chunks:
                if isinstance(item, BaseException):
                    raise item
                yield item

        return generate()


class FakeSession:
    def __init__(self, outcomes=()):
        self.outcomes = list(outcomes)
        self.get_calls = []
        self.close_calls = 0

    async def get(self, url, **kwargs):
        self.get_calls.append((url, kwargs))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    async def close(self):
        self.close_calls += 1


class FakeDatabaseSession:
    def __init__(self, inserted_urls=None):
        self.statements = []
        self.commits = 0
        self.inserted_urls = inserted_urls

    async def execute(self, statement):
        self.statements.append(statement)
        params = statement.compile().params
        urls = [value for key, value in params.items() if key.startswith("url_m")]
        returned = urls if self.inserted_urls is None else self.inserted_urls
        return SimpleNamespace(scalars=lambda: iter(returned))

    async def scalars(self, _statement):
        return ()

    async def commit(self):
        self.commits += 1


class FakeClient:
    def __init__(self, outcome):
        self.outcome = outcome
        self.calls = []

    async def fetch(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        if isinstance(self.outcome, BaseException):
            raise self.outcome
        return self.outcome


class FakeRandom:
    def __init__(self, value):
        self.value = value
        self.calls = []

    def uniform(self, lower, upper):
        self.calls.append((lower, upper))
        return self.value


class RaisingSemaphore:
    def __init__(self, error):
        self.error = error
        self.releases = 0

    async def acquire(self):
        raise self.error

    def release(self):
        self.releases += 1


def make_settings(**changes):
    values = {
        "interval": timedelta(hours=6),
        "concurrency": 3,
        "per_host_concurrency": 2,
        "request_delay": 0,
        "request_timeout": 7,
        "max_response_bytes": 10,
        "max_redirects": 2,
        "max_attempts": 3,
    }
    values.update(changes)
    return CrawlSettings(**values)


def make_page(**changes):
    values = {
        "id": 11,
        "domain_id": 4,
        "url": "https://example.com/start",
        "discovered_at": NOW - timedelta(days=1),
        "discovered_from_id": None,
        "first_crawled_at": None,
        "last_crawled_at": None,
        "next_crawl_at": NOW,
        "next_archive_at": NOW,
        "status_code": None,
        "final_url": None,
        "content_type": None,
        "etag": '"old"',
        "last_modified": "old-date",
        "error": "previous error",
        "sitemap_depth": 0,
        "crawl_attempts": 0,
        "crawl_lease_owner": "worker",
        "crawl_lease_expires_at": NOW + timedelta(minutes=1),
        "archive_lease_owner": None,
        "archive_lease_expires_at": None,
    }
    values.update(changes)
    return Page(**values)


def make_result(**changes):
    values = {
        "status_code": 200,
        "final_url": "https://example.com/start",
        "headers": {"Content-Type": "text/html"},
        "body": b"<!doctype html><title>ok</title>",
        "encoding": "utf-8",
        "redirect_urls": (),
    }
    values.update(changes)
    return crawler.FetchResult(**values)


def install_client(monkeypatch, outcomes=(), **settings_changes):
    session = FakeSession(outcomes)
    constructor_calls = []

    def factory(**kwargs):
        constructor_calls.append(kwargs)
        return session

    monkeypatch.setattr(crawler.niquests, "AsyncSession", factory)
    client = crawler.CrawlerClient(make_settings(**settings_changes))
    return client, session, constructor_calls


async def run_crawl(page, outcome, *, include_subdomains=False, random_value=0.25):
    session = FakeDatabaseSession()
    client = FakeClient(outcome)
    random = FakeRandom(random_value)
    await crawler.crawl_page(
        session,
        page,
        configured_host="example.com",
        include_subdomains=include_subdomains,
        client=client,
        settings=make_settings(),
        now=NOW,
        random_source=random,
    )
    return session, client, random


async def test_client_context_configuration_and_idempotent_close(monkeypatch):
    client, session, constructor_calls = install_client(monkeypatch)

    async with client as entered:
        assert entered is client

    await client.close()
    assert constructor_calls == [
        {
            "retries": 0,
            "headers": {"User-Agent": "Ziggy/0.1 (+https://github.com/EthanC/Ziggy)"},
        }
    ]
    assert session.close_calls == 1


async def test_wait_for_host_delays_only_until_next_allowed_time(monkeypatch):
    client, _, _ = install_client(monkeypatch, request_delay=2.5)
    limit = client._host_limit("example.com")  # noqa: SLF001
    assert client._host_limit("example.com") is limit  # noqa: SLF001
    sleeps = []

    async def fake_sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr(crawler.asyncio, "sleep", fake_sleep)
    await client._wait_for_host(limit)  # noqa: SLF001
    assert sleeps == []
    await client._wait_for_host(limit)  # noqa: SLF001
    assert len(sleeps) == 1
    assert 2 < sleeps[0] <= 2.5


async def test_request_rejects_closed_client(monkeypatch):
    client, _, _ = install_client(monkeypatch)
    await client.close()

    with pytest.raises(RuntimeError, match="crawler client is closed"):
        await client._request("https://example.com/", {})  # noqa: SLF001


async def test_request_releases_global_slot_when_host_acquire_is_cancelled(
    monkeypatch,
):
    client, _, _ = install_client(monkeypatch)
    semaphore = RaisingSemaphore(asyncio.CancelledError())
    client._hosts["example.com"] = crawler._HostLimit(  # noqa: SLF001
        semaphore, asyncio.Lock()
    )

    with pytest.raises(asyncio.CancelledError):
        await client._request("https://example.com/", {})  # noqa: SLF001

    assert client._global._value == client.settings.concurrency  # noqa: SLF001
    assert semaphore.releases == 0


@pytest.mark.parametrize(
    ("error", "expected_type"),
    [
        (niquests.exceptions.ConnectionError("offline"), crawler.FetchError),
        (asyncio.CancelledError(), asyncio.CancelledError),
    ],
)
async def test_request_classifies_network_errors_and_releases_slots(
    monkeypatch, error, expected_type
):
    client, _, _ = install_client(monkeypatch, [error])

    with pytest.raises(expected_type) as captured:
        await client._request("https://example.com/", {})  # noqa: SLF001

    assert client._global._value == client.settings.concurrency  # noqa: SLF001
    limit = client._hosts["example.com"]  # noqa: SLF001
    assert limit.semaphore._value == client.settings.per_host_concurrency  # noqa: SLF001
    if isinstance(captured.value, crawler.FetchError):
        assert str(captured.value) == "ConnectionError"
        assert captured.value.transient is True


async def test_fetch_sends_conditionals_only_on_initial_request_and_follows_subdomain(
    monkeypatch,
):
    redirect = FakeResponse(302, headers={"Location": "https://api.example.com/end"})
    final = FakeResponse(
        200,
        headers={"content-length": "not-a-number", "X-Test": 3},
        body=b"done",
        encoding=None,
    )
    client, session, _ = install_client(monkeypatch, [redirect, final])

    result = await client.fetch(
        "HTTPS://EXAMPLE.COM/start#fragment",
        "example.com",
        include_subdomains=True,
        etag='"tag"',
        last_modified="yesterday",
    )

    assert result == crawler.FetchResult(
        200,
        "https://api.example.com/end",
        {"content-length": "not-a-number", "X-Test": "3"},
        b"done",
        None,
        ("https://api.example.com/end",),
    )
    assert session.get_calls[0] == (
        "https://example.com/start",
        {
            "headers": {
                "If-None-Match": '"tag"',
                "If-Modified-Since": "yesterday",
            },
            "timeout": 7,
            "allow_redirects": False,
            "stream": True,
        },
    )
    assert session.get_calls[1][1]["headers"] == {}
    assert final.chunk_sizes == [11]
    assert redirect.exited == final.exited == 1
    assert client._global._value == client.settings.concurrency  # noqa: SLF001


async def test_fetch_omits_empty_conditional_headers(monkeypatch):
    client, session, _ = install_client(monkeypatch, [FakeResponse(body=b"ok")])

    await client.fetch("https://example.com/", "example.com", include_subdomains=False)

    assert session.get_calls[0][1]["headers"] == {}


async def test_fetch_returns_redirect_without_location(monkeypatch):
    response = FakeResponse(301, headers={"Server": "fake"}, encoding="ascii")
    client, _, _ = install_client(monkeypatch, [response])

    result = await client.fetch(
        "https://example.com/", "example.com", include_subdomains=False
    )

    assert result == crawler.FetchResult(
        301, "https://example.com/", {"Server": "fake"}, b"", "ascii", ()
    )


@pytest.mark.parametrize(
    ("target", "include_subdomains"),
    [
        ("https://api.example.com/path", False),
        ("https://evil-example.com/path", True),
    ],
)
async def test_fetch_blocks_redirects_outside_exact_host_scope(
    monkeypatch, target, include_subdomains
):
    client, _, _ = install_client(
        monkeypatch, [FakeResponse(308, headers={"Location": target})]
    )

    result = await client.fetch(
        "https://example.com/",
        "example.com",
        include_subdomains=include_subdomains,
    )

    assert result.final_url == "https://example.com/"
    assert result.redirect_urls == (target,)
    assert result.blocked_redirect == target
    assert result.body == b""


async def test_fetch_blocks_sensitive_redirect_without_retaining_target(monkeypatch):
    client, _, _ = install_client(
        monkeypatch,
        [
            FakeResponse(
                302,
                headers={"Location": "https://example.com/login?loginToken=secret"},
            )
        ],
    )

    result = await client.fetch(
        "https://example.com/", "example.com", include_subdomains=False
    )

    assert result.redirect_urls == ()
    assert result.blocked_redirect == "sensitive query"
    assert "secret" not in repr(result)


async def test_fetch_rejects_invalid_redirect_and_releases_slots(monkeypatch):
    client, _, _ = install_client(
        monkeypatch,
        [FakeResponse(302, headers={"Location": "mailto:test@example.com"})],
    )

    with pytest.raises(crawler.FetchError, match="absolute HTTP or HTTPS") as captured:
        await client.fetch(
            "https://example.com/", "example.com", include_subdomains=False
        )

    assert captured.value.transient is False
    assert client._global._value == client.settings.concurrency  # noqa: SLF001


async def test_fetch_rejects_redirect_limit(monkeypatch):
    responses = [
        FakeResponse(301, headers={"Location": "/two"}),
        FakeResponse(307, headers={"Location": "/three"}),
    ]
    client, _, _ = install_client(monkeypatch, responses, max_redirects=1)

    with pytest.raises(crawler.FetchError, match="redirect limit exceeded") as captured:
        await client.fetch(
            "https://example.com/one", "example.com", include_subdomains=False
        )

    assert captured.value.transient is False
    assert all(response.exited == 1 for response in responses)


async def test_fetch_defensive_assertion_for_impossible_negative_redirect_limit(
    monkeypatch,
):
    client, _, _ = install_client(monkeypatch, max_redirects=-1)

    with pytest.raises(AssertionError, match="redirect loop did not return"):
        await client.fetch(
            "https://example.com/", "example.com", include_subdomains=False
        )


async def test_fetch_classifies_missing_status_as_transient(monkeypatch):
    client, _, _ = install_client(monkeypatch, [FakeResponse(None)])

    with pytest.raises(
        crawler.FetchError, match="response has no HTTP status"
    ) as error:
        await client.fetch(
            "https://example.com/", "example.com", include_subdomains=False
        )

    assert error.value.transient is True


async def test_fetch_304_does_not_read_body(monkeypatch):
    response = FakeResponse(304, body=b"must not be read")
    client, _, _ = install_client(monkeypatch, [response])

    result = await client.fetch(
        "https://example.com/", "example.com", include_subdomains=False
    )

    assert result.body == b""
    assert response.chunk_sizes == []


async def test_fetch_decompresses_raw_query_bearing_gzip_sitemap(monkeypatch):
    xml = b"<urlset><url><loc>/found</loc></url></urlset>"
    client, _, _ = install_client(
        monkeypatch,
        [FakeResponse(body=gzip.compress(xml))],
        max_response_bytes=1000,
    )

    result = await client.fetch(
        "https://example.com/sitemap.xml.gz?v=1",
        "example.com",
        include_subdomains=False,
    )

    assert result.body == xml
    assert crawler.discoveries(result, 0) == (("https://example.com/found",), True)


async def test_fetch_does_not_double_decode_http_decoded_gzip_sitemap(monkeypatch):
    xml = b"<urlset><url><loc>/found</loc></url></urlset>"
    client, _, _ = install_client(
        monkeypatch,
        [FakeResponse(body=xml)],
        max_response_bytes=1000,
    )

    result = await client.fetch(
        "https://example.com/sitemap.xml.gz",
        "example.com",
        include_subdomains=False,
    )

    assert result.body == xml


def test_gzip_sitemap_decompression_is_bounded_and_rejects_invalid_data():
    exact = gzip.compress(b"x" * 100)
    assert crawler._decompress_gzip_bounded(exact, 100) == b"x" * 100  # noqa: SLF001

    concatenated = gzip.compress(b"x" * 50) + gzip.compress(b"y" * 51)
    with pytest.raises(crawler.ResponseTooLargeError, match="exceeds 100 bytes"):
        crawler._decompress_gzip_bounded(concatenated, 100)  # noqa: SLF001

    with pytest.raises(crawler.FetchError, match="invalid gzip sitemap") as error:
        crawler._decompress_gzip_bounded(b"\x1f\x8btruncated", 100)  # noqa: SLF001
    assert error.value.transient is False


@pytest.mark.parametrize("content_length", ["11", "999999"])
async def test_read_bounded_rejects_declared_oversize(content_length):
    response = FakeResponse(headers={"Content-Length": content_length}, body=b"small")

    with pytest.raises(
        crawler.ResponseTooLargeError, match="exceeds 10 bytes"
    ) as error:
        await crawler._read_bounded(response, 10)  # noqa: SLF001

    assert error.value.transient is False
    assert response.chunk_sizes == []


async def test_read_bounded_accepts_exact_limit_and_uses_capped_chunk_size():
    response = FakeResponse(
        headers={"Content-Length": "10"}, chunks=[b"1234", b"567890"]
    )

    body = await crawler._read_bounded(response, 10)  # noqa: SLF001

    assert body == b"1234567890"
    assert response.chunk_sizes == [11]


async def test_read_bounded_rejects_streamed_oversize():
    response = FakeResponse(
        headers={"Content-Length": "invalid"}, chunks=[b"12345", b"678901"]
    )

    with pytest.raises(crawler.ResponseTooLargeError, match="exceeds 10 bytes"):
        await crawler._read_bounded(response, 10)  # noqa: SLF001


async def test_read_bounded_classifies_stream_network_error():
    response = FakeResponse(
        chunks=[b"part", niquests.exceptions.ConnectionError("disconnected")]
    )

    with pytest.raises(crawler.FetchError, match="ConnectionError") as captured:
        await crawler._read_bounded(response, 10)  # noqa: SLF001

    assert captured.value.transient is True


async def test_read_bounded_classifies_stream_setup_network_error():
    response = FakeResponse(iter_error=niquests.exceptions.ConnectionError("setup"))

    with pytest.raises(crawler.FetchError, match="ConnectionError"):
        await crawler._read_bounded(response, 10)  # noqa: SLF001


async def test_fetch_releases_slots_when_response_context_is_cancelled(monkeypatch):
    response = FakeResponse(enter_error=asyncio.CancelledError())
    client, _, _ = install_client(monkeypatch, [response])

    with pytest.raises(asyncio.CancelledError):
        await client.fetch(
            "https://example.com/", "example.com", include_subdomains=False
        )

    assert client._global._value == client.settings.concurrency  # noqa: SLF001
    limit = client._hosts["example.com"]  # noqa: SLF001
    assert limit.semaphore._value == client.settings.per_host_concurrency  # noqa: SLF001


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (408, True),
        (425, True),
        (429, True),
        (500, True),
        (599, True),
        (499, False),
        (600, False),
    ],
)
def test_is_transient_status(status, expected):
    assert crawler.is_transient_status(status) is expected


def test_parse_retry_after_absent_seconds_dates_and_invalid():
    assert crawler.parse_retry_after(None, NOW) is None
    assert crawler.parse_retry_after(" 120 ", NOW) == NOW + timedelta(seconds=120)
    assert crawler.parse_retry_after("garbage", NOW) is None
    assert crawler.parse_retry_after("Fri, 28 Aug 2026 08:00:00 -0400", NOW) == NOW


def test_parse_retry_after_assumes_utc_for_naive_parser_result(monkeypatch):
    parsed = datetime(2026, 8, 28, 14, 30)  # noqa: DTZ001 - exercising naive input
    monkeypatch.setattr(
        crawler.email.utils, "parsedate_to_datetime", lambda value: parsed
    )

    assert crawler.parse_retry_after("date", NOW) == parsed.replace(tzinfo=UTC)


def test_parse_retry_after_rejects_unrepresentable_seconds():
    assert crawler.parse_retry_after("999999999999", NOW) is None


def test_retry_time_uses_full_jitter_caps_backoff_and_honors_retry_after():
    random = FakeRandom(7.5)
    retry_after = NOW + timedelta(minutes=2)

    result = crawler.retry_time(NOW, 20, retry_after, random)

    assert random.calls == [(0.0, 3600.0)]
    assert result == retry_after

    immediate = FakeRandom(0.5)
    assert crawler.retry_time(NOW, 0, None, immediate) == NOW + timedelta(seconds=0.5)
    assert immediate.calls == [(0.0, 1.0)]


def test_discoveries_merge_link_and_html_urls_without_duplicates():
    result = make_result(
        final_url="https://example.com/base/index.html",
        headers={
            "CONTENT-TYPE": " Text/HTML ; charset=latin-1 ",
            "Link": "</shared>; rel=next, </header>; rel=alternate",
            "link": "</second-header>; rel=canonical",
        },
        body=(
            b'<a href="/shared">same</a><area href="relative">map</area>'
            b'<link rel="canonical alternate" href="/canonical">'
        ),
        encoding=None,
    )

    found, is_sitemap = crawler.discoveries(result, 0)

    assert found == (
        "https://example.com/shared",
        "https://example.com/header",
        "https://example.com/second-header",
        "https://example.com/base/relative",
        "https://example.com/canonical",
    )
    assert is_sitemap is False


def test_discoveries_sniffs_doctype_without_content_type():
    result = make_result(
        headers={}, body=b" \n<!DOCTYPE HTML><a href='/found'>found</a>"
    )

    assert crawler.discoveries(result, 0) == (
        ("https://example.com/found",),
        False,
    )


def test_discoveries_falls_back_to_utf8_for_unknown_html_charset():
    result = make_result(
        encoding="definitely-not-a-codec",
        body=b'<!doctype html><a href="/found">found</a>',
    )

    assert crawler.discoveries(result, 0) == (
        ("https://example.com/found",),
        False,
    )


@pytest.mark.parametrize(
    ("content_type", "final_url"),
    [
        ("application/xml", "https://example.com/map"),
        ("text/xml", "https://example.com/map"),
        ("application/rss+xml", "https://example.com/map"),
        ("application/octet-stream", "https://example.com/MAP.XML"),
        (None, "https://example.com/map.xml.gz?version=1"),
    ],
)
def test_discoveries_parses_sitemap_types_and_suffixes(content_type, final_url):
    headers = {} if content_type is None else {"Content-Type": content_type}
    result = make_result(
        final_url=final_url,
        headers=headers,
        body=(
            b'<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
            b"<sitemap><loc>/nested.xml</loc></sitemap></sitemapindex>"
        ),
    )

    assert crawler.discoveries(result, 0) == (
        ("https://example.com/nested.xml",),
        True,
    )


def test_discoveries_marks_but_does_not_parse_sitemap_at_depth_limit():
    result = make_result(
        final_url="https://example.com/map.xml",
        headers={"Link": "</from-header>; rel=next"},
        body=b"not XML",
    )

    assert crawler.discoveries(result, crawler._MAX_SITEMAP_DEPTH) == (  # noqa: SLF001
        ("https://example.com/from-header",),
        True,
    )


def test_discoveries_ignores_non_html_non_sitemap_body():
    result = make_result(
        headers={"Content-Type": "application/json"},
        body=b'{"url": "https://example.com/not-discovered"}',
    )

    assert crawler.discoveries(result, 0) == ((), False)


def test_discoveries_propagates_invalid_sitemap_error():
    result = make_result(
        final_url="https://example.com/map.xml", headers={}, body=b"broken"
    )

    with pytest.raises(UrlError, match="invalid sitemap XML"):
        crawler.discoveries(result, 0)


async def test_crawl_page_success_updates_metadata_inserts_and_logs_scoped_urls(
    monkeypatch,
):
    page = make_page()
    logged = []
    monkeypatch.setattr(
        crawler.logger, "info", lambda message, url: logged.append((message, url))
    )
    result = make_result(
        final_url="https://example.com/final",
        headers={
            "content-type": "text/html; charset=utf-8",
            "ETAG": '"new"',
            "Link": "</header>; rel=next",
        },
        body=(
            b'<!doctype html><a href="/child">child</a>'
            b'<a href="https://api.example.com/sub">sub</a>'
            b'<a href="https://evil-example.com/out">out</a>'
        ),
        redirect_urls=(
            "https://example.com/child",
            "https://example.com/redirect",
            "https://example.com/redirect",
        ),
    )

    session, client, _ = await run_crawl(page, result)

    assert client.calls == [
        (
            ("https://example.com/start", "example.com"),
            {
                "include_subdomains": False,
                "etag": '"old"',
                "last_modified": "old-date",
            },
        )
    ]
    assert page.status_code == 200
    assert page.final_url == "https://example.com/final"
    assert page.content_type == "text/html"
    assert page.first_crawled_at == page.last_crawled_at == NOW
    assert page.etag == '"new"'
    assert page.last_modified == "old-date"
    assert page.error is None
    assert page.next_crawl_at == NOW + make_settings().interval
    assert page.crawl_attempts == 0
    assert page.crawl_lease_owner is None
    assert page.crawl_lease_expires_at is None
    assert session.commits == 1
    assert len(session.statements) == 1
    statement = session.statements[0]
    params = statement.compile().params
    assert [value for key, value in params.items() if key.startswith("url_m")] == [
        "https://example.com/child",
        "https://example.com/redirect",
        "https://example.com/header",
    ]
    assert logged == [
        ("Page found: {}", "https://example.com/child"),
        ("Page found: {}", "https://example.com/redirect"),
        ("Page found: {}", "https://example.com/header"),
    ]
    assert "ON CONFLICT DO NOTHING" in str(statement)


async def test_crawl_page_logs_only_urls_inserted_by_database(monkeypatch):
    page = make_page()
    session = FakeDatabaseSession(inserted_urls=("https://example.com/new",))
    logged = []
    monkeypatch.setattr(
        crawler.logger, "info", lambda message, url: logged.append((message, url))
    )
    result = make_result(
        body=(b'<!doctype html><a href="/existing">existing</a><a href="/new">new</a>')
    )

    await crawler.crawl_page(
        session,
        page,
        configured_host="example.com",
        include_subdomains=False,
        client=FakeClient(result),
        settings=make_settings(),
        now=NOW,
    )

    assert logged == [("Page found: {}", "https://example.com/new")]


async def test_crawl_page_allows_exact_subdomains_when_configured():
    page = make_page()
    result = make_result(
        body=(
            b'<!doctype html><a href="https://api.example.com/in">in</a>'
            b'<a href="https://notexample.com/out">out</a>'
            b'<a href="https://example.com.evil/out">out</a>'
        )
    )

    session, _, _ = await run_crawl(page, result, include_subdomains=True)
    params = session.statements[0].compile().params

    assert [value for key, value in params.items() if key.startswith("url_m")] == [
        "https://api.example.com/in"
    ]


async def test_crawl_page_sitemap_increments_found_depth_but_not_redirect_depth():
    page = make_page(sitemap_depth=2)
    result = make_result(
        final_url="https://example.com/map.xml",
        headers={"Content-Type": "application/xml"},
        body=b"<urlset><url><loc>/page</loc></url></urlset>",
        redirect_urls=("https://example.com/redirect",),
    )

    session, _, _ = await run_crawl(page, result)
    params = session.statements[0].compile().params

    assert [value for key, value in params.items() if key.startswith("url_m")] == [
        "https://example.com/redirect",
        "https://example.com/page",
    ]
    assert [
        value for key, value in params.items() if key.startswith("sitemap_depth_m")
    ] == [0, 3]


async def test_crawl_page_304_preserves_existing_metadata_and_skips_discovery():
    first = NOW - timedelta(days=2)
    page = make_page(first_crawled_at=first, crawl_attempts=2)
    result = make_result(
        status_code=304,
        headers={"ETag": '"same"'},
        body=b"",
        redirect_urls=("https://example.com/ignored",),
    )

    session, _, _ = await run_crawl(page, result)

    assert page.first_crawled_at == first
    assert page.last_crawled_at == NOW
    assert page.etag == '"same"'
    assert page.last_modified == "old-date"
    assert page.crawl_attempts == 0
    assert page.next_crawl_at == NOW + make_settings().interval
    assert session.statements == []
    assert session.commits == 1
    assert page.crawl_lease_owner is None


async def test_crawl_page_transient_status_honors_retry_after():
    page = make_page()
    result = make_result(
        status_code=429,
        headers={"Retry-After": "120", "Content-Type": "text/plain"},
        body=b"slow down",
    )

    session, _, random = await run_crawl(page, result)

    assert page.crawl_attempts == 1
    assert page.next_crawl_at == NOW + timedelta(seconds=120)
    assert random.calls == [(0.0, 1.0)]
    assert session.statements == []
    assert session.commits == 1


async def test_crawl_page_exhausted_transient_status_resets_attempts():
    page = make_page(crawl_attempts=2)

    session, _, random = await run_crawl(page, make_result(status_code=503))

    assert page.crawl_attempts == 0
    assert page.next_crawl_at == NOW + make_settings().interval
    assert random.calls == []
    assert session.commits == 1


async def test_crawl_page_permanent_status_and_blocked_redirect_schedule_interval():
    page = make_page(crawl_attempts=2)
    result = make_result(
        status_code=302,
        final_url="https://example.com/start",
        body=b"",
        redirect_urls=("https://outside.example/path",),
        blocked_redirect="https://outside.example/path",
    )

    session, _, random = await run_crawl(page, result)

    assert page.error == "redirect left scope: https://outside.example/path"
    assert page.crawl_attempts == 0
    assert page.next_crawl_at == NOW + make_settings().interval
    assert random.calls == []
    assert session.statements == []


@pytest.mark.parametrize(
    ("error", "initial_attempts", "expected_attempts", "expected_delay"),
    [
        (crawler.FetchError("timeout", transient=True), 0, 1, timedelta(seconds=0.25)),
        (crawler.FetchError("timeout", transient=True), 2, 0, timedelta(hours=6)),
        (crawler.FetchError("bad URL", transient=False), 1, 0, timedelta(hours=6)),
        (crawler.ResponseTooLargeError(10), 0, 0, timedelta(hours=6)),
    ],
)
async def test_crawl_page_fetch_errors_release_lease_and_schedule(
    error, initial_attempts, expected_attempts, expected_delay
):
    page = make_page(crawl_attempts=initial_attempts)

    session, _, random = await run_crawl(page, error)

    assert page.error == str(error)
    assert page.crawl_attempts == expected_attempts
    assert page.next_crawl_at == NOW + expected_delay
    assert page.crawl_lease_owner is None
    assert page.crawl_lease_expires_at is None
    assert session.statements == []
    assert session.commits == 1
    assert bool(random.calls) is (error.transient and initial_attempts == 0)


async def test_crawl_page_records_discovery_parse_error_without_inserting():
    page = make_page()
    result = make_result(
        final_url="https://example.com/map.xml",
        headers={"Content-Type": "application/xml"},
        body=b"not xml",
    )

    session, _, _ = await run_crawl(page, result)

    assert page.error == "invalid sitemap XML"
    assert page.status_code == 200
    assert page.next_crawl_at == NOW + make_settings().interval
    assert session.statements == []
    assert session.commits == 1


async def test_crawl_page_propagates_cancellation_without_committing():
    page = make_page()
    session = FakeDatabaseSession()

    with pytest.raises(asyncio.CancelledError):
        await crawler.crawl_page(
            session,
            page,
            configured_host="example.com",
            include_subdomains=False,
            client=FakeClient(asyncio.CancelledError()),
            settings=make_settings(),
            now=NOW,
        )

    assert session.commits == 0
    assert page.crawl_lease_owner == "worker"


def test_fetch_error_and_response_too_large_classification():
    transient = crawler.FetchError("temporary", transient=True)
    oversize = crawler.ResponseTooLargeError(42)

    assert str(transient) == "temporary"
    assert transient.transient is True
    assert str(oversize) == "response exceeds 42 bytes"
    assert oversize.transient is False


def test_system_random_source_delegates_uniform(monkeypatch):
    source = crawler._SystemRandomSource()  # noqa: SLF001
    fake = SimpleNamespace(uniform=lambda lower, upper: (lower + upper) / 2)
    source._source = fake  # noqa: SLF001

    assert source.uniform(2, 6) == 4
