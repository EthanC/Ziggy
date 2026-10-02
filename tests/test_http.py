from __future__ import annotations

import asyncio
import socket
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import niquests
import pytest
import uvicorn
from sqlalchemy import func, select
from sqlalchemy.exc import OperationalError
from starlette.requests import ClientDisconnect

from ziggy import service
from ziggy.archive import (
    FailedStatus,
    PendingStatus,
    SuccessStatus,
    check_archive_history,
    claim_archive_job,
    create_archive_intent,
    poll_archive_job,
    submit_archive_job,
)
from ziggy.config import ArchiveSettings, CrawlSettings
from ziggy.crawler import FetchResult, crawl_page
from ziggy.database import (
    claim_due_page,
    create_engine,
    run_migrations,
    session_factory,
)
from ziggy.http import routes
from ziggy.http.app import create_app
from ziggy.http.routes import MAX_BODY_BYTES, AdmissionRateLimiter
from ziggy.models import (
    ArchiveJob,
    ArchiveJobKind,
    ArchiveJobState,
    ArchiveSubmission,
    Domain,
    Page,
    ServiceState,
)


@pytest.fixture
async def http_server(tmp_path):
    path = tmp_path / "http.sqlite3"
    await run_migrations(path)
    app = create_app(path)
    listener = socket.socket()
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    port = listener.getsockname()[1]
    server = uvicorn.Server(
        uvicorn.Config(app, loop="asyncio", ws="none", access_log=False)
    )
    task = asyncio.create_task(server.serve(sockets=[listener]))
    for _ in range(100):
        if server.started:
            break
        await asyncio.sleep(0.01)
    assert server.started
    try:
        yield f"http://127.0.0.1:{port}", path, app
    finally:
        server.should_exit = True
        await task
        listener.close()


async def post_json(base_url, payload):
    client = niquests.AsyncSession()
    try:
        return await client.post(f"{base_url}/v1/queue", json=payload, timeout=5)
    finally:
        await client.close()


async def test_queue_accepts_canonical_deduplicated_batch_after_commit(http_server):
    base_url, path, _app = http_server
    response = await post_json(
        base_url,
        {
            "identifier": " external-service ",
            "urls": [
                "HTTPS://Example.org:443/a/../article#fragment",
                "https://example.org/article",
            ],
            "priority": 10,
        },
    )
    assert response.status_code == 202
    payload = response.json()
    assert len(payload["submissions"]) == 1
    assert payload["submissions"][0]["url"] == "https://example.org/article"

    engine = create_engine(path)
    try:
        sessions = session_factory(engine)
        async with sessions() as session:
            page = await session.scalar(select(Page))
            submission = await session.scalar(select(ArchiveSubmission))
            assert page is not None
            assert submission is not None
            assert (page.domain_id, page.in_scope) == (None, False)
            assert (submission.identifier, submission.priority) == (
                "external-service",
                10,
            )
            assert submission.id == payload["submissions"][0]["receipt_id"]
    finally:
        await engine.dispose()


async def test_workers_admission_heartbeat_and_archive_restart_together(  # noqa: PLR0915
    http_server, monkeypatch
):
    base_url, path, _app = http_server
    engine = create_engine(path)
    sessions = session_factory(engine)
    now = datetime.now(UTC)
    ready = asyncio.Event()
    stop = asyncio.Event()
    arrivals = 0
    monkeypatch.setattr(service, "_HEARTBEAT_INTERVAL", 0.01)

    async def fetch(url, *args, **kwargs):
        nonlocal arrivals
        arrivals += 1
        if arrivals == 8:
            ready.set()
        await ready.wait()
        body = f'<a href="/variant?p={url.rsplit("/", 1)[-1]}">child</a>'.encode()
        return FetchResult(200, url, {"Content-Type": "text/html"}, body, None, ())

    try:
        async with sessions() as session:
            domain = Domain(
                host="example.com", scheme="https", include_subdomains=False
            )
            session.add(domain)
            session.add(
                ServiceState(instance_id="live", started_at=now, heartbeat_at=now)
            )
            await session.flush()
            pages = [
                Page(
                    domain_id=domain.id, url=f"https://example.com/{i}", status_code=200
                )
                for i in range(10)
            ]
            session.add_all(pages)
            await session.flush()
            session.add(
                ArchiveJob(
                    id="polling",
                    page_id=pages[9].id,
                    kind=ArchiveJobKind.DIRECT,
                    state=ArchiveJobState.PENDING,
                    cycle_key="polling",
                    external_job_id="remote",
                )
            )
            await session.commit()
            page_ids = [page.id for page in pages]
            domain_id = domain.id

        async def crawl(page_id):
            async with sessions() as session:
                page = await session.get(Page, page_id)
                await crawl_page(
                    session,
                    page,
                    configured_host="example.com",
                    include_subdomains=False,
                    client=SimpleNamespace(fetch=fetch),
                    settings=CrawlSettings(
                        concurrency=8, max_query_variants_per_base=2
                    ),
                    now=now,
                )

        async def history():
            await ready.wait()
            async with sessions() as session:
                page = await session.get(Page, page_ids[8])
                await check_archive_history(
                    session,
                    page,
                    SimpleNamespace(latest_capture_at=AsyncMock(return_value=None)),
                    now,
                )

        async def poll():
            await ready.wait()
            async with sessions() as session:
                job = await session.get(ArchiveJob, "polling")
                page = await session.get(Page, page_ids[9])
                domain = await session.get(Domain, domain_id)
                await poll_archive_job(
                    session,
                    job,
                    page=page,
                    domain=domain,
                    client=SimpleNamespace(
                        status=AsyncMock(return_value=PendingStatus("remote", None))
                    ),
                    settings=ArchiveSettings(),
                    now=now,
                )

        async def admit():
            await ready.wait()
            return await post_json(
                base_url,
                {
                    "identifier": "concurrent",
                    "urls": ["https://outside.example/"],
                    "priority": 5,
                },
            )

        heartbeat = asyncio.create_task(service._heartbeat(sessions, "live", stop))  # noqa: SLF001
        try:
            *_, response = await asyncio.wait_for(
                asyncio.gather(
                    *(crawl(page_id) for page_id in page_ids[:8]),
                    history(),
                    poll(),
                    admit(),
                ),
                10,
            )
        finally:
            stop.set()
            await heartbeat
        assert response.status_code == 202

        async with sessions() as session:
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(Page)
                    .where(Page.query_variant_slot.is_not(None))
                )
                == 2
            )
            assert (await session.get(ServiceState, "live")).heartbeat_at > now
            assert (
                await session.get(Page, page_ids[8])
            ).archive_history_checked_at == now
            claimed = await claim_due_page(
                session,
                "archive",
                "worker",
                now,
                timedelta(minutes=5),
                archive_interval=timedelta(days=30),
            )
            assert claimed.url == "https://outside.example/"
            job = await create_archive_intent(session, claimed, now, archive_only=True)
            await submit_archive_job(
                session,
                job,
                page=claimed,
                client=SimpleNamespace(submit=AsyncMock(return_value="api-remote")),
                settings=ArchiveSettings(),
                now=now,
            )
            await poll_archive_job(
                session,
                job,
                page=claimed,
                domain=None,
                client=SimpleNamespace(
                    status=AsyncMock(
                        return_value=FailedStatus("api-remote", "error:no-request")
                    )
                ),
                settings=ArchiveSettings(),
                now=now,
            )
            job_id = job.id

        async with sessions() as session:
            job = await claim_archive_job(
                session, "restart", now + timedelta(minutes=15), timedelta(minutes=5)
            )
            assert job.id == job_id
            page = await session.get(Page, job.page_id)
            capture = SuccessStatus(
                "history", page.url, now, "https://web.archive.org/capture", None, None
            )
            client = SimpleNamespace(
                captures_since=AsyncMock(return_value=(capture,)), submit=AsyncMock()
            )
            await submit_archive_job(
                session,
                job,
                page=page,
                client=client,
                settings=ArchiveSettings(),
                now=now + timedelta(minutes=15),
            )
            client.submit.assert_not_awaited()
            assert job.state is ArchiveJobState.SUCCEEDED
            assert (
                await session.scalar(select(ArchiveSubmission))
            ).archive_job_id == job_id
    finally:
        await engine.dispose()


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"identifier": "caller", "urls": []},
        {"identifier": "caller", "urls": ["relative"]},
        {"identifier": "caller", "urls": ["https://user:pass@example.org/"]},
        {"identifier": "caller", "urls": ["https://example.org/?token=secret"]},
        {"identifier": "caller", "urls": ["https://example.org/\nnext"]},
        {"identifier": "caller", "urls": ["https://example.org/not valid"]},
        {"identifier": "caller", "urls": ["https://example.org/\x00"]},
        {"identifier": "caller", "urls": ["https://example.org/\x7f"]},
        {"identifier": "caller", "urls": ["https://example.org/\u0080"]},
        {"identifier": "caller", "urls": ["https://example.org/\ud800"]},
        {"identifier": "caller", "urls": ["https://example.org/"], "priority": 101},
    ],
)
async def test_queue_rejects_invalid_batches_without_writes(http_server, payload):
    base_url, path, _app = http_server
    response = await post_json(base_url, payload)
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "invalid_request"

    engine = create_engine(path)
    try:
        async with session_factory(engine)() as session:
            assert await session.scalar(select(func.count()).select_from(Page)) == 0
    finally:
        await engine.dispose()


@pytest.mark.parametrize(
    "body",
    [
        pytest.param(b"{", id="malformed-json"),
        pytest.param(b"\xff", id="invalid-utf8"),
        pytest.param(
            b'{"identifier":"caller","urls":["https://example.org/"],"priority":'
            + b"9" * 5000
            + b"}",
            id="oversized-integer",
        ),
    ],
)
async def test_queue_rejects_undecodable_json_without_writes(http_server, body):
    base_url, path, _app = http_server
    client = niquests.AsyncSession()
    try:
        response = await client.post(
            f"{base_url}/v1/queue",
            data=body,
            headers={"Content-Type": "application/json"},
            timeout=5,
        )
    finally:
        await client.close()
    assert response.status_code == 422
    assert response.json()["error"] == {
        "code": "invalid_request",
        "message": "request body must be valid JSON",
    }

    engine = create_engine(path)
    try:
        async with session_factory(engine)() as session:
            for model in (Page, ArchiveSubmission):
                assert (
                    await session.scalar(select(func.count()).select_from(model)) == 0
                )
    finally:
        await engine.dispose()


@pytest.mark.parametrize("identifier", ["\x00caller", "call\x00er", "caller\x00"])
async def test_queue_rejects_nul_identifiers_without_writes(http_server, identifier):
    base_url, path, _app = http_server
    response = await post_json(
        base_url, {"identifier": identifier, "urls": ["https://example.org/"]}
    )
    assert response.status_code == 422
    assert response.json()["error"] == {
        "code": "invalid_request",
        "message": "identifier must not contain NUL characters",
    }

    engine = create_engine(path)
    try:
        async with session_factory(engine)() as session:
            for model in (Page, ArchiveSubmission):
                assert (
                    await session.scalar(select(func.count()).select_from(model)) == 0
                )
    finally:
        await engine.dispose()


async def test_queue_enforces_body_media_rate_and_transaction_limits(http_server):
    base_url, _path, app = http_server
    client = niquests.AsyncSession()
    try:
        media = await client.post(f"{base_url}/v1/queue", data=b"{}", timeout=5)
        assert media.status_code == 415

        oversized = await client.post(
            f"{base_url}/v1/queue",
            data=b"{" + b" " * MAX_BODY_BYTES + b"}",
            headers={"Content-Type": "application/json"},
            timeout=5,
        )
        assert oversized.status_code == 413

        app.state.rate_limiter = AdmissionRateLimiter(1)
        valid = {"identifier": "caller", "urls": ["https://example.org/"]}
        assert (
            await client.post(f"{base_url}/v1/queue", json=valid)
        ).status_code == 202
        limited = await client.post(f"{base_url}/v1/queue", json=valid)
        assert limited.status_code == 429
        assert limited.headers["Retry-After"] == "60"

        app.state.rate_limiter = AdmissionRateLimiter()
        await app.state.admission_lock.acquire()
        overloaded = await client.post(f"{base_url}/v1/queue", json=valid)
        app.state.admission_lock.release()
        assert overloaded.status_code == 503
        assert overloaded.json()["error"]["code"] == "overloaded"
    finally:
        await client.close()


async def test_concurrent_submissions_share_one_page_and_keep_attribution(http_server):
    base_url, path, _app = http_server
    responses = await asyncio.gather(
        post_json(
            base_url,
            {"identifier": "first", "urls": ["https://shared.example/"]},
        ),
        post_json(
            base_url,
            {"identifier": "second", "urls": ["https://shared.example/"]},
        ),
    )
    accepted = sum(response.status_code == 202 for response in responses)
    assert accepted in {1, 2}
    assert all(response.status_code in {202, 503} for response in responses)

    engine = create_engine(path)
    try:
        async with session_factory(engine)() as session:
            assert await session.scalar(select(func.count()).select_from(Page)) == 1
            assert (
                await session.scalar(
                    select(func.count()).select_from(ArchiveSubmission)
                )
                == accepted
            )
    finally:
        await engine.dispose()


@pytest.mark.parametrize(
    "payload",
    [
        [],
        {"identifier": "caller", "urls": ["https://example.org/"], "extra": 1},
        {"identifier": "x" * 129, "urls": ["https://example.org/"]},
        {"identifier": "caller\ud800", "urls": ["https://example.org/"]},
        {"identifier": "caller", "urls": ["https://example.org/"] * 101},
        {"identifier": "caller", "urls": ["https://example.org/"], "priority": True},
        {"identifier": "caller", "urls": [1]},
        {"identifier": "caller", "urls": ["https://example.org/" + "x" * 8192]},
    ],
)
def test_payload_validation_covers_public_bounds(payload):
    with pytest.raises(routes.RequestValidationError):
        routes.validate_payload(payload)


def test_rate_limiter_expires_old_entries():
    limiter = AdmissionRateLimiter(1)
    assert limiter.allow(10.0) is True
    assert limiter.allow(10.0) is False
    assert limiter.allow(70.0) is True


async def test_queue_maps_capacity_storage_timeout_and_disconnect_errors(
    http_server, monkeypatch
):
    base_url, _path, app = http_server
    client = niquests.AsyncSession()
    valid = {"identifier": "caller", "urls": ["https://example.org/"]}
    original_admit = routes.admit_archive_submissions
    original_read = routes.read_limited_body
    try:

        async def capacity(*_args, **_kwargs):
            raise routes.AdmissionCapacityError("full")

        monkeypatch.setattr(routes, "admit_archive_submissions", capacity)
        assert (
            await client.post(f"{base_url}/v1/queue", json=valid)
        ).status_code == 503

        async def locked(*_args, **_kwargs):
            await app.state.admission_lock.acquire()
            return b'{"identifier":"caller","urls":["https://example.org/"]}'

        monkeypatch.setattr(routes, "admit_archive_submissions", original_admit)
        monkeypatch.setattr(routes, "read_limited_body", locked)
        response = await client.post(f"{base_url}/v1/queue", json=valid)
        assert response.status_code == 503
        app.state.admission_lock.release()

        async def unavailable(*_args, **_kwargs):
            raise OperationalError("insert", {}, OSError("locked"))

        monkeypatch.setattr(routes, "read_limited_body", original_read)
        monkeypatch.setattr(routes, "admit_archive_submissions", unavailable)
        response = await client.post(f"{base_url}/v1/queue", json=valid)
        assert response.status_code == 503
        assert response.json()["error"]["code"] == "temporarily_unavailable"

        async def timed_out(*_args, **_kwargs):
            raise TimeoutError

        monkeypatch.setattr(routes, "read_limited_body", timed_out)
        response = await client.post(f"{base_url}/v1/queue", json=valid)
        assert response.status_code == 503

        async def disconnected(*_args, **_kwargs):
            raise ClientDisconnect

        monkeypatch.setattr(routes, "read_limited_body", disconnected)
        response = await client.post(f"{base_url}/v1/queue", json=valid)
        assert response.status_code == 400
    finally:
        if app.state.admission_lock.locked():
            app.state.admission_lock.release()
        await client.close()
