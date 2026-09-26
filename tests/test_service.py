from __future__ import annotations

import asyncio
import signal
import sqlite3
from contextlib import AbstractAsyncContextManager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, call

import pytest
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from ziggy import service
from ziggy.archive import (
    ArchiveAuthenticationError,
    ArchiveError,
    ArchiveJobState,
    ArchiveRateLimitError,
)
from ziggy.config import (
    ArchiveSettings,
    Config,
    ConfigError,
    CrawlSettings,
    DomainSettings,
    HttpSettings,
    LoggingSettings,
    ReportingSettings,
    Secrets,
    ZiggySettings,
)
from ziggy.crawler import FetchError, FetchResult
from ziggy.models import Base, Domain, Page, ServiceState

# This suite deliberately covers module-private orchestration boundaries.
# ruff: noqa: FBT003, S106, SLF001


NOW = datetime(2026, 8, 28, 12, tzinfo=UTC)


def database_lock(message: str = "database is locked") -> OperationalError:
    return OperationalError("UPDATE example", {}, sqlite3.OperationalError(message))


def make_config(database: Path = Path("ziggy.sqlite3"), **changes: object) -> Config:
    config = Config(
        ziggy=ZiggySettings(database, timedelta(seconds=1)),
        crawl=CrawlSettings(concurrency=2),
        archive=ArchiveSettings(),
        reporting=ReportingSettings(interval=timedelta(hours=1)),
        logging=LoggingSettings(),
        domains=(DomainSettings("example.test"),),
    )
    return replace(config, **changes)


def make_secrets(**changes: object) -> Secrets:
    values = {
        "archive_email": "user@example.com",
        "archive_password": "password",
        "reporting_webhook_url": None,
        "logging_webhook_url": None,
    }
    values.update(changes)
    return Secrets(**values)


class SessionContext(AbstractAsyncContextManager):
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, exc_type, exc_value, traceback):
        return False


class Sessions:
    def __init__(self, *sessions):
        self.sessions = sessions
        self.calls = 0

    def __call__(self):
        index = min(self.calls, len(self.sessions) - 1)
        self.calls += 1
        return SessionContext(self.sessions[index])


class SequencedStop:
    def __init__(self, *answers: bool):
        self.answers = iter(answers)
        self.was_set = False

    def is_set(self):
        return next(self.answers, True)

    def set(self):
        self.was_set = True


class FakeChildProcess:
    def __init__(self, *, alive=True, start_error=None, ignore_terminate=False):
        self.alive = alive
        self.start_error = start_error
        self.ignore_terminate = ignore_terminate
        self.exitcode = 7
        self.started = False
        self.terminated = False
        self.killed = False
        self.joins = []

    def start(self):
        if self.start_error is not None:
            raise self.start_error
        self.started = True

    def is_alive(self):
        return self.alive

    def join(self, timeout=None):
        self.joins.append(timeout)

    def terminate(self):
        self.terminated = True
        if not self.ignore_terminate:
            self.alive = False

    def kill(self):
        self.killed = True
        self.alive = False


def make_state(config: Config | None = None, secrets: Secrets | None = None):
    return service.RuntimeState(
        config or make_config(),
        secrets or make_secrets(),
        SimpleNamespace(
            close=AsyncMock(),
            latest_capture_at=AsyncMock(return_value=None),
            submission_capacity=AsyncMock(return_value=None),
            my_web_archive_url=AsyncMock(
                return_value="https://archive.org/details/@ziggy/web-archive"
            ),
        ),
        SimpleNamespace(close=AsyncMock(), fetch=AsyncMock()),
        SimpleNamespace(configure=MagicMock()),
        web_archive_url="https://archive.org/details/@ziggy/web-archive",
    )


async def test_runtime_state_closes_current_and_retired_clients():
    state = make_state()
    retired_archive = SimpleNamespace(close=AsyncMock())
    retired_crawler = SimpleNamespace(close=AsyncMock())
    state.retired_archive_clients.append(retired_archive)
    state.retired_crawlers.append(retired_crawler)

    await state.close()

    state.archive_client.close.assert_awaited_once_with()
    retired_archive.close.assert_awaited_once_with()
    state.crawler.close.assert_awaited_once_with()
    retired_crawler.close.assert_awaited_once_with()


async def test_wait_returns_when_event_is_set_and_when_timeout_expires():
    stopped = asyncio.Event()
    stopped.set()
    await service._wait(stopped, 10)

    running = asyncio.Event()
    await service._wait(running, 0)
    assert not running.is_set()


async def test_resilient_worker_retries_database_lock(monkeypatch):
    worker = AsyncMock(side_effect=[database_lock(), None])
    stop = asyncio.Event()
    wait = AsyncMock()
    warning = MagicMock()
    monkeypatch.setattr(service, "_wait", wait)
    monkeypatch.setattr(service.logger, "warning", warning)

    await service._run_resilient_worker("worker", worker, stop)

    assert worker.await_count == 2
    wait.assert_awaited_once_with(stop, service._IDLE_DELAY)
    warning.assert_called_once_with("Transient SQLite lock in {}; retrying", "worker")


async def test_resilient_worker_stops_after_nested_database_lock(monkeypatch):
    lock = ExceptionGroup("worker failed", [database_lock("database table is locked")])
    worker = AsyncMock(side_effect=lock)
    stop = asyncio.Event()

    async def stop_wait(event, seconds):
        assert event is stop
        assert seconds == service._IDLE_DELAY
        event.set()

    monkeypatch.setattr(service, "_wait", stop_wait)

    await service._run_resilient_worker("worker", worker, stop)

    worker.assert_awaited_once_with()


@pytest.mark.parametrize(
    "error",
    [
        RuntimeError("failure"),
        OperationalError("SELECT 1", {}, sqlite3.OperationalError("disk I/O error")),
        OperationalError("SELECT 1", {}, RuntimeError("database is locked")),
        ExceptionGroup("mixed", [database_lock(), RuntimeError("failure")]),
    ],
)
async def test_resilient_worker_preserves_non_lock_errors(error):
    with pytest.raises(type(error)):
        await service._run_resilient_worker(
            "worker", AsyncMock(side_effect=error), asyncio.Event()
        )


def test_signal_handlers_set_stop_and_are_removed(monkeypatch):
    loop = MagicMock()
    callbacks = {}

    def add_handler(signum, callback):
        callbacks[signum] = callback

    loop.add_signal_handler.side_effect = add_handler
    monkeypatch.setattr(service.asyncio, "get_running_loop", lambda: loop)
    stop = MagicMock()

    remove = service._install_signal_handlers(stop)
    callbacks[signal.SIGINT]()
    remove()

    stop.set.assert_called_once_with()
    assert loop.remove_signal_handler.call_args_list == [
        call(signal.SIGINT),
        call(signal.SIGTERM),
    ]


def test_signal_handlers_fall_back_on_platform_without_loop_support(monkeypatch):
    loop = MagicMock()
    loop.add_signal_handler.side_effect = NotImplementedError
    loop.call_soon_threadsafe.side_effect = lambda callback: callback()
    installed = {}
    monkeypatch.setattr(service.asyncio, "get_running_loop", lambda: loop)
    monkeypatch.setattr(service.signal, "signal", installed.__setitem__)
    stop = MagicMock()

    remove = service._install_signal_handlers(stop)
    installed[signal.SIGTERM](signal.SIGTERM, None)
    remove()

    loop.call_soon_threadsafe.assert_called_once()
    stop.set.assert_called_once_with()
    loop.remove_signal_handler.assert_not_called()


async def test_check_health_handles_absence_empty_recent_and_stale_database(
    monkeypatch, tmp_path
):
    path = tmp_path / "health.sqlite3"
    log_error = MagicMock()
    monkeypatch.setattr(service.logger, "error", log_error)

    assert await service.check_health(path, NOW) is False
    log_error.assert_called_once_with(
        "Health check failed: database does not exist at {}", path
    )

    engine = create_async_engine(f"sqlite+aiosqlite:///{path.as_posix()}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    assert await service.check_health(path, NOW) is False
    log_error.assert_called_with("Health check failed: no service heartbeat found")

    async with factory() as session:
        session.add(ServiceState(instance_id="old", started_at=NOW, heartbeat_at=NOW))
        await session.commit()
    assert await service.check_health(path, NOW + timedelta(seconds=90)) is True
    assert await service.check_health(path, NOW + timedelta(seconds=91)) is False
    log_error.assert_called_with(
        "Health check failed: service heartbeat is {:.0f}s old", 91.0
    )
    assert log_error.call_count == 3
    await engine.dispose()


async def test_check_health_returns_false_on_database_error(monkeypatch, tmp_path):
    path = tmp_path / "broken.sqlite3"
    path.touch()
    log_exception = MagicMock()
    session = MagicMock()
    session.scalar = AsyncMock(side_effect=SQLAlchemyError("broken"))
    engine = SimpleNamespace(dispose=AsyncMock())
    monkeypatch.setattr(service.logger, "exception", log_exception)
    monkeypatch.setattr(service, "create_async_engine", lambda _url: engine)
    monkeypatch.setattr(
        service, "async_sessionmaker", lambda *_args, **_kwargs: Sessions(session)
    )

    assert await service.check_health(path, NOW) is False
    log_exception.assert_called_once_with(
        "Health check failed while reading the service heartbeat"
    )
    engine.dispose.assert_awaited_once_with()


async def test_check_health_requires_enabled_http_listener(monkeypatch, tmp_path):
    path = tmp_path / "http-health.sqlite3"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path.as_posix()}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        session.add(ServiceState(instance_id="live", started_at=NOW, heartbeat_at=NOW))
        await session.commit()
    check_http = AsyncMock(side_effect=[True, False])
    monkeypatch.setattr(service, "_check_http_health", check_http)
    settings = HttpSettings(enabled=True)

    assert await service.check_health(path, NOW, http=HttpSettings()) is True
    check_http.assert_not_awaited()
    assert await service.check_health(path, NOW, http=settings) is True
    assert await service.check_health(path, NOW, http=settings) is False
    assert check_http.await_count == 2
    await engine.dispose()


@pytest.mark.parametrize(
    ("configured_host", "connection_host"),
    [
        ("0.0.0.0", "127.0.0.1"),  # noqa: S104
        ("::", "::1"),
        ("localhost", "localhost"),
    ],
)
async def test_http_health_probe_maps_bind_hosts_and_requires_405(
    monkeypatch, configured_host, connection_host
):
    reader = SimpleNamespace(
        readline=AsyncMock(return_value=b"HTTP/1.1 405 Method Not Allowed\r\n")
    )
    writer = SimpleNamespace(
        write=MagicMock(),
        drain=AsyncMock(),
        close=MagicMock(),
        wait_closed=AsyncMock(),
    )
    connect = AsyncMock(return_value=(reader, writer))
    monkeypatch.setattr(service.asyncio, "open_connection", connect)

    assert (
        await service._check_http_health(
            HttpSettings(enabled=True, host=configured_host, port=9449)
        )
        is True
    )
    connect.assert_awaited_once_with(connection_host, 9449)
    assert writer.write.call_args.args[0].startswith(b"GET /v1/queue HTTP/1.1\r\n")
    writer.drain.assert_awaited_once_with()
    writer.close.assert_called_once_with()
    writer.wait_closed.assert_awaited_once_with()


async def test_http_health_probe_rejects_bad_response_and_connection_error(
    monkeypatch,
):
    reader = SimpleNamespace(readline=AsyncMock(return_value=b"invalid\r\n"))
    writer = SimpleNamespace(
        write=MagicMock(),
        drain=AsyncMock(),
        close=MagicMock(),
        wait_closed=AsyncMock(side_effect=OSError("closed")),
    )
    monkeypatch.setattr(
        service.asyncio,
        "open_connection",
        AsyncMock(return_value=(reader, writer)),
    )
    settings = HttpSettings(enabled=True)
    assert await service._check_http_health(settings) is False

    monkeypatch.setattr(
        service.asyncio,
        "open_connection",
        AsyncMock(side_effect=OSError("refused")),
    )
    assert await service._check_http_health(settings) is False


async def test_heartbeat_updates_and_commits_once(monkeypatch):
    session = MagicMock(execute=AsyncMock(), commit=AsyncMock())
    stop = asyncio.Event()

    async def stop_wait(event, seconds):
        assert seconds == service._HEARTBEAT_INTERVAL
        event.set()

    monkeypatch.setattr(service, "_wait", stop_wait)
    await service._heartbeat(Sessions(session), "instance", stop)

    session.execute.assert_awaited_once()
    session.commit.assert_awaited_once_with()


async def test_run_service_owns_startup_tasks_and_cleanup(  # noqa: PLR0915
    monkeypatch, tmp_path
):
    config = make_config(tmp_path / "data" / "ziggy.sqlite3")
    secrets = make_secrets()
    logging_controller = SimpleNamespace(configure=MagicMock(), close=AsyncMock())
    archive_client = SimpleNamespace(
        login=AsyncMock(),
        my_web_archive_url=AsyncMock(
            return_value="https://archive.org/details/@ziggy/web-archive"
        ),
        close=AsyncMock(),
    )
    crawler = SimpleNamespace(close=AsyncMock())
    engine = SimpleNamespace(dispose=AsyncMock())
    session = MagicMock(add=MagicMock(), commit=AsyncMock(), execute=AsyncMock())
    sessions = Sessions(session)
    remove_signals = MagicMock()
    heartbeat_started = asyncio.Event()
    monkeypatch.setattr(service, "load_config", MagicMock(return_value=config))
    monkeypatch.setattr(service, "resolve_secrets", MagicMock(return_value=secrets))
    http_settings = HttpSettings(enabled=True)
    http_child = service.HttpChild(FakeChildProcess(alive=False), MagicMock())
    start_http = MagicMock(return_value=http_child)
    monitor_http = AsyncMock()
    stop_http = AsyncMock()
    monkeypatch.setattr(
        service, "resolve_http_settings", MagicMock(return_value=http_settings)
    )
    monkeypatch.setattr(service, "_start_http_child", start_http)
    monkeypatch.setattr(service, "_monitor_http_child", monitor_http)
    monkeypatch.setattr(service, "_stop_http_child", stop_http)
    monkeypatch.setattr(
        service, "LoggingController", MagicMock(return_value=logging_controller)
    )
    monkeypatch.setattr(service, "run_migrations", AsyncMock())
    monkeypatch.setattr(service, "create_engine", MagicMock(return_value=engine))
    monkeypatch.setattr(service, "session_factory", MagicMock(return_value=sessions))
    monkeypatch.setattr(
        service, "ArchivistClient", MagicMock(return_value=archive_client)
    )
    monkeypatch.setattr(service, "CrawlerClient", MagicMock(return_value=crawler))
    monkeypatch.setattr(service, "uuid4", lambda: "fixed-instance")
    monkeypatch.setattr(
        service, "_install_signal_handlers", MagicMock(return_value=remove_signals)
    )

    async def reconcile(*_args):
        await asyncio.sleep(0)
        assert session.add.called
        assert heartbeat_started.is_set()

    monkeypatch.setattr(service, "reconcile_domains", AsyncMock(side_effect=reconcile))
    release = AsyncMock()
    monkeypatch.setattr(service, "release_leases", release)
    workers = [
        "_config_watcher",
        "_crawl_scheduler",
        "_archive_history_scheduler",
        "_archive_submission_scheduler",
        "_archive_poll_scheduler",
        "_report_scheduler",
    ]
    worker_mocks = {}
    for name in workers:
        worker_mocks[name] = AsyncMock()
        monkeypatch.setattr(service, name, worker_mocks[name])
    worker_mocks["_config_watcher"].side_effect = [database_lock(), None]

    async def heartbeat(*_args):
        heartbeat_started.set()

    heartbeat_mock = AsyncMock(side_effect=heartbeat)
    monkeypatch.setattr(service, "_heartbeat", heartbeat_mock)
    monkeypatch.setattr(service, "_wait", AsyncMock())

    await service.run_service(tmp_path / "ziggy.toml")

    logging_controller.configure.assert_called_once_with(config.logging, secrets)
    start_http.assert_called_once_with(http_settings, config.ziggy.database)
    monitor_http.assert_awaited_once()
    stop_http.assert_awaited_once_with(http_child)
    archive_client.login.assert_awaited_once_with()
    archive_client.my_web_archive_url.assert_awaited_once_with()
    assert session.add.call_args.args[0].instance_id == "fixed-instance"
    for name, worker in worker_mocks.items():
        if name == "_config_watcher":
            assert worker.await_count == 2
            continue
        worker.assert_awaited_once()
    heartbeat_mock.assert_awaited_once()
    remove_signals.assert_called_once_with()
    release.assert_awaited_once_with(session, "fixed-instance")
    assert session.execute.await_count == 2
    archive_client.close.assert_awaited_once_with()
    crawler.close.assert_awaited_once_with()
    engine.dispose.assert_awaited_once_with()
    logging_controller.close.assert_awaited_once_with()


async def test_run_service_logs_runtime_exception_group(monkeypatch, tmp_path):
    config = make_config(tmp_path / "ziggy.sqlite3")
    logging_controller = SimpleNamespace(configure=MagicMock(), close=AsyncMock())
    archive_client = SimpleNamespace(
        login=AsyncMock(),
        my_web_archive_url=AsyncMock(return_value=None),
        close=AsyncMock(),
    )
    crawler = SimpleNamespace(close=AsyncMock())
    engine = SimpleNamespace(dispose=AsyncMock())
    session = MagicMock(add=MagicMock(), commit=AsyncMock(), execute=AsyncMock())
    remove_signals = MagicMock()
    runtime_error = RuntimeError("worker failed")
    log_exception = MagicMock()
    monkeypatch.setattr(service, "load_config", MagicMock(return_value=config))
    monkeypatch.setattr(
        service, "resolve_secrets", MagicMock(return_value=make_secrets())
    )
    monkeypatch.setattr(
        service, "LoggingController", MagicMock(return_value=logging_controller)
    )
    monkeypatch.setattr(service, "run_migrations", AsyncMock())
    monkeypatch.setattr(service, "create_engine", MagicMock(return_value=engine))
    monkeypatch.setattr(
        service, "session_factory", MagicMock(return_value=Sessions(session))
    )
    monkeypatch.setattr(
        service, "ArchivistClient", MagicMock(return_value=archive_client)
    )
    monkeypatch.setattr(service, "CrawlerClient", MagicMock(return_value=crawler))
    monkeypatch.setattr(
        service, "_install_signal_handlers", MagicMock(return_value=remove_signals)
    )
    monkeypatch.setattr(service, "reconcile_domains", AsyncMock())
    monkeypatch.setattr(service, "release_leases", AsyncMock())
    monkeypatch.setattr(service.logger, "exception", log_exception)
    monkeypatch.setattr(
        service, "_config_watcher", AsyncMock(side_effect=runtime_error)
    )
    for name in (
        "_crawl_scheduler",
        "_archive_history_scheduler",
        "_archive_submission_scheduler",
        "_archive_poll_scheduler",
        "_report_scheduler",
        "_heartbeat",
    ):
        monkeypatch.setattr(service, name, AsyncMock())

    with pytest.raises(ExceptionGroup) as raised:
        await service.run_service(tmp_path / "ziggy.toml")

    assert runtime_error in raised.value.exceptions
    log_exception.assert_called_once_with("Ziggy service failed")
    logging_controller.close.assert_awaited_once_with()


async def test_run_service_cleans_resources_when_archive_login_fails(
    monkeypatch, tmp_path
):
    config = make_config(tmp_path / "ziggy.sqlite3")
    logging_controller = SimpleNamespace(configure=MagicMock(), close=AsyncMock())
    archive_client = SimpleNamespace(
        login=AsyncMock(side_effect=ArchiveAuthenticationError("invalid")),
        close=AsyncMock(),
    )
    engine = SimpleNamespace(dispose=AsyncMock())
    monkeypatch.setattr(service, "load_config", MagicMock(return_value=config))
    monkeypatch.setattr(
        service, "resolve_secrets", MagicMock(return_value=make_secrets())
    )
    monkeypatch.setattr(
        service, "LoggingController", MagicMock(return_value=logging_controller)
    )
    monkeypatch.setattr(service, "run_migrations", AsyncMock())
    monkeypatch.setattr(service, "create_engine", MagicMock(return_value=engine))
    monkeypatch.setattr(service, "session_factory", MagicMock())
    monkeypatch.setattr(
        service, "ArchivistClient", MagicMock(return_value=archive_client)
    )

    with pytest.raises(ArchiveAuthenticationError):
        await service.run_service(tmp_path / "ziggy.toml")

    archive_client.close.assert_awaited_once_with()
    engine.dispose.assert_awaited_once_with()
    logging_controller.close.assert_awaited_once_with()


async def test_run_service_cleans_logging_when_migrations_fail_before_resources(
    monkeypatch, tmp_path
):
    config = make_config(tmp_path / "ziggy.sqlite3")
    logging_controller = SimpleNamespace(configure=MagicMock(), close=AsyncMock())
    create_engine_mock = MagicMock()
    archive_factory = MagicMock()
    monkeypatch.setattr(service, "load_config", MagicMock(return_value=config))
    monkeypatch.setattr(
        service, "resolve_secrets", MagicMock(return_value=make_secrets())
    )
    monkeypatch.setattr(
        service, "LoggingController", MagicMock(return_value=logging_controller)
    )
    monkeypatch.setattr(
        service,
        "run_migrations",
        AsyncMock(side_effect=RuntimeError("migration failed")),
    )
    monkeypatch.setattr(service, "create_engine", create_engine_mock)
    monkeypatch.setattr(service, "ArchivistClient", archive_factory)

    with pytest.raises(RuntimeError, match="migration failed"):
        await service.run_service(tmp_path / "ziggy.toml")

    create_engine_mock.assert_not_called()
    archive_factory.assert_not_called()
    logging_controller.close.assert_awaited_once_with()


async def test_config_watcher_rejects_invalid_reload(monkeypatch):
    state = make_state()
    stop = SequencedStop(False, False, True)
    monkeypatch.setattr(service, "_wait", AsyncMock())
    monkeypatch.setattr(
        service, "load_config", MagicMock(side_effect=ConfigError("invalid"))
    )

    await service._config_watcher(Path("ziggy.toml"), state, MagicMock(), stop)

    assert state.config == make_config()


async def test_config_watcher_returns_when_stopped_during_wait(monkeypatch):
    state = make_state()
    stop = SequencedStop(False, True)
    load = MagicMock()
    monkeypatch.setattr(service, "_wait", AsyncMock())
    monkeypatch.setattr(service, "load_config", load)

    await service._config_watcher(Path("ziggy.toml"), state, MagicMock(), stop)

    load.assert_not_called()


async def test_config_watcher_rejects_database_change(monkeypatch, tmp_path):
    state = make_state()
    replacement = replace(
        state.config,
        ziggy=replace(state.config.ziggy, database=tmp_path / "replacement.sqlite3"),
    )
    stop = SequencedStop(False, False, True)
    monkeypatch.setattr(service, "_wait", AsyncMock())
    monkeypatch.setattr(service, "load_config", MagicMock(return_value=replacement))
    monkeypatch.setattr(
        service, "resolve_secrets", MagicMock(return_value=state.secrets)
    )

    await service._config_watcher(Path("ziggy.toml"), state, MagicMock(), stop)

    assert state.config != replacement


async def test_config_watcher_skips_unchanged_healthy_configuration(monkeypatch):
    state = make_state()
    stop = SequencedStop(False, False, True)
    reconcile = AsyncMock()
    monkeypatch.setattr(service, "_wait", AsyncMock())
    monkeypatch.setattr(service, "load_config", MagicMock(return_value=state.config))
    monkeypatch.setattr(
        service, "resolve_secrets", MagicMock(return_value=state.secrets)
    )
    monkeypatch.setattr(service, "reconcile_domains", reconcile)

    await service._config_watcher(Path("ziggy.toml"), state, MagicMock(), stop)

    reconcile.assert_not_awaited()


async def test_config_watcher_keeps_archive_paused_after_bad_credentials(monkeypatch):
    state = make_state()
    state.archive_paused = True
    candidate = SimpleNamespace(
        login=AsyncMock(side_effect=ArchiveAuthenticationError("bad credentials")),
        close=AsyncMock(),
    )
    stop = SequencedStop(False, False, True)
    monkeypatch.setattr(service, "_wait", AsyncMock())
    monkeypatch.setattr(service, "load_config", MagicMock(return_value=state.config))
    monkeypatch.setattr(
        service, "resolve_secrets", MagicMock(return_value=state.secrets)
    )
    monkeypatch.setattr(service, "ArchivistClient", MagicMock(return_value=candidate))

    await service._config_watcher(Path("ziggy.toml"), state, MagicMock(), stop)

    candidate.close.assert_awaited_once_with()
    assert state.archive_paused is True


@pytest.mark.parametrize(
    "error",
    [ArchiveAuthenticationError("bad credentials"), ArchiveError("network failed")],
)
async def test_config_watcher_rejected_candidate_preserves_healthy_state(
    monkeypatch, error
):
    state = make_state()
    previous_config = state.config
    previous_secrets = state.secrets
    previous_client = state.archive_client
    replacement_secrets = replace(state.secrets, archive_password="replacement")
    candidate = SimpleNamespace(login=AsyncMock(side_effect=error), close=AsyncMock())
    stop = SequencedStop(False, False, True)
    reconcile = AsyncMock()
    monkeypatch.setattr(service, "_wait", AsyncMock())
    monkeypatch.setattr(service, "load_config", MagicMock(return_value=state.config))
    monkeypatch.setattr(
        service, "resolve_secrets", MagicMock(return_value=replacement_secrets)
    )
    monkeypatch.setattr(service, "ArchivistClient", MagicMock(return_value=candidate))
    monkeypatch.setattr(service, "reconcile_domains", reconcile)

    await service._config_watcher(Path("ziggy.toml"), state, MagicMock(), stop)

    candidate.close.assert_awaited_once_with()
    assert state.archive_client is previous_client
    assert state.config is previous_config
    assert state.secrets is previous_secrets
    assert state.archive_paused is False
    assert state.retired_archive_clients == []
    reconcile.assert_not_awaited()
    state.logging.configure.assert_not_called()


async def test_config_watcher_closes_unadopted_candidate_on_cancellation(monkeypatch):
    state = make_state()
    replacement_secrets = replace(state.secrets, archive_password="replacement")
    candidate = SimpleNamespace(
        login=AsyncMock(side_effect=asyncio.CancelledError()),
        close=AsyncMock(side_effect=RuntimeError("close failed")),
    )
    monkeypatch.setattr(service, "_wait", AsyncMock())
    monkeypatch.setattr(service, "load_config", MagicMock(return_value=state.config))
    monkeypatch.setattr(
        service, "resolve_secrets", MagicMock(return_value=replacement_secrets)
    )
    monkeypatch.setattr(service, "ArchivistClient", MagicMock(return_value=candidate))

    with pytest.raises(asyncio.CancelledError):
        await service._config_watcher(
            Path("ziggy.toml"), state, MagicMock(), SequencedStop(False, False)
        )

    candidate.close.assert_awaited_once_with()


async def test_config_watcher_swaps_changed_boundaries_and_applies_reload(monkeypatch):
    state = make_state()
    old_archive = state.archive_client
    old_crawler = state.crawler
    replacement = replace(
        state.config,
        crawl=replace(state.config.crawl, request_timeout=15.0),
        logging=replace(state.config.logging, level="DEBUG"),
    )
    replacement_secrets = replace(state.secrets, archive_password="new-password")
    candidate = SimpleNamespace(
        login=AsyncMock(),
        my_web_archive_url=AsyncMock(
            return_value="https://archive.org/details/@replacement/web-archive"
        ),
        close=AsyncMock(),
    )
    crawler = SimpleNamespace(close=AsyncMock())
    session = MagicMock()
    reconcile = AsyncMock()
    stop = SequencedStop(False, False, True)
    monkeypatch.setattr(service, "_wait", AsyncMock())
    monkeypatch.setattr(service, "load_config", MagicMock(return_value=replacement))
    monkeypatch.setattr(
        service, "resolve_secrets", MagicMock(return_value=replacement_secrets)
    )
    archive_factory = MagicMock(return_value=candidate)
    crawler_factory = MagicMock(return_value=crawler)
    monkeypatch.setattr(service, "ArchivistClient", archive_factory)
    monkeypatch.setattr(service, "CrawlerClient", crawler_factory)
    monkeypatch.setattr(service, "reconcile_domains", reconcile)

    await service._config_watcher(Path("ziggy.toml"), state, Sessions(session), stop)

    archive_factory.assert_called_once_with(
        "user@example.com",
        "new-password",
        timeout=15.0,
        request_delay=1.0,
        server_error_recovery_period=timedelta(minutes=15),
    )
    candidate.login.assert_awaited_once_with()
    candidate.my_web_archive_url.assert_awaited_once_with()
    assert state.archive_client is candidate
    assert state.retired_archive_clients == [old_archive]
    assert state.crawler is crawler
    assert state.retired_crawlers == [old_crawler]
    reconcile.assert_awaited_once()
    state.logging.configure.assert_called_once_with(
        replacement.logging, replacement_secrets
    )
    assert state.config == replacement
    assert state.secrets == replacement_secrets
    assert state.archive_paused is False
    assert state.web_archive_url == (
        "https://archive.org/details/@replacement/web-archive"
    )


async def test_config_watcher_applies_logging_only_change_without_client_swap(
    monkeypatch,
):
    state = make_state()
    archive = state.archive_client
    crawler = state.crawler
    replacement = replace(
        state.config, logging=replace(state.config.logging, level="DEBUG")
    )
    session = MagicMock()
    stop = SequencedStop(False, False, True)
    monkeypatch.setattr(service, "_wait", AsyncMock())
    monkeypatch.setattr(service, "load_config", MagicMock(return_value=replacement))
    monkeypatch.setattr(
        service, "resolve_secrets", MagicMock(return_value=state.secrets)
    )
    monkeypatch.setattr(service, "reconcile_domains", AsyncMock())
    archive_factory = MagicMock()
    crawler_factory = MagicMock()
    monkeypatch.setattr(service, "ArchivistClient", archive_factory)
    monkeypatch.setattr(service, "CrawlerClient", crawler_factory)

    await service._config_watcher(Path("ziggy.toml"), state, Sessions(session), stop)

    archive_factory.assert_not_called()
    crawler_factory.assert_not_called()
    assert state.archive_client is archive
    assert state.crawler is crawler
    assert state.config == replacement


@pytest.mark.parametrize(
    "replacement",
    [
        replace(
            make_config(), crawl=replace(make_config().crawl, request_timeout=15.0)
        ),
        replace(
            make_config(), archive=replace(make_config().archive, request_delay=2.0)
        ),
        replace(
            make_config(),
            archive=replace(
                make_config().archive,
                server_error_recovery_period=timedelta(minutes=5),
            ),
        ),
    ],
)
async def test_config_watcher_replaces_archive_client_for_network_settings(
    monkeypatch, replacement
):
    state = make_state()
    candidate = SimpleNamespace(
        login=AsyncMock(),
        my_web_archive_url=AsyncMock(
            return_value="https://archive.org/details/@replacement/web-archive"
        ),
        close=AsyncMock(),
    )
    stop = SequencedStop(False, False, True)
    monkeypatch.setattr(service, "_wait", AsyncMock())
    monkeypatch.setattr(service, "load_config", MagicMock(return_value=replacement))
    monkeypatch.setattr(
        service, "resolve_secrets", MagicMock(return_value=state.secrets)
    )
    archive_factory = MagicMock(return_value=candidate)
    monkeypatch.setattr(service, "ArchivistClient", archive_factory)
    monkeypatch.setattr(service, "reconcile_domains", AsyncMock())

    await service._config_watcher(
        Path("ziggy.toml"), state, Sessions(MagicMock()), stop
    )

    archive_factory.assert_called_once_with(
        "user@example.com",
        "password",
        timeout=replacement.crawl.request_timeout,
        request_delay=replacement.archive.request_delay,
        server_error_recovery_period=(replacement.archive.server_error_recovery_period),
    )
    assert state.archive_client is candidate


async def test_crawl_one_handles_missing_and_inactive_records(monkeypatch):
    state = make_state()
    missing = MagicMock(get=AsyncMock(return_value=None))
    await service._crawl_one(state, Sessions(missing), 1)

    page = SimpleNamespace(
        domain_id=2,
        active=True,
        in_scope=True,
        crawl_lease_owner="owner",
        crawl_lease_expires_at=NOW,
    )
    inactive = SimpleNamespace(active=False)
    session = MagicMock(get=AsyncMock(side_effect=[page, inactive]), commit=AsyncMock())
    crawl = AsyncMock()
    monkeypatch.setattr(service, "crawl_page", crawl)
    await service._crawl_one(state, Sessions(session), 1)

    assert page.crawl_lease_owner is None
    assert page.crawl_lease_expires_at is None
    session.commit.assert_awaited_once_with()
    crawl.assert_not_awaited()

    page.in_scope = False
    session.get = AsyncMock(side_effect=[page, SimpleNamespace(active=True)])
    session.commit.reset_mock()
    await service._crawl_one(state, Sessions(session), 1)
    session.commit.assert_awaited_once_with()
    crawl.assert_not_awaited()

    page.active = False
    page.in_scope = True
    session.get = AsyncMock(
        side_effect=[
            page,
            SimpleNamespace(active=True, host="example.test", include_subdomains=False),
        ]
    )
    await service._crawl_one(state, Sessions(session), 1)
    crawl.assert_awaited_once()


async def test_crawl_one_runs_worker_and_isolates_failure(monkeypatch):
    state = make_state()
    page = SimpleNamespace(
        domain_id=2, active=True, in_scope=True, url="https://example.test/"
    )
    domain = SimpleNamespace(active=True, host="example.test", include_subdomains=True)
    session = MagicMock(get=AsyncMock(side_effect=[page, domain]))
    crawl = AsyncMock()
    monkeypatch.setattr(service, "crawl_page", crawl)
    await service._crawl_one(state, Sessions(session), 1)
    crawl.assert_awaited_once()

    session.get = AsyncMock(side_effect=[page, domain])
    crawl.reset_mock(side_effect=True)
    crawl.side_effect = RuntimeError("worker")
    failure = AsyncMock()
    monkeypatch.setattr(service, "_worker_failure", failure)
    await service._crawl_one(state, Sessions(session), 1)
    failure.assert_awaited_once()


async def test_submit_one_handles_missing_inactive_authentication_and_failure(
    monkeypatch,
):
    state = make_state()
    await service._submit_one(
        state, Sessions(MagicMock(get=AsyncMock(return_value=None))), 1
    )

    page = SimpleNamespace(
        id=1,
        domain_id=2,
        active=True,
        deactivated_at=None,
        in_scope=True,
        url="https://example.test/",
        first_crawled_at=None,
        last_crawled_at=None,
        status_code=None,
        final_url=None,
        error=None,
        archive_lease_owner="owner",
        archive_lease_expires_at=NOW,
    )
    session = MagicMock(
        get=AsyncMock(side_effect=[page, SimpleNamespace(active=False)]),
        scalar=AsyncMock(return_value=None),
        commit=AsyncMock(),
    )
    await service._submit_one(state, Sessions(session), 1)
    assert page.archive_lease_owner is None
    session.commit.assert_awaited_once_with()

    page.in_scope = False
    session.get = AsyncMock(side_effect=[page, SimpleNamespace(active=True)])
    session.commit.reset_mock()
    await service._submit_one(state, Sessions(session), 1)
    session.commit.assert_awaited_once_with()
    page.in_scope = True

    job = SimpleNamespace()
    create_intent = AsyncMock(return_value=job)
    submit = AsyncMock()
    monkeypatch.setattr(service, "create_archive_intent", create_intent)
    monkeypatch.setattr(service, "submit_archive_job", submit)
    active = SimpleNamespace(active=True, host="example.test", include_subdomains=False)
    state.crawler.fetch.return_value = FetchResult(200, page.url, {}, b"", None, ())
    session.get = AsyncMock(side_effect=[page, active])
    await service._submit_one(state, Sessions(session), 1)
    submit.assert_awaited_once()
    state.crawler.fetch.assert_awaited_once_with(
        page.url,
        active.host,
        include_subdomains=False,
        read_body=False,
    )

    session.get = AsyncMock(side_effect=[page, active])
    submit.reset_mock(side_effect=True)
    submit.side_effect = ArchiveAuthenticationError("expired")
    await service._submit_one(state, Sessions(session), 1)
    assert state.archive_paused is True

    state.archive_paused = False
    session.get = AsyncMock(side_effect=[page, active])
    submit.side_effect = RuntimeError("unexpected")
    failure = AsyncMock()
    monkeypatch.setattr(service, "_archive_worker_failure", failure)
    await service._submit_one(state, Sessions(session), 1)
    failure.assert_awaited_once_with(session, job, page, submit.side_effect)


@pytest.mark.parametrize("status_code", [400, 404, 500, 503])
async def test_submit_one_marks_page_inactive_on_http_error(monkeypatch, status_code):
    state = make_state()
    page = SimpleNamespace(
        id=1,
        domain_id=2,
        active=True,
        in_scope=True,
        url="https://example.test/old",
        first_crawled_at=NOW,
        last_crawled_at=NOW,
        status_code=200,
        final_url="https://example.test/old",
        error=None,
        archive_lease_owner="worker",
        archive_lease_expires_at=NOW,
    )
    domain = SimpleNamespace(active=True, host="example.test", include_subdomains=False)
    session = MagicMock(
        get=AsyncMock(side_effect=[page, domain]),
        scalar=AsyncMock(return_value=1),
        commit=AsyncMock(),
    )
    state.crawler.fetch = AsyncMock(
        return_value=FetchResult(
            status_code,
            page.url,
            {},
            b"",
            None,
            (),
        )
    )
    create_intent = AsyncMock()
    monkeypatch.setattr(service, "create_archive_intent", create_intent)

    before = datetime.now(UTC)
    await service._submit_one(state, Sessions(session), page.id)
    after = datetime.now(UTC)

    assert page.active is False
    assert before <= page.deactivated_at <= after
    assert page.status_code == status_code
    assert page.error == f"HTTP {status_code}"
    assert page.archive_lease_owner is None
    assert page.archive_lease_expires_at is None
    assert session.commit.await_count == 2
    create_intent.assert_not_awaited()


async def test_submit_one_preflights_recurring_page_before_creating_intent(monkeypatch):
    state = make_state()
    page = SimpleNamespace(
        id=1,
        domain_id=2,
        active=True,
        in_scope=True,
        url="https://example.test/current",
        first_crawled_at=None,
        last_crawled_at=None,
        status_code=None,
        final_url=None,
        error="old error",
        archive_lease_owner="worker",
        archive_lease_expires_at=NOW,
    )
    domain = SimpleNamespace(active=True, host="example.test", include_subdomains=True)
    session = MagicMock(
        get=AsyncMock(side_effect=[page, domain]),
        scalar=AsyncMock(return_value=1),
        commit=AsyncMock(),
    )
    state.crawler.fetch = AsyncMock(
        return_value=FetchResult(204, page.url, {}, b"", None, ())
    )
    job = SimpleNamespace()
    create_intent = AsyncMock(return_value=job)
    submit = AsyncMock()
    monkeypatch.setattr(service, "create_archive_intent", create_intent)
    monkeypatch.setattr(service, "submit_archive_job", submit)

    await service._submit_one(state, Sessions(session), page.id)

    state.crawler.fetch.assert_awaited_once_with(
        page.url,
        domain.host,
        include_subdomains=True,
        read_body=False,
    )
    assert page.status_code == 204
    assert page.first_crawled_at is not None
    assert page.last_crawled_at == page.first_crawled_at
    assert page.error is None
    create_intent.assert_awaited_once()
    submit.assert_awaited_once()


async def test_submit_one_retries_recurring_page_after_preflight_fetch_error(
    monkeypatch,
):
    state = make_state()
    page = SimpleNamespace(
        id=1,
        domain_id=2,
        active=True,
        in_scope=True,
        url="https://example.test/unreachable",
        error=None,
        next_archive_at=NOW,
        archive_lease_owner="worker",
        archive_lease_expires_at=NOW,
    )
    domain = SimpleNamespace(active=True, host="example.test", include_subdomains=False)
    session = MagicMock(
        get=AsyncMock(side_effect=[page, domain]),
        scalar=AsyncMock(return_value=1),
        commit=AsyncMock(),
    )
    state.crawler.fetch = AsyncMock(
        side_effect=FetchError("connection failed", transient=True)
    )
    create_intent = AsyncMock()
    monkeypatch.setattr(service, "create_archive_intent", create_intent)

    before = datetime.now(UTC) + timedelta(minutes=1)
    await service._submit_one(state, Sessions(session), page.id)
    after = datetime.now(UTC) + timedelta(minutes=1)

    assert page.active is True
    assert page.error == "connection failed"
    assert before <= page.next_archive_at <= after
    assert page.archive_lease_owner is None
    assert session.commit.await_count == 2
    create_intent.assert_not_awaited()


async def test_poll_one_validates_records_and_submits_or_polls(monkeypatch):
    state = make_state()
    missing = MagicMock(get=AsyncMock(return_value=None))
    await service._poll_one(state, Sessions(missing), "job")

    job = SimpleNamespace(page_id=1, state=ArchiveJobState.INTENT, external_job_id=None)
    no_page = MagicMock(get=AsyncMock(side_effect=[job, None]))
    await service._poll_one(state, Sessions(no_page), "job")

    page = SimpleNamespace(
        domain_id=2,
        active=True,
        in_scope=True,
        url="https://example.test/",
        first_crawled_at=None,
        last_crawled_at=None,
        status_code=None,
        final_url=None,
        error=None,
    )
    no_domain = MagicMock(get=AsyncMock(side_effect=[job, page, None]))
    await service._poll_one(state, Sessions(no_domain), "job")

    domain = SimpleNamespace(active=True, host="example.test", include_subdomains=False)
    session = MagicMock(
        get=AsyncMock(side_effect=[job, page, domain]), commit=AsyncMock()
    )
    submit = AsyncMock()
    poll = AsyncMock()
    monkeypatch.setattr(service, "submit_archive_job", submit)
    monkeypatch.setattr(service, "poll_archive_job", poll)
    await service._poll_one(state, Sessions(session), "job")
    submit.assert_awaited_once()
    assert job.state is ArchiveJobState.UNCERTAIN
    assert submit.call_args.kwargs["allow_submission"] is True
    state.crawler.fetch.return_value = FetchResult(200, page.url, {}, b"", None, ())
    assert await submit.call_args.kwargs["preflight"]() is True
    state.crawler.fetch.assert_awaited_once_with(
        page.url,
        domain.host,
        include_subdomains=False,
        read_body=False,
    )
    poll.assert_not_awaited()

    job.state = ArchiveJobState.RATE_LIMITED
    job.external_job_id = "external"
    session.get = AsyncMock(side_effect=[job, page, domain])
    await service._poll_one(state, Sessions(session), "job")
    poll.assert_awaited_once()


async def test_poll_one_pauses_on_auth_and_isolates_other_failures(monkeypatch):
    state = make_state()
    job = SimpleNamespace(
        page_id=1, state=ArchiveJobState.RATE_LIMITED, external_job_id=None
    )
    page = SimpleNamespace(
        domain_id=2, active=True, in_scope=True, url="https://example.test/"
    )
    domain = SimpleNamespace(active=True)
    session = MagicMock(
        get=AsyncMock(side_effect=[job, page, domain]), commit=AsyncMock()
    )
    submit = AsyncMock(side_effect=ArchiveAuthenticationError("expired"))
    monkeypatch.setattr(service, "submit_archive_job", submit)
    await service._poll_one(state, Sessions(session), "job")
    assert state.archive_paused is True

    state.archive_paused = False
    error = RuntimeError("unexpected")
    submit.side_effect = error
    session.get = AsyncMock(side_effect=[job, page, domain])
    failure = AsyncMock()
    monkeypatch.setattr(service, "_archive_worker_failure", failure)
    await service._poll_one(state, Sessions(session), "job")
    failure.assert_awaited_once_with(session, job, page, error)


async def test_poll_one_delays_no_id_work_while_archive_is_paused(monkeypatch):
    state = make_state()
    state.archive_paused = True
    job = SimpleNamespace(
        page_id=1,
        state=ArchiveJobState.UNCERTAIN,
        external_job_id=None,
        lease_owner="worker",
        lease_expires_at=NOW,
        next_attempt_at=NOW,
    )
    page = SimpleNamespace(domain_id=2, active=True, in_scope=True)
    domain = SimpleNamespace(active=True)
    session = MagicMock(
        get=AsyncMock(side_effect=[job, page, domain]), commit=AsyncMock()
    )
    submit = AsyncMock()
    monkeypatch.setattr(service, "submit_archive_job", submit)

    before = datetime.now(UTC) + timedelta(minutes=5)
    await service._poll_one(state, Sessions(session), "job")
    after = datetime.now(UTC) + timedelta(minutes=5)

    submit.assert_not_awaited()
    assert job.lease_owner is None
    assert before <= job.next_attempt_at <= after
    session.commit.assert_awaited_once_with()


async def test_persisted_archive_preflight_retries_fetch_errors_and_fails_http_errors():
    page = SimpleNamespace(
        active=True,
        deactivated_at=None,
        url="https://example.test/page",
        first_crawled_at=None,
        last_crawled_at=None,
        status_code=None,
        final_url=None,
        error=None,
    )
    domain = SimpleNamespace(host="example.test", include_subdomains=False)
    job = SimpleNamespace(
        state=ArchiveJobState.UNCERTAIN,
        completed_at=None,
        error=None,
        next_attempt_at=NOW,
        lease_owner="worker",
        lease_expires_at=NOW,
    )
    session = MagicMock(commit=AsyncMock())
    crawler = SimpleNamespace(
        fetch=AsyncMock(side_effect=FetchError("connection failed", transient=True))
    )

    before = datetime.now(UTC) + timedelta(minutes=1)
    assert (
        await service._archive_preflight(session, page, domain, crawler, job=job)
        is False
    )
    after = datetime.now(UTC) + timedelta(minutes=1)
    assert page.error == "connection failed"
    assert job.error == "connection failed"
    assert before <= job.next_attempt_at <= after
    assert job.lease_owner is None
    assert job.lease_expires_at is None

    crawler.fetch = AsyncMock(
        return_value=FetchResult(404, page.url, {}, b"", None, ())
    )
    job.lease_owner = "worker"
    job.lease_expires_at = NOW
    before = datetime.now(UTC)
    assert (
        await service._archive_preflight(session, page, domain, crawler, job=job)
        is False
    )
    after = datetime.now(UTC)
    assert page.active is False
    assert before <= page.deactivated_at <= after
    assert page.error == "HTTP 404"
    assert job.state is ArchiveJobState.FAILED
    assert before <= job.completed_at <= after
    assert job.error == "HTTP 404"
    assert job.lease_owner is None
    assert job.lease_expires_at is None


async def test_worker_failure_helpers_release_and_delay_work():
    session = MagicMock(commit=AsyncMock(), rollback=AsyncMock(), refresh=AsyncMock())
    page = SimpleNamespace(
        error=None,
        url="https://example.test/",
        crawl_lease_owner="worker",
        crawl_lease_expires_at=NOW,
        next_crawl_at=NOW,
    )
    before = datetime.now(UTC) + timedelta(minutes=1)
    await service._worker_failure(session, page, "crawl", RuntimeError("failure"))
    after = datetime.now(UTC) + timedelta(minutes=1)
    assert page.error == "RuntimeError"
    assert page.crawl_lease_owner is None
    assert before <= page.next_crawl_at <= after

    job = SimpleNamespace(
        error=None, lease_owner="worker", lease_expires_at=NOW, next_attempt_at=NOW
    )
    before = datetime.now(UTC) + timedelta(minutes=1)
    await service._archive_worker_failure(session, job, page, ValueError("failure"))
    after = datetime.now(UTC) + timedelta(minutes=1)
    assert job.error == "ValueError"
    assert job.lease_owner is None
    assert before <= job.next_attempt_at <= after
    assert session.commit.await_count == 2
    assert session.rollback.await_count == 2

    page.archive_history_check_attempts = 0
    page.archive_history_check_error = None
    page.next_archive_history_check_at = NOW
    page.archive_history_lease_owner = "worker"
    page.archive_history_lease_expires_at = NOW
    before = datetime.now(UTC) + timedelta(minutes=1)
    await service._archive_history_worker_failure(
        session, page, RuntimeError("failure")
    )
    after = datetime.now(UTC) + timedelta(minutes=1)
    assert page.archive_history_check_attempts == 1
    assert page.archive_history_check_error == "RuntimeError"
    assert before <= page.next_archive_history_check_at <= after
    assert page.archive_history_lease_owner is None
    assert page.archive_history_lease_expires_at is None
    assert session.commit.await_count == 3
    assert session.rollback.await_count == 3
    assert session.refresh.await_count == 4


async def test_archive_history_worker_failure_refreshes_rollback_expired_page(
    monkeypatch, tmp_path
):
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{(tmp_path / 'failure.sqlite3').as_posix()}"
    )
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    monkeypatch.setattr(service.logger, "exception", MagicMock())

    try:
        async with sessions() as session:
            domain = Domain(
                host="example.test",
                scheme="https",
                include_subdomains=False,
                active=True,
                created_at=NOW,
                configured_at=NOW,
            )
            session.add(domain)
            await session.flush()
            page = Page(
                domain_id=domain.id,
                url="https://example.test/",
                discovered_at=NOW,
                next_crawl_at=NOW,
                next_archive_at=NOW,
                archive_history_check_attempts=2,
                archive_history_lease_owner="worker",
                archive_history_lease_expires_at=NOW,
            )
            session.add(page)
            await session.commit()

            page.archive_history_check_error = "pending"
            await session.flush()
            before = datetime.now(UTC) + timedelta(minutes=1)
            await service._archive_history_worker_failure(
                session, page, RuntimeError("failure")
            )
            after = datetime.now(UTC) + timedelta(minutes=1)

            assert page.archive_history_check_attempts == 3
            assert page.archive_history_check_error == "RuntimeError"
            assert before <= page.next_archive_history_check_at <= after
            assert page.archive_history_lease_owner is None
            assert page.archive_history_lease_expires_at is None
    finally:
        await engine.dispose()


async def test_crawl_scheduler_claims_batch_and_waits_when_idle(monkeypatch):
    state = make_state()
    page = SimpleNamespace(id=7)
    claim = AsyncMock(side_effect=[page, None])
    stop = asyncio.Event()

    async def crawl_one(*_args):
        stop.set()

    monkeypatch.setattr(service, "claim_due_page", claim)
    monkeypatch.setattr(service, "_crawl_one", crawl_one)
    await service._crawl_scheduler(state, Sessions(MagicMock()), "instance", stop)
    assert claim.await_count == 2

    stop.clear()
    claim.reset_mock(side_effect=True)
    claim.return_value = None

    async def stop_wait(event, _seconds):
        event.set()

    monkeypatch.setattr(service, "_wait", stop_wait)
    await service._crawl_scheduler(state, Sessions(MagicMock()), "instance", stop)
    claim.assert_awaited_once()

    stop.clear()
    claim.reset_mock(side_effect=True)
    claim.side_effect = [SimpleNamespace(id=8), SimpleNamespace(id=9)]
    await service._crawl_scheduler(state, Sessions(MagicMock()), "instance", stop)
    assert claim.await_count == state.config.crawl.concurrency


async def test_archive_submission_scheduler_covers_pause_idle_and_work(monkeypatch):
    state = make_state()
    stop = asyncio.Event()

    async def stop_wait(event, _seconds):
        event.set()

    monkeypatch.setattr(service, "_wait", stop_wait)
    monkeypatch.setattr(
        service, "available_archive_submission_slots", AsyncMock(return_value=1)
    )
    state.archive_paused = True
    claim = AsyncMock()
    monkeypatch.setattr(service, "claim_due_page", claim)
    await service._archive_submission_scheduler(
        state, Sessions(MagicMock()), "instance", stop
    )
    claim.assert_not_awaited()

    state.archive_paused = False
    stop.clear()
    claim.return_value = None
    await service._archive_submission_scheduler(
        state, Sessions(MagicMock()), "instance", stop
    )

    stop.clear()
    claim.return_value = SimpleNamespace(id=8)

    async def submit_one(*_args):
        stop.set()

    monkeypatch.setattr(service, "_submit_one", submit_one)
    await service._archive_submission_scheduler(
        state, Sessions(MagicMock()), "instance", stop
    )


async def test_archive_history_scheduler_covers_idle_and_work(monkeypatch):
    state = make_state()
    stop = asyncio.Event()
    claim = AsyncMock(return_value=None)

    async def stop_wait(event, _seconds):
        event.set()

    monkeypatch.setattr(service, "claim_due_archive_history_check", claim)
    monkeypatch.setattr(service, "_wait", stop_wait)
    await service._archive_history_scheduler(
        state, Sessions(MagicMock()), "instance", stop
    )

    stop.clear()
    claim.return_value = SimpleNamespace(id=8)

    async def check_one(*_args):
        stop.set()

    monkeypatch.setattr(service, "_check_archive_history_one", check_one)
    await service._archive_history_scheduler(
        state, Sessions(MagicMock()), "instance", stop
    )


async def test_check_archive_history_one_handles_missing_invalid_and_failure(
    monkeypatch,
):
    state = make_state()
    check = AsyncMock()
    failure = AsyncMock()
    monkeypatch.setattr(service, "check_archive_history", check)
    monkeypatch.setattr(service, "_archive_history_worker_failure", failure)

    await service._check_archive_history_one(
        state, Sessions(MagicMock(get=AsyncMock(return_value=None))), 1
    )
    check.assert_not_awaited()

    page = SimpleNamespace(
        active=False,
        in_scope=True,
        blocked_reason=None,
        status_code=200,
        error=None,
        archive_history_lease_owner="worker",
        archive_history_lease_expires_at=NOW,
    )
    session = MagicMock(get=AsyncMock(return_value=page), commit=AsyncMock())
    await service._check_archive_history_one(state, Sessions(session), 1)
    assert page.archive_history_lease_owner is None
    assert page.archive_history_lease_expires_at is None
    session.commit.assert_awaited_once_with()

    page.active = True
    page.url = "https://example.test/"
    session.get = AsyncMock(return_value=page)
    await service._check_archive_history_one(state, Sessions(session), 1)
    check.assert_awaited_once()

    check.side_effect = RuntimeError("failure")
    session.get = AsyncMock(return_value=page)
    await service._check_archive_history_one(state, Sessions(session), 1)
    failure.assert_awaited_once_with(session, page, check.side_effect)


async def test_archive_submission_scheduler_stops_after_admission_wait(monkeypatch):
    state = make_state()
    stop = asyncio.Event()

    async def no_slots(_state, _sessions, event):
        event.set()
        return 0

    monkeypatch.setattr(service, "_archive_submission_slots", no_slots)
    claim = AsyncMock()
    monkeypatch.setattr(service, "claim_due_page", claim)

    await service._archive_submission_scheduler(
        state, Sessions(MagicMock()), "instance", stop
    )

    claim.assert_not_awaited()


@pytest.mark.parametrize(
    ("local_slots", "remote_result", "expected", "paused", "wait_seconds"),
    [
        (0, None, 0, False, service._IDLE_DELAY),
        (2, None, 1, False, None),
        (2, 1, 1, False, None),
        (2, 0, 0, False, service._ARCHIVE_CAPACITY_RECHECK_DELAY),
        (2, ArchiveAuthenticationError("denied"), 0, True, None),
        (
            2,
            ArchiveRateLimitError(datetime.now(UTC) + timedelta(seconds=20)),
            0,
            False,
            None,
        ),
        (
            2,
            ArchiveRateLimitError(None),
            0,
            False,
            service._ARCHIVE_CAPACITY_RECHECK_DELAY,
        ),
        (
            2,
            ArchiveError("offline"),
            0,
            False,
            service._ARCHIVE_CAPACITY_RECHECK_DELAY,
        ),
    ],
)
async def test_archive_submission_admission(  # noqa: PLR0913, PLR0917
    monkeypatch,
    local_slots,
    remote_result,
    expected,
    paused,
    wait_seconds,
):
    state = make_state()
    available = AsyncMock(return_value=local_slots)
    monkeypatch.setattr(service, "available_archive_submission_slots", available)
    if isinstance(remote_result, Exception):
        state.archive_client.submission_capacity.side_effect = remote_result
    else:
        state.archive_client.submission_capacity.return_value = remote_result
    wait = AsyncMock()
    monkeypatch.setattr(service, "_wait", wait)
    stop = asyncio.Event()

    result = await service._archive_submission_slots(state, Sessions(MagicMock()), stop)

    assert result == expected
    assert state.archive_paused is paused
    if local_slots == 0:
        state.archive_client.submission_capacity.assert_not_awaited()
    if wait_seconds is None:
        if isinstance(remote_result, ArchiveRateLimitError) and remote_result.retry_at:
            wait.assert_awaited_once()
            assert 0 < wait.await_args.args[1] <= 20
        else:
            wait.assert_not_awaited()
    else:
        wait.assert_awaited_once_with(stop, wait_seconds)


async def test_archive_poll_scheduler_covers_idle_and_work(monkeypatch):
    state = make_state()
    stop = asyncio.Event()
    claim = AsyncMock(return_value=None)

    async def stop_wait(event, _seconds):
        event.set()

    monkeypatch.setattr(service, "claim_archive_job", claim)
    monkeypatch.setattr(service, "_wait", stop_wait)
    await service._archive_poll_scheduler(
        state, Sessions(MagicMock()), "instance", stop
    )

    stop.clear()
    claim.return_value = SimpleNamespace(id="job")

    async def poll_one(*_args):
        stop.set()

    monkeypatch.setattr(service, "_poll_one", poll_one)
    await service._archive_poll_scheduler(
        state, Sessions(MagicMock()), "instance", stop
    )


async def test_report_scheduler_creates_claims_and_delivers(monkeypatch):
    state = make_state(secrets=make_secrets(reporting_webhook_url="discord"))
    state.started_at = (
        datetime.now(UTC)
        - state.config.reporting.interval
        - state.config.reporting.finalization_grace
    )
    stop = asyncio.Event()
    window_session = MagicMock(scalar=AsyncMock(return_value=None))
    delivery_session = MagicMock()
    window = SimpleNamespace()
    report = SimpleNamespace()
    monkeypatch.setattr(service, "next_report_window", MagicMock(return_value=window))
    create = AsyncMock()
    claim_report = AsyncMock(return_value=report)
    deliver = AsyncMock()
    monkeypatch.setattr(service, "create_report", create)
    monkeypatch.setattr(service, "claim_report", claim_report)
    monkeypatch.setattr(service, "deliver_report", deliver)

    async def stop_wait(event, _seconds):
        event.set()

    monkeypatch.setattr(service, "_wait", stop_wait)
    await service._report_scheduler(
        state, Sessions(window_session, delivery_session), "instance", stop
    )

    create.assert_awaited_once()
    assert create.call_args.args[0] is window_session
    assert claim_report.call_args.args[0] is delivery_session
    deliver.assert_awaited_once()
    assert deliver.call_args.args[0] is delivery_session
    assert deliver.call_args.args[2] == "discord"
    assert deliver.call_args.args[4] == state.web_archive_url


async def test_report_scheduler_waits_one_interval_before_first_report(monkeypatch):
    state = make_state()
    state.started_at = datetime.now(UTC)
    stop = asyncio.Event()
    session = MagicMock(scalar=AsyncMock(return_value=None))
    next_window = MagicMock()
    create = AsyncMock()
    claim_report = AsyncMock(return_value=None)
    monkeypatch.setattr(service, "next_report_window", next_window)
    monkeypatch.setattr(service, "create_report", create)
    monkeypatch.setattr(service, "claim_report", claim_report)

    async def stop_wait(event, _seconds):
        event.set()

    monkeypatch.setattr(service, "_wait", stop_wait)
    await service._report_scheduler(state, Sessions(session), "instance", stop)

    next_window.assert_not_called()
    create.assert_not_awaited()
    claim_report.assert_awaited_once()


async def test_report_scheduler_skips_absent_window_and_report(monkeypatch):
    state = make_state()
    stop = asyncio.Event()
    session = MagicMock(scalar=AsyncMock(return_value=NOW))
    monkeypatch.setattr(service, "next_report_window", MagicMock(return_value=None))
    create = AsyncMock()
    monkeypatch.setattr(service, "create_report", create)
    monkeypatch.setattr(service, "claim_report", AsyncMock(return_value=None))

    async def stop_wait(event, _seconds):
        event.set()

    monkeypatch.setattr(service, "_wait", stop_wait)
    await service._report_scheduler(state, Sessions(session), "instance", stop)
    create.assert_not_awaited()


def test_http_child_is_disabled_by_default_and_start_failures_are_isolated(
    monkeypatch,
):
    assert service._start_http_child(HttpSettings(), Path("ziggy.sqlite3")) is None

    failed = FakeChildProcess(start_error=OSError("spawn failed"))
    context = SimpleNamespace(
        Event=MagicMock(return_value=MagicMock()),
        Process=MagicMock(return_value=failed),
    )
    monkeypatch.setattr(service.multiprocessing, "get_context", lambda _name: context)
    assert (
        service._start_http_child(HttpSettings(enabled=True), Path("ziggy.sqlite3"))
        is None
    )
    assert failed.started is False


def test_http_child_starts_with_spawn_and_isolated_arguments(monkeypatch):
    process = FakeChildProcess()
    shutdown = MagicMock()
    context = SimpleNamespace(
        Event=MagicMock(return_value=shutdown),
        Process=MagicMock(return_value=process),
    )
    get_context = MagicMock(return_value=context)
    monkeypatch.setattr(service.multiprocessing, "get_context", get_context)
    settings = HttpSettings(enabled=True, host="127.0.0.1", port=9449)

    child = service._start_http_child(settings, Path("ziggy.sqlite3"))

    assert child is not None
    assert child.process is process
    assert child.shutdown is shutdown
    assert process.started is True
    get_context.assert_called_once_with("spawn")
    context.Process.assert_called_once_with(
        target=service.run_http_child,
        args=(Path("ziggy.sqlite3"), "127.0.0.1", 9449, shutdown),
        name="ziggy-http",
    )


async def test_http_child_monitor_returns_without_stopping_workers():
    process = FakeChildProcess(alive=False)
    child = service.HttpChild(process, MagicMock())
    stop = asyncio.Event()

    await service._monitor_http_child(child, stop)

    assert process.joins == [0]
    assert stop.is_set() is False

    alive = FakeChildProcess(alive=True)
    stopped = asyncio.Event()

    async def stop_wait(event, _seconds):
        event.set()

    original_wait = service._wait
    service._wait = stop_wait
    try:
        await service._monitor_http_child(
            service.HttpChild(alive, MagicMock()), stopped
        )
    finally:
        service._wait = original_wait
    assert alive.joins == []


async def test_http_child_shutdown_reaps_and_terminates_stuck_process():
    shutdown = MagicMock()
    graceful = FakeChildProcess(alive=False)
    await service._stop_http_child(service.HttpChild(graceful, shutdown))
    shutdown.set.assert_called_once_with()
    assert graceful.joins == [service._HTTP_SHUTDOWN_GRACE]
    assert graceful.terminated is False

    stuck_shutdown = MagicMock()
    stuck = FakeChildProcess(alive=True)
    await service._stop_http_child(service.HttpChild(stuck, stuck_shutdown))
    stuck_shutdown.set.assert_called_once_with()
    assert stuck.terminated is True
    assert stuck.joins == [service._HTTP_SHUTDOWN_GRACE, 5.0]

    stubborn = FakeChildProcess(alive=True, ignore_terminate=True)
    await service._stop_http_child(service.HttpChild(stubborn, MagicMock()))
    assert stubborn.terminated is True
    assert stubborn.killed is True
    assert stubborn.joins == [service._HTTP_SHUTDOWN_GRACE, 5.0, None]


async def test_api_only_submission_skips_local_preflight(monkeypatch):
    state = make_state()
    page = SimpleNamespace(
        id=1,
        domain_id=None,
        active=True,
        in_scope=False,
        blocked_reason=None,
        url="https://outside.example/",
        next_archive_at=NOW,
        archive_lease_owner="worker",
        archive_lease_expires_at=NOW,
    )
    session = MagicMock(
        get=AsyncMock(return_value=page),
        scalar=AsyncMock(return_value="receipt"),
        commit=AsyncMock(),
    )
    job = SimpleNamespace(archive_only=True)
    create_intent = AsyncMock(return_value=job)
    submit = AsyncMock()
    monkeypatch.setattr(service, "create_archive_intent", create_intent)
    monkeypatch.setattr(service, "submit_archive_job", submit)

    await service._submit_one(state, Sessions(session), page.id)

    state.crawler.fetch.assert_not_awaited()
    assert create_intent.call_args.kwargs == {"archive_only": True}
    submit.assert_awaited_once()
