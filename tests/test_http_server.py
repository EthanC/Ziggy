from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

from ziggy.http import server as http_server


class Shutdown:
    def __init__(self):
        self.checks = 0

    def is_set(self):
        self.checks += 1
        return self.checks > 1

    def wait(self, _timeout=None):
        return False


async def test_serve_configures_uvicorn_and_observes_shutdown(monkeypatch):
    application = object()
    config = object()
    instance = SimpleNamespace(should_exit=False)

    async def serve():
        await asyncio.sleep(0.01)

    instance.serve = serve
    create_app = MagicMock(return_value=application)
    config_factory = MagicMock(return_value=config)
    server_factory = MagicMock(return_value=instance)
    monkeypatch.setattr(http_server, "create_app", create_app)
    monkeypatch.setattr(http_server.uvicorn, "Config", config_factory)
    monkeypatch.setattr(http_server.uvicorn, "Server", server_factory)

    database = Path("ziggy.sqlite3")
    await http_server._serve(  # noqa: SLF001
        database, "127.0.0.1", 9449, Shutdown()
    )

    create_app.assert_called_once_with(database)
    config_factory.assert_called_once_with(
        application,
        host="127.0.0.1",
        port=9449,
        loop="auto",
        http="auto",
        ws="none",
        workers=1,
        reload=False,
        access_log=False,
        limit_concurrency=32,
        timeout_graceful_shutdown=10,
    )
    server_factory.assert_called_once_with(config)
    assert instance.should_exit is True


def test_run_http_child_owns_event_loop(monkeypatch):
    run = MagicMock(side_effect=lambda coroutine: coroutine.close())
    monkeypatch.setattr(http_server.asyncio, "run", run)

    shutdown = Shutdown()
    http_server.run_http_child(Path("ziggy.sqlite3"), "127.0.0.1", 9449, shutdown)

    run.assert_called_once()
