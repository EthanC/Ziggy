"""Uvicorn child-process entry point."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Protocol

import uvicorn

from ziggy.http.app import create_app

if TYPE_CHECKING:
    from pathlib import Path


class ShutdownSignal(Protocol):
    """Portable subset of multiprocessing.Event used by the child."""

    def is_set(self) -> bool:
        """Return whether shutdown was requested."""
        ...

    def wait(self, timeout: float | None = None) -> bool:
        """Wait until shutdown is requested or the timeout elapses."""
        ...


async def _serve(
    database: Path, host: str, port: int, shutdown: ShutdownSignal
) -> None:
    app = create_app(database)
    config = uvicorn.Config(
        app,
        host=host,
        port=port,
        loop="auto",
        http="auto",
        ws="none",
        workers=1,
        reload=False,
        access_log=False,
        limit_concurrency=32,
        timeout_graceful_shutdown=10,
    )
    server = uvicorn.Server(config)

    async def watch_shutdown() -> None:
        while not shutdown.is_set():
            await asyncio.to_thread(shutdown.wait, 0.1)
        server.should_exit = True

    watcher = asyncio.create_task(watch_shutdown())
    try:
        await server.serve()
    finally:
        watcher.cancel()
        await asyncio.gather(watcher, return_exceptions=True)


def run_http_child(
    database: Path, host: str, port: int, shutdown: ShutdownSignal
) -> None:
    """Run exactly one HTTP server in an isolated event loop."""
    asyncio.run(_serve(database, host, port, shutdown))
