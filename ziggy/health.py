"""Read-only heartbeat and HTTP probes for short-lived health-check processes."""

from __future__ import annotations

import asyncio
import sqlite3
from contextlib import closing, suppress
from datetime import UTC, datetime, timedelta
from time import monotonic
from typing import TYPE_CHECKING

from loguru import logger

if TYPE_CHECKING:
    from pathlib import Path

    from ziggy.config import HttpSettings

_HEALTH_MAX_AGE = timedelta(seconds=90)
_DATABASE_TIMEOUT = 1.0
_HTTP_HEALTH_TIMEOUT = 2.0
_HTTP_STATUS_PARTS = 2
_WILDCARD_IPV4 = "0.0.0.0"  # noqa: S104


def _heartbeat(path: Path) -> datetime | None:
    deadline = monotonic() + _DATABASE_TIMEOUT
    with closing(
        sqlite3.connect(
            f"{path.resolve().as_uri()}?mode=ro",
            uri=True,
            timeout=_DATABASE_TIMEOUT,
        )
    ) as connection:
        connection.set_progress_handler(lambda: monotonic() >= deadline, 1_000)
        row = connection.execute(
            "SELECT max(heartbeat_at) FROM service_state"
        ).fetchone()
    return None if row[0] is None else datetime.fromisoformat(row[0]).astimezone(UTC)


async def check_health(
    path: Path,
    now: datetime | None = None,
    *,
    http: HttpSettings | None = None,
) -> bool:
    """Require a fresh heartbeat and a responding listener when HTTP is enabled."""
    if not await asyncio.to_thread(path.exists):
        logger.error("Health check failed: database does not exist at {}", path)
        return False
    try:
        heartbeat = await asyncio.to_thread(_heartbeat, path)
        if heartbeat is None:
            logger.error("Health check failed: no service heartbeat found")
            return False
        age = (now or datetime.now(UTC)) - heartbeat
        if age > _HEALTH_MAX_AGE:
            logger.error(
                "Health check failed: service heartbeat is {:.0f}s old",
                age.total_seconds(),
            )
            return False
    except OSError, sqlite3.Error, ValueError:
        logger.exception("Health check failed while reading the service heartbeat")
        return False
    return http is None or not http.enabled or await _check_http_health(http)


async def _check_http_health(settings: HttpSettings) -> bool:
    host = {_WILDCARD_IPV4: "127.0.0.1", "::": "::1"}.get(settings.host, settings.host)
    host_header = f"[{host}]" if ":" in host else host
    writer: asyncio.StreamWriter | None = None
    try:
        async with asyncio.timeout(_HTTP_HEALTH_TIMEOUT):
            reader, writer = await asyncio.open_connection(host, settings.port)
            writer.write(
                b"GET /v1/queue HTTP/1.1\r\n"
                + f"Host: {host_header}\r\n".encode()
                + b"Connection: close\r\n\r\n"
            )
            await writer.drain()
            status_line = await reader.readline()
    except OSError, TimeoutError, ValueError:
        logger.exception(
            "Health check failed while connecting to HTTP listener at {}:{}",
            settings.host,
            settings.port,
        )
        return False
    else:
        parts = status_line.split(maxsplit=2)
        healthy = len(parts) >= _HTTP_STATUS_PARTS and parts[1] == b"405"
        if not healthy:
            logger.error(
                "Health check failed: HTTP listener returned an unexpected response"
            )
        return healthy
    finally:
        if writer is not None:
            writer.close()
            with suppress(OSError, TimeoutError):
                async with asyncio.timeout(0.1):
                    await writer.wait_closed()
