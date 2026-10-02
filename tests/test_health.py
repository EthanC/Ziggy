from __future__ import annotations

import asyncio
import sqlite3
from contextlib import closing
from datetime import UTC, datetime
from time import perf_counter

import pytest

from ziggy import health
from ziggy.config import HttpSettings


def heartbeat_database(path, value=None):
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("CREATE TABLE service_state (heartbeat_at TEXT)")
        connection.execute(
            "INSERT INTO service_state VALUES (?)",
            (value or datetime.now(UTC).isoformat(),),
        )
        connection.commit()


async def test_health_database_lock_has_bounded_deadline(tmp_path, monkeypatch):
    path = tmp_path / "locked.sqlite3"
    heartbeat_database(path)
    monkeypatch.setattr(health, "_DATABASE_TIMEOUT", 0.05)
    with closing(sqlite3.connect(path)) as writer:
        writer.execute("BEGIN EXCLUSIVE")
        started = perf_counter()
        assert await health.check_health(path) is False
        assert perf_counter() - started < 1


async def test_health_query_deadline_interrupts_large_heartbeat_scan(
    tmp_path, monkeypatch
):
    path = tmp_path / "scan.sqlite3"
    heartbeat_database(path)
    with closing(sqlite3.connect(path)) as connection:
        connection.execute(
            "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL "
            "SELECT x+1 FROM n WHERE x<10000) "
            "INSERT INTO service_state SELECT '2026-10-02' FROM n"
        )
        connection.commit()
    monkeypatch.setattr(health, "_DATABASE_TIMEOUT", 0)
    assert await health.check_health(path) is False


async def test_health_never_opens_a_writable_database(tmp_path, monkeypatch):
    path = tmp_path / "read only%#.sqlite3"
    heartbeat_database(path)
    connect = sqlite3.connect

    def read_only(*args, **kwargs):
        connection = connect(*args, **kwargs)
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            connection.execute("DELETE FROM service_state")
        return connection

    monkeypatch.setattr(health.sqlite3, "connect", read_only)
    assert await health.check_health(path) is True


async def test_health_rejects_malformed_heartbeat(tmp_path):
    path = tmp_path / "malformed.sqlite3"
    heartbeat_database(path, "invalid")
    assert await health.check_health(path) is False


async def test_health_http_deadline_includes_silent_listener(monkeypatch):
    finished = asyncio.Event()

    async def silent(reader, writer):
        await reader.read()
        writer.close()
        await writer.wait_closed()
        finished.set()

    monkeypatch.setattr(health, "_HTTP_HEALTH_TIMEOUT", 0.05)
    async with await asyncio.start_server(silent, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        started = perf_counter()
        assert (
            await health._check_http_health(HttpSettings(enabled=True, port=port))  # noqa: SLF001
            is False
        )
        assert perf_counter() - started < 1
        await asyncio.wait_for(finished.wait(), 1)
