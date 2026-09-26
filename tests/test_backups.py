from __future__ import annotations

import asyncio
import sqlite3
import threading
from contextlib import closing
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock
from zoneinfo import ZoneInfo

import pytest

from ziggy import backups
from ziggy.config import BackupSettings

# This suite deliberately covers the synchronous snapshot boundary.
# ruff: noqa: SLF001


def backup_settings(tmp_path, **changes):
    values = {
        "enabled": True,
        "schedule": "0 7 * * *",
        "directory": tmp_path / "backups",
        "retention_count": 7,
        "timezone": ZoneInfo("UTC"),
    }
    values.update(changes)
    return BackupSettings(**values)


async def test_create_backup_captures_committed_wal_and_is_standalone(tmp_path):
    database = tmp_path / "ziggy.db"
    directory = tmp_path / "backups"
    connection = sqlite3.connect(database)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA wal_autocheckpoint=0")
    connection.execute("CREATE TABLE records (id INTEGER PRIMARY KEY, value TEXT)")
    connection.execute("CREATE INDEX records_value ON records (value)")
    connection.execute("CREATE TABLE alembic_version (version_num TEXT PRIMARY KEY)")
    connection.execute("INSERT INTO alembic_version VALUES ('current-revision')")
    connection.execute("INSERT INTO records (value) VALUES ('committed')")
    connection.commit()
    connection.execute("INSERT INTO records (value) VALUES ('uncommitted')")
    assert database.with_name(f"{database.name}-wal").exists()

    snapshot = await backups.create_backup(database, directory, 7)
    assert not snapshot.with_name(f"{snapshot.name}-wal").exists()
    assert not snapshot.with_name(f"{snapshot.name}-shm").exists()

    with closing(sqlite3.connect(snapshot)) as restored:
        assert restored.execute("SELECT value FROM records").fetchall() == [
            ("committed",)
        ]
        assert restored.execute(
            "SELECT version_num FROM alembic_version"
        ).fetchone() == ("current-revision",)
        assert restored.execute(
            "SELECT name FROM sqlite_master WHERE type = 'index' AND name = ?",
            ("records_value",),
        ).fetchone() == ("records_value",)
        assert restored.execute("PRAGMA quick_check").fetchone() == ("ok",)

    connection.rollback()
    connection.close()
    database.unlink()
    for suffix in ("-wal", "-shm"):
        database.with_name(f"{database.name}{suffix}").unlink(missing_ok=True)
        snapshot.with_name(f"{snapshot.name}{suffix}").unlink(missing_ok=True)
    with closing(
        sqlite3.connect(f"{snapshot.resolve().as_uri()}?mode=ro&immutable=1", uri=True)
    ) as restored:
        assert restored.execute("SELECT count(*) FROM records").fetchone() == (1,)
    assert not snapshot.with_name(f"{snapshot.name}-wal").exists()
    assert not snapshot.with_name(f"{snapshot.name}-shm").exists()


async def test_create_backup_opens_source_read_only(monkeypatch, tmp_path):
    database = tmp_path / "source.db"
    with closing(sqlite3.connect(database)) as connection:
        connection.execute("CREATE TABLE example (value INTEGER)")
        connection.commit()
    real_connect = sqlite3.connect
    calls = []

    def recording_connect(target, *args, **kwargs):
        calls.append((target, kwargs.copy()))
        return real_connect(target, *args, **kwargs)

    monkeypatch.setattr(backups.sqlite3, "connect", recording_connect)
    await backups.create_backup(database, tmp_path / "backups", 7)

    source, options = calls[0]
    assert source == f"{database.resolve().as_uri()}?mode=ro"
    assert options == {"uri": True}


def test_sync_backup_uses_utc_timestamp_and_applies_retention(monkeypatch, tmp_path):
    database = tmp_path / "ziggy.sqlite3"
    with closing(sqlite3.connect(database)) as connection:
        connection.execute("CREATE TABLE example (value INTEGER)")
        connection.commit()
    retain = MagicMock()
    monkeypatch.setattr(backups, "apply_retention", retain)

    snapshot = backups._create_backup_sync(
        database,
        tmp_path / "backups",
        3,
        timestamp=datetime(2026, 1, 2, 4, 5, 6, 7, tzinfo=ZoneInfo("Europe/Paris")),
    )

    assert snapshot.name == "ziggy.sqlite3-20260102T030506.000007Z.backup.sqlite3"
    assert snapshot.is_file()
    assert not list(snapshot.parent.glob("*.tmp"))
    retain.assert_called_once_with(database, snapshot.parent, 3)


def test_backup_failure_cleans_temporary_file_without_retention(monkeypatch, tmp_path):
    retain = MagicMock()
    monkeypatch.setattr(backups, "apply_retention", retain)
    directory = tmp_path / "backups"
    directory.mkdir()
    previous = directory / "missing.db-20260101T070000.000000Z.backup.sqlite3"
    previous.write_text("previous snapshot", encoding="utf-8")

    with pytest.raises(sqlite3.OperationalError):
        backups._create_backup_sync(tmp_path / "missing.db", directory, 2)

    assert directory.is_dir()
    assert list(directory.iterdir()) == [previous]
    assert previous.read_text(encoding="utf-8") == "previous snapshot"
    retain.assert_not_called()


def test_failed_quick_check_closes_connections_and_does_not_publish(
    monkeypatch, tmp_path
):
    connections = []

    class FakeConnection:
        def __init__(self, *, check=None):
            self.check = check
            self.closed = False

        def backup(self, destination):
            assert destination is connections[1]

        def execute(self, statement):
            assert statement == "PRAGMA quick_check"
            return MagicMock(fetchone=MagicMock(return_value=self.check))

        def close(self):
            self.closed = True

    def connect(*_args, **_kwargs):
        connection = FakeConnection(check=None if not connections else ("corrupt",))
        connections.append(connection)
        return connection

    retain = MagicMock()
    monkeypatch.setattr(backups.sqlite3, "connect", connect)
    monkeypatch.setattr(backups, "apply_retention", retain)
    directory = tmp_path / "backups"

    with pytest.raises(sqlite3.DatabaseError, match="quick check failed"):
        backups._create_backup_sync(tmp_path / "source.db", directory, 7)

    assert all(connection.closed for connection in connections)
    assert list(directory.iterdir()) == []
    retain.assert_not_called()


def test_retention_keeps_newest_matching_regular_files(tmp_path):
    database = tmp_path / "ziggy.db"
    directory = tmp_path / "backups"
    directory.mkdir()
    names = [
        f"ziggy.db-2026010{day}T070000.000000Z.backup.sqlite3" for day in range(1, 6)
    ]
    for name in names:
        (directory / name).write_text(name, encoding="utf-8")
    unrelated = [
        directory / "other.db-20260105T070000.000000Z.backup.sqlite3",
        directory / "ziggy.db-invalid.backup.sqlite3",
        directory / ".ziggy.db-temporary.tmp",
    ]
    for path in unrelated:
        path.write_text("keep", encoding="utf-8")
    matching_directory = directory / "ziggy.db-20260106T070000.000000Z.backup.sqlite3"
    matching_directory.mkdir()

    backups.apply_retention(database, directory, 2)

    assert sorted(path.name for path in directory.iterdir()) == sorted(
        [*names[-2:], *(path.name for path in unrelated), matching_directory.name]
    )


def test_retention_ignores_symlinks(monkeypatch, tmp_path):
    directory = tmp_path / "backups"
    directory.mkdir()
    target = directory / "target"
    target.write_text("keep", encoding="utf-8")
    link = directory / "ziggy.db-20260101T070000.000000Z.backup.sqlite3"
    try:
        link.symlink_to(target)
    except OSError:
        pytest.skip("creating symlinks is not permitted")

    backups.apply_retention(tmp_path / "ziggy.db", directory, 1)

    assert link.is_symlink()
    assert target.read_text(encoding="utf-8") == "keep"


def test_zero_retention_keeps_all_without_reading_directory(tmp_path):
    backups.apply_retention(tmp_path / "ziggy.db", tmp_path / "missing", 0)


@pytest.mark.parametrize(
    ("now", "expected", "utc_hours"),
    [
        (
            datetime(2026, 3, 7, 7, tzinfo=ZoneInfo("America/New_York")),
            datetime(2026, 3, 8, 7, tzinfo=ZoneInfo("America/New_York")),
            23,
        ),
        (
            datetime(2026, 10, 31, 7, tzinfo=ZoneInfo("America/New_York")),
            datetime(2026, 11, 1, 7, tzinfo=ZoneInfo("America/New_York")),
            25,
        ),
    ],
)
def test_next_backup_run_keeps_local_time_across_dst(now, expected, utc_hours):
    result = backups.next_backup_run("0 7 * * *", now.tzinfo, now)

    assert result == expected
    assert (result.astimezone(UTC) - now.astimezone(UTC)) == timedelta(hours=utc_hours)


def test_next_backup_run_is_strictly_after_boundary_and_requires_aware_time():
    timezone = ZoneInfo("UTC")
    now = datetime(2026, 1, 1, 7, tzinfo=timezone)

    assert backups.next_backup_run("0 7 * * *", timezone, now) == datetime(
        2026, 1, 2, 7, tzinfo=timezone
    )
    with pytest.raises(ValueError, match="timezone-aware"):
        backups.next_backup_run("0 7 * * *", timezone, now.replace(tzinfo=None))


@pytest.mark.parametrize("now_timezone", [UTC, ZoneInfo("America/New_York")])
@pytest.mark.parametrize(
    ("schedule", "now", "expected"),
    [
        (
            "30 1 * * *",
            "2026-11-01T01:00:00-05:00",
            "2026-11-02T01:30:00-05:00",
        ),
        (
            "15,30,45 1,2 * * *",
            "2026-11-01T01:00:00-05:00",
            "2026-11-01T02:15:00-05:00",
        ),
        (
            "30 * * * *",
            "2026-11-01T01:00:00-05:00",
            "2026-11-01T01:30:00-05:00",
        ),
        (
            "30 * * * *",
            "2026-11-01T01:45:00-04:00",
            "2026-11-01T01:30:00-05:00",
        ),
    ],
)
def test_next_backup_run_is_future_during_repeated_dst_hour(
    schedule, now, expected, now_timezone
):
    timezone = ZoneInfo("America/New_York")
    now = datetime.fromisoformat(now).astimezone(now_timezone)

    result = backups.next_backup_run(schedule, timezone, now)

    assert result.tzinfo is timezone
    assert result.isoformat() == expected
    assert result.astimezone(UTC) > now.astimezone(UTC)


async def test_create_backup_waits_for_worker_cleanup_when_cancelled(
    monkeypatch, tmp_path
):
    started = threading.Event()
    release = threading.Event()

    def blocking_backup(*_args):
        started.set()
        assert release.wait(timeout=2)
        return tmp_path / "snapshot.db"

    monkeypatch.setattr(backups, "_create_backup_sync", blocking_backup)
    task = asyncio.create_task(
        backups.create_backup(tmp_path / "source.db", tmp_path / "backups", 7)
    )
    assert await asyncio.to_thread(started.wait, 1)

    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_scheduler_waits_until_next_run_and_stops(monkeypatch, tmp_path):
    stop = asyncio.Event()
    create = AsyncMock()
    delays = []

    def next_run(_schedule, _timezone, now):
        return now + timedelta(hours=1)

    async def wait(event, seconds):
        delays.append(seconds)
        event.set()

    monkeypatch.setattr(backups, "next_backup_run", next_run)
    monkeypatch.setattr(backups, "_wait", wait)
    monkeypatch.setattr(backups, "create_backup", create)

    await backups.run_backup_scheduler(
        tmp_path / "ziggy.db", backup_settings(tmp_path), stop
    )

    assert delays == [pytest.approx(3600)]
    create.assert_not_awaited()


async def test_scheduler_recovers_after_failure_and_recalculates(monkeypatch, tmp_path):
    stop = asyncio.Event()
    attempts = 0

    def next_run(_schedule, _timezone, now):
        return now

    async def create(*_args):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OSError("disk unavailable")
        stop.set()
        return tmp_path / "snapshot.db"

    log_exception = MagicMock()
    log_info = MagicMock()
    monkeypatch.setattr(backups, "next_backup_run", MagicMock(side_effect=next_run))
    monkeypatch.setattr(backups, "_wait", AsyncMock())
    monkeypatch.setattr(backups, "create_backup", create)
    monkeypatch.setattr(backups.logger, "exception", log_exception)
    monkeypatch.setattr(backups.logger, "info", log_info)

    await backups.run_backup_scheduler(
        tmp_path / "ziggy.db", backup_settings(tmp_path), stop
    )

    assert attempts == 2
    assert backups.next_backup_run.call_count == 2
    log_exception.assert_called_once_with("Database backup failed")
    assert log_info.call_count == 3


async def test_wait_returns_for_stop_and_timeout():
    stop = asyncio.Event()
    stop.set()
    await backups._wait(stop, 10)
    await backups._wait(asyncio.Event(), 0)
