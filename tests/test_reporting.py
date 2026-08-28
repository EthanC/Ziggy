from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import msgspec
import pytest
from clyde.components import Container, Seperator, TextDisplay
from clyde.webhook import MessageFlags
from niquests.exceptions import RequestException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from ziggy import reporting
from ziggy.models import (
    ArchiveJob,
    ArchiveJobKind,
    ArchiveJobState,
    Base,
    Capture,
    Domain,
    Page,
    Report,
    ReportState,
)
from ziggy.reporting import ReportWindow

# Tests intentionally exercise private pure helpers and Clyde's serialized fields.
# ruff: noqa: SLF001


NOW = datetime(2026, 8, 28, 12, 34, 56, tzinfo=UTC)


def make_report(**overrides: object) -> Report:
    values = {
        "window_start": NOW - timedelta(days=1),
        "window_end": NOW,
        "generated_at": NOW,
        "discovered_count": 5,
        "archived_count": 3,
        "outstanding_count": 2,
        "active_domain_count": 4,
        "state": ReportState.PENDING,
        "attempts": 0,
        "next_attempt_at": NOW,
        "lease_owner": "worker",
        "lease_expires_at": NOW + timedelta(minutes=5),
    }
    values.update(overrides)
    return Report(**values)


@pytest.fixture
async def sessions():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    yield async_sessionmaker(engine, expire_on_commit=False)
    await engine.dispose()


@pytest.mark.parametrize("interval", [timedelta(0), timedelta(seconds=-1)])
def test_next_report_window_rejects_nonpositive_interval(interval):
    with pytest.raises(ValueError, match="interval must be positive"):
        reporting.next_report_window(None, NOW, interval)


def test_next_report_window_rejects_naive_now():
    with pytest.raises(ValueError, match="timezone-aware"):
        reporting.next_report_window(None, NOW.replace(tzinfo=None), timedelta(hours=1))


def test_next_report_window_aligns_first_window_to_last_closed_boundary():
    window = reporting.next_report_window(None, NOW, timedelta(hours=1))

    assert window == ReportWindow(
        datetime(2026, 8, 28, 11, tzinfo=UTC),
        datetime(2026, 8, 28, 12, tzinfo=UTC),
    )


def test_next_report_window_preserves_checkpoint_and_waits_for_open_window():
    interval = timedelta(hours=1)
    checkpoint = datetime(2026, 8, 28, 9, tzinfo=UTC)

    assert reporting.next_report_window(checkpoint, NOW, interval) == ReportWindow(
        checkpoint, datetime(2026, 8, 28, 10, tzinfo=UTC)
    )
    assert (
        reporting.next_report_window(
            datetime(2026, 8, 28, 12, tzinfo=UTC), NOW, interval
        )
        is None
    )


async def test_create_report_uses_fixed_half_open_counts_and_is_idempotent(sessions):
    start = datetime(2026, 8, 27, tzinfo=UTC)
    end = start + timedelta(days=1)
    generated = end + timedelta(minutes=5)
    async with sessions() as session:
        active = Domain(
            host="active.example",
            scheme="https",
            include_subdomains=False,
            active=True,
            created_at=start,
            configured_at=start,
        )
        inactive = Domain(
            host="inactive.example",
            scheme="https",
            include_subdomains=False,
            active=False,
            created_at=start,
            configured_at=start,
        )
        session.add_all([active, inactive])
        await session.flush()
        archived = Page(
            domain_id=active.id,
            url="https://active.example/archived",
            discovered_at=start,
            next_crawl_at=start,
            next_archive_at=start,
        )
        outstanding = Page(
            domain_id=active.id,
            url="https://active.example/outstanding",
            discovered_at=end - timedelta(microseconds=1),
            next_crawl_at=start,
            next_archive_at=start,
        )
        before = Page(
            domain_id=active.id,
            url="https://active.example/before",
            discovered_at=start - timedelta(microseconds=1),
            next_crawl_at=start,
            next_archive_at=start,
        )
        boundary = Page(
            domain_id=active.id,
            url="https://active.example/boundary",
            discovered_at=end,
            next_crawl_at=start,
            next_archive_at=start,
        )
        session.add_all([archived, outstanding, before, boundary])
        await session.flush()
        job = ArchiveJob(
            page_id=archived.id,
            kind=ArchiveJobKind.DIRECT,
            state=ArchiveJobState.SUCCEEDED,
            cycle_key="archived-cycle",
            intent_at=start,
            next_attempt_at=start,
        )
        session.add(job)
        await session.flush()
        session.add(
            Capture(
                page_id=archived.id,
                archive_job_id=job.id,
                captured_at=end,
                wayback_url="https://web.archive.invalid/capture",
                completed_at=end,
            )
        )
        await session.commit()

        report = await reporting.create_report(
            session, ReportWindow(start, end), generated
        )
        assert (
            report.discovered_count,
            report.archived_count,
            report.outstanding_count,
            report.active_domain_count,
        ) == (2, 1, 1, 1)
        assert report.state is ReportState.PENDING
        assert report.next_attempt_at == generated

        outstanding.discovered_at = start - timedelta(days=2)
        await session.commit()
        duplicate = await reporting.create_report(
            session, ReportWindow(start, end), generated + timedelta(hours=1)
        )
        assert duplicate.id == report.id
        assert duplicate.discovered_count == 2
        assert len((await session.scalars(select(Report))).all()) == 1


async def test_create_report_raises_if_insert_cannot_be_read():
    session = MagicMock()
    session.scalar = AsyncMock(side_effect=[None, None, None, None])
    session.execute = AsyncMock()
    session.commit = AsyncMock()

    with pytest.raises(RuntimeError, match="was not persisted"):
        await reporting.create_report(
            session, ReportWindow(NOW - timedelta(hours=1), NOW), NOW
        )


@pytest.mark.parametrize(
    ("outstanding", "color"),
    [(0, reporting._COMPLETE_COLOR), (2, reporting._OUTSTANDING_COLOR)],
)
def test_build_report_webhook_is_components_v2_without_legacy_fields(
    outstanding, color
):
    report = make_report(outstanding_count=outstanding)

    webhook = reporting.build_report_webhook(
        report, "https://discord.invalid/api/webhooks/id/token"
    )

    assert webhook.get_flag(MessageFlags.IS_COMPONENTS_V2)
    assert webhook._query_params == {"wait": "True"}
    assert webhook.allowed_mentions.parse == []
    assert webhook.allowed_mentions.replied_user is False
    assert len(webhook.components) == 1
    container = webhook.components[0]
    assert isinstance(container, Container)
    assert container.accent_color == color
    assert [type(component) for component in container.components] == [
        TextDisplay,
        Seperator,
        TextDisplay,
        TextDisplay,
    ]
    assert "2026-08-27T12:34:56+00:00 to 2026-08-28T12:34:56+00:00" in (
        container.components[0].content
    )
    assert container.components[2].content == (
        f"**Discovered:** 5\n**Archived:** 3\n**Outstanding:** {outstanding}"
    )
    assert "Active domains: 4" in container.components[3].content
    assert webhook.content is msgspec.UNSET
    assert webhook.embeds is msgspec.UNSET
    assert webhook.poll is msgspec.UNSET
    assert webhook._attachments == []


def test_build_report_webhook_defensively_rejects_missing_v2_flag(monkeypatch):
    webhook = MagicMock()
    webhook.get_flag.return_value = False
    monkeypatch.setattr(reporting, "Webhook", MagicMock(return_value=webhook))

    with pytest.raises(RuntimeError, match="flag was not set"):
        reporting.build_report_webhook(make_report(), "https://discord.invalid/hook")

    webhook.set_wait.assert_called_once_with(True)  # noqa: FBT003
    webhook.add_component.assert_called_once()


async def test_deliver_report_without_webhook_logs_and_releases_lease(monkeypatch):
    report = make_report()
    session = MagicMock()
    session.commit = AsyncMock()
    info = MagicMock()
    monkeypatch.setattr(reporting.logger, "info", info)

    await reporting.deliver_report(session, report, None, NOW)

    assert report.state is ReportState.LOGGED
    assert report.delivered_at == NOW
    assert report.lease_owner is None
    assert report.lease_expires_at is None
    session.commit.assert_awaited_once()
    info.assert_called_once()


async def test_deliver_report_with_no_changes_logs_without_discord(monkeypatch):
    report = make_report(
        discovered_count=0,
        archived_count=0,
        outstanding_count=0,
    )
    session = MagicMock()
    session.commit = AsyncMock()
    info = MagicMock()
    build_webhook = MagicMock()
    monkeypatch.setattr(reporting.logger, "info", info)
    monkeypatch.setattr(reporting, "build_report_webhook", build_webhook)

    await reporting.deliver_report(session, report, "https://discord.invalid", NOW)

    assert report.state is ReportState.LOGGED
    assert report.delivered_at == NOW
    assert report.lease_owner is None
    assert report.lease_expires_at is None
    info.assert_called_once_with(
        "Archive report {} to {}: no changes to report",
        report.window_start,
        report.window_end,
    )
    build_webhook.assert_not_called()
    session.commit.assert_awaited_once()


async def test_deliver_report_persists_discord_metadata(monkeypatch):
    report = make_report(error="old")
    response = MagicMock()
    response.json.return_value = {
        "id": "message",
        "channel_id": "channel",
        "webhook_id": "webhook",
    }
    webhook = MagicMock()
    webhook.execute_async = AsyncMock(return_value=response)
    monkeypatch.setattr(reporting, "build_report_webhook", lambda *_: webhook)
    session = MagicMock()
    session.commit = AsyncMock()

    await reporting.deliver_report(session, report, "https://discord.invalid", NOW)

    assert report.state is ReportState.DELIVERED
    assert report.delivered_at == NOW
    assert report.error is None
    assert (
        report.discord_message_id,
        report.discord_channel_id,
        report.discord_webhook_id,
    ) == ("message", "channel", "webhook")
    assert report.lease_owner is None
    session.commit.assert_awaited_once()


@pytest.mark.parametrize(
    "failure",
    [RequestException("offline"), ValueError("bad json"), TypeError("bad json")],
)
async def test_deliver_report_retries_boundary_failures(monkeypatch, failure):
    report = make_report(attempts=0)
    webhook = MagicMock()
    webhook.execute_async = AsyncMock(side_effect=failure)
    monkeypatch.setattr(reporting, "build_report_webhook", lambda *_: webhook)
    session = MagicMock()
    session.commit = AsyncMock()

    await reporting.deliver_report(session, report, "https://discord.invalid", NOW)

    assert report.state is ReportState.FAILED
    assert report.attempts == 1
    assert report.next_attempt_at == NOW + timedelta(seconds=2)
    assert report.error == type(failure).__name__
    assert report.lease_owner is None
    session.commit.assert_awaited_once()


async def test_deliver_report_retries_invalid_discord_response(monkeypatch):
    report = make_report()
    response = MagicMock()
    response.json.return_value = {"id": "message"}
    webhook = MagicMock()
    webhook.execute_async = AsyncMock(return_value=response)
    monkeypatch.setattr(reporting, "build_report_webhook", lambda *_: webhook)
    session = MagicMock()
    session.commit = AsyncMock()

    await reporting.deliver_report(session, report, "https://discord.invalid", NOW)

    assert report.state is ReportState.FAILED
    assert report.error == "InvalidDiscordResponse"


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        (None, None),
        ({}, None),
        ({"id": 1, "channel_id": "2"}, None),
        ({"id": "1", "channel_id": 2}, None),
        ({"id": "1", "channel_id": "2", "webhook_id": 3}, None),
        ({"id": "1", "channel_id": "2"}, ("1", "2", None)),
    ],
)
def test_message_metadata_validation(payload, expected):
    assert reporting._message_metadata(payload) == expected


def test_fail_report_caps_exponential_retry_and_releases_lease():
    report = make_report(attempts=11)

    reporting._fail_report(report, NOW, "Failure")

    assert report.attempts == 12
    assert report.next_attempt_at == NOW + timedelta(hours=1)
    assert report.error == "Failure"
    assert report.lease_owner is None
    assert report.lease_expires_at is None


async def test_claim_report_takes_oldest_eligible_and_respects_lease(sessions):
    async with sessions() as session:
        reports = [
            make_report(
                window_start=NOW - timedelta(days=3),
                window_end=NOW - timedelta(days=2),
                next_attempt_at=NOW,
                lease_owner="other",
                lease_expires_at=NOW + timedelta(minutes=1),
            ),
            make_report(
                window_start=NOW - timedelta(days=2),
                window_end=NOW - timedelta(days=1),
                state=ReportState.FAILED,
                next_attempt_at=NOW,
                lease_owner=None,
                lease_expires_at=None,
            ),
            make_report(
                window_start=NOW - timedelta(days=1),
                window_end=NOW,
                next_attempt_at=NOW + timedelta(seconds=1),
                lease_owner=None,
                lease_expires_at=None,
            ),
        ]
        session.add_all(reports)
        await session.commit()

        claimed = await reporting.claim_report(
            session, "mine", NOW, timedelta(minutes=5)
        )
        assert claimed is not None
        assert claimed.window_start == NOW - timedelta(days=2)
        assert claimed.lease_owner == "mine"
        assert claimed.lease_expires_at == NOW + timedelta(minutes=5)

        assert (
            await reporting.claim_report(session, "mine", NOW, timedelta(minutes=5))
            is None
        )
