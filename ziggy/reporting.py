"""Fixed report windows and Discord Components v2 delivery."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from clyde import AllowedMentions, Markdown, Timestamp, Webhook
from clyde.components import (
    ActionRow,
    Container,
    LinkButton,
    Seperator,
    SeperatorSpacing,
    TextDisplay,
)
from clyde.webhook import MessageFlags
from loguru import logger
from niquests import exceptions as niquests_exceptions
from sqlalchemy import distinct, exists, func, or_, select, update
from sqlalchemy.dialects.sqlite import insert

from ziggy.models import Capture, Domain, Page, Report, ReportState

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


@dataclass(frozen=True, slots=True)
class ReportWindow:
    """One closed, fixed reporting interval."""

    start: datetime
    end: datetime


def next_report_window(
    last_end: datetime | None,
    now: datetime,
    interval: timedelta,
    finalization_grace: timedelta = timedelta(minutes=5),
) -> ReportWindow | None:
    """Return the next closed interval without skipping persisted checkpoints."""
    seconds = int(interval.total_seconds())
    if seconds <= 0:
        raise ValueError("report interval must be positive")
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("current time must be timezone-aware")
    closed_end = datetime.fromtimestamp(
        int((now - finalization_grace).timestamp()) // seconds * seconds, tz=UTC
    )
    end = closed_end if last_end is None else last_end + interval
    if end > closed_end:
        return None
    return ReportWindow(end - interval, end)


async def create_report(
    session: AsyncSession, window: ReportWindow, generated_at: datetime
) -> Report:
    """Calculate counts once and commit the report before delivery."""
    range_filter = (
        Page.discovered_at >= window.start,
        Page.discovered_at < window.end,
    )
    discovered = await session.scalar(select(func.count(Page.id)).where(*range_filter))
    archived = await session.scalar(
        select(func.count(distinct(Capture.page_id))).where(
            Capture.completed_at >= window.start,
            Capture.completed_at < window.end,
        )
    )
    outstanding = await session.scalar(
        select(func.count(Page.id)).where(
            *range_filter,
            ~exists(
                select(Capture.id).where(
                    Capture.page_id == Page.id,
                    Capture.completed_at < window.end,
                )
            ),
        )
    )
    first_archives = await session.scalar(
        select(func.count(Capture.id)).where(
            Capture.completed_at >= window.start,
            Capture.completed_at < window.end,
            Capture.first_archive.is_(True),
        )
    )
    deactivated = await session.scalar(
        select(func.count(Page.id)).where(
            Page.deactivated_at >= window.start,
            Page.deactivated_at < window.end,
        )
    )
    active_domains = await session.scalar(
        select(func.count(Domain.id)).where(Domain.active.is_(True))
    )
    lifetime_discovered = await session.scalar(
        select(func.count(Page.id)).where(Page.discovered_at < window.end)
    )
    lifetime_archived = await session.scalar(
        select(func.count(distinct(Capture.page_id))).where(
            Capture.completed_at < window.end
        )
    )
    lifetime_first_archives = await session.scalar(
        select(func.count(Capture.id)).where(
            Capture.completed_at < window.end,
            Capture.first_archive.is_(True),
        )
    )
    lifetime_deactivated = await session.scalar(
        select(func.count(Page.id)).where(Page.deactivated_at < window.end)
    )
    discovered_count = discovered or 0
    archived_count = archived or 0
    await session.execute(
        insert(Report)
        .values(
            window_start=window.start,
            window_end=window.end,
            generated_at=generated_at,
            discovered_count=discovered_count,
            archived_count=archived_count,
            outstanding_count=outstanding or 0,
            lifetime_discovered_count=lifetime_discovered or 0,
            lifetime_archived_count=lifetime_archived or 0,
            first_archive_count=first_archives or 0,
            lifetime_first_archive_count=lifetime_first_archives or 0,
            deactivated_count=deactivated or 0,
            lifetime_deactivated_count=lifetime_deactivated or 0,
            active_domain_count=active_domains or 0,
            state=ReportState.PENDING,
            next_attempt_at=generated_at,
        )
        .on_conflict_do_nothing(index_elements=[Report.window_start, Report.window_end])
    )
    await session.commit()
    report = await session.scalar(
        select(Report).where(
            Report.window_start == window.start,
            Report.window_end == window.end,
        )
    )
    if report is None:
        raise RuntimeError("report row was not persisted")
    return report


def build_report_webhook(
    report: Report,
    webhook_url: str,
    previous_report: Report | None = None,
    web_archive_url: str | None = None,
) -> Webhook:
    """Construct the required Components v2 message without legacy fields."""
    previous_discovered = (
        previous_report.discovered_count if previous_report is not None else 0
    )
    previous_archived = (
        previous_report.archived_count if previous_report is not None else 0
    )
    previous_first_archives = (
        previous_report.first_archive_count if previous_report is not None else 0
    )
    previous_deactivated = (
        previous_report.deactivated_count if previous_report is not None else 0
    )
    previous_outstanding = (
        previous_report.outstanding_count if previous_report is not None else 0
    )
    first_archive_change = _change_stat(
        report.first_archive_count, previous_first_archives
    )
    page_stats = Markdown.bulleted_list(
        [
            (
                f"Discovered: {Markdown.bold(f'{report.discovered_count:,}')}"
                f"{_change_stat(report.discovered_count, previous_discovered)} | "
                f"{Markdown.bold(f'{report.lifetime_discovered_count:,}')} Lifetime"
            ),
            (
                f"Archived: {Markdown.bold(f'{report.archived_count:,}')}"
                f"{_change_stat(report.archived_count, previous_archived)} | "
                f"{Markdown.bold(f'{report.lifetime_archived_count:,}')} Lifetime"
            ),
            {
                "value": (
                    "First Archives: "
                    f"{Markdown.bold(f'{report.first_archive_count:,}')}"
                    f"{first_archive_change} | "
                    f"{Markdown.bold(f'{report.lifetime_first_archive_count:,}')} "
                    "Lifetime"
                ),
                "indent": 1,
            },
            (
                f"Deactivated: {Markdown.bold(f'{report.deactivated_count:,}')}"
                f"{_change_stat(report.deactivated_count, previous_deactivated)} | "
                f"{Markdown.bold(f'{report.lifetime_deactivated_count:,}')} Lifetime"
            ),
            (
                f"Pending: {Markdown.bold(f'{report.outstanding_count:,}')}"
                f"{_change_stat(report.outstanding_count, previous_outstanding)}"
            ),
        ]
    )
    counts = page_stats
    context = Markdown.subtext(
        f"Report for {Timestamp.short_date(report.window_start)} to "
        f"{Timestamp.short_date(report.window_end)} "
        f"(Generated {Timestamp.relative_time(report.generated_at)})"
    )
    container = Container(
        components=[
            TextDisplay(content=Markdown.header_2("Archival Report")),
            TextDisplay(content=counts),
            Seperator(divider=True, spacing=SeperatorSpacing.SMALL),
            TextDisplay(content=context),
        ],
        accent_color=0xDA3E44,
    )
    webhook = Webhook(
        url=webhook_url,
        allowed_mentions=AllowedMentions(parse=[], replied_user=False),
    )
    webhook.set_wait(True)
    webhook.add_component(container)
    if web_archive_url is not None:
        webhook.add_component(
            ActionRow(
                components=[LinkButton(label="View Archive", url=web_archive_url)]
            )
        )
    if not webhook.get_flag(MessageFlags.IS_COMPONENTS_V2):
        raise RuntimeError("Components v2 flag was not set")
    return webhook


async def deliver_report(
    session: AsyncSession,
    report: Report,
    webhook_url: str | None,
    now: datetime,
    web_archive_url: str | None = None,
) -> None:
    """Deliver, log, or reschedule one persisted report row."""
    if not any(
        (
            report.discovered_count,
            report.archived_count,
            report.outstanding_count,
            report.first_archive_count,
            report.deactivated_count,
        )
    ):
        logger.info(
            "Archive report {} to {}: no changes to report",
            report.window_start,
            report.window_end,
        )
        report.state = ReportState.LOGGED
        report.delivered_at = now
        _release_report(report)
        await session.commit()
        return
    if webhook_url is None:
        logger.info(
            "Archive report {} to {}: {} discovered, {} archived, "
            "{} first archives, {} deactivated, {} outstanding",
            report.window_start,
            report.window_end,
            report.discovered_count,
            report.archived_count,
            report.first_archive_count,
            report.deactivated_count,
            report.outstanding_count,
        )
        report.state = ReportState.LOGGED
        report.delivered_at = now
        _release_report(report)
        await session.commit()
        return
    previous_report = await session.scalar(
        select(Report)
        .where(Report.window_end <= report.window_start)
        .order_by(Report.window_end.desc())
        .limit(1)
    )
    try:
        response = await build_report_webhook(
            report, webhook_url, previous_report, web_archive_url
        ).execute_async()
        payload = response.json()
    except (niquests_exceptions.RequestException, ValueError, TypeError) as error:
        _fail_report(report, now, type(error).__name__)
        await session.commit()
        return
    metadata = _message_metadata(payload)
    if metadata is None:
        _fail_report(report, now, "InvalidDiscordResponse")
        await session.commit()
        return
    message_id, channel_id, webhook_id = metadata
    report.state = ReportState.DELIVERED
    report.delivered_at = now
    report.error = None
    report.discord_message_id = message_id
    report.discord_channel_id = channel_id
    report.discord_webhook_id = webhook_id
    _release_report(report)
    await session.commit()


async def claim_report(
    session: AsyncSession,
    owner: str,
    now: datetime,
    lease_duration: timedelta,
) -> Report | None:
    """Atomically claim the oldest report eligible for delivery retry."""
    candidate = (
        select(Report.id)
        .where(
            Report.state.in_((ReportState.PENDING, ReportState.FAILED)),
            Report.next_attempt_at <= now,
            or_(Report.lease_expires_at.is_(None), Report.lease_expires_at <= now),
        )
        .order_by(Report.window_start)
        .limit(1)
        .scalar_subquery()
    )
    statement = (
        update(Report)
        .where(
            Report.id == candidate,
            or_(Report.lease_expires_at.is_(None), Report.lease_expires_at <= now),
        )
        .values(lease_owner=owner, lease_expires_at=now + lease_duration)
        .returning(Report)
    )
    report = (await session.scalars(statement)).one_or_none()
    await session.commit()
    return report


def _release_report(report: Report) -> None:
    report.lease_owner = None
    report.lease_expires_at = None


def _change_stat(current: int, previous: int) -> str:
    change = current - previous
    return "" if change == 0 else f" ({change:+,})"


def _message_metadata(payload: object) -> tuple[str, str, str | None] | None:
    if not isinstance(payload, dict):
        return None
    message_id = payload.get("id")
    channel_id = payload.get("channel_id")
    webhook_id = payload.get("webhook_id")
    if not isinstance(message_id, str) or not isinstance(channel_id, str):
        return None
    if webhook_id is not None and not isinstance(webhook_id, str):
        return None
    return message_id, channel_id, webhook_id


def _fail_report(report: Report, now: datetime, error: str) -> None:
    report.state = ReportState.FAILED
    report.attempts += 1
    report.next_attempt_at = now + timedelta(seconds=min(3600, 2**report.attempts))
    report.error = error
    _release_report(report)
