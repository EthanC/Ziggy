"""Loguru setup, standard-library interception, and Discord sink reloads."""

from __future__ import annotations

import logging
import re
import sys
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING

from loguru import logger
from loguru_discord import DiscordSink

if TYPE_CHECKING:
    from collections.abc import Callable

    from loguru import Record

    from ziggy.config import LoggingSettings, Secrets

_URL_QUERY_RE = re.compile(r"(https?://[^\s?#]+)\?[^\s#]*", re.IGNORECASE)
_PLAIN_FORMAT = (
    "{time:YYYY-MM-DDTHH:mm:ss.SSSZ} | {level:<8} | "
    "{name}:{function}:{line} | {message}"
)
_COLOR_FORMAT = (
    "<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
    "<level>{message}</level>"
)


class InterceptHandler(logging.Handler):
    """Route standard-library records through Loguru."""

    def emit(self, record: logging.LogRecord) -> None:
        """Forward one record while preserving exception information."""
        try:
            level: str | int = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno
        frame = logging.currentframe()
        depth = 2
        while frame is not None and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1
        logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )


@dataclass(slots=True)
class LoggingController:
    """Own Loguru sink IDs so configuration reloads replace them safely."""

    stderr_sink_id: int | None = None
    discord_sink_id: int | None = None
    discord_key: tuple[str, str] | None = None

    def configure(self, settings: LoggingSettings, secrets: Secrets) -> None:
        """Apply local logging and replace the Discord sink when needed."""
        sensitive = tuple(
            value
            for value in (
                secrets.archive_email,
                secrets.archive_password,
                secrets.reporting_webhook_url,
                secrets.logging_webhook_url,
            )
            if value
        )
        logger.configure(patcher=_redactor(sensitive))
        if self.stderr_sink_id is None:
            with suppress(ValueError):
                logger.remove(0)
        else:
            logger.remove(self.stderr_sink_id)
        interactive = sys.stderr.isatty()
        self.stderr_sink_id = logger.add(
            sys.stderr,
            level=settings.level,
            colorize=interactive,
            format=_COLOR_FORMAT if interactive else _PLAIN_FORMAT,
        )
        _intercept_standard_logging()

        key = (
            (secrets.logging_webhook_url, settings.discord_min_level)
            if secrets.logging_webhook_url
            else None
        )
        if key == self.discord_key:
            return
        if self.discord_sink_id is not None:
            logger.remove(self.discord_sink_id)
            self.discord_sink_id = None
        self.discord_key = key
        if key is not None:
            webhook_url, level = key
            self.discord_sink_id = logger.add(
                DiscordSink(webhook_url),
                level=level,
                enqueue=True,
            )

    async def close(self) -> None:
        """Flush queued records before process shutdown."""
        await logger.complete()


def _redactor(sensitive: tuple[str, ...]) -> Callable[[Record], None]:
    def patch(record: Record) -> None:
        message = record["message"]
        for value in sensitive:
            message = message.replace(value, "<redacted>")
        record["message"] = _URL_QUERY_RE.sub(r"\1?<redacted>", message)

    return patch


def _intercept_standard_logging() -> None:
    handler = InterceptHandler()
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(logging.NOTSET)
    for name in ("alembic", "archivist", "clyde", "niquests", "sqlalchemy"):
        dependency = logging.getLogger(name)
        dependency.handlers.clear()
        dependency.propagate = True
