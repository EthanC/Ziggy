from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, call

import pytest

from ziggy import logging as ziggy_logging
from ziggy.config import LoggingSettings, Secrets

# Private helpers are the logging boundaries under test.
# ruff: noqa: SLF001


def secrets(webhook: str | None = None) -> Secrets:
    return Secrets(
        archive_username="archive-user",
        archive_password="archive-password",  # noqa: S106
        reporting_webhook_url="https://reports.invalid/hook?token=report-token",
        logging_webhook_url=webhook,
    )


def test_redactor_replaces_secrets_and_url_queries_case_insensitively():
    record = {
        "message": (
            "archive-password https://EXAMPLE.test/path?a=1#fragment "
            "https://example.test/no-query and archive-password"
        )
    }

    ziggy_logging._redactor(("archive-password",))(record)

    assert record["message"] == (
        "<redacted> https://EXAMPLE.test/path?<redacted>#fragment "
        "https://example.test/no-query and <redacted>"
    )


@pytest.mark.parametrize(
    ("isatty", "expected_format"), [(True, "color"), (False, "plain")]
)
def test_configure_replaces_stderr_and_selects_terminal_format(
    monkeypatch, isatty, expected_format
):
    fake_logger = MagicMock()
    fake_logger.add.return_value = 12
    fake_stderr = MagicMock()
    fake_stderr.isatty.return_value = isatty
    intercept = MagicMock()
    monkeypatch.setattr(ziggy_logging, "logger", fake_logger)
    monkeypatch.setattr(ziggy_logging.sys, "stderr", fake_stderr)
    monkeypatch.setattr(ziggy_logging, "_intercept_standard_logging", intercept)
    controller = ziggy_logging.LoggingController(stderr_sink_id=4)

    controller.configure(LoggingSettings(level="DEBUG"), secrets())

    fake_logger.configure.assert_called_once()
    patcher = fake_logger.configure.call_args.kwargs["patcher"]
    record = {"message": "archive-user archive-password https://x.invalid/?secret=1"}
    patcher(record)
    assert record["message"] == "<redacted> <redacted> https://x.invalid/?<redacted>"
    assert fake_logger.remove.call_args_list == [call(4)]
    assert fake_logger.add.call_args.kwargs == {
        "level": "DEBUG",
        "colorize": isatty,
        "format": (
            ziggy_logging._COLOR_FORMAT
            if expected_format == "color"
            else ziggy_logging._PLAIN_FORMAT
        ),
    }
    assert controller.stderr_sink_id == 12
    assert controller.discord_sink_id is None
    intercept.assert_called_once_with()


def test_first_configuration_removes_only_loguru_default_handler(monkeypatch):
    fake_logger = MagicMock()
    fake_logger.add.return_value = 12
    monkeypatch.setattr(ziggy_logging, "logger", fake_logger)
    monkeypatch.setattr(ziggy_logging, "_intercept_standard_logging", MagicMock())

    ziggy_logging.LoggingController().configure(LoggingSettings(), secrets())

    assert fake_logger.remove.call_args_list == [call(0)]
    assert call() not in fake_logger.remove.call_args_list


def test_first_configuration_tolerates_missing_loguru_default_handler(monkeypatch):
    fake_logger = MagicMock()
    fake_logger.remove.side_effect = ValueError("already removed")
    fake_logger.add.return_value = 12
    monkeypatch.setattr(ziggy_logging, "logger", fake_logger)
    monkeypatch.setattr(ziggy_logging, "_intercept_standard_logging", MagicMock())

    controller = ziggy_logging.LoggingController()
    controller.configure(LoggingSettings(), secrets())

    assert controller.stderr_sink_id == 12


def test_configure_reloads_discord_sink_only_when_key_changes(monkeypatch):
    fake_logger = MagicMock()
    fake_logger.add.side_effect = [10, 20, 11, 12, 21, 13]
    sink = MagicMock(side_effect=lambda url: ("discord", url))
    monkeypatch.setattr(ziggy_logging, "logger", fake_logger)
    monkeypatch.setattr(ziggy_logging, "DiscordSink", sink)
    monkeypatch.setattr(ziggy_logging, "_intercept_standard_logging", MagicMock())
    monkeypatch.setattr(
        ziggy_logging.sys, "stderr", SimpleNamespace(isatty=lambda: False)
    )
    controller = ziggy_logging.LoggingController()
    first = secrets("https://logs.invalid/one")

    controller.configure(LoggingSettings(discord_min_level="WARNING"), first)
    assert controller.discord_sink_id == 20
    assert controller.discord_key == ("https://logs.invalid/one", "WARNING")
    assert fake_logger.add.call_args_list[-1] == call(
        ("discord", "https://logs.invalid/one"), level="WARNING", enqueue=True
    )

    controller.configure(LoggingSettings(discord_min_level="WARNING"), first)
    assert sink.call_count == 1
    assert controller.discord_sink_id == 20
    assert call(20) not in fake_logger.remove.call_args_list

    controller.configure(LoggingSettings(discord_min_level="ERROR"), first)
    assert call(20) in fake_logger.remove.call_args_list
    assert controller.discord_sink_id == 21
    assert controller.discord_key == ("https://logs.invalid/one", "ERROR")

    controller.configure(LoggingSettings(), secrets())
    assert call(21) in fake_logger.remove.call_args_list
    assert controller.discord_sink_id is None
    assert controller.discord_key is None


async def test_close_flushes_loguru(monkeypatch):
    complete = AsyncMock()
    monkeypatch.setattr(ziggy_logging, "logger", SimpleNamespace(complete=complete))

    await ziggy_logging.LoggingController().close()

    complete.assert_awaited_once_with()


def test_intercept_handler_forwards_named_level_exception_and_message(monkeypatch):
    fake_logger = MagicMock()
    fake_logger.level.return_value.name = "WARNING"
    optimized = fake_logger.opt.return_value
    monkeypatch.setattr(ziggy_logging, "logger", fake_logger)
    record = logging.LogRecord(
        "dependency", logging.WARNING, __file__, 10, "hello %s", ("world",), None
    )

    ziggy_logging.InterceptHandler().emit(record)

    fake_logger.level.assert_called_once_with("WARNING")
    fake_logger.opt.assert_called_once_with(depth=2, exception=None)
    optimized.log.assert_called_once_with("WARNING", "hello world")


def test_intercept_handler_falls_back_to_numeric_custom_level(monkeypatch):
    fake_logger = MagicMock()
    fake_logger.level.side_effect = ValueError
    optimized = fake_logger.opt.return_value
    monkeypatch.setattr(ziggy_logging, "logger", fake_logger)
    record = logging.LogRecord("custom", 35, __file__, 10, "custom", (), None)

    ziggy_logging.InterceptHandler().emit(record)

    optimized.log.assert_called_once_with(35, "custom")


def test_intercept_handler_skips_standard_logging_frames(monkeypatch):
    fake_logger = MagicMock()
    fake_logger.level.return_value.name = "INFO"
    logging_frame = SimpleNamespace(
        f_code=SimpleNamespace(co_filename=logging.__file__),
        f_back=SimpleNamespace(f_code=SimpleNamespace(co_filename=__file__)),
    )
    monkeypatch.setattr(ziggy_logging, "logger", fake_logger)
    monkeypatch.setattr(ziggy_logging.logging, "currentframe", lambda: logging_frame)
    record = logging.LogRecord(
        "dependency", logging.INFO, __file__, 10, "hello", (), None
    )

    ziggy_logging.InterceptHandler().emit(record)

    fake_logger.opt.assert_called_once_with(depth=3, exception=None)


def test_intercept_standard_logging_replaces_root_and_normalizes_dependencies():
    root = logging.getLogger()
    dependency_names = ("alembic", "archivist", "clyde", "niquests", "sqlalchemy")
    old_root_handlers = root.handlers[:]
    old_root_level = root.level
    old_dependencies = {
        name: (logging.getLogger(name).handlers[:], logging.getLogger(name).propagate)
        for name in dependency_names
    }
    try:
        for name in dependency_names:
            dependency = logging.getLogger(name)
            dependency.handlers = [logging.NullHandler()]
            dependency.propagate = False

        ziggy_logging._intercept_standard_logging()

        assert len(root.handlers) == 1
        assert isinstance(root.handlers[0], ziggy_logging.InterceptHandler)
        assert root.level == logging.NOTSET
        for name in dependency_names:
            dependency = logging.getLogger(name)
            assert dependency.handlers == []
            assert dependency.propagate is True
    finally:
        root.handlers = old_root_handlers
        root.setLevel(old_root_level)
        for name, (handlers, propagate) in old_dependencies.items():
            dependency = logging.getLogger(name)
            dependency.handlers = handlers
            dependency.propagate = propagate
