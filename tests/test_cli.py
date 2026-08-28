from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from ziggy import cli
from ziggy.config import ConfigError

CONFIG_PATH = Path("custom.toml")


def config(database: Path = Path("database.sqlite3")):
    return SimpleNamespace(ziggy=SimpleNamespace(database=database))


def test_parser_requires_command():
    with pytest.raises(SystemExit) as caught:
        cli.main([])

    assert caught.value.code == 2


def test_check_config_resolves_secrets_and_returns_success(monkeypatch):
    loaded = config()
    load = MagicMock(return_value=loaded)
    resolve = MagicMock()
    monkeypatch.setattr(cli, "load_config", load)
    monkeypatch.setattr(cli, "resolve_secrets", resolve)

    assert cli.main(["check-config", "--config", str(CONFIG_PATH)]) == 0
    load.assert_called_once_with(CONFIG_PATH)
    resolve.assert_called_once_with()


@pytest.mark.parametrize(("healthy", "exit_code"), [(True, 0), (False, 1)])
def test_healthcheck_exit_code(monkeypatch, healthy, exit_code):
    database = Path("health.sqlite3")
    monkeypatch.setattr(cli, "load_config", MagicMock(return_value=config(database)))
    check = AsyncMock(return_value=healthy)
    monkeypatch.setattr(cli, "check_health", check)

    assert cli.main(["healthcheck", "--config", str(CONFIG_PATH)]) == exit_code
    check.assert_awaited_once_with(database)


def test_run_starts_service_and_returns_success(monkeypatch):
    monkeypatch.setattr(cli, "load_config", MagicMock(return_value=config()))
    run = AsyncMock()
    monkeypatch.setattr(cli, "run_service", run)

    assert cli.main(["run", "--config", str(CONFIG_PATH)]) == 0
    run.assert_awaited_once_with(CONFIG_PATH)


@pytest.mark.parametrize("command", ["run", "check-config", "healthcheck"])
def test_config_error_is_safe_and_returns_two(monkeypatch, capsys, command):
    monkeypatch.setattr(
        cli, "load_config", MagicMock(side_effect=ConfigError("invalid secret value"))
    )

    assert cli.main([command]) == 2
    assert capsys.readouterr().err == "ziggy: invalid secret value\n"


def test_keyboard_interrupt_returns_shell_interrupt_code(monkeypatch, capsys):
    monkeypatch.setattr(cli, "load_config", MagicMock(side_effect=KeyboardInterrupt))

    assert cli.main(["run"]) == 130
    assert capsys.readouterr().err == ""


def test_unexpected_error_redacts_details_and_returns_one(monkeypatch, capsys):
    monkeypatch.setattr(
        cli, "load_config", MagicMock(side_effect=RuntimeError("sensitive details"))
    )

    assert cli.main(["run"]) == 1
    assert capsys.readouterr().err == "ziggy: RuntimeError\n"
