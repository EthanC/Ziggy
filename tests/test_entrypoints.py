from __future__ import annotations

import runpy
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from ziggy.models import utc_now


def test_module_entrypoint_exits_with_cli_status(monkeypatch):
    main = MagicMock(return_value=7)
    monkeypatch.setattr("ziggy.cli.main", main)

    with pytest.raises(SystemExit) as raised:
        runpy.run_module("ziggy.__main__", run_name="__main__")

    assert raised.value.code == 7
    main.assert_called_once_with()


def test_utc_now_returns_current_aware_utc_timestamp():
    before = datetime.now(UTC)
    current = utc_now()
    after = datetime.now(UTC)

    assert current.tzinfo is UTC
    assert before <= current <= after
