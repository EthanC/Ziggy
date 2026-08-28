"""Ziggy command-line interface."""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

from ziggy.config import ConfigError, load_config, resolve_secrets
from ziggy.service import check_health, run_service


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ziggy")
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("run", "check-config", "healthcheck"):
        child = subparsers.add_parser(command)
        child.add_argument("--config", type=Path, default=Path("ziggy.toml"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Execute a Ziggy command and return its process exit code."""
    arguments = _parser().parse_args(argv)
    try:
        config = load_config(arguments.config)
        if arguments.command == "check-config":
            resolve_secrets()
            return 0
        if arguments.command == "healthcheck":
            return 0 if asyncio.run(check_health(config.ziggy.database)) else 1
        asyncio.run(run_service(arguments.config))
    except KeyboardInterrupt:
        return 130
    except ConfigError as error:
        print(f"ziggy: {error}", file=sys.stderr)  # noqa: T201
        return 2
    except Exception as error:  # noqa: BLE001 - CLI boundary returns a safe failure.
        print(f"ziggy: {type(error).__name__}", file=sys.stderr)  # noqa: T201
        return 1
    else:
        return 0
