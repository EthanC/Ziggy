"""Strict TOML configuration loading and secret resolution."""

from __future__ import annotations

import re
import tomllib
from dataclasses import dataclass, fields
from datetime import timedelta
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlsplit

from environs import Env
from loguru import logger

from ziggy.urls import normalize_host, normalize_url

_DURATION_RE = re.compile(r"^(?P<amount>[1-9][0-9]*)(?P<unit>s|m|h|d)$")
_INTERNET_ARCHIVE_EMAIL_ENV = "ZIGGY_INTERNET_ARCHIVE_EMAIL"
_INTERNET_ARCHIVE_PASSWORD_ENV = "ZIGGY_INTERNET_ARCHIVE_PASSWORD"  # noqa: S105
_DISCORD_WEBHOOK_URL_ENV = "ZIGGY_DISCORD_WEBHOOK_URL"
_LOG_DISCORD_WEBHOOK_URL_ENV = "ZIGGY_LOG_DISCORD_WEBHOOK_URL"


class ConfigError(ValueError):
    """Raised when configuration is malformed or incomplete."""


def parse_duration(value: object) -> timedelta:
    """Parse a positive integer duration using seconds, minutes, hours, or days."""
    if not isinstance(value, str) or (match := _DURATION_RE.fullmatch(value)) is None:
        raise ConfigError(
            "duration must be a positive integer followed by s, m, h, or d"
        )
    multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    return timedelta(seconds=int(match["amount"]) * multipliers[match["unit"]])


def _positive_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ConfigError(f"{name} must be a positive integer")
    return value


def _nonnegative_float(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float) or value < 0:
        raise ConfigError(f"{name} must be a nonnegative number")
    return float(value)


def _positive_float(value: object, name: str) -> float:
    result = _nonnegative_float(value, name)
    if result == 0:
        raise ConfigError(f"{name} must be greater than zero")
    return result


def _table(value: object, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigError(f"{name} must be a TOML table")
    return cast("dict[str, Any]", value)


def _only(table: dict[str, Any], allowed: set[str], name: str) -> None:
    unknown = sorted(table.keys() - allowed)
    if unknown:
        raise ConfigError(f"unknown {name} field(s): {', '.join(unknown)}")


@dataclass(frozen=True, slots=True)
class ZiggySettings:
    """Process-wide settings."""

    database: Path
    config_reload_interval: timedelta = timedelta(seconds=30)


@dataclass(frozen=True, slots=True)
class CrawlSettings:
    """Crawler scheduling and network limits."""

    interval: timedelta = timedelta(hours=24)
    concurrency: int = 8
    per_host_concurrency: int = 2
    request_delay: float = 1.0
    request_timeout: float = 30.0
    max_response_bytes: int = 10_485_760
    max_redirects: int = 10
    max_attempts: int = 5


@dataclass(frozen=True, slots=True)
class ArchiveSettings:
    """Internet Archive scheduling and authentication settings."""

    interval: timedelta = timedelta(days=30)
    concurrency: int = 1
    dedupe_window: timedelta = timedelta(hours=24)
    max_attempts: int = 5


@dataclass(frozen=True, slots=True)
class ReportingSettings:
    """Periodic report settings."""

    interval: timedelta = timedelta(hours=24)


@dataclass(frozen=True, slots=True)
class LoggingSettings:
    """Local and Discord logging settings."""

    level: str = "INFO"
    discord_min_level: str = "WARNING"


@dataclass(frozen=True, slots=True)
class DomainSettings:
    """One configured crawl and archive scope."""

    host: str
    scheme: str = "https"
    include_subdomains: bool = False
    seeds: tuple[str, ...] = ("/",)

    @property
    def base_url(self) -> str:
        """Return the normalized origin used to resolve seeds."""
        return f"{self.scheme}://{self.host}/"

    @property
    def seed_urls(self) -> tuple[str, ...]:
        """Return normalized seed URLs in this domain's scope."""
        return tuple(normalize_url(seed, base=self.base_url) for seed in self.seeds)


@dataclass(frozen=True, slots=True)
class Config:
    """A complete, validated Ziggy configuration."""

    ziggy: ZiggySettings
    crawl: CrawlSettings
    archive: ArchiveSettings
    reporting: ReportingSettings
    logging: LoggingSettings
    domains: tuple[DomainSettings, ...]


@dataclass(frozen=True, slots=True)
class Secrets:
    """Resolved credentials that are never persisted."""

    archive_email: str | None
    archive_password: str | None
    reporting_webhook_url: str | None
    logging_webhook_url: str | None


def _section(data: dict[str, Any], name: str) -> dict[str, Any]:
    return _table(data.get(name, {}), name)


def _parse_ziggy(data: dict[str, Any], config_dir: Path) -> ZiggySettings:
    _only(data, {"database", "config_reload_interval"}, "ziggy")
    database = data.get("database")
    if not isinstance(database, str) or not database:
        raise ConfigError("ziggy.database must be a nonempty path")
    path = Path(database).expanduser()
    if not path.is_absolute():
        path = config_dir / path
    return ZiggySettings(
        database=path.resolve(),
        config_reload_interval=parse_duration(
            data.get("config_reload_interval", "30s")
        ),
    )


def _parse_crawl(data: dict[str, Any]) -> CrawlSettings:
    names = {field.name for field in fields(CrawlSettings)}
    _only(data, names, "crawl")
    return CrawlSettings(
        interval=parse_duration(data.get("interval", "24h")),
        concurrency=_positive_int(data.get("concurrency", 8), "crawl.concurrency"),
        per_host_concurrency=_positive_int(
            data.get("per_host_concurrency", 2), "crawl.per_host_concurrency"
        ),
        request_delay=_nonnegative_float(
            data.get("request_delay", 1.0), "crawl.request_delay"
        ),
        request_timeout=_positive_float(
            data.get("request_timeout", 30.0), "crawl.request_timeout"
        ),
        max_response_bytes=_positive_int(
            data.get("max_response_bytes", 10_485_760), "crawl.max_response_bytes"
        ),
        max_redirects=_positive_int(
            data.get("max_redirects", 10), "crawl.max_redirects"
        ),
        max_attempts=_positive_int(data.get("max_attempts", 5), "crawl.max_attempts"),
    )


def _parse_archive(data: dict[str, Any]) -> ArchiveSettings:
    names = {field.name for field in fields(ArchiveSettings)}
    _only(data, names, "archive")
    return ArchiveSettings(
        interval=parse_duration(data.get("interval", "30d")),
        concurrency=_positive_int(data.get("concurrency", 1), "archive.concurrency"),
        dedupe_window=parse_duration(data.get("dedupe_window", "24h")),
        max_attempts=_positive_int(data.get("max_attempts", 5), "archive.max_attempts"),
    )


def _parse_reporting(data: dict[str, Any]) -> ReportingSettings:
    names = {field.name for field in fields(ReportingSettings)}
    _only(data, names, "reporting")
    return ReportingSettings(interval=parse_duration(data.get("interval", "24h")))


def _parse_logging(data: dict[str, Any]) -> LoggingSettings:
    names = {field.name for field in fields(LoggingSettings)}
    _only(data, names, "logging")
    level = data.get("level", "INFO")
    discord_min_level = data.get("discord_min_level", "WARNING")
    if not isinstance(level, str) or not isinstance(discord_min_level, str):
        raise ConfigError("logging levels must be strings")
    normalized_level = _log_level(level, "logging.level")
    normalized_discord_level = _log_level(
        discord_min_level, "logging.discord_min_level"
    )
    return LoggingSettings(
        level=normalized_level,
        discord_min_level=normalized_discord_level,
    )


def _log_level(value: str, name: str) -> str:
    normalized = value.upper()
    try:
        return logger.level(normalized).name
    except ValueError as error:
        raise ConfigError(f"{name} is not a registered Loguru level") from error


def _parse_domain(value: object, index: int) -> DomainSettings:
    data = _table(value, f"domains[{index}]")
    _only(data, {"host", "scheme", "include_subdomains", "seeds"}, f"domains[{index}]")
    host_value = data.get("host")
    if not isinstance(host_value, str):
        raise ConfigError(f"domains[{index}].host must be a string")
    try:
        host = normalize_host(host_value)
    except ValueError as error:
        raise ConfigError(f"domains[{index}].host is invalid: {error}") from error
    scheme = data.get("scheme", "https")
    if not isinstance(scheme, str) or scheme.lower() not in {"http", "https"}:
        raise ConfigError(f"domains[{index}].scheme must be http or https")
    include_subdomains = data.get("include_subdomains", False)
    if not isinstance(include_subdomains, bool):
        raise ConfigError(f"domains[{index}].include_subdomains must be a boolean")
    seeds_value = data.get("seeds", ["/"])
    if not isinstance(seeds_value, list) or not seeds_value:
        raise ConfigError(f"domains[{index}].seeds must be a nonempty array")
    if not all(isinstance(seed, str) and seed for seed in seeds_value):
        raise ConfigError(f"domains[{index}].seeds must contain nonempty strings")
    domain = DomainSettings(
        host, scheme.lower(), include_subdomains, tuple(seeds_value)
    )
    try:
        for seed in domain.seed_urls:
            seed_host = normalize_host(cast("str", urlsplit(seed).hostname))
            if not _host_in_scope(seed_host, domain):
                raise ConfigError(f"domains[{index}] seed is outside its host scope")
    except ValueError as error:
        raise ConfigError(f"domains[{index}] has an invalid seed: {error}") from error
    return domain


def _host_in_scope(host: str, domain: DomainSettings) -> bool:
    return host == domain.host or (
        domain.include_subdomains and host.endswith(f".{domain.host}")
    )


def _validate_domain_scopes(domains: tuple[DomainSettings, ...]) -> None:
    for index, left in enumerate(domains):
        for right in domains[index + 1 :]:
            if left.host == right.host:
                raise ConfigError(f"duplicate domain host: {left.host}")
            if _host_in_scope(right.host, left) or _host_in_scope(left.host, right):
                raise ConfigError(
                    f"overlapping domain scopes: {left.host} and {right.host}"
                )


def load_config(path: Path) -> Config:
    """Read and validate a complete configuration file."""
    try:
        with path.open("rb") as file:
            data = tomllib.load(file)
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise ConfigError(f"cannot load configuration: {error}") from error
    _only(
        data,
        {"ziggy", "crawl", "archive", "reporting", "logging", "domains"},
        "top-level",
    )
    if "ziggy" not in data:
        raise ConfigError("missing [ziggy] table")
    domains_value = data.get("domains", [])
    if not isinstance(domains_value, list):
        raise ConfigError("domains must be an array of tables")
    domains = tuple(
        _parse_domain(value, index) for index, value in enumerate(domains_value)
    )
    _validate_domain_scopes(domains)
    return Config(
        ziggy=_parse_ziggy(_section(data, "ziggy"), path.parent.resolve()),
        crawl=_parse_crawl(_section(data, "crawl")),
        archive=_parse_archive(_section(data, "archive")),
        reporting=_parse_reporting(_section(data, "reporting")),
        logging=_parse_logging(_section(data, "logging")),
        domains=domains,
    )


def resolve_secrets(env: Env | None = None) -> Secrets:
    """Resolve optional secrets from environment variables."""
    env = env or Env()
    email = env.str(_INTERNET_ARCHIVE_EMAIL_ENV, default=None) or None
    password = env.str(_INTERNET_ARCHIVE_PASSWORD_ENV, default=None) or None
    if (email is None) != (password is None):
        raise ConfigError(
            "Internet Archive email and password must both be set or both be unset"
        )

    return Secrets(
        archive_email=email,
        archive_password=password,
        reporting_webhook_url=env.str(_DISCORD_WEBHOOK_URL_ENV, default=None) or None,
        logging_webhook_url=env.str(_LOG_DISCORD_WEBHOOK_URL_ENV, default=None) or None,
    )


def database_change_requires_restart(previous: Config, replacement: Config) -> bool:
    """Return whether a valid reload changes the open database."""
    return previous.ziggy.database != replacement.ziggy.database
