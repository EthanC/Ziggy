from __future__ import annotations

from dataclasses import replace
from datetime import timedelta

import pytest

import ziggy.config as config_module
from ziggy.config import (
    ConfigError,
    database_change_requires_restart,
    load_config,
    parse_duration,
    resolve_secrets,
)


def write_config(tmp_path, body: str):
    path = tmp_path / "config.toml"
    path.write_text(body, encoding="utf-8")
    return path


def minimal_config(tmp_path, extra: str = ""):
    return load_config(
        write_config(tmp_path, f'[ziggy]\ndatabase = "ziggy.db"\n{extra}')
    )


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("1s", timedelta(seconds=1)),
        ("2m", timedelta(minutes=2)),
        ("3h", timedelta(hours=3)),
        ("4d", timedelta(days=4)),
    ],
)
def test_parse_duration_accepts_supported_units(value, expected):
    assert parse_duration(value) == expected


@pytest.mark.parametrize("value", [None, 1, "", "0s", "1.5h", "1w", "+1s", " 1s"])
def test_parse_duration_rejects_invalid_values(value):
    with pytest.raises(ConfigError, match="duration must be a positive integer"):
        parse_duration(value)


def test_load_minimal_config_uses_defaults_and_resolves_database(tmp_path):
    config = minimal_config(tmp_path)

    assert config.ziggy.database == (tmp_path / "ziggy.db").resolve()
    assert config.ziggy.config_reload_interval == timedelta(seconds=30)
    assert config.crawl.interval == timedelta(hours=24)
    assert config.crawl.concurrency == 8
    assert config.crawl.per_host_concurrency == 2
    assert config.crawl.request_delay == 1.0
    assert config.crawl.request_timeout == 30.0
    assert config.crawl.max_response_bytes == 10_485_760
    assert config.crawl.max_redirects == 10
    assert config.crawl.max_attempts == 5
    assert config.archive.interval == timedelta(days=30)
    assert config.archive.concurrency == 1
    assert config.archive.dedupe_window == timedelta(hours=24)
    assert config.archive.username_env == "ZIGGY_ARCHIVE_ORG_USERNAME"
    assert config.archive.password_env == "ZIGGY_ARCHIVE_ORG_PASSWORD"  # noqa: S105
    assert config.archive.max_attempts == 5
    assert config.reporting.interval == timedelta(hours=24)
    assert config.reporting.discord_webhook_url_env == "ZIGGY_DISCORD_WEBHOOK_URL"
    assert config.logging.level == "INFO"
    assert config.logging.discord_webhook_url_env == "ZIGGY_LOG_DISCORD_WEBHOOK_URL"
    assert config.logging.discord_min_level == "WARNING"
    assert config.domains == ()


def test_load_complete_valid_config(tmp_path):
    absolute_database = (tmp_path / "absolute.db").resolve()
    config = load_config(
        write_config(
            tmp_path,
            f"""
[ziggy]
database = {str(absolute_database)!r}
config_reload_interval = "2m"

[crawl]
interval = "2h"
concurrency = 3
per_host_concurrency = 1
request_delay = 0
request_timeout = 4.5
max_response_bytes = 100
max_redirects = 2
max_attempts = 3

[archive]
interval = "5d"
concurrency = 2
dedupe_window = "10m"
username_env = "ARCHIVE_USER"
password_env = "ARCHIVE_PASSWORD"
max_attempts = 4

[reporting]
interval = "6h"
discord_webhook_url_env = "REPORT_HOOK"

[logging]
level = "debug"
discord_webhook_url_env = "LOG_HOOK"
discord_min_level = "error"

[[domains]]
host = "Example.COM."
scheme = "HTTP"
include_subdomains = true
seeds = ["/news/../", "https://child.example.com/path#fragment"]
""",
        )
    )

    assert config.ziggy.database == absolute_database
    assert config.ziggy.config_reload_interval == timedelta(minutes=2)
    assert config.crawl.request_delay == 0.0
    assert config.crawl.request_timeout == 4.5
    assert config.archive.username_env == "ARCHIVE_USER"
    assert config.reporting.discord_webhook_url_env == "REPORT_HOOK"
    assert config.logging.level == "DEBUG"
    assert config.logging.discord_min_level == "ERROR"
    assert config.domains[0].host == "example.com"
    assert config.domains[0].scheme == "http"
    assert config.domains[0].base_url == "http://example.com/"
    assert config.domains[0].seed_urls == (
        "http://example.com/",
        "https://child.example.com/path",
    )


@pytest.mark.parametrize(
    ("body", "message"),
    [
        ("", r"missing \[ziggy\] table"),
        ('unknown = 1\n[ziggy]\ndatabase = "x"', "unknown top-level field"),
        ('ziggy = "x"', "ziggy must be a TOML table"),
        ("[ziggy]", "ziggy.database must be a nonempty path"),
        ('[ziggy]\ndatabase = ""', "ziggy.database must be a nonempty path"),
        ("[ziggy]\ndatabase = 1", "ziggy.database must be a nonempty path"),
        ('[ziggy]\ndatabase = "x"\nextra = true', "unknown ziggy field"),
        ('[ziggy]\ndatabase = "x"\n[crawl]\nextra = 1', "unknown crawl field"),
        ('archive = 1\n[ziggy]\ndatabase = "x"', "archive must be a TOML table"),
        ('domains = {}\n[ziggy]\ndatabase = "x"', "domains must be an array"),
    ],
)
def test_load_config_rejects_structure_errors(tmp_path, body, message):
    with pytest.raises(ConfigError, match=message):
        load_config(write_config(tmp_path, body))


@pytest.mark.parametrize(
    ("section", "field", "value", "message"),
    [
        ("crawl", "concurrency", "0", "must be a positive integer"),
        ("crawl", "per_host_concurrency", "true", "must be a positive integer"),
        ("crawl", "max_response_bytes", "1.5", "must be a positive integer"),
        ("crawl", "max_redirects", "-1", "must be a positive integer"),
        ("crawl", "max_attempts", '"5"', "must be a positive integer"),
        ("archive", "concurrency", "0", "must be a positive integer"),
        ("archive", "max_attempts", "false", "must be a positive integer"),
        ("crawl", "request_delay", "-0.1", "must be a nonnegative number"),
        ("crawl", "request_delay", "true", "must be a nonnegative number"),
        ("crawl", "request_timeout", "0", "must be greater than zero"),
        ("crawl", "request_timeout", '"slow"', "must be a nonnegative number"),
    ],
)
def test_load_config_rejects_invalid_numbers(tmp_path, section, field, value, message):
    body = f'[ziggy]\ndatabase = "x"\n[{section}]\n{field} = {value}\n'

    with pytest.raises(ConfigError, match=message):
        load_config(write_config(tmp_path, body))


@pytest.mark.parametrize(
    ("section", "field", "value"),
    [
        ("archive", "username_env", '"1USER"'),
        ("archive", "password_env", '"BAD-NAME"'),
        ("reporting", "discord_webhook_url_env", '"HAS SPACE"'),
        ("logging", "discord_webhook_url_env", "42"),
    ],
)
def test_load_config_rejects_invalid_environment_names(tmp_path, section, field, value):
    body = f'[ziggy]\ndatabase = "x"\n[{section}]\n{field} = {value}\n'

    with pytest.raises(ConfigError, match="valid environment-variable name"):
        load_config(write_config(tmp_path, body))


def test_optional_environment_names_reject_boolean_values(tmp_path):
    with pytest.raises(ConfigError, match="valid environment-variable name"):
        minimal_config(
            tmp_path,
            "[reporting]\ndiscord_webhook_url_env = false\n",
        )


def test_optional_environment_name_helper_accepts_none():
    assert config_module._env_name(None, "optional", optional=True) is None  # noqa: SLF001


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("level", "1", "logging levels must be strings"),
        ("discord_min_level", "false", "logging levels must be strings"),
        ("level", '"not-a-level"', "logging.level is not a registered"),
        (
            "discord_min_level",
            '"not-a-level"',
            "logging.discord_min_level is not a registered",
        ),
    ],
)
def test_load_config_rejects_invalid_logging_levels(tmp_path, field, value, message):
    body = f'[ziggy]\ndatabase = "x"\n[logging]\n{field} = {value}\n'

    with pytest.raises(ConfigError, match=message):
        load_config(write_config(tmp_path, body))


@pytest.mark.parametrize(
    ("domain", "message"),
    [
        ('"not a table"', r"domains\[0\] must be a TOML table"),
        ('{host = "example.com", extra = 1}', "unknown domains"),
        ("{}", r"domains\[0\]\.host must be a string"),
        ('{host = "bad host"}', r"domains\[0\]\.host is invalid"),
        ('{host = "example.com:443"}', r"domains\[0\]\.host is invalid"),
        ('{host = "example.com", scheme = "ftp"}', "scheme must be http or https"),
        ('{host = "example.com", scheme = 1}', "scheme must be http or https"),
        (
            '{host = "example.com", include_subdomains = "yes"}',
            "include_subdomains must be a boolean",
        ),
        ('{host = "example.com", seeds = []}', "seeds must be a nonempty array"),
        ('{host = "example.com", seeds = "/"}', "seeds must be a nonempty array"),
        ('{host = "example.com", seeds = [""]}', "seeds must contain nonempty"),
        ('{host = "example.com", seeds = [1]}', "seeds must contain nonempty"),
        (
            '{host = "example.com", seeds = ["http://other.example/"]}',
            "seed is outside its host scope",
        ),
        ('{host = "example.com", seeds = ["http://[bad"]}', "has an invalid seed"),
    ],
)
def test_load_config_rejects_invalid_domains(tmp_path, domain, message):
    body = f'domains = [{domain}]\n[ziggy]\ndatabase = "x"\n'

    with pytest.raises(ConfigError, match=message):
        load_config(write_config(tmp_path, body))


@pytest.mark.parametrize(
    ("include_subdomains", "seed", "accepted"),
    [
        (False, "https://abc.example.com/page", True),
        (False, "https://child.abc.example.com/page", False),
        (True, "https://child.abc.example.com/page", True),
        (True, "https://deep.child.abc.example.com/page", True),
        (True, "https://example.com/page", False),
        (True, "https://sibling.example.com/page", False),
        (True, "https://notabc.example.com/page", False),
    ],
)
def test_domain_seed_scope_uses_exact_subdomain_boundary(
    tmp_path, include_subdomains, seed, accepted
):
    body = (
        '[ziggy]\ndatabase = "x"\n[[domains]]\n'
        'host = "abc.example.com"\n'
        f"include_subdomains = {str(include_subdomains).lower()}\n"
        f"seeds = [{seed!r}]\n"
    )

    if accepted:
        assert load_config(write_config(tmp_path, body)).domains[0].seed_urls == (seed,)
    else:
        with pytest.raises(ConfigError, match="outside its host scope"):
            load_config(write_config(tmp_path, body))


@pytest.mark.parametrize(
    ("domains", "message"),
    [
        (
            '[{host = "example.com"}, {host = "EXAMPLE.COM."}]',
            "duplicate domain host: example.com",
        ),
        (
            (
                '[{host = "example.com", include_subdomains = true}, '
                '{host = "child.example.com"}]'
            ),
            "overlapping domain scopes",
        ),
        (
            (
                '[{host = "child.example.com"}, '
                '{host = "example.com", include_subdomains = true}]'
            ),
            "overlapping domain scopes",
        ),
    ],
)
def test_load_config_rejects_duplicate_or_overlapping_scopes(
    tmp_path, domains, message
):
    body = f'domains = {domains}\n[ziggy]\ndatabase = "x"\n'

    with pytest.raises(ConfigError, match=message):
        load_config(write_config(tmp_path, body))


def test_non_overlapping_parent_and_child_scopes_are_allowed(tmp_path):
    config = load_config(
        write_config(
            tmp_path,
            'domains = [{host = "example.com"}, {host = "child.example.com"}]\n'
            '[ziggy]\ndatabase = "x"\n',
        )
    )

    assert [domain.host for domain in config.domains] == [
        "example.com",
        "child.example.com",
    ]


def test_load_config_wraps_io_and_toml_errors(tmp_path):
    with pytest.raises(ConfigError, match="cannot load configuration"):
        load_config(tmp_path / "missing.toml")

    with pytest.raises(ConfigError, match="cannot load configuration"):
        load_config(write_config(tmp_path, "not = [valid"))


def test_resolve_secrets_reads_required_and_optional_environment(tmp_path, monkeypatch):
    config = minimal_config(
        tmp_path,
        '[archive]\nusername_env = "USER_SECRET"\npassword_env = "PASS_SECRET"\n'
        '[reporting]\ndiscord_webhook_url_env = "REPORT_SECRET"\n'
        '[logging]\ndiscord_webhook_url_env = "LOG_SECRET"\n',
    )
    monkeypatch.setenv("USER_SECRET", "alice")
    monkeypatch.setenv("PASS_SECRET", "password")
    monkeypatch.setenv("REPORT_SECRET", "https://report.example/")
    monkeypatch.setenv("LOG_SECRET", "https://log.example/")

    secrets = resolve_secrets(config)

    assert secrets.archive_username == "alice"
    assert secrets.archive_password == "password"  # noqa: S105
    assert secrets.reporting_webhook_url == "https://report.example/"
    assert secrets.logging_webhook_url == "https://log.example/"


def test_resolve_secrets_uses_process_environment(tmp_path, monkeypatch):
    config = minimal_config(tmp_path)
    monkeypatch.setenv("ZIGGY_ARCHIVE_ORG_USERNAME", "user")
    monkeypatch.setenv("ZIGGY_ARCHIVE_ORG_PASSWORD", "pass")

    assert resolve_secrets(config).archive_username == "user"


@pytest.mark.parametrize(
    ("values", "names"),
    [
        ({}, "ZIGGY_ARCHIVE_ORG_USERNAME, ZIGGY_ARCHIVE_ORG_PASSWORD"),
        (
            {
                "ZIGGY_ARCHIVE_ORG_USERNAME": "user",
                "ZIGGY_ARCHIVE_ORG_PASSWORD": "",
            },
            "ZIGGY_ARCHIVE_ORG_PASSWORD",
        ),
    ],
)
def test_resolve_secrets_rejects_missing_or_empty_required_values(
    tmp_path, monkeypatch, values, names
):
    config = minimal_config(tmp_path)
    for name in ("ZIGGY_ARCHIVE_ORG_USERNAME", "ZIGGY_ARCHIVE_ORG_PASSWORD"):
        monkeypatch.delenv(name, raising=False)
    for name, value in values.items():
        monkeypatch.setenv(name, value)

    with pytest.raises(ConfigError, match=names):
        resolve_secrets(config)


def test_resolve_secrets_supports_disabled_optional_values(tmp_path, monkeypatch):
    parsed = minimal_config(tmp_path)
    config = replace(
        parsed,
        reporting=replace(parsed.reporting, discord_webhook_url_env=None),
        logging=replace(parsed.logging, discord_webhook_url_env=None),
    )
    monkeypatch.setenv("ZIGGY_ARCHIVE_ORG_USERNAME", "user")
    monkeypatch.setenv("ZIGGY_ARCHIVE_ORG_PASSWORD", "pass")
    secrets = resolve_secrets(config)

    assert secrets.reporting_webhook_url is None
    assert secrets.logging_webhook_url is None


def test_resolve_secrets_treats_empty_optional_values_as_unset(tmp_path, monkeypatch):
    monkeypatch.setenv("ZIGGY_ARCHIVE_ORG_USERNAME", "user")
    monkeypatch.setenv("ZIGGY_ARCHIVE_ORG_PASSWORD", "pass")
    monkeypatch.setenv("ZIGGY_DISCORD_WEBHOOK_URL", "")
    monkeypatch.setenv("ZIGGY_LOG_DISCORD_WEBHOOK_URL", "")
    secrets = resolve_secrets(minimal_config(tmp_path))

    assert secrets.reporting_webhook_url is None
    assert secrets.logging_webhook_url is None


def test_database_change_requires_restart_only_for_database_change(tmp_path):
    config = minimal_config(tmp_path)

    assert not database_change_requires_restart(config, config)
    replacement = replace(
        config,
        ziggy=replace(config.ziggy, database=tmp_path / "replacement.db"),
    )
    assert database_change_requires_restart(config, replacement)
