# Ziggy

Ziggy crawls configured websites, stores a durable URL frontier in SQLite, and
submits due pages to Internet Archive Save Page Now. One `asyncio` process owns
the crawl, archive, report, configuration-reload, and heartbeat schedulers.

## Requirements

- Python 3.14, the current stable Python target
- [UV](https://docs.astral.sh/uv/)
- An Archive.org account with Save Page Now access
- A Discord webhook only if reports or remote logs are wanted

Ziggy tracks the latest stable Python and dependency releases. The lockfile
records the exact environment used by a build; older Python and dependency
combinations are not compatibility targets.

## Local setup

```console
uv sync --locked
cp ziggy.example.toml ziggy.toml
cp .env.example .env
```

Export the variables from `.env` with your shell or secret manager. Ziggy does
not read `.env` files itself.

```console
uv run python -m ziggy check-config --config ziggy.toml
uv run python -m ziggy run --config ziggy.toml
```

Ziggy is a UV application, not an installable Python distribution. `check-config`
validates TOML and required Archive.org credentials without starting workers. `healthcheck` exits
successfully only when the database contains a heartbeat no more than 90 seconds
old.

## Configuration

Unknown fields, invalid durations, duplicate or overlapping scopes, URL
credentials, unsupported schemes, and invalid environment-variable names reject
the complete configuration. Durations use a positive integer followed by `s`,
`m`, `h`, or `d`.

`[ziggy]`:

| Field | Meaning |
| --- | --- |
| `database` | SQLite path, resolved relative to the TOML file |
| `config_reload_interval` | Poll interval for complete configuration replacement; default `30s` |

`[crawl]`:

| Field | Meaning |
| --- | --- |
| `interval` | Known-page recrawl interval; default `24h` |
| `concurrency` | Global concurrent request limit; default `8` |
| `per_host_concurrency` | Concurrent request limit per host; default `2` |
| `request_delay` | Minimum seconds between request starts on one host; default `1.0` |
| `request_timeout` | Request timeout in seconds; default `30.0` |
| `max_response_bytes` | Maximum decoded body retained for parsing; default 10 MiB |
| `max_redirects` | Redirect-hop limit; default `10` |
| `max_attempts` | Short-backoff attempts before normal recrawl timing; default `5` |

`[archive]`:

| Field | Meaning |
| --- | --- |
| `interval` | Refresh interval after the latest successful capture; default `30d` |
| `concurrency` | Archive submission worker count; default `1` |
| `dedupe_window` | Save Page Now `if_not_archived_within` value; default `24h` |
| `username_env` | Environment variable containing the Archive.org username |
| `password_env` | Environment variable containing the Archive.org password |
| `max_attempts` | Retry limit for archive service failures; default `5` |

`[reporting]` sets `interval` (default `24h`) and
`discord_webhook_url_env`. If the named webhook is absent, Ziggy logs the
persisted report and handles that interval without creating a backlog.

`[logging]` sets the stderr `level`, `discord_webhook_url_env`, and
`discord_min_level` (default `WARNING`). Any registered Loguru level is valid.
The Discord sink is replaced when its URL or threshold changes.

Each `[[domains]]` entry accepts `host`, `scheme`, `include_subdomains`, and a
nonempty `seeds` array. Seeds may be paths or absolute in-scope URLs.

## Host boundaries

The configured host is the scope boundary for both crawling and archival.
Configuring `abc.example.com` permits `abc.example.com`; it never permits
`example.com` or `def.example.com`. With `include_subdomains = true`, descendants
such as `one.abc.example.com` are also allowed. Parent and sibling hosts remain
out of scope at discovery, every redirect hop, and archive association.

Overlapping configured scopes are rejected. This prevents two domain schedulers
from owning the same normalized URL.

## Discovery and retries

Ziggy discovers navigable HTTP and HTTPS URLs from anchors, image-map areas,
canonical links, the first valid HTML base, HTTP Link headers, redirects, URL-set
sitemaps, and sitemap indexes. It does not execute JavaScript. Images, scripts,
stylesheets, fonts, and other embedded resources do not enter the frontier solely
because a page references them. Save Page Now handles capture resources.

Normalization lowercases and IDNA-normalizes hosts, removes fragments and default
ports, resolves relative references and dot segments, and converts an empty path
to `/`. Path case, trailing slashes, query ordering, repeated query keys, and
query values remain significant.

Ziggy ignores `robots.txt`. Per-host delays and concurrency limits still apply.
Transport failures, HTTP 408, 425, 429, and 5xx responses use bounded exponential
backoff with jitter. A later valid `Retry-After` value takes precedence. Other
failures return to the normal recrawl interval.

## Archive recovery

Direct Save Page Now requests enable outlink capture, screenshots, and My Web
Archive. Ziggy commits submission intent before the network call and polls the
returned job ID from SQLite. Restarting resumes expired leases and accepted jobs.
If a process stops between remote acceptance and storing the job ID, Ziggy checks
CDX capture history from the intent time before resubmitting.

Authentication failure pauses new submissions. A valid configuration reload with
working credentials resumes them. Removing a domain keeps all rows but prevents
new crawl and archive work; already accepted remote jobs are still recorded.
Re-adding the same host reactivates its due work.

## Discord reports

Reports use Discord Components v2, not embeds or ordinary message content. Each
message contains one colored `Container`, title and period text, a separator,
discovered/archived/outstanding counts, and active-domain context. Allowed
mentions are disabled. Delivery retries reuse the committed report counts.

The reporting and Loguru-Discord webhooks are independent. They may name the same
environment variable when one Discord destination is desired.

## Docker and GHCR

The image runs as UID/GID `10001`, reads `/config/ziggy.toml`, and stores its
database under `/data`. Set `database = "/data/ziggy.sqlite3"` in the mounted
container configuration.

```console
docker run --name ziggy \
  --env-file .env \
  --mount type=bind,src="$PWD/ziggy.toml",dst=/config/ziggy.toml,readonly \
  --mount type=volume,src=ziggy-data,dst=/data \
  ghcr.io/ethanc/ziggy:latest
```

For a bind-mounted data directory, make it writable by UID/GID `10001` before
starting the container. Stop or replace the container with enough time for the
process to release leases and close clients; do not send an immediate kill.

Every pushed commit is published to GHCR with its full commit SHA and sanitized
branch tag. `main` also receives `latest`; version tags receive semantic-version
tags. Images target `linux/amd64` and `linux/arm64`.

## SQLite backups

SQLite runs with foreign keys, WAL mode, `synchronous=NORMAL`, and a busy timeout.
Do not copy only `ziggy.sqlite3` while Ziggy is running: committed pages may still
reside in `ziggy.sqlite3-wal`. Use SQLite's online backup API or stop Ziggy and
copy the database together with any `-wal` and `-shm` sidecars. Test restoration
before relying on a backup.

Alembic upgrades run before workers start. Migration failure stops startup with a
nonzero exit code.

## Development

```console
uv lock --check
uv run ruff format --check .
uv run ruff check .
uv run ty check
uv run pytest --cov=ziggy --cov-branch \
  --cov-report=term-missing --cov-report=xml --cov-report=html \
  --cov-fail-under=100
```

Tests use fakes and local persistence only; they do not contact websites,
Archive.org, or Discord. CI requires 100% statement and branch coverage.
