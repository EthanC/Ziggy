<div align="center">

<img src="brand/Ziggy_Lockup_Color.png" alt="Ziggy">

Ziggy crawls and preserves websites you care about.

[![Python 3.14+](https://img.shields.io/badge/Python-3.14%2B-3776AB?style=flat-square&logo=python&logoColor=white)](https://www.python.org/)
[![CI](https://img.shields.io/github/actions/workflow/status/EthanC/Ziggy/ci.yml?branch=main&style=flat-square&label=build)](https://github.com/EthanC/Ziggy/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/badge/coverage-100%25-brightgreen?style=flat-square)](https://github.com/EthanC/Ziggy/actions/workflows/ci.yml)

</div>

## Features

- Crawl multiple websites on configurable schedules.
- Find pages through links, redirects, and sitemaps.
- Submit pages to Internet Archive Save Page Now.
- Prioritize pages that lack a recent Wayback capture.
- Resume work after restarts with a SQLite-backed queue.
- Send crawl reports and logs to Discord.
- Reload configuration without restarting Ziggy.
- Accept one-time archive requests through an optional HTTP queue.

## Docker Compose

Create `/path/to/ziggy` and copy `ziggy.example.toml` there as `ziggy.toml`. Configure the domains, copy `.env.example` to `.env`, and create `compose.yaml` beside it. Internet Archive credentials are optional. Set `ZIGGY_INTERNET_ARCHIVE_EMAIL` and `ZIGGY_INTERNET_ARCHIVE_PASSWORD` to enable authenticated captures, screenshots, and My Web Archive.

| Environment variable | Description | Required | Default |
| --- | --- | :---: | --- |
| `PUID` | UID for the container process and files under `/ziggy` | No | `1000` |
| `PGID` | GID for the container process and files under `/ziggy` | No | `1000` |

```yaml
services:
  ziggy:
    container_name: ziggy
    image: ghcr.io/ethanc/ziggy:latest
    env_file: .env
    volumes:
      - /path/to/ziggy:/ziggy
    restart: unless-stopped
```

The container changes ownership of `/ziggy` to `PUID:PGID` at startup, then runs with those IDs. Mount a dedicated directory. Docker's `--user` option prevents ownership changes and overrides `PUID` and `PGID`.

Start Ziggy:

```console
docker compose up -d
```

## Python

Ziggy requires Python 3.14 and [`uv`](https://docs.astral.sh/uv/).

```console
uv sync --locked
```

Copy `ziggy.example.toml` to `ziggy.toml`. For authenticated captures, export `ZIGGY_INTERNET_ARCHIVE_EMAIL` and `ZIGGY_INTERNET_ARCHIVE_PASSWORD` through your shell or secret manager. `ZIGGY_INTERNET_ARCHIVE_RECOVERY_PERIOD` is the time without Archive.org HTTP 5XX responses before Ziggy resets its backoff; the default is `15m`. Ziggy does not load `.env` files.

```console
uv run python -m ziggy check-config --config ziggy.toml
uv run python -m ziggy run --config ziggy.toml
```

## Configuration

Each `[[domains]]` table defines one website and its recurring seeds. The host and its exact `www.` alias share a scope; set `include_subdomains = true` for other subdomains. Seeds use `crawl.seed_interval` (one hour by default) and run before pages scheduled with `crawl.interval`.

```toml
[[domains]]
host = "example.com"
scheme = "https"
include_subdomains = false
seeds = ["/", "/sitemap.xml"]
```

| Key | Description | Required | Default |
| --- | --- | :---: | --- |
| `host` | Hostname to crawl, without the scheme or path | Yes | None |
| `scheme` | Scheme for relative seeds | No | `"https"` |
| `include_subdomains` | Whether to crawl subdomains of `host` | No | `false` |
| `seeds` | High-priority paths or in-scope URLs for discovery | No | `["/"]` |

[`ziggy.example.toml`](ziggy.example.toml) lists all settings and defaults. `archive.interval` sets the capture schedule and Wayback recency window. `archive.max_pending_jobs` limits pending captures, and `archive.request_delay` spaces Archive.org operations. Authenticated instances also check the account's Save Page Now capacity. Durations are an integer followed by `s`, `m`, `h`, or `d`.

## HTTP Queue

Set `ZIGGY_HTTP_ENABLED=true` to enable the queue. It defaults to `127.0.0.1:9449`; `ZIGGY_HTTP_HOST` and `ZIGGY_HTTP_PORT` override the host and port. The listener has no authentication. Binding to `0.0.0.0` exposes it to every network that can reach the port.

Submit up to 100 absolute HTTP or HTTPS URLs per request:

```console
curl --fail-with-body http://127.0.0.1:9449/v1/queue \
  --header "Content-Type: application/json" \
  --data '{"identifier":"external-service","urls":["https://example.org/article"],"priority":10}'
```

`identifier` is caller-supplied attribution, not an authenticated identity, and must contain 1 to 128 characters. `priority` ranges from `-100` to `100` and defaults to `0`, the same priority as recurring archive work. Ziggy normalizes and deduplicates URLs, rejecting credentials, control characters, malformed authorities, and sensitive query parameters.

A successful request returns `202 Accepted` after SQLite commits the receipts:

```json
{
  "submissions": [
    {
      "receipt_id": "8c829f35-d47f-40a6-93dd-3066b5f86a6d",
      "url": "https://example.org/article"
    }
  ]
}
```

`202 Accepted` confirms queueing, not capture. The listener limits request bodies to 256 KiB, accepts 60 requests per minute, and allows 10,000 outstanding receipts. Overload returns `429` or `503`; application responses include `Retry-After`, while Uvicorn may return a plain `503` for excess connections. Disabling the listener stops submissions; queued receipts continue through the archive scheduler.

Docker does not publish the listener. To expose it, publish port `9449` and set `ZIGGY_HTTP_HOST=0.0.0.0` in the container; publishing the port alone leaves it bound to container loopback.
