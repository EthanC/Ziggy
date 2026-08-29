<div align="center">

<img src="brand/Ziggy_Lockup_Color.png" alt="Ziggy">

Ziggy crawls and preserves the websites you care about.

[![Python 3.14+](https://img.shields.io/badge/Python-3.14%2B-3776AB?style=flat-square&logo=python&logoColor=white)](https://www.python.org/)
[![CI](https://img.shields.io/github/actions/workflow/status/EthanC/Ziggy/ci.yml?branch=main&style=flat-square&label=build)](https://github.com/EthanC/Ziggy/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/badge/coverage-100%25-brightgreen?style=flat-square)](https://github.com/EthanC/Ziggy/actions/workflows/ci.yml)
[![GHCR image size](https://ghcr-badge.egpl.dev/ethanc/ziggy/size?tag=latest&label=image%20size)](https://github.com/EthanC/Ziggy/pkgs/container/ziggy)

</div>

## Features

- Crawl multiple websites on configurable schedules.
- Discover pages from links, redirects, and sitemaps.
- Submit pages to Internet Archive Save Page Now.
- Resume crawl and archive work after restarts with a SQLite-backed queue.
- Send crawl reports and application logs to Discord.
- Reload configuration without restarting the process.

## Docker Compose

Create `/path/to/ziggy`, copy `ziggy.example.toml` to `/path/to/ziggy/ziggy.toml`, and configure the domains to crawl. Copy `.env.example` to `.env` and create `compose.yaml` beside it. Internet Archive credentials are optional; set `ZIGGY_INTERNET_ARCHIVE_EMAIL` to the account email address and `ZIGGY_INTERNET_ARCHIVE_PASSWORD` to its password. Authenticated captures also request screenshots and add captures to My Web Archive.

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

On Linux, `/path/to/ziggy` must be writable by UID/GID `10001` so Ziggy can create `ziggy.db` and its sidecar files.

Start Ziggy:

```console
docker compose up -d
```

## Python

Python 3.14 and [`uv`](https://docs.astral.sh/uv/) are required.

```console
uv sync --locked
```

Copy `ziggy.example.toml` to `ziggy.toml`. Internet Archive credentials are optional. If used, export `ZIGGY_INTERNET_ARCHIVE_EMAIL` with the account email address and `ZIGGY_INTERNET_ARCHIVE_PASSWORD` with its password through your shell or secret manager. Ziggy does not load `.env` files itself.

```console
uv run python -m ziggy check-config --config ziggy.toml
uv run python -m ziggy run --config ziggy.toml
```

## Configuration

Each `[[domains]]` table defines one website scope and its starting URLs. Add another table for each website Ziggy should crawl.

```toml
[[domains]]
host = "example.com"
scheme = "https"
include_subdomains = false
seeds = ["/", "/sitemap.xml"]
```

| Key | Description | Required | Default |
| --- | --- | :---: | --- |
| `host` | Hostname to crawl, without a scheme or path | Yes | None |
| `scheme` | Scheme used for relative seeds | No | `"https"` |
| `include_subdomains` | Include descendants of `host` in the crawl | No | `false` |
| `seeds` | Starting paths or in-scope URLs | No | `["/"]` |

Crawler, archive, reporting, and logging settings are documented with their defaults in [`ziggy.example.toml`](ziggy.example.toml). `archive.max_pending_jobs` limits captures still processing at Internet Archive, while `archive.request_delay` sets the minimum delay between Archive.org operations. Authenticated instances also consult the account's live Save Page Now capacity before submitting. Durations use an integer followed by `s`, `m`, `h`, or `d`.
