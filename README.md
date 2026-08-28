<div align="center">

# Ziggy

Ziggy crawls and preserves the websites you care about.

[![Python 3.14+](https://img.shields.io/badge/Python-3.14%2B-3776AB?style=flat-square&logo=python&logoColor=white)](https://www.python.org/)
[![CI](https://img.shields.io/github/actions/workflow/status/EthanC/Ziggy/ci.yml?branch=main&style=flat-square&label=build)](https://github.com/EthanC/Ziggy/actions/workflows/ci.yml)

</div>

## Features

- Crawl multiple websites on configurable schedules.
- Discover pages from links, redirects, and sitemaps.
- Submit pages to Internet Archive Save Page Now.
- Resume crawl and archive work after restarts with a SQLite-backed queue.
- Send crawl reports and application logs to Discord.
- Reload configuration without restarting the process.

## Docker Compose

Copy `ziggy.example.toml` to `ziggy.toml`, set `database = "/data/ziggy.sqlite3"`, and configure the domains to crawl. Copy `.env.example` to `.env` and add your Archive.org credentials, then create `compose.yaml` beside them:

```yaml
services:
  ziggy:
    container_name: ziggy
    image: ghcr.io/ethanc/ziggy:latest
    env_file: .env
    volumes:
      - ./ziggy.toml:/config/ziggy.toml:ro
      - ziggy-data:/data
    restart: unless-stopped

volumes:
  ziggy-data:
```

Start Ziggy:

```console
docker compose up -d
```

## Python

Python 3.14 and [`uv`](https://docs.astral.sh/uv/) are required.

```console
uv sync --locked
```

Copy `ziggy.example.toml` to `ziggy.toml`. Copy `.env.example` to `.env`, add your Archive.org credentials, and export its variables through your shell or secret manager. Ziggy does not load `.env` files itself.

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

Crawler, archive, reporting, and logging settings are documented with their defaults in [`ziggy.example.toml`](ziggy.example.toml). Durations use an integer followed by `s`, `m`, `h`, or `d`.
