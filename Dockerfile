# syntax=docker/dockerfile:1

FROM ghcr.io/astral-sh/uv:latest AS uv

FROM python:alpine AS builder
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /build
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project \
    && chmod -R a-w /build/.venv

FROM python:alpine AS runtime
ARG VERSION=0.1.0
ARG REVISION=unknown
LABEL org.opencontainers.image.title="Ziggy" \
      org.opencontainers.image.description="Continuous website crawler and Internet Archive scheduler" \
      org.opencontainers.image.source="https://github.com/EthanC/Ziggy" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.licenses="AGPL-3.0-only" \
      org.opencontainers.image.version="${VERSION}"

WORKDIR /app
ENV PATH="/app/.venv/bin:${PATH}" \
    PGID=1000 \
    PUID=1000 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
RUN python -m pip uninstall --yes pip \
    && mkdir /ziggy \
    && chmod 0755 /ziggy
COPY --from=builder --chown=0:0 /build/.venv /app/.venv
COPY --chown=0:0 ziggy /app/ziggy
COPY --chown=0:0 --chmod=0555 docker-entrypoint.py /usr/local/bin/docker-entrypoint.py
RUN chmod -R a-w /app

ENTRYPOINT ["python", "/usr/local/bin/docker-entrypoint.py"]
HEALTHCHECK --interval=30s --timeout=5s --start-period=10m --retries=3 \
    CMD ["python", "/usr/local/bin/docker-entrypoint.py", "--skip-chown", "python", "-m", "ziggy", "healthcheck", "--config", "/ziggy/ziggy.toml"]
CMD ["python", "-m", "ziggy", "run", "--config", "/ziggy/ziggy.toml"]
