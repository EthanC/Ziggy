# syntax=docker/dockerfile:1.7

ARG PYTHON_VERSION=3.14
ARG UV_VERSION=0.12.7

FROM ghcr.io/astral-sh/uv:${UV_VERSION} AS uv

FROM python:${PYTHON_VERSION}-slim AS builder
COPY --from=uv /uv /bin/uv
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
COPY pyproject.toml uv.lock README.md LICENSE ./
RUN uv sync --locked --no-dev

FROM python:${PYTHON_VERSION}-slim AS runtime
ARG VERSION=0.1.0
ARG REVISION=unknown
LABEL org.opencontainers.image.title="Ziggy" \
      org.opencontainers.image.description="Continuous website crawler and Internet Archive scheduler" \
      org.opencontainers.image.source="https://github.com/EthanC/Ziggy" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.licenses="AGPL-3.0-only" \
      org.opencontainers.image.version="${VERSION}"

RUN groupadd --gid 10001 ziggy \
    && useradd --uid 10001 --gid ziggy --no-create-home --shell /usr/sbin/nologin ziggy \
    && mkdir -p /app /config /data \
    && chown ziggy:ziggy /config /data
WORKDIR /app
COPY --from=builder /app /app
COPY ziggy ./ziggy
ENV PATH="/app/.venv/bin:${PATH}" PYTHONUNBUFFERED=1
USER 10001:10001
VOLUME ["/data"]
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD ["python", "-m", "ziggy", "healthcheck", "--config", "/config/ziggy.toml"]
CMD ["python", "-m", "ziggy", "run", "--config", "/config/ziggy.toml"]
