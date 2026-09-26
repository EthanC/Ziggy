"""Starlette application factory for HTTP admission."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from starlette.applications import Starlette
from starlette.routing import Route

from ziggy.database import create_engine, session_factory
from ziggy.http.routes import AdmissionRateLimiter, queue_submission

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path


def create_app(database: Path) -> Starlette:
    """Create an HTTP-only application with isolated database resources."""
    engine = create_engine(database, busy_timeout_ms=250)

    @asynccontextmanager
    async def lifespan(app: Starlette) -> AsyncIterator[None]:
        del app
        try:
            yield
        finally:
            await engine.dispose()

    app = Starlette(
        routes=[Route("/v1/queue", queue_submission, methods=["POST"])],
        lifespan=lifespan,
    )
    app.state.sessions = session_factory(engine)
    app.state.admission_lock = asyncio.Lock()
    app.state.rate_limiter = AdmissionRateLimiter()
    return app
