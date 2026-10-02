"""Bounded SQLite transaction retries and lease-checked result persistence."""

from __future__ import annotations

import asyncio
import sqlite3
from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import text
from sqlalchemy.exc import OperationalError

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence

    from sqlalchemy.ext.asyncio import AsyncSession

    from ziggy.models import Base

_TRANSACTION_ATTEMPTS = 6


def is_database_lock(error: BaseException) -> bool:
    """Recognize SQLite busy/locked errors, including extended result codes."""
    if isinstance(error, BaseExceptionGroup):
        return all(is_database_lock(nested) for nested in error.exceptions)
    if not isinstance(error, OperationalError) or not isinstance(
        error.orig, sqlite3.OperationalError
    ):
        return False
    code = getattr(error.orig, "sqlite_errorcode", 0) & 0xFF
    return code in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED) or any(
        value in str(error.orig).casefold() for value in ("locked", "busy")
    )


async def write_transaction[T](
    session: AsyncSession, operation: Callable[[], Awaitable[T]]
) -> T:
    """Retry an entire database-only operation after rolling back contention."""
    attempt = 0
    while True:
        try:
            await session.execute(text("BEGIN IMMEDIATE"))
            result = await operation()
            await session.commit()
        except BaseException as error:
            await session.rollback()
            if not is_database_lock(error) or attempt == _TRANSACTION_ATTEMPTS - 1:
                raise
            await asyncio.sleep(0.01 * 2**attempt)
            attempt += 1
        else:
            return result


class LeaseLostError(RuntimeError):
    """Another worker changed the lease while remote work was in progress."""


@dataclass
class Lease:
    """Snapshot ownership before network work and recheck it before each write."""

    record: Base
    fields: tuple[str, ...]
    values: tuple[object, ...]

    @classmethod
    def capture(cls, record: Base, *fields: str) -> Lease:
        """Remember the lease token, including its expiration to prevent ABA races."""
        return cls(record, fields, tuple(getattr(record, name) for name in fields))

    async def persist[T](
        self,
        session: AsyncSession,
        operation: Callable[[], Awaitable[T]],
        related: Sequence[Base] = (),
    ) -> T:
        """Refresh under the writer lock; retry persistence without remote calls.

        Rollback expires every loaded ORM object. Callers must snapshot scalar
        values or explicitly refresh dependencies outside ``record`` and ``related``.
        """

        async def apply() -> T:
            await session.refresh(self.record)
            if tuple(getattr(self.record, name) for name in self.fields) != self.values:
                raise LeaseLostError("work lease changed before persistence")
            for record in related:
                await session.refresh(record)
            return await operation()

        result = await write_transaction(session, apply)
        self.values = tuple(getattr(self.record, name) for name in self.fields)
        return result
