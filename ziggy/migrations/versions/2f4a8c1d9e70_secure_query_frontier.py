"""Secure and bound query-bearing crawl frontier entries."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast
from urllib.parse import parse_qsl, urlsplit

import sqlalchemy as sa
from alembic import op

if TYPE_CHECKING:
    from collections.abc import Sequence

revision: str = "2f4a8c1d9e70"
down_revision: str | None = "c83d91e4a672"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_DEFAULT_QUERY_VARIANT_CAP = 20
_SENSITIVE_KEYS = {
    "access_token",
    "api_key",
    "apikey",
    "auth",
    "authorization",
    "code",
    "credential",
    "csrf",
    "error_key",
    "errorkey",
    "id_token",
    "jwt",
    "password",
    "recaptcha",
    "refresh_token",
    "session",
    "session_id",
    "sessionid",
    "sig",
    "signature",
    "token",
}
_SENSITIVE_PREFIXES = ("x-amz-", "x-goog-")
_SENSITIVE_SUFFIXES = ("secret", "signature", "token")


def _has_sensitive_query(url: str) -> int:
    for key, _value in parse_qsl(urlsplit(url).query, keep_blank_values=True):
        normalized = key.casefold()
        if (
            normalized in _SENSITIVE_KEYS
            or normalized.startswith(_SENSITIVE_PREFIXES)
            or normalized.endswith(_SENSITIVE_SUFFIXES)
        ):
            return 1
    return 0


def _delete_target_pages() -> None:
    op.execute(
        "UPDATE pages SET discovered_from_id = NULL "
        "WHERE discovered_from_id IN (SELECT page_id FROM ziggy_cleanup_pages)"
    )
    op.execute(
        "DELETE FROM captures "
        "WHERE page_id IN (SELECT page_id FROM ziggy_cleanup_pages)"
    )
    op.execute(
        "DELETE FROM archive_jobs "
        "WHERE page_id IN (SELECT page_id FROM ziggy_cleanup_pages)"
    )
    op.execute(
        "DELETE FROM pages WHERE id IN (SELECT page_id FROM ziggy_cleanup_pages)"
    )
    op.execute("DELETE FROM ziggy_cleanup_pages")


def upgrade() -> None:
    op.add_column("pages", sa.Column("query_base_url", sa.Text(), nullable=True))
    op.add_column("pages", sa.Column("query_variant_slot", sa.Integer(), nullable=True))
    op.add_column("pages", sa.Column("blocked_reason", sa.String(32), nullable=True))

    bind = op.get_bind()
    raw_connection = cast("Any", bind.connection.driver_connection)
    raw_connection.create_function("ziggy_sensitive_query", 1, _has_sensitive_query)
    cap = int(
        cast("Any", op.get_context().config).attributes.get(
            "query_variant_cap", _DEFAULT_QUERY_VARIANT_CAP
        )
    )
    cap = max(0, cap)

    op.execute("CREATE TEMP TABLE ziggy_cleanup_pages (page_id INTEGER PRIMARY KEY)")
    op.execute(
        "INSERT INTO ziggy_cleanup_pages(page_id) "
        "SELECT id FROM pages WHERE ziggy_sensitive_query(url) = 1"
    )
    _delete_target_pages()

    op.execute(
        "UPDATE pages SET query_base_url = substr(url, 1, instr(url, '?') - 1) "
        "WHERE instr(url, '?') > 0"
    )
    op.execute(
        "CREATE TEMP TABLE ziggy_variant_ranks AS "
        "SELECT p.id AS page_id, "
        "row_number() OVER ("
        "PARTITION BY p.query_base_url "
        "ORDER BY EXISTS(SELECT 1 FROM captures c WHERE c.page_id = p.id) DESC, "
        "p.discovered_at, p.id"
        ") AS slot, "
        "EXISTS(SELECT 1 FROM captures c WHERE c.page_id = p.id) AS captured "
        "FROM pages p WHERE p.query_base_url IS NOT NULL"
    )
    op.execute(
        sa.text(
            "UPDATE pages SET query_variant_slot = ("
            "SELECT slot FROM ziggy_variant_ranks r WHERE r.page_id = pages.id"
            ") WHERE id IN ("
            "SELECT page_id FROM ziggy_variant_ranks WHERE slot <= :cap"
            ")"
        ).bindparams(cap=cap)
    )
    op.execute(
        sa.text(
            "UPDATE pages SET blocked_reason = 'query_variant_cap', in_scope = 0 "
            "WHERE id IN (SELECT page_id FROM ziggy_variant_ranks "
            "WHERE slot > :cap AND captured = 1)"
        ).bindparams(cap=cap)
    )
    op.execute(
        sa.text(
            "INSERT INTO ziggy_cleanup_pages(page_id) "
            "SELECT page_id FROM ziggy_variant_ranks "
            "WHERE slot > :cap AND captured = 0"
        ).bindparams(cap=cap)
    )
    _delete_target_pages()
    op.execute("DROP TABLE ziggy_variant_ranks")
    op.execute("DROP TABLE ziggy_cleanup_pages")

    op.create_index(
        "uq_pages_query_variant_slot",
        "pages",
        ["query_base_url", "query_variant_slot"],
        unique=True,
        sqlite_where=sa.text("query_variant_slot IS NOT NULL"),
    )
    violations = bind.exec_driver_sql("PRAGMA foreign_key_check").fetchall()
    if violations:  # pragma: no cover - aborts only on SQLite invariant failure.
        raise RuntimeError("query frontier cleanup violated foreign keys")


def downgrade() -> None:
    op.drop_index("uq_pages_query_variant_slot", table_name="pages")
    with op.batch_alter_table("pages", schema=None) as batch_op:
        batch_op.drop_column("blocked_reason")
        batch_op.drop_column("query_variant_slot")
        batch_op.drop_column("query_base_url")
