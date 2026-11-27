from __future__ import annotations

import asyncio
import logging
from typing import Any

import pytest
from psycopg import AsyncConnection
from psycopg.errors import SyntaxError as PgSyntaxError

from rez_ctx_consumer.db import Pool
from rez_ctx_consumer.migrations import MIGRATIONS_TABLE, Migration, MigrationError, migrate


async def tables(conn: AsyncConnection[Any]) -> set[str]:
    cur = await conn.execute("SELECT tablename FROM pg_tables WHERE schemaname = current_schema()")
    return {row[0] for row in await cur.fetchall()}


async def test_fresh_database(conn: AsyncConnection[Any]) -> None:
    assert await migrate(conn) == [1]
    assert {
        "rez_context_event",
        "rez_context_event_default",
        "rez_context_event_reject",
        MIGRATIONS_TABLE,
    } <= await tables(conn)


async def test_idempotent(conn: AsyncConnection[Any]) -> None:
    await migrate(conn)
    assert await migrate(conn) == []


async def test_concurrent_replicas(pool: Pool) -> None:
    async def one() -> list[int]:
        async with pool.connection() as c:
            return await migrate(c)

    results = await asyncio.gather(one(), one(), one())
    assert sorted(results) == [[], [], [1]]


async def test_changed_migration_is_refused(conn: AsyncConnection[Any]) -> None:
    await migrate(conn)
    await conn.execute(f"UPDATE {MIGRATIONS_TABLE} SET checksum = 'edited' WHERE version = 1")
    with pytest.raises(MigrationError, match="changed after it was applied"):
        await migrate(conn)


async def test_newer_database_only_warns(
    conn: AsyncConnection[Any], caplog: pytest.LogCaptureFixture
) -> None:
    await migrate(conn)
    await conn.execute(
        f"INSERT INTO {MIGRATIONS_TABLE} (version, name, checksum) VALUES (99, 'future', 'x')"
    )
    with caplog.at_level(logging.WARNING):
        assert await migrate(conn) == []
    assert "doesn't know" in caplog.text


async def test_failed_migration_rolls_back_everything(conn: AsyncConnection[Any]) -> None:
    migrations = [
        Migration(1, "good", "CREATE TABLE good (id int);"),
        Migration(2, "bad", "CREATE TABLE bad (;"),
    ]
    with pytest.raises(PgSyntaxError):
        await migrate(conn, migrations)
    assert "good" not in await tables(conn)
    assert MIGRATIONS_TABLE not in await tables(conn)
