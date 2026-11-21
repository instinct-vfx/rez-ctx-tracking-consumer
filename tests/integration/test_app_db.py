"""Service startup against a real database."""

from __future__ import annotations

import asyncio

import psycopg
import pytest

from rez_ctx_consumer.app import run
from rez_ctx_consumer.config import Settings
from rez_ctx_consumer.db import SchemaError


async def wait_for_table(dsn: str, table: str) -> None:
    while True:
        async with await psycopg.AsyncConnection.connect(dsn) as conn:
            cur = await conn.execute("SELECT to_regclass(%s)", (table,))
            row = await cur.fetchone()
            if row is not None and row[0] is not None:
                return
        await asyncio.sleep(0.1)


async def test_startup_migrates_and_stops(env: pytest.MonkeyPatch, database: str) -> None:
    env.setenv("PG_DSN", database)
    stop = asyncio.Event()
    task = asyncio.create_task(run(Settings(), stop))

    # Partitions are created after the migration, so this means "startup done".
    async with asyncio.timeout(15):
        await wait_for_table(database, "rez_context_event_reject")
    await asyncio.sleep(0.5)
    assert not task.done()

    stop.set()
    await asyncio.wait_for(task, timeout=10)

    async with await psycopg.AsyncConnection.connect(database) as conn:
        cur = await conn.execute(
            "SELECT count(*) FROM pg_inherits WHERE inhparent = 'rez_context_event'::regclass"
        )
        assert await cur.fetchone() == (3,)  # default + current + next month


async def test_missing_extra_column_is_fatal(env: pytest.MonkeyPatch, database: str) -> None:
    env.setenv("PG_DSN", database)
    env.setenv("EXTRA_FIELD_COLUMNS", '{"show": "show_name"}')
    with pytest.raises(SchemaError):
        await asyncio.wait_for(run(Settings(), asyncio.Event()), timeout=30)
