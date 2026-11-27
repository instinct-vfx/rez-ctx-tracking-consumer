from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

from psycopg import AsyncConnection

from rez_ctx_consumer.db import Pool
from rez_ctx_consumer.migrations import migrate
from rez_ctx_consumer.partitions import DEFAULT_PARTITION, ensure_partitions

NOV_2026 = datetime(2026, 11, 20, 15, 30, tzinfo=UTC)


async def insert(conn: AsyncConnection[Any], received_at: datetime) -> tuple[int, str]:
    """Insert a minimal row; return its id and the partition it landed in."""
    cur = await conn.execute(
        "INSERT INTO rez_context_event (received_at, routing_key, action, context)"
        " VALUES (%s, 'REZ.CONTEXT.CREATED', 'created', '{}')"
        " RETURNING id, tableoid::regclass::text",
        (received_at,),
    )
    row = await cur.fetchone()
    assert row is not None
    return row[0], row[1]


async def partitions(conn: AsyncConnection[Any]) -> list[str]:
    cur = await conn.execute(
        "SELECT inhrelid::regclass::text FROM pg_inherits"
        " WHERE inhparent = 'rez_context_event'::regclass ORDER BY 1"
    )
    return [row[0] for row in await cur.fetchall()]


async def test_creates_current_and_next_month(conn: AsyncConnection[Any]) -> None:
    await migrate(conn)
    created = await ensure_partitions(conn, now=NOV_2026)
    assert created == ["rez_context_event_y2026m11", "rez_context_event_y2026m12"]
    assert await partitions(conn) == [DEFAULT_PARTITION, *created]


async def test_idempotent(conn: AsyncConnection[Any]) -> None:
    await migrate(conn)
    await ensure_partitions(conn, now=NOV_2026)
    assert await ensure_partitions(conn, now=NOV_2026) == []


async def test_year_boundary(conn: AsyncConnection[Any]) -> None:
    await migrate(conn)
    created = await ensure_partitions(conn, now=datetime(2026, 12, 31, 23, 59, tzinfo=UTC))
    assert created == ["rez_context_event_y2026m12", "rez_context_event_y2027m01"]


async def test_month_bounds_are_utc(conn: AsyncConnection[Any]) -> None:
    await migrate(conn)
    await conn.execute("SET TIME ZONE 'America/Los_Angeles'")
    await ensure_partitions(conn, now=NOV_2026)
    _, last_oct = await insert(conn, datetime(2026, 10, 31, 23, 59, 59, tzinfo=UTC))
    _, first_nov = await insert(conn, datetime(2026, 11, 1, 0, 0, tzinfo=UTC))
    _, last_dec = await insert(conn, datetime(2026, 12, 31, 23, 59, 59, tzinfo=UTC))
    assert last_oct == DEFAULT_PARTITION
    assert first_nov == "rez_context_event_y2026m11"
    assert last_dec == "rez_context_event_y2026m12"


async def test_rows_in_default_are_moved(conn: AsyncConnection[Any]) -> None:
    await migrate(conn)
    old = datetime(2020, 1, 15, tzinfo=UTC)
    row_id, where = await insert(conn, old)
    assert where == DEFAULT_PARTITION

    created = await ensure_partitions(conn, now=NOV_2026)
    assert "rez_context_event_y2020m01" in created

    cur = await conn.execute(
        "SELECT tableoid::regclass::text FROM rez_context_event WHERE id = %s", (row_id,)
    )
    assert await cur.fetchone() == ("rez_context_event_y2020m01",)
    cur = await conn.execute(f"SELECT count(*) FROM {DEFAULT_PARTITION}")
    assert await cur.fetchone() == (0,)

    # The attached partition works like any other: new rows get fresh ids.
    new_id, where = await insert(conn, old)
    assert where == "rez_context_event_y2020m01"
    assert new_id > row_id


async def test_concurrent_replicas(pool: Pool) -> None:
    async with pool.connection() as c:
        await migrate(c)

    async def one() -> list[str]:
        async with pool.connection() as c:
            return await ensure_partitions(c, now=NOV_2026)

    results = await asyncio.gather(one(), one(), one())
    assert sorted(len(r) for r in results) == [0, 0, 2]


async def test_documented_partition_removal_and_restore(conn: AsyncConnection[Any]) -> None:
    """The recipe in docs/operations.md: detach, drop or re-attach a month."""
    await migrate(conn)
    await ensure_partitions(conn, now=datetime(2025, 1, 10, tzinfo=UTC))
    await insert(conn, datetime(2025, 1, 15, tzinfo=UTC))

    await conn.execute("ALTER TABLE rez_context_event DETACH PARTITION rez_context_event_y2025m01")
    cur = await conn.execute("SELECT count(*) FROM rez_context_event")
    assert await cur.fetchone() == (0,)
    cur = await conn.execute("SELECT count(*) FROM rez_context_event_y2025m01")
    assert await cur.fetchone() == (1,)  # standalone table, ready to archive

    await conn.execute(
        "ALTER TABLE rez_context_event ATTACH PARTITION rez_context_event_y2025m01"
        " FOR VALUES FROM ('2025-01-01 00:00+00') TO ('2025-02-01 00:00+00')"
    )
    cur = await conn.execute("SELECT count(*) FROM rez_context_event")
    assert await cur.fetchone() == (1,)

    await conn.execute("ALTER TABLE rez_context_event DETACH PARTITION rez_context_event_y2025m01")
    await conn.execute("DROP TABLE rez_context_event_y2025m01")
    # The consumer doesn't recreate a month that is in the past.
    assert await ensure_partitions(conn, now=NOV_2026) == [
        "rez_context_event_y2026m11",
        "rez_context_event_y2026m12",
    ]
