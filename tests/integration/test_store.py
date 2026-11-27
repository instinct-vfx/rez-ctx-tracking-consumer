from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from psycopg.rows import dict_row

from rez_ctx_consumer.db import EventStore, Pool, Reject, SchemaError
from rez_ctx_consumer.payload import EVENT_COLUMNS, EventRow, parse_message

FIXTURES = sorted((Path(__file__).parent.parent / "fixtures").glob("rez-*/*.json"))


def fixture_message(path: Path) -> tuple[EventRow, bytes]:
    fixture = json.loads(path.read_text())
    body = json.dumps(fixture["payload"]).encode()
    return parse_message(body, fixture["routing_key"]), body


def message(
    extra_field_columns: dict[str, str] | None = None, **top_level: Any
) -> tuple[EventRow, bytes]:
    payload = {"action": "created", "context": {"status": "solved"}, **top_level}
    body = json.dumps(payload).encode()
    row = parse_message(body, "REZ.CONTEXT.CREATED", extra_field_columns=extra_field_columns)
    return row, body


async def fetch_all(pool: Pool, table: str = "rez_context_event") -> list[dict[str, Any]]:
    async with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(f"SELECT * FROM {table} ORDER BY id")
        return await cur.fetchall()


async def test_empty_batch(migrated: Pool) -> None:
    result = await EventStore(migrated).store([])
    assert (result.stored, result.rejected) == (0, 0)


async def test_round_trip_of_real_rez_messages(migrated: Pool) -> None:
    events = [fixture_message(path) for path in FIXTURES]
    result = await EventStore(migrated).store(events)
    assert (result.stored, result.rejected) == (len(events), 0)

    rows = await fetch_all(migrated)
    assert len(rows) == len(events)
    for stored, (event, _) in zip(rows, events, strict=True):
        for column in EVENT_COLUMNS:
            assert stored[column] == getattr(event, column), column


async def test_failed_resolve_keeps_empty_array(migrated: Pool) -> None:
    [path] = [p for p in FIXTURES if p.stem == "created_failed"]
    await EventStore(migrated).store([fixture_message(path)])
    [row] = await fetch_all(migrated)
    assert row["resolved_packages"] == []  # not NULL: "resolved nothing" is real data


async def test_query_by_resolved_package(migrated: Pool) -> None:
    await EventStore(migrated).store([fixture_message(p) for p in FIXTURES])
    async with migrated.connection() as conn:
        cur = await conn.execute(
            "SELECT count(*) FROM rez_context_event WHERE resolved_packages @> ARRAY['foo-1.0.0']"
        )
        row = await cur.fetchone()
    assert row is not None
    assert row[0] == 3  # created_default, sourced_default, created_all_fields


async def test_rejects_are_stored(migrated: Pool) -> None:
    received = datetime(2026, 11, 20, tzinfo=UTC)
    reject = Reject(received, "REZ.CONTEXT.CREATED", b"\xffnot json\x00", "bad\x00 body")
    result = await EventStore(migrated).store([message()], [reject])
    assert (result.stored, result.rejected) == (1, 1)

    [row] = await fetch_all(migrated, "rez_context_event_reject")
    assert row["body"] == b"\xffnot json\x00"  # raw bytes kept as-is
    assert row["error"] == "bad body"
    assert row["received_at"] == received


async def test_cleaned_characters_are_storable(migrated: Pool) -> None:
    body = b'{"action": "created", "host": "ws\\u0000", "context": {"p": "/caf\\udce9", "t": NaN}}'
    row = parse_message(body, "REZ.CONTEXT.CREATED")
    await EventStore(migrated).store([(row, body)])
    [stored] = await fetch_all(migrated)
    assert stored["host"] == "ws"
    assert stored["context"] == {"p": "/caf�", "t": None}


async def test_extra_field_column(migrated: Pool) -> None:
    async with migrated.connection() as conn:
        await conn.execute("ALTER TABLE rez_context_event ADD COLUMN show_name text")
    mapping = {"show": "show_name"}
    store = EventStore(migrated, list(mapping.values()))
    await store.verify_schema()

    await store.store([message(mapping, show="demo", site="lon")])
    [row] = await fetch_all(migrated)
    assert row["show_name"] == "demo"
    assert row["extra"] == {"site": "lon"}


async def test_extra_field_jsonb_column(migrated: Pool) -> None:
    async with migrated.connection() as conn:
        await conn.execute("ALTER TABLE rez_context_event ADD COLUMN job jsonb")
    mapping = {"job": "job"}
    await EventStore(migrated, ["job"]).store([message(mapping, job={"id": 7, "tags": ["a"]})])
    [row] = await fetch_all(migrated)
    assert row["job"] == {"id": 7, "tags": ["a"]}


async def test_missing_extra_column_fails_fast(migrated: Pool) -> None:
    with pytest.raises(SchemaError, match="show_name"):
        await EventStore(migrated, ["show_name"]).verify_schema()


async def test_bad_row_is_rejected_others_stored(migrated: Pool) -> None:
    async with migrated.connection() as conn:
        await conn.execute("ALTER TABLE rez_context_event ADD COLUMN shot_number integer")
    mapping = {"shot": "shot_number"}
    store = EventStore(migrated, ["shot_number"])
    good1, bad, good2 = (
        message(mapping, shot=10),
        message(mapping, shot="not-a-number"),
        message(mapping, shot=30),
    )

    result = await store.store([good1, bad, good2])
    assert (result.stored, result.rejected) == (2, 1)

    assert [r["shot_number"] for r in await fetch_all(migrated)] == [10, 30]
    [reject] = await fetch_all(migrated, "rez_context_event_reject")
    assert reject["body"] == bad[1]
    assert reject["error"].startswith("insert failed:")


async def test_dedicated_schema_via_search_path(database: str) -> None:
    """Postgres 15+ only lets the owner create tables in ``public``, so the docs
    recommend a role that owns its own schema. Everything must then work through
    ``search_path`` alone."""
    from psycopg import AsyncConnection
    from psycopg.conninfo import make_conninfo
    from psycopg_pool import AsyncConnectionPool

    from rez_ctx_consumer.migrations import migrate
    from rez_ctx_consumer.partitions import ensure_partitions

    async with await AsyncConnection.connect(database, autocommit=True) as admin:
        await admin.execute("CREATE SCHEMA rez_tracking")
    dsn = make_conninfo(database, options="-csearch_path=rez_tracking")
    pool: Pool = AsyncConnectionPool(dsn, min_size=1, max_size=2, open=False)
    await pool.open(wait=True)
    try:
        async with pool.connection() as conn:
            await migrate(conn)
            assert len(await ensure_partitions(conn)) == 2
            await conn.execute("ALTER TABLE rez_context_event ADD COLUMN show_name text")
        store = EventStore(pool, ["show_name"])
        await store.verify_schema()
        await store.store([message({"show": "show_name"}, show="demo")])
    finally:
        await pool.close()

    async with await AsyncConnection.connect(database) as conn:
        cur = await conn.execute(
            "SELECT schemaname, count(*) FROM pg_tables"
            " WHERE tablename LIKE 'rez_c%' GROUP BY schemaname"
        )
        assert await cur.fetchall() == [("rez_tracking", 6)]  # parent, 3 partitions, reject, runner
        cur = await conn.execute("SELECT show_name FROM rez_tracking.rez_context_event")
        assert await cur.fetchall() == [("demo",)]
