"""Monthly partitions of ``rez_context_event``, created by the consumer itself.

At startup and periodically, ``ensure_partitions`` creates the partitions for
the current and next month (UTC). If rows ended up in the DEFAULT partition,
because a month had no partition yet, a partition is created for each of those
months too and the rows are moved into it. Nothing is ever dropped; old months
can be removed by hand with ``ALTER TABLE ... DETACH PARTITION`` / ``DROP TABLE``.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any, Final

from psycopg import AsyncConnection, sql

log = logging.getLogger(__name__)

PARENT_TABLE: Final = "rez_context_event"
DEFAULT_PARTITION: Final = "rez_context_event_default"
# Arbitrary app-specific key for pg_advisory_xact_lock (see migrations.py).
PARTITION_LOCK_ID: Final = 0x72657A_0002


def month_start(moment: datetime) -> datetime:
    return moment.astimezone(UTC).replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def add_months(month: datetime, count: int) -> datetime:
    index = month.year * 12 + month.month - 1 + count
    return month.replace(year=index // 12, month=index % 12 + 1)


def partition_name(month: datetime) -> str:
    return f"{PARENT_TABLE}_y{month.year:04d}m{month.month:02d}"


async def ensure_partitions(
    conn: AsyncConnection[Any], *, now: datetime | None = None, months_ahead: int = 1
) -> list[str]:
    """Create missing partitions. Returns the names of partitions created."""
    current = month_start(now or datetime.now(UTC))
    wanted = {add_months(current, i) for i in range(months_ahead + 1)}
    created: list[str] = []

    async with conn.transaction():
        await conn.execute("SELECT pg_advisory_xact_lock(%s)", (PARTITION_LOCK_ID,))

        cur = await conn.execute(
            sql.SQL("SELECT DISTINCT date_trunc('month', received_at, 'UTC') FROM {}").format(
                sql.Identifier(DEFAULT_PARTITION)
            )
        )
        stray = {month_start(row[0]) for row in await cur.fetchall()}
        if stray:
            log.warning(
                "rows found in the default partition",
                extra={"months": sorted(m.strftime("%Y-%m") for m in stray)},
            )

        for month in sorted(wanted | stray):
            name = partition_name(month)
            cur = await conn.execute("SELECT to_regclass(%s)", (name,))
            row = await cur.fetchone()
            if row is not None and row[0] is not None:
                continue
            if month in stray:
                await _create_and_move(conn, name, month)
            else:
                await conn.execute(
                    sql.SQL("CREATE TABLE {} PARTITION OF {} FOR VALUES FROM ({}) TO ({})").format(
                        sql.Identifier(name),
                        sql.Identifier(PARENT_TABLE),
                        sql.Literal(month),
                        sql.Literal(add_months(month, 1)),
                    )
                )
            log.info("created partition", extra={"partition": name})
            created.append(name)

    return created


async def _create_and_move(conn: AsyncConnection[Any], name: str, month: datetime) -> None:
    """Create a partition for a month that already has rows in DEFAULT.

    Postgres refuses to add a partition while DEFAULT holds rows for its range,
    so build the table standalone, move the rows into it, then attach it.
    """
    ident = sql.Identifier(name)
    parent = sql.Identifier(PARENT_TABLE)
    default = sql.Identifier(DEFAULT_PARTITION)
    lower, upper = sql.Literal(month), sql.Literal(add_months(month, 1))

    await conn.execute(
        sql.SQL("CREATE TABLE {} (LIKE {} INCLUDING DEFAULTS INCLUDING CONSTRAINTS)").format(
            ident, parent
        )
    )
    cur = await conn.execute(
        sql.SQL(
            "WITH moved AS ("
            " DELETE FROM {default} WHERE received_at >= {lower} AND received_at < {upper}"
            " RETURNING *"
            ") INSERT INTO {ident} SELECT * FROM moved"
        ).format(default=default, lower=lower, upper=upper, ident=ident)
    )
    moved = cur.rowcount
    await conn.execute(
        sql.SQL("ALTER TABLE {} ATTACH PARTITION {} FOR VALUES FROM ({}) TO ({})").format(
            parent, ident, lower, upper
        )
    )
    log.info("moved rows out of the default partition", extra={"partition": name, "rows": moved})
