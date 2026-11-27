"""Connection pool and batch writes to ``rez_context_event``.

``EventStore.store`` writes a batch in one transaction. If the batch fails
because of the data in some row (a value that doesn't fit a mapped extra column,
for example), it is retried row by row with a savepoint per row. Rows that still
fail go to the rejects table, so one bad message can't block the whole queue.
Connection or server errors are raised to the caller, which keeps the messages
unacked and retries.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final

import psycopg
from psycopg import AsyncConnection, sql
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from rez_ctx_consumer.config import Settings
from rez_ctx_consumer.payload import EVENT_COLUMNS, EventRow, clean_text

log = logging.getLogger(__name__)

EVENT_TABLE: Final = "rez_context_event"
REJECT_TABLE: Final = "rez_context_event_reject"
_JSONB_COLUMNS: Final = frozenset({"context", "extra"})

# Errors caused by the values in a row, as opposed to the connection or schema.
_ROW_ERRORS: Final = (psycopg.DataError, psycopg.IntegrityError)

Pool = AsyncConnectionPool[AsyncConnection[Any]]


class SchemaError(RuntimeError):
    """The database schema doesn't match the configuration."""


@dataclass(frozen=True, slots=True)
class Reject:
    received_at: datetime
    routing_key: str
    body: bytes
    error: str


@dataclass(frozen=True, slots=True)
class StoreResult:
    stored: int
    rejected: int


def create_pool(settings: Settings) -> Pool:
    """Create a pool (not opened yet; call ``await pool.open()``)."""
    return AsyncConnectionPool(
        settings.pg_dsn.get_secret_value(),
        min_size=1,
        max_size=settings.pg_pool_size,
        open=False,
        # Validate connections before handing them out, so a DB restart
        # doesn't fail the first batch afterwards.
        check=AsyncConnectionPool.check_connection,
        kwargs={"application_name": "rez-ctx-consumer"},
    )


class EventStore:
    def __init__(self, pool: Pool, extra_columns: Sequence[str] = ()) -> None:
        self._pool = pool
        self._extra_columns = tuple(extra_columns)
        self._insert_event = _insert_sql(EVENT_TABLE, (*EVENT_COLUMNS, *self._extra_columns))
        self._insert_reject = _insert_sql(
            REJECT_TABLE, ("received_at", "routing_key", "body", "error")
        )

    async def verify_schema(self) -> None:
        """Fail fast if a column mapped via EXTRA_FIELD_COLUMNS doesn't exist."""
        if not self._extra_columns:
            return
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                "SELECT column_name FROM information_schema.columns"
                " WHERE table_schema = current_schema() AND table_name = %s",
                (EVENT_TABLE,),
            )
            existing = {row[0] for row in await cur.fetchall()}
        missing = sorted(set(self._extra_columns) - existing)
        if missing:
            raise SchemaError(
                f"EXTRA_FIELD_COLUMNS maps to column(s) missing from {EVENT_TABLE}: {missing}. "
                "Add them with a migration first."
            )

    async def store(
        self, events: Sequence[tuple[EventRow, bytes]], rejects: Sequence[Reject] = ()
    ) -> StoreResult:
        """Write events (with their raw bodies, for rejects) and rejects atomically."""
        if not events and not rejects:
            return StoreResult(0, 0)

        async with self._pool.connection() as conn:
            try:
                async with conn.transaction(), conn.cursor() as cur:
                    if events:
                        await cur.executemany(
                            self._insert_event, [self._event_params(row) for row, _ in events]
                        )
                    await self._write_rejects(cur, rejects)
                return StoreResult(stored=len(events), rejected=len(rejects))
            except _ROW_ERRORS as exc:
                log.warning(
                    "batch insert failed, retrying row by row",
                    extra={"error": str(exc), "batch": len(events)},
                )
            return await self._store_row_by_row(conn, events, rejects)

    async def _store_row_by_row(
        self,
        conn: AsyncConnection[Any],
        events: Sequence[tuple[EventRow, bytes]],
        rejects: Sequence[Reject],
    ) -> StoreResult:
        all_rejects = list(rejects)
        stored = 0
        async with conn.transaction(), conn.cursor() as cur:
            for row, body in events:
                try:
                    async with conn.transaction():  # savepoint
                        await cur.execute(self._insert_event, self._event_params(row))
                    stored += 1
                except _ROW_ERRORS as exc:
                    log.warning(
                        "rejected event at insert",
                        extra={"routing_key": row.routing_key, "error": str(exc)},
                    )
                    all_rejects.append(
                        Reject(row.received_at, row.routing_key, body, f"insert failed: {exc}")
                    )
            await self._write_rejects(cur, all_rejects)
        return StoreResult(stored=stored, rejected=len(all_rejects))

    async def _write_rejects(
        self, cur: psycopg.AsyncCursor[Any], rejects: Sequence[Reject]
    ) -> None:
        if rejects:
            await cur.executemany(
                self._insert_reject,
                [
                    (r.received_at, clean_text(r.routing_key), r.body, clean_text(r.error))
                    for r in rejects
                ],
            )

    def _event_params(self, row: EventRow) -> tuple[Any, ...]:
        values: list[Any] = []
        for column in EVENT_COLUMNS:
            value = getattr(row, column)
            values.append(Jsonb(value) if column in _JSONB_COLUMNS else value)
        for column in self._extra_columns:
            value = row.extra_columns.get(column)
            # Structured values only fit json/jsonb columns; let Postgres decide.
            values.append(Jsonb(value) if isinstance(value, dict | list) else value)
        return tuple(values)


def _insert_sql(table: str, columns: Sequence[str]) -> sql.Composed:
    return sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier(table),
        sql.SQL(", ").join(map(sql.Identifier, columns)),
        sql.SQL(", ").join(sql.Placeholder() * len(columns)),
    )
