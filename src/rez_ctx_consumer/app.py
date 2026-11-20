"""Service lifecycle: start components, wait for a stop signal, shut down cleanly."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
from collections.abc import Coroutine
from typing import Any

import psycopg
from psycopg_pool import PoolTimeout

from rez_ctx_consumer import __version__
from rez_ctx_consumer.config import Settings
from rez_ctx_consumer.db import EventStore, Pool, create_pool
from rez_ctx_consumer.migrations import migrate
from rez_ctx_consumer.partitions import ensure_partitions

log = logging.getLogger(__name__)

# How often partitions for the coming month are checked (cheap, idempotent).
PARTITION_CHECK_INTERVAL_S = 6 * 3600
# Startup retry while Postgres is unreachable.
DB_RETRY_INITIAL_S = 1.0
DB_RETRY_MAX_S = 30.0
DB_CONNECT_TIMEOUT_S = 10.0


def install_signal_handlers(stop: asyncio.Event) -> None:
    """Set ``stop`` on SIGTERM/SIGINT."""
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # Windows event loops (local dev only)
            signal.signal(sig, lambda *_: loop.call_soon_threadsafe(stop.set))


async def wait_or_stop(stop: asyncio.Event, seconds: float) -> bool:
    """Sleep up to ``seconds``; return True if ``stop`` was set meanwhile."""
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    return stop.is_set()


async def until_stopped(coro: Coroutine[Any, Any, bool], stop: asyncio.Event) -> bool:
    """Run ``coro`` but cancel it as soon as ``stop`` is set (returns False then)."""
    task = asyncio.ensure_future(coro)
    stopper = asyncio.ensure_future(stop.wait())
    try:
        await asyncio.wait({task, stopper}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        stopper.cancel()
    if not task.done():
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        return False
    return task.result()


async def prepare_database(pool: Pool, stop: asyncio.Event) -> bool:
    """Migrate and create partitions, retrying while the DB is unreachable.

    Returns False if ``stop`` was set before the database became ready.
    Schema problems (e.g. a changed migration) are raised, not retried.
    """
    delay = DB_RETRY_INITIAL_S
    while not stop.is_set():
        try:
            async with pool.connection(timeout=DB_CONNECT_TIMEOUT_S) as conn:
                applied = await migrate(conn)
                created = await ensure_partitions(conn)
            log.info("database ready", extra={"migrations": applied, "partitions": created})
            return True
        except (psycopg.OperationalError, PoolTimeout) as exc:
            log.warning(
                "database not reachable, retrying", extra={"error": str(exc), "in_s": delay}
            )
            if await wait_or_stop(stop, delay):
                break
            delay = min(delay * 2, DB_RETRY_MAX_S)
    return False


async def maintain_partitions(pool: Pool, stop: asyncio.Event) -> None:
    while not await wait_or_stop(stop, PARTITION_CHECK_INTERVAL_S):
        try:
            async with pool.connection(timeout=DB_CONNECT_TIMEOUT_S) as conn:
                await ensure_partitions(conn)
        except Exception:
            # Not fatal: the DEFAULT partition catches rows and the next run
            # moves them, so log and retry later.
            log.exception("partition maintenance failed")


async def run(settings: Settings, stop: asyncio.Event | None = None) -> None:
    """Run the service until ``stop`` is set (by a signal unless supplied)."""
    if stop is None:
        stop = asyncio.Event()
        install_signal_handlers(stop)

    log.info("starting", extra={"version": __version__, "settings": settings.redacted()})
    pool = create_pool(settings)
    await pool.open()
    try:
        if not await until_stopped(prepare_database(pool, stop), stop):
            return
        store = EventStore(pool, list(settings.extra_field_columns.values()))
        await store.verify_schema()

        maintenance = asyncio.create_task(maintain_partitions(pool, stop))
        # The AMQP consumer gets wired in here in phase 4 (see PLAN.md).
        log.info("ready")
        await stop.wait()
        await maintenance
    finally:
        await pool.close()
        log.info("stopped")
