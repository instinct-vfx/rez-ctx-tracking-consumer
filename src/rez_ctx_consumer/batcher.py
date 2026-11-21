"""Collect deliveries into batches, store them, then acknowledge them.

Independent of the AMQP library: anything that looks like a ``Delivery`` works,
which keeps this logic unit-testable.

Rules:

- A batch is flushed when it reaches ``batch_size`` messages or its oldest
  message has waited ``max_wait_s``, whichever comes first. After a stop it is
  flushed right away.
- Messages are acked only after the database commit (at-least-once delivery).
- If storing fails (database down, ...), the same batch is retried with
  exponential backoff. Nothing is acked meanwhile, and because the broker never
  has more than ``prefetch`` unacked messages outstanding, consumption pauses
  by itself.
- Messages whose channel has closed (broker connection lost) are dropped from
  the batch without storing them. The broker redelivers them on the new
  channel, so storing them now would only create duplicates.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from rez_ctx_consumer.db import Reject, StoreResult
from rez_ctx_consumer.metrics import Metrics
from rez_ctx_consumer.payload import EventRow, PayloadError, parse_message

log = logging.getLogger(__name__)


class Delivery(Protocol):
    """One message received from the broker."""

    @property
    def body(self) -> bytes: ...
    @property
    def routing_key(self) -> str: ...
    @property
    def channel_open(self) -> bool: ...
    async def ack(self) -> None: ...
    async def nack(self, requeue: bool) -> None: ...


class Store(Protocol):
    async def store(
        self, events: Sequence[tuple[EventRow, bytes]], rejects: Sequence[Reject] = ()
    ) -> StoreResult: ...


@dataclass(slots=True)
class _Item:
    delivery: Delivery
    arrived: float  # monotonic clock, for the batch deadline
    row: EventRow | None
    reject: Reject | None


class Batcher:
    def __init__(
        self,
        store: Store,
        *,
        batch_size: int,
        max_wait_s: float,
        extra_field_columns: Mapping[str, str] | None = None,
        retry_initial_s: float = 1.0,
        retry_max_s: float = 30.0,
        clock: Callable[[], float] = time.monotonic,
        metrics: Metrics | None = None,
    ) -> None:
        self._store = store
        self._batch_size = batch_size
        self._max_wait_s = max_wait_s
        self._extra_field_columns = dict(extra_field_columns or {})
        self._retry_initial_s = retry_initial_s
        self._retry_max_s = retry_max_s
        self._clock = clock
        self._pending: list[_Item] = []
        self._in_flight: list[_Item] = []
        self._wake = asyncio.Event()
        self._failing_since: float | None = None  # monotonic; first failed store attempt
        self.metrics = metrics or Metrics()

    @property
    def pending(self) -> int:
        return len(self._pending) + len(self._in_flight)

    def failing_for(self) -> float | None:
        """Seconds the current batch has been failing to store, or None."""
        if self._failing_since is None:
            return None
        return self._clock() - self._failing_since

    def add(self, delivery: Delivery) -> None:
        """Accept one delivery (called from the consumer callback)."""
        received_at = datetime.now(UTC)
        self.metrics.messages_received.inc()
        row: EventRow | None = None
        reject: Reject | None = None
        try:
            row = parse_message(
                delivery.body,
                delivery.routing_key,
                received_at=received_at,
                extra_field_columns=self._extra_field_columns,
            )
        except PayloadError as exc:
            log.warning(
                "rejected message", extra={"routing_key": delivery.routing_key, "error": str(exc)}
            )
            reject = Reject(received_at, delivery.routing_key, delivery.body, str(exc))
        else:
            if row.issues:
                self.metrics.field_issues.inc()
                log.warning(
                    "stored message with field issues",
                    extra={"routing_key": row.routing_key, "issues": list(row.issues)},
                )
        self._pending.append(_Item(delivery, self._clock(), row, reject))
        self._wake.set()

    async def run(self, stop: asyncio.Event) -> None:
        """Flush batches until ``stop`` is set and nothing is pending."""
        while True:
            if not self._pending:
                if stop.is_set():
                    return
                await self._sleep(stop, None)
                continue
            if len(self._pending) < self._batch_size and not stop.is_set():
                remaining = self._pending[0].arrived + self._max_wait_s - self._clock()
                if remaining > 0:
                    await self._sleep(stop, remaining)
                    continue
            self._in_flight = self._pending[: self._batch_size]
            del self._pending[: self._batch_size]
            await self._flush(self._in_flight)
            self._in_flight = []

    async def release(self) -> None:
        """Hand unflushed messages back to the broker (shutdown timed out)."""
        items, self._pending, self._in_flight = [*self._in_flight, *self._pending], [], []
        for item in items:
            if item.delivery.channel_open:
                try:
                    await item.delivery.nack(requeue=True)
                except Exception:  # channel closed meanwhile: requeued anyway
                    log.debug("nack failed", exc_info=True)
        if items:
            log.warning("returned unstored messages to the broker", extra={"count": len(items)})

    async def _sleep(self, stop: asyncio.Event, seconds: float | None) -> None:
        """Wait for a new delivery, a stop, or ``seconds`` (None: no limit)."""
        self._wake.clear()
        waiters = {asyncio.ensure_future(self._wake.wait()), asyncio.ensure_future(stop.wait())}
        try:
            await asyncio.wait(waiters, timeout=seconds, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for waiter in waiters:
                waiter.cancel()

    async def _flush(self, batch: list[_Item]) -> None:
        started = self._clock()
        delay = self._retry_initial_s
        while True:
            live = [item for item in batch if item.delivery.channel_open]
            if len(live) < len(batch):
                self.metrics.dropped_closed_channel.inc(len(batch) - len(live))
                log.info(
                    "dropped messages from a closed channel; the broker redelivers them",
                    extra={"count": len(batch) - len(live)},
                )
                batch = live
            if not batch:
                self._failing_since = None
                return
            events = [(item.row, item.delivery.body) for item in batch if item.row is not None]
            rejects = [item.reject for item in batch if item.reject is not None]
            try:
                result = await self._store.store(events, rejects)
                break
            except Exception as exc:
                self.metrics.store_errors.inc()
                if self._failing_since is None:
                    self._failing_since = self._clock()
                log.error(
                    "storing batch failed, retrying",
                    extra={
                        "error": f"{type(exc).__name__}: {exc}",
                        "batch": len(batch),
                        "in_s": delay,
                    },
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, self._retry_max_s)

        unacked = 0
        for item in batch:
            try:
                await item.delivery.ack()
            except Exception:
                # Channel closed after the commit: the broker redelivers and
                # the row is stored twice. Accepted (at-least-once).
                unacked += 1

        self._failing_since = None
        m = self.metrics
        m.events_stored.inc(result.stored)
        m.messages_rejected.labels("parse").inc(len(rejects))
        m.messages_rejected.labels("insert").inc(result.rejected - len(rejects))
        m.ack_failures.inc(unacked)
        m.batch_size.observe(len(batch))
        m.batch_seconds.observe(self._clock() - started)
        m.last_flush.set_to_current_time()
        log.debug(
            "flushed batch",
            extra={"stored": result.stored, "rejected": result.rejected, "unacked": unacked},
        )
        if unacked:
            log.warning(
                "could not ack stored messages; expect duplicates", extra={"count": unacked}
            )
