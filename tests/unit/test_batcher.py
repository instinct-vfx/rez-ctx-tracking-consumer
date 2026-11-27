from __future__ import annotations

import asyncio
import json
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

import pytest

from rez_ctx_consumer.batcher import Batcher
from rez_ctx_consumer.db import Reject, StoreResult
from rez_ctx_consumer.payload import EventRow


@dataclass
class FakeDelivery:
    body: bytes
    routing_key: str = "REZ.CONTEXT.CREATED"
    channel_open: bool = True
    acked: bool = False
    nacked: bool = False
    fail_ack: bool = False

    async def ack(self) -> None:
        if self.fail_ack:
            raise RuntimeError("channel closed")
        self.acked = True

    async def nack(self, requeue: bool) -> None:
        assert requeue
        self.nacked = True


@dataclass
class FakeStore:
    fail_times: int = 0
    block: asyncio.Event | None = None
    calls: list[tuple[list[EventRow], list[Reject]]] = field(default_factory=list)
    attempts: int = 0

    async def store(
        self, events: Sequence[tuple[EventRow, bytes]], rejects: Sequence[Reject] = ()
    ) -> StoreResult:
        self.attempts += 1
        if self.block is not None:
            await self.block.wait()
        if self.attempts <= self.fail_times:
            raise ConnectionError("database down")
        self.calls.append(([row for row, _ in events], list(rejects)))
        return StoreResult(len(events), len(rejects))


def delivery(n: int = 0, **kwargs: Any) -> FakeDelivery:
    body = json.dumps({"action": "created", "context": {"n": n}}).encode()
    return FakeDelivery(body, **kwargs)


def batch_sizes(store: FakeStore) -> list[int]:
    return [len(events) + len(rejects) for events, rejects in store.calls]


@pytest.fixture
async def stop() -> asyncio.Event:
    return asyncio.Event()


async def eventually(condition: Callable[[], object], timeout: float = 2) -> None:
    async with asyncio.timeout(timeout):
        while not condition():
            await asyncio.sleep(0.002)


async def run_until(
    batcher: Batcher, stop: asyncio.Event, condition: Callable[[], object]
) -> asyncio.Task[None]:
    task = asyncio.create_task(batcher.run(stop))
    await eventually(condition)
    return task


async def finish(task: asyncio.Task[None], stop: asyncio.Event) -> None:
    stop.set()
    await asyncio.wait_for(task, 5)


async def test_flush_on_batch_size(stop: asyncio.Event) -> None:
    store = FakeStore()
    batcher = Batcher(store, batch_size=3, max_wait_s=3600)
    deliveries = [delivery(i) for i in range(7)]
    for d in deliveries:
        batcher.add(d)
    task = await run_until(batcher, stop, lambda: len(store.calls) == 2)
    assert batch_sizes(store) == [3, 3]  # the 7th waits for its deadline
    assert [d.acked for d in deliveries] == [True] * 6 + [False]
    await finish(task, stop)
    assert batch_sizes(store) == [3, 3, 1]  # flushed on stop
    assert all(d.acked for d in deliveries)


async def test_flush_on_deadline(stop: asyncio.Event) -> None:
    store = FakeStore()
    batcher = Batcher(store, batch_size=100, max_wait_s=0.05)
    task = asyncio.create_task(batcher.run(stop))
    d1, d2 = delivery(1), delivery(2)
    batcher.add(d1)
    batcher.add(d2)
    await asyncio.sleep(0.02)
    assert store.calls == []
    await eventually(lambda: store.calls)
    assert batch_sizes(store) == [2]
    assert d1.acked
    assert d2.acked
    await finish(task, stop)


async def test_messages_arriving_while_idle_wake_the_flusher(stop: asyncio.Event) -> None:
    store = FakeStore()
    batcher = Batcher(store, batch_size=2, max_wait_s=3600)
    task = asyncio.create_task(batcher.run(stop))
    await asyncio.sleep(0.02)  # flusher is idle now
    batcher.add(delivery(1))
    batcher.add(delivery(2))
    await eventually(lambda: store.calls)
    await finish(task, stop)


async def test_invalid_message_becomes_reject_and_is_acked(stop: asyncio.Event) -> None:
    store = FakeStore()
    batcher = Batcher(store, batch_size=2, max_wait_s=3600)
    good, bad = delivery(1), FakeDelivery(b"not json")
    batcher.add(good)
    batcher.add(bad)
    task = await run_until(batcher, stop, lambda: store.calls)
    [(events, rejects)] = store.calls
    assert len(events) == 1
    assert rejects[0].body == b"not json"
    assert "JSON" in rejects[0].error
    assert good.acked
    assert bad.acked
    await finish(task, stop)


async def test_extra_field_columns_are_applied(stop: asyncio.Event) -> None:
    store = FakeStore()
    batcher = Batcher(store, batch_size=1, max_wait_s=3600, extra_field_columns={"show": "show"})
    body = json.dumps({"action": "created", "context": {}, "show": "demo"}).encode()
    batcher.add(FakeDelivery(body))
    task = await run_until(batcher, stop, lambda: store.calls)
    assert store.calls[0][0][0].extra_columns == {"show": "demo"}
    await finish(task, stop)


async def test_store_failure_is_retried_without_acking(stop: asyncio.Event) -> None:
    store = FakeStore(fail_times=2)
    batcher = Batcher(store, batch_size=1, max_wait_s=3600, retry_initial_s=0.01)
    d = delivery()
    batcher.add(d)
    task = asyncio.create_task(batcher.run(stop))
    await eventually(lambda: store.attempts >= 2)
    assert not d.acked
    await eventually(lambda: store.calls)
    assert store.attempts == 3
    assert d.acked
    await finish(task, stop)


async def test_messages_from_closed_channel_are_not_stored(stop: asyncio.Event) -> None:
    store = FakeStore()
    batcher = Batcher(store, batch_size=3, max_wait_s=3600)
    stale = [delivery(1, channel_open=False), delivery(2, channel_open=False)]
    fresh = delivery(3)
    for d in [*stale, fresh]:
        batcher.add(d)
    task = await run_until(batcher, stop, lambda: store.calls)
    assert batch_sizes(store) == [1]
    assert fresh.acked
    assert not any(d.acked for d in stale)
    await finish(task, stop)


async def test_channel_closing_during_db_outage_drops_batch(stop: asyncio.Event) -> None:
    store = FakeStore(fail_times=1)
    batcher = Batcher(store, batch_size=1, max_wait_s=3600, retry_initial_s=0.05)
    d = delivery()
    batcher.add(d)
    task = asyncio.create_task(batcher.run(stop))
    await eventually(lambda: store.attempts >= 1)
    d.channel_open = False  # broker connection lost while DB was down
    await asyncio.sleep(0.1)
    assert store.calls == []  # not stored: the broker redelivers it
    assert store.attempts == 1
    await finish(task, stop)


async def test_failed_ack_does_not_stop_flusher(stop: asyncio.Event) -> None:
    store = FakeStore()
    batcher = Batcher(store, batch_size=1, max_wait_s=3600)
    batcher.add(delivery(1, fail_ack=True))
    task = await run_until(batcher, stop, lambda: store.calls)
    later = delivery(2)
    batcher.add(later)
    await eventually(lambda: later.acked)
    await finish(task, stop)


async def test_run_returns_when_stopped_and_idle(stop: asyncio.Event) -> None:
    batcher = Batcher(FakeStore(), batch_size=10, max_wait_s=3600)
    task = asyncio.create_task(batcher.run(stop))
    await asyncio.sleep(0.01)
    await finish(task, stop)


async def test_release_nacks_pending_and_in_flight(stop: asyncio.Event) -> None:
    block = asyncio.Event()
    store = FakeStore(block=block)
    batcher = Batcher(store, batch_size=1, max_wait_s=3600)
    in_flight, pending, closed = delivery(1), delivery(2), delivery(3, channel_open=False)
    for d in (in_flight, pending, closed):
        batcher.add(d)
    task = asyncio.create_task(batcher.run(stop))
    await eventually(lambda: store.attempts >= 1)
    assert batcher.pending == 3

    task.cancel()  # shutdown timeout while the store call hangs
    with pytest.raises(asyncio.CancelledError):
        await task
    await batcher.release()
    assert in_flight.nacked
    assert pending.nacked
    assert not closed.nacked  # its channel is gone; the broker requeues it anyway
    assert batcher.pending == 0


async def test_metrics_and_failure_tracking(stop: asyncio.Event) -> None:
    store = FakeStore(fail_times=1)
    batcher = Batcher(store, batch_size=4, max_wait_s=3600, retry_initial_s=0.05)

    def sample(name: str, **labels: str) -> float | None:
        return batcher.metrics.registry.get_sample_value(f"rez_ctx_consumer_{name}", labels)

    issue = json.dumps({"action": "created", "context": {"status": 5}}).encode()
    for d in (
        delivery(1),
        FakeDelivery(b"nope"),
        FakeDelivery(issue),
        delivery(2, channel_open=False),
    ):
        batcher.add(d)
    task = asyncio.create_task(batcher.run(stop))
    await eventually(lambda: store.attempts >= 1)
    await eventually(lambda: batcher.failing_for() is not None)
    await eventually(lambda: store.calls)
    assert batcher.failing_for() is None

    assert sample("messages_received_total") == 4
    assert sample("events_stored_total") == 2
    assert sample("messages_rejected_total", stage="parse") == 1
    assert sample("field_issues_total") == 1
    assert sample("store_errors_total") == 1
    assert sample("dropped_closed_channel_total") == 1
    assert sample("batch_size_count") == 1
    assert (sample("last_flush_timestamp_seconds") or 0) > 0
    await finish(task, stop)
