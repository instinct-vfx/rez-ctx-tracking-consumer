from __future__ import annotations

import asyncio

import pytest

from rez_ctx_consumer.app import run, until_stopped, wait_or_stop
from rez_ctx_consumer.config import Settings


async def test_stop_while_database_unreachable(env: pytest.MonkeyPatch) -> None:
    # Nothing listens on port 1; startup keeps retrying until stopped.
    env.setenv("PG_DSN", "postgresql://u:p@127.0.0.1:1/db?connect_timeout=1")
    stop = asyncio.Event()
    task = asyncio.create_task(run(Settings(), stop))
    await asyncio.sleep(0.3)
    assert not task.done()
    stop.set()
    await asyncio.wait_for(task, timeout=10)


async def test_until_stopped_returns_result() -> None:
    async def work() -> bool:
        return True

    assert await until_stopped(work(), asyncio.Event()) is True


async def test_until_stopped_cancels_on_stop() -> None:
    cancelled = asyncio.Event()

    async def forever() -> bool:
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            cancelled.set()
            raise
        return True

    stop = asyncio.Event()
    asyncio.get_running_loop().call_later(0.05, stop.set)
    assert await until_stopped(forever(), stop) is False
    assert cancelled.is_set()


async def test_until_stopped_propagates_errors() -> None:
    async def fail() -> bool:
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        await until_stopped(fail(), asyncio.Event())


async def test_wait_or_stop() -> None:
    stop = asyncio.Event()
    assert await wait_or_stop(stop, 0.01) is False
    stop.set()
    assert await wait_or_stop(stop, 10) is True
