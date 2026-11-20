from __future__ import annotations

import asyncio

import pytest

from rez_ctx_consumer.app import run
from rez_ctx_consumer.config import Settings


async def test_run_stops_on_event(env: pytest.MonkeyPatch) -> None:
    stop = asyncio.Event()
    task = asyncio.create_task(run(Settings(), stop))
    await asyncio.sleep(0)
    assert not task.done()
    stop.set()
    await asyncio.wait_for(task, timeout=1)
