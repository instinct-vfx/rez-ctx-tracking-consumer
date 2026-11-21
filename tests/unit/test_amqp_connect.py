"""Connection attempts must never leave aio-pika reconnect tasks behind."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest

from rez_ctx_consumer.amqp import AmqpConsumer
from rez_ctx_consumer.app import until_stopped
from rez_ctx_consumer.config import Settings


def other_tasks() -> set[asyncio.Task[object]]:
    return asyncio.all_tasks() - {asyncio.current_task()}


@pytest.fixture
async def silent_server() -> AsyncIterator[int]:
    """Accepts TCP connections but never speaks AMQP, so connecting hangs."""
    writers: list[asyncio.StreamWriter] = []

    async def accept(_: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writers.append(writer)

    server = await asyncio.start_server(accept, "127.0.0.1", 0)
    port: int = server.sockets[0].getsockname()[1]
    try:
        yield port
    finally:
        for writer in writers:
            writer.close()
        server.close()
        await server.wait_closed()


async def test_refused_connection_retries_until_stopped(env: pytest.MonkeyPatch) -> None:
    env.setenv("AMQP_URL", "amqp://guest:guest@127.0.0.1:1/")
    consumer = AmqpConsumer(Settings(), lambda _: None)
    stop = asyncio.Event()
    asyncio.get_running_loop().call_later(0.5, stop.set)
    assert await consumer.start(stop) is False
    await asyncio.sleep(0.05)
    assert other_tasks() == set()


async def test_cancel_during_hanging_connect_cleans_up(
    env: pytest.MonkeyPatch, silent_server: int
) -> None:
    env.setenv("AMQP_URL", f"amqp://guest:guest@127.0.0.1:{silent_server}/")
    consumer = AmqpConsumer(Settings(), lambda _: None)
    stop = asyncio.Event()
    asyncio.get_running_loop().call_later(0.5, stop.set)
    # Like app.run on SIGTERM: the attempt is cancelled mid-handshake.
    assert await until_stopped(consumer.start(stop), stop) is False
    await asyncio.sleep(0.05)
    assert other_tasks() == set()
