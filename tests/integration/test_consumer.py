"""The whole service against real RabbitMQ and Postgres.

Messages are published the way rez publishes them (``rez/utils/amqp.py``):
body ``json.dumps(data)``, content type ``application/json``, to
``exchange_name`` with routing key ``{exchange_routing_key}.{ACTION}``.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import aio_pika
import psycopg
import pytest
from aio_pika.exceptions import ChannelClosed

from rez_ctx_consumer.app import run
from rez_ctx_consumer.config import Settings

from .conftest import Broker, start_rabbitmq

FIXTURES = sorted((Path(__file__).parent.parent / "fixtures").glob("rez-*/*.json"))


# --- helpers ------------------------------------------------------------------


async def eventually(condition: Callable[[], Any], timeout: float = 20) -> None:
    async with asyncio.timeout(timeout):
        while not await condition():
            await asyncio.sleep(0.1)


@asynccontextmanager
async def service(env: pytest.MonkeyPatch) -> AsyncIterator[asyncio.Task[None]]:
    """Run the consumer in the background until the block exits."""
    stop = asyncio.Event()
    task = asyncio.create_task(run(Settings(), stop))
    try:
        yield task
    finally:
        stop.set()
        await asyncio.wait_for(task, 30)


async def publish(url: str, exchange: str, routing_key: str, body: bytes | dict[str, Any]) -> None:
    if isinstance(body, dict):
        body = json.dumps(body).encode()
    connection = await aio_pika.connect(url)
    async with connection:
        channel = await connection.channel()
        target = await channel.get_exchange(exchange) if exchange else channel.default_exchange
        message = aio_pika.Message(
            body,
            content_type="application/json",
            content_encoding="utf-8",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )
        await target.publish(message, routing_key=routing_key)


async def queue_state(url: str, queue: str) -> tuple[int, int] | None:
    """(ready messages, consumers), or None if the queue doesn't exist."""
    connection = await aio_pika.connect(url)
    async with connection:
        channel = await connection.channel()
        try:
            q = await channel.declare_queue(queue, passive=True)
        except ChannelClosed:
            return None
        result = q.declaration_result
        return result.message_count or 0, result.consumer_count or 0


async def has_consumer(url: str, queue: str) -> bool:
    state = await queue_state(url, queue)
    return state is not None and state[1] > 0


async def count(dsn: str, table: str = "rez_context_event") -> int:
    async with await psycopg.AsyncConnection.connect(dsn) as conn:
        cur = await conn.execute(f"SELECT count(*) FROM {table}")
        row = await cur.fetchone()
        assert row is not None
        return int(row[0])


def fixture_messages() -> list[tuple[str, dict[str, Any]]]:
    out = []
    for path in FIXTURES:
        fixture = json.loads(path.read_text())
        out.append((fixture["routing_key"], fixture["payload"]))
    return out


@pytest.fixture
def names() -> dict[str, str]:
    suffix = uuid.uuid4().hex[:8]
    return {"exchange": f"rez.test.{suffix}", "queue": f"rez.test.{suffix}.tracking"}


@pytest.fixture
def consumer_env(
    env: pytest.MonkeyPatch, rabbitmq: Broker, database: str, names: dict[str, str]
) -> pytest.MonkeyPatch:
    env.setenv("AMQP_URL", rabbitmq.url)
    env.setenv("PG_DSN", database)
    env.setenv("AMQP_EXCHANGE", names["exchange"])
    env.setenv("AMQP_QUEUE", names["queue"])
    env.setenv("BATCH_MAX_WAIT_MS", "100")
    return env


# --- tests ----------------------------------------------------------------------


async def test_end_to_end(
    consumer_env: pytest.MonkeyPatch, rabbitmq: Broker, database: str, names: dict[str, str]
) -> None:
    messages = fixture_messages()
    async with service(consumer_env):
        await eventually(lambda: has_consumer(rabbitmq.url, names["queue"]))
        for routing_key, payload in messages:
            await publish(rabbitmq.url, names["exchange"], routing_key, payload)
        await publish(rabbitmq.url, names["exchange"], "REZ.CONTEXT.CREATED", b"garbage")
        # Not matching the REZ.CONTEXT.* binding: never reaches the queue.
        await publish(rabbitmq.url, names["exchange"], "OTHER.KEY", messages[0][1])

        await eventually(lambda: _equals(count(database), len(messages)))
        await eventually(lambda: _equals(count(database, "rez_context_event_reject"), 1))

    assert await queue_state(rabbitmq.url, names["queue"]) == (0, 0)  # all acked
    async with await psycopg.AsyncConnection.connect(database) as conn:
        cur = await conn.execute(
            "SELECT action, count(*) FROM rez_context_event GROUP BY action ORDER BY action"
        )
        assert await cur.fetchall() == [("created", len(messages) - 1), ("sourced", 1)]


async def test_default_exchange_mode(
    consumer_env: pytest.MonkeyPatch, rabbitmq: Broker, database: str, names: dict[str, str]
) -> None:
    # rez's default config: exchange_name '' and the routing key is the queue name.
    created, sourced = f"{names['queue']}.CREATED", f"{names['queue']}.SOURCED"
    consumer_env.setenv("AMQP_EXCHANGE", "")
    consumer_env.setenv("AMQP_DEFAULT_EXCHANGE_QUEUES", f"{created},{sourced}")
    payload = fixture_messages()[0][1]
    async with service(consumer_env):
        await eventually(lambda: has_consumer(rabbitmq.url, sourced))
        await publish(rabbitmq.url, "", created, payload)
        await publish(rabbitmq.url, "", sourced, payload | {"action": "sourced"})
        await eventually(lambda: _equals(count(database), 2))


async def test_messages_wait_while_consumer_is_down(
    consumer_env: pytest.MonkeyPatch, rabbitmq: Broker, database: str, names: dict[str, str]
) -> None:
    async with service(consumer_env):  # first start declares the durable queue
        await eventually(lambda: has_consumer(rabbitmq.url, names["queue"]))

    for routing_key, payload in fixture_messages():
        await publish(rabbitmq.url, names["exchange"], routing_key, payload)
    assert await queue_state(rabbitmq.url, names["queue"]) == (len(FIXTURES), 0)

    async with service(consumer_env):
        await eventually(lambda: _equals(count(database), len(FIXTURES)))


async def test_shutdown_stores_unflushed_batch(
    consumer_env: pytest.MonkeyPatch, rabbitmq: Broker, database: str, names: dict[str, str]
) -> None:
    consumer_env.setenv("BATCH_MAX_WAIT_MS", "600000")  # never flush on time
    async with service(consumer_env):
        await eventually(lambda: has_consumer(rabbitmq.url, names["queue"]))
        for routing_key, payload in fixture_messages():
            await publish(rabbitmq.url, names["exchange"], routing_key, payload)
        # Delivered to the consumer (no longer "ready") but not yet stored.
        await eventually(lambda: _equals(_ready(rabbitmq.url, names["queue"]), 0))
        assert await count(database) == 0
    assert await count(database) == len(FIXTURES)
    assert await queue_state(rabbitmq.url, names["queue"]) == (0, 0)


async def test_database_outage(
    consumer_env: pytest.MonkeyPatch,
    rabbitmq: Broker,
    postgres: str,
    database: str,
    names: dict[str, str],
) -> None:
    dbname = psycopg.conninfo.conninfo_to_dict(database)["dbname"]
    async with service(consumer_env):
        await eventually(lambda: has_consumer(rabbitmq.url, names["queue"]))
        await _set_db_available(postgres, str(dbname), False)
        try:
            for routing_key, payload in fixture_messages():
                await publish(rabbitmq.url, names["exchange"], routing_key, payload)
            await asyncio.sleep(2)  # consumer is retrying now
        finally:
            await _set_db_available(postgres, str(dbname), True)
        await eventually(lambda: _equals(count(database), len(FIXTURES)), timeout=60)
    assert await queue_state(rabbitmq.url, names["queue"]) == (0, 0)


async def test_broker_restart(env: pytest.MonkeyPatch, database: str) -> None:
    payload = fixture_messages()[0][1]
    with start_rabbitmq() as broker:  # own broker: restarting it must not affect other tests
        env.setenv("AMQP_URL", broker.url)
        env.setenv("PG_DSN", database)
        env.setenv("BATCH_MAX_WAIT_MS", "100")
        async with service(env):
            await eventually(lambda: has_consumer(broker.url, "rez.context.tracking"))
            await publish(broker.url, "rez.context", "REZ.CONTEXT.CREATED", payload)
            await eventually(lambda: _equals(count(database), 1))

            await asyncio.to_thread(broker.restart)

            # aio-pika reconnects and restores exchange, queue, binding and consumer.
            await eventually(lambda: has_consumer(broker.url, "rez.context.tracking"), timeout=60)
            await publish(broker.url, "rez.context", "REZ.CONTEXT.CREATED", payload)
            await eventually(lambda: _equals(count(database), 2))


async def test_service_waits_for_broker(env: pytest.MonkeyPatch, database: str) -> None:
    """Started before the broker is up, the service connects once it is."""
    with start_rabbitmq() as broker:
        url = broker.url
        broker.container.get_wrapped_container().stop(timeout=10)
        env.setenv("AMQP_URL", url)
        env.setenv("PG_DSN", database)
        async with service(env) as task:
            await asyncio.sleep(2)
            assert not task.done()
            await asyncio.to_thread(broker.restart)
            await eventually(lambda: has_consumer(url, "rez.context.tracking"), timeout=60)


# --- small async predicates ----------------------------------------------------------


async def _equals(value: Any, expected: Any) -> bool:
    return bool(await value == expected)


async def _ready(url: str, queue: str) -> int | None:
    state = await queue_state(url, queue)
    return None if state is None else state[0]


async def _set_db_available(admin_dsn: str, dbname: str, available: bool) -> None:
    async with await psycopg.AsyncConnection.connect(admin_dsn, autocommit=True) as conn:
        await conn.execute(
            psycopg.sql.SQL("ALTER DATABASE {} WITH ALLOW_CONNECTIONS {}").format(
                psycopg.sql.Identifier(dbname), psycopg.sql.SQL("true" if available else "false")
            )
        )
        if not available:
            await conn.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s",
                (dbname,),
            )
