"""AMQP side: connect, declare the topology, consume into a callback.

The connection is an aio-pika *robust* connection. After a broker restart or a
network drop it reconnects by itself and restores the channel, QoS, exchange,
queues, bindings and consumers. Only the first connection attempt is retried
here, because aio-pika's reconnect logic takes over once connected.

Topology (all durable, so messages survive consumer downtime):

- Exchange mode (default): a ``topic`` exchange ``AMQP_EXCHANGE`` and one queue
  ``AMQP_QUEUE`` bound with each of ``AMQP_BINDING_KEYS``. rez needs
  ``exchange_name`` set to the same exchange.
- Default-exchange mode (``AMQP_EXCHANGE=""``, rez's default config): rez
  publishes to the default exchange with the routing key as the queue name, so
  queues named ``AMQP_DEFAULT_EXCHANGE_QUEUES`` are declared and consumed.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import socket
from collections.abc import Callable

import aio_pika
from aio_pika import RobustConnection
from aio_pika.abc import (
    AbstractIncomingMessage,
    AbstractQueue,
    AbstractRobustChannel,
    AbstractRobustConnection,
)
from aio_pika.connection import make_url
from aio_pika.exceptions import AMQPConnectionError, ChannelInvalidStateError

from rez_ctx_consumer.config import Settings

log = logging.getLogger(__name__)

CONNECT_RETRY_INITIAL_S = 1.0
CONNECT_RETRY_MAX_S = 30.0
CONNECT_TIMEOUT_S = 10.0


class AioPikaDelivery:
    """Adapts an aio-pika message to ``batcher.Delivery``."""

    __slots__ = ("_message",)

    def __init__(self, message: AbstractIncomingMessage) -> None:
        self._message = message

    @property
    def body(self) -> bytes:
        return self._message.body

    @property
    def routing_key(self) -> str:
        return self._message.routing_key or ""

    @property
    def channel_open(self) -> bool:
        try:
            return not self._message.channel.is_closed
        except ChannelInvalidStateError:
            return False

    async def ack(self) -> None:
        await self._message.ack()

    async def nack(self, requeue: bool) -> None:
        await self._message.nack(requeue=requeue)


class AmqpConsumer:
    def __init__(self, settings: Settings, on_delivery: Callable[[AioPikaDelivery], None]) -> None:
        self._settings = settings
        self._on_delivery = on_delivery
        self._connection: AbstractRobustConnection | None = None
        self._consumers: list[tuple[AbstractQueue, str]] = []

    @property
    def connected(self) -> bool:
        return self._connection is not None and not self._connection.is_closed

    async def start(self, stop: asyncio.Event) -> bool:
        """Connect (retrying until reachable), declare, start consuming.

        Returns False if ``stop`` was set before the broker became reachable.
        """
        delay = CONNECT_RETRY_INITIAL_S
        while not stop.is_set():
            try:
                self._connection = await self._connect_once()
                break
            except (AMQPConnectionError, OSError, TimeoutError) as exc:
                log.warning(
                    "broker not reachable, retrying",
                    extra={"error": f"{type(exc).__name__}: {exc}", "in_s": delay},
                )
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=delay)
                delay = min(delay * 2, CONNECT_RETRY_MAX_S)
        if self._connection is None:
            return False

        channel = await self._connection.channel()
        assert isinstance(channel, AbstractRobustChannel)
        await channel.set_qos(prefetch_count=self._settings.amqp_prefetch)
        for queue in await self._declare(channel):
            tag = await queue.consume(self._handle)
            self._consumers.append((queue, tag))
        log.info("consuming", extra={"queues": [q.name for q, _ in self._consumers]})
        return True

    async def _connect_once(self) -> AbstractRobustConnection:
        """One connection attempt that never leaves a reconnect loop behind.

        A RobustConnection starts a background reconnect task as soon as it
        connects. ``aio_pika.connect_robust()`` gives no handle to that task when
        the first attempt times out or is cancelled, so the task keeps retrying
        forever (and even survives event-loop shutdown). Holding the object
        means it can always be closed.
        """
        connection = RobustConnection(
            make_url(self._settings.amqp_url.get_secret_value()),
            client_properties={"connection_name": f"rez-ctx-consumer@{socket.gethostname()}"},
        )
        try:
            async with asyncio.timeout(CONNECT_TIMEOUT_S):
                await connection.connect(timeout=CONNECT_TIMEOUT_S)
        except BaseException:
            await connection.close()
            raise
        return connection

    async def stop_consuming(self) -> None:
        """Stop new deliveries; already received ones can still be acked."""
        for queue, tag in self._consumers:
            try:
                await queue.cancel(tag)
            except Exception:  # connection already gone
                log.debug("cancel failed", exc_info=True)
        self._consumers.clear()

    async def close(self) -> None:
        """Close the connection; the broker requeues anything still unacked."""
        if self._connection is not None:
            await self._connection.close()
            self._connection = None

    async def _declare(self, channel: AbstractRobustChannel) -> list[AbstractQueue]:
        s = self._settings
        if s.default_exchange_mode:
            return [
                await channel.declare_queue(name, durable=True)
                for name in s.amqp_default_exchange_queues
            ]
        exchange = await channel.declare_exchange(
            s.amqp_exchange, aio_pika.ExchangeType(s.amqp_exchange_type), durable=True
        )
        queue = await channel.declare_queue(s.amqp_queue, durable=True)
        for key in s.amqp_binding_keys:
            await queue.bind(exchange, routing_key=key)
        return [queue]

    async def _handle(self, message: AbstractIncomingMessage) -> None:
        self._on_delivery(AioPikaDelivery(message))
