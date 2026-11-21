"""Prometheus metrics, served on ``/metrics`` (see health.py).

Each ``Metrics`` instance has its own registry, so tests and several service
instances in one process don't collide.
"""

from __future__ import annotations

from collections.abc import Callable

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest

PREFIX = "rez_ctx_consumer"


class Metrics:
    def __init__(self) -> None:
        self.registry = CollectorRegistry()
        r = self.registry

        self.messages_received = Counter(
            f"{PREFIX}_messages_received", "Messages delivered by the broker", registry=r
        )
        self.events_stored = Counter(
            f"{PREFIX}_events_stored", "Rows written to rez_context_event", registry=r
        )
        self.messages_rejected = Counter(
            f"{PREFIX}_messages_rejected",
            "Messages written to rez_context_event_reject",
            ["stage"],  # parse: not a valid rez message; insert: refused by Postgres
            registry=r,
        )
        self.field_issues = Counter(
            f"{PREFIX}_field_issues",
            "Stored messages with fields that were NULLed or cleaned",
            registry=r,
        )
        self.store_errors = Counter(
            f"{PREFIX}_store_errors", "Failed attempts to store a batch (retried)", registry=r
        )
        self.dropped_closed_channel = Counter(
            f"{PREFIX}_dropped_closed_channel",
            "Messages dropped because their channel closed (the broker redelivers them)",
            registry=r,
        )
        self.ack_failures = Counter(
            f"{PREFIX}_ack_failures",
            "Stored messages that could not be acked (will be stored again)",
            registry=r,
        )
        self.partition_errors = Counter(
            f"{PREFIX}_partition_maintenance_errors",
            "Failed partition maintenance runs",
            registry=r,
        )
        self.batch_size = Histogram(
            f"{PREFIX}_batch_size",
            "Messages per stored batch",
            buckets=(1, 5, 10, 25, 50, 100, 200, 500, 1000),
            registry=r,
        )
        self.batch_seconds = Histogram(
            f"{PREFIX}_batch_store_seconds",
            "Time to store one batch, including retries",
            buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60),
            registry=r,
        )
        self.last_flush = Gauge(
            f"{PREFIX}_last_flush_timestamp_seconds",
            "Unix time of the last successfully stored batch",
            registry=r,
        )
        self.pending = Gauge(
            f"{PREFIX}_pending_messages", "Received messages not yet stored and acked", registry=r
        )
        self.amqp_connected = Gauge(
            f"{PREFIX}_amqp_connected", "1 while connected to the broker", registry=r
        )

    def bind(self, *, pending: Callable[[], float], amqp_connected: Callable[[], float]) -> None:
        """Read the live gauges from the running components at scrape time."""
        self.pending.set_function(pending)
        self.amqp_connected.set_function(amqp_connected)

    def render(self) -> bytes:
        return generate_latest(self.registry)
