"""Tiny HTTP server for ``/healthz`` and ``/metrics``.

Plain asyncio rather than a web framework: two read-only endpoints don't need
one. ``/healthz`` answers 200 when the service is doing its job and 503
otherwise, with a JSON body explaining why. That means the service is ready,
connected to the broker, and storing batches hasn't been failing for longer
than ``store_stall_s``. An idle consumer is healthy: having no traffic is not
an error.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import Callable
from dataclasses import asdict, dataclass

from prometheus_client import CONTENT_TYPE_LATEST

from rez_ctx_consumer.metrics import Metrics

log = logging.getLogger(__name__)

HEALTH_PATH = "/healthz"
METRICS_PATH = "/metrics"
_MAX_REQUEST_BYTES = 8192
_READ_TIMEOUT_S = 5.0


@dataclass(frozen=True, slots=True)
class HealthStatus:
    ready: bool
    amqp_connected: bool
    store_failing_s: float | None  # how long the current batch has failed to store
    pending: int

    def healthy(self, store_stall_s: float) -> bool:
        stalled = self.store_failing_s is not None and self.store_failing_s >= store_stall_s
        return self.ready and self.amqp_connected and not stalled


class HealthServer:
    def __init__(
        self,
        port: int,
        metrics: Metrics,
        status: Callable[[], HealthStatus],
        *,
        host: str = "0.0.0.0",  # all interfaces: must be reachable from outside the container
        store_stall_s: float = 60.0,
    ) -> None:
        self._host = host
        self._port = port
        self._metrics = metrics
        self._status = status
        self._store_stall_s = store_stall_s
        self._server: asyncio.Server | None = None

    @property
    def port(self) -> int:
        """The bound port (useful when started with port 0)."""
        assert self._server is not None
        port: int = self._server.sockets[0].getsockname()[1]
        return port

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle, self._host, self._port)
        log.info("health server listening", extra={"port": self.port})

    async def close(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            async with asyncio.timeout(_READ_TIMEOUT_S):
                head = await reader.readuntil(b"\r\n\r\n")
            status, content_type, body = self._respond(head)
        except (asyncio.IncompleteReadError, asyncio.LimitOverrunError, TimeoutError):
            status, content_type, body = 400, "text/plain", b"bad request\n"
        except Exception:
            log.exception("health endpoint failed")
            status, content_type, body = 500, "text/plain", b"internal error\n"
        reason = {200: "OK", 400: "Bad Request", 404: "Not Found", 405: "Method Not Allowed",
                  500: "Internal Server Error", 503: "Service Unavailable"}[status]  # fmt: skip
        writer.write(
            f"HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\n"
            f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode()
            + body
        )
        with contextlib.suppress(ConnectionError):
            await writer.drain()
            writer.close()
            await writer.wait_closed()

    def _respond(self, head: bytes) -> tuple[int, str, bytes]:
        if len(head) > _MAX_REQUEST_BYTES:
            return 400, "text/plain", b"bad request\n"
        parts = head.split(b"\r\n", 1)[0].decode("latin-1").split()
        if len(parts) != 3:
            return 400, "text/plain", b"bad request\n"
        method, target, _ = parts
        path = target.split("?", 1)[0]
        if path not in (HEALTH_PATH, METRICS_PATH):
            return 404, "text/plain", b"not found\n"
        if method != "GET":
            return 405, "text/plain", b"method not allowed\n"
        if path == METRICS_PATH:
            return 200, CONTENT_TYPE_LATEST, self._metrics.render()
        status = self._status()
        healthy = status.healthy(self._store_stall_s)
        body = json.dumps({"healthy": healthy, **asdict(status)}).encode() + b"\n"
        return (200 if healthy else 503), "application/json", body
