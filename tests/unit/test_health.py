from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass

import pytest

from rez_ctx_consumer.__main__ import healthcheck, main
from rez_ctx_consumer.health import HealthServer, HealthStatus
from rez_ctx_consumer.metrics import Metrics

HEALTHY = HealthStatus(ready=True, amqp_connected=True, store_failing_s=None, pending=0)


@dataclass
class Holder:
    status: HealthStatus = HEALTHY


@pytest.fixture
async def server() -> AsyncIterator[tuple[HealthServer, Holder, Metrics]]:
    holder, metrics = Holder(), Metrics()
    srv = HealthServer(0, metrics, lambda: holder.status, host="127.0.0.1", store_stall_s=60)
    await srv.start()
    try:
        yield srv, holder, metrics
    finally:
        await srv.close()


async def request(port: int, raw: bytes) -> tuple[int, bytes]:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(raw)
    await writer.drain()
    response = await reader.read()
    writer.close()
    head, _, body = response.partition(b"\r\n\r\n")
    return int(head.split()[1]), body


def get(path: str) -> bytes:
    return f"GET {path} HTTP/1.1\r\nHost: x\r\n\r\n".encode()


@pytest.mark.parametrize(
    ("status", "healthy"),
    [
        (HEALTHY, True),
        (HealthStatus(ready=True, amqp_connected=True, store_failing_s=59, pending=5), True),
        (HealthStatus(ready=False, amqp_connected=False, store_failing_s=None, pending=0), False),
        (HealthStatus(ready=True, amqp_connected=False, store_failing_s=None, pending=0), False),
        (HealthStatus(ready=True, amqp_connected=True, store_failing_s=60, pending=5), False),
    ],
)
def test_healthy_rules(status: HealthStatus, healthy: bool) -> None:
    assert status.healthy(store_stall_s=60) is healthy


async def test_healthz(server: tuple[HealthServer, Holder, Metrics]) -> None:
    srv, holder, _ = server
    code, body = await request(srv.port, get("/healthz"))
    assert code == 200
    assert b'"healthy": true' in body

    holder.status = HealthStatus(ready=True, amqp_connected=False, store_failing_s=None, pending=3)
    code, body = await request(srv.port, get("/healthz?verbose=1"))
    assert code == 503
    assert b'"amqp_connected": false' in body


async def test_metrics(server: tuple[HealthServer, Holder, Metrics]) -> None:
    srv, _, metrics = server
    metrics.events_stored.inc(7)
    code, body = await request(srv.port, get("/metrics"))
    assert code == 200
    assert b"rez_ctx_consumer_events_stored_total 7.0" in body


@pytest.mark.parametrize(
    ("raw", "code"),
    [
        (get("/nope"), 404),
        (b"POST /healthz HTTP/1.1\r\n\r\n", 405),
        (b"garbage\r\n\r\n", 400),
        (b"GET /healthz", 400),  # incomplete request, then EOF
    ],
)
async def test_bad_requests(
    server: tuple[HealthServer, Holder, Metrics], raw: bytes, code: int
) -> None:
    srv, _, _ = server
    reader, writer = await asyncio.open_connection("127.0.0.1", srv.port)
    writer.write(raw)
    await writer.drain()
    writer.write_eof()
    response = await reader.read()
    writer.close()
    assert int(response.split()[1]) == code


async def test_healthcheck_command(
    server: tuple[HealthServer, Holder, Metrics], monkeypatch: pytest.MonkeyPatch
) -> None:
    srv, holder, _ = server
    monkeypatch.setenv("METRICS_PORT", str(srv.port))
    assert await asyncio.to_thread(healthcheck) == 0
    holder.status = HealthStatus(ready=False, amqp_connected=False, store_failing_s=None, pending=0)
    assert await asyncio.to_thread(healthcheck) == 1


def test_healthcheck_without_server(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("METRICS_PORT", "1")
    assert healthcheck(timeout_s=1) == 1
    monkeypatch.setenv("METRICS_PORT", "0")
    assert healthcheck() == 1


def test_unknown_command() -> None:
    assert main(["frobnicate"]) == 2
