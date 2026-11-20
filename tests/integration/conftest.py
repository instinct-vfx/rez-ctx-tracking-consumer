"""Fixtures backed by a real Postgres in Docker (testcontainers).

One Postgres and one RabbitMQ container per test session. Every test gets its
own fresh database (and picks its own exchange/queue names).
``POSTGRES_IMAGE`` / ``RABBITMQ_IMAGE`` select the server versions.
Without Docker the tests are skipped locally but fail on CI (``CI`` set).
"""

from __future__ import annotations

import os
import socket
import time
import uuid
from collections.abc import AsyncIterator, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

import psycopg
import pytest
from docker.errors import DockerException
from psycopg import AsyncConnection, sql
from psycopg.conninfo import make_conninfo
from psycopg_pool import AsyncConnectionPool
from testcontainers.community.postgres import PostgresContainer
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

from rez_ctx_consumer.db import Pool
from rez_ctx_consumer.migrations import migrate
from rez_ctx_consumer.partitions import ensure_partitions

POSTGRES_IMAGE = os.environ.get("POSTGRES_IMAGE", "postgres:17")
RABBITMQ_IMAGE = os.environ.get("RABBITMQ_IMAGE", "rabbitmq:4")
_RABBIT_READY = "Server startup complete"


@pytest.fixture(scope="session")
def postgres() -> Iterator[str]:
    """Admin DSN of the session's Postgres container."""
    container = PostgresContainer(POSTGRES_IMAGE, driver=None)
    try:
        container.start()
    except DockerException as exc:
        if os.environ.get("CI"):
            raise
        pytest.skip(f"Docker not available: {exc}")
    try:
        yield container.get_connection_url()
    finally:
        container.stop()


@dataclass
class Broker:
    container: DockerContainer
    url: str

    def restart(self) -> None:
        """Restart the broker process (container keeps its data and port)."""
        self.container.get_wrapped_container().restart(timeout=10)
        deadline = time.monotonic() + 60
        while _startups(self.container) < 2:
            if time.monotonic() > deadline:
                raise TimeoutError("RabbitMQ did not come back after restart")
            time.sleep(0.2)


def _startups(container: DockerContainer) -> int:
    stdout, _ = container.get_logs()
    return stdout.decode(errors="replace").count(_RABBIT_READY)


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port: int = s.getsockname()[1]
        return port


@contextmanager
def start_rabbitmq() -> Iterator[Broker]:
    # A fixed host port, because Docker may pick a new random port on restart.
    port = _free_port()
    container = (
        DockerContainer(RABBITMQ_IMAGE)
        .with_bind_ports(5672, port)
        .waiting_for(LogMessageWaitStrategy(_RABBIT_READY))
    )
    try:
        container.start()
    except DockerException as exc:
        if os.environ.get("CI"):
            raise
        pytest.skip(f"Docker not available: {exc}")
    try:
        yield Broker(container, f"amqp://guest:guest@{container.get_container_host_ip()}:{port}/")
    finally:
        container.stop()


@pytest.fixture(scope="session")
def rabbitmq() -> Iterator[Broker]:
    """A RabbitMQ shared by the session; tests use their own exchange/queue names."""
    with start_rabbitmq() as broker:
        yield broker


@pytest.fixture
def database(postgres: str) -> Iterator[str]:
    """DSN of a new, empty database, dropped after the test."""
    name = f"t_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(postgres, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    try:
        yield make_conninfo(postgres, dbname=name)
    finally:
        with psycopg.connect(postgres, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name))
            )


@pytest.fixture
async def conn(database: str) -> AsyncIterator[AsyncConnection[Any]]:
    async with await AsyncConnection.connect(database, autocommit=True) as connection:
        yield connection


@pytest.fixture
async def pool(database: str) -> AsyncIterator[Pool]:
    p: Pool = AsyncConnectionPool(database, min_size=1, max_size=3, open=False)
    await p.open(wait=True)
    try:
        yield p
    finally:
        await p.close()


@pytest.fixture
async def migrated(pool: Pool) -> Pool:
    """Pool on a database with the schema and current partitions in place."""
    async with pool.connection() as c:
        await migrate(c)
        await ensure_partitions(c)
    return pool
