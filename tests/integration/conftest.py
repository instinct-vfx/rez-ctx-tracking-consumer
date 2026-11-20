"""Fixtures backed by a real Postgres in Docker (testcontainers).

One container per test session; every test gets its own fresh database.
``POSTGRES_IMAGE`` selects the server version (default ``postgres:17``).
Without Docker the tests are skipped locally but fail on CI (``CI`` set).
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator, Iterator
from typing import Any

import psycopg
import pytest
from psycopg import AsyncConnection, sql
from psycopg.conninfo import make_conninfo
from psycopg_pool import AsyncConnectionPool

from rez_ctx_consumer.db import Pool
from rez_ctx_consumer.migrations import migrate
from rez_ctx_consumer.partitions import ensure_partitions

POSTGRES_IMAGE = os.environ.get("POSTGRES_IMAGE", "postgres:17")


@pytest.fixture(scope="session")
def postgres() -> Iterator[str]:
    """Admin DSN of the session's Postgres container."""
    from docker.errors import DockerException
    from testcontainers.community.postgres import PostgresContainer

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
