"""Apply the numbered SQL files in ``rez_ctx_consumer/sql/`` once each.

Files are named ``NNNN_description.sql`` and numbered 1..N without gaps. Each
run takes a transaction-scoped advisory lock, so replicas starting at the same
time apply every migration exactly once. A checksum of every applied file is
recorded, and editing an already applied migration is treated as an error.
"""

from __future__ import annotations

import hashlib
import logging
import re
from collections.abc import Sequence
from dataclasses import dataclass
from importlib.resources import files
from importlib.resources.abc import Traversable
from typing import Any, Final

from psycopg import AsyncConnection

log = logging.getLogger(__name__)

MIGRATIONS_TABLE: Final = "rez_ctx_consumer_migrations"
# Arbitrary app-specific key for pg_advisory_xact_lock.
MIGRATION_LOCK_ID: Final = 0x72657A_0001

_FILE_RE = re.compile(r"^(\d{4})_([a-z0-9_]+)\.sql$")


class MigrationError(RuntimeError):
    """The migration files or the recorded history are inconsistent."""


@dataclass(frozen=True, slots=True)
class Migration:
    version: int
    name: str
    sql: str

    @property
    def checksum(self) -> str:
        return hashlib.sha256(self.sql.encode()).hexdigest()


def discover(root: Traversable | None = None) -> list[Migration]:
    root = root or files("rez_ctx_consumer") / "sql"
    found: list[Migration] = []
    for entry in root.iterdir():
        if not entry.name.endswith(".sql"):
            continue
        match = _FILE_RE.match(entry.name)
        if match is None:
            raise MigrationError(f"migration file name must be NNNN_name.sql: {entry.name}")
        # Normalise line endings so a git checkout with autocrlf hashes the same.
        sql = entry.read_text(encoding="utf-8").replace("\r\n", "\n")
        found.append(Migration(int(match[1]), match[2], sql))

    found.sort(key=lambda m: m.version)
    versions = [m.version for m in found]
    if versions != list(range(1, len(found) + 1)):
        raise MigrationError(f"migration versions must be 1..N without gaps, got {versions}")
    return found


async def migrate(
    conn: AsyncConnection[Any], migrations: Sequence[Migration] | None = None
) -> list[int]:
    """Apply pending migrations in one transaction. Returns the versions applied."""
    if migrations is None:
        migrations = discover()
    applied_now: list[int] = []

    async with conn.transaction():
        await conn.execute("SELECT pg_advisory_xact_lock(%s)", (MIGRATION_LOCK_ID,))
        await conn.execute(
            f"""CREATE TABLE IF NOT EXISTS {MIGRATIONS_TABLE} (
                version     integer PRIMARY KEY,
                name        text NOT NULL,
                checksum    text NOT NULL,
                applied_at  timestamptz NOT NULL DEFAULT now()
            )"""
        )
        cur = await conn.execute(f"SELECT version, checksum FROM {MIGRATIONS_TABLE}")
        applied: dict[int, str] = {version: checksum for version, checksum in await cur.fetchall()}

        newer = sorted(set(applied) - {m.version for m in migrations})
        if newer:
            # E.g. a rollback to an older image. Additive migrations stay compatible,
            # so warn instead of refusing to start.
            log.warning("database has migrations this version doesn't know", extra={"newer": newer})

        for migration in migrations:
            if migration.version in applied:
                if applied[migration.version] != migration.checksum:
                    raise MigrationError(
                        f"migration {migration.version:04d}_{migration.name} was changed "
                        "after it was applied; add a new migration instead"
                    )
                continue
            log.info("applying migration", extra={"version": migration.version})
            # Multi-statement script; only allowed without parameters.
            await conn.execute(migration.sql.encode())
            await conn.execute(
                f"INSERT INTO {MIGRATIONS_TABLE} (version, name, checksum) VALUES (%s, %s, %s)",
                (migration.version, migration.name, migration.checksum),
            )
            applied_now.append(migration.version)

    return applied_now
