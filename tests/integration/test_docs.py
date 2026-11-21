"""Every ```sql block in docs/queries.md runs and returns rows on real rez data."""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from rez_ctx_consumer.db import EventStore, Pool, Reject
from rez_ctx_consumer.payload import parse_message

DOCS = Path(__file__).parents[2] / "docs" / "queries.md"
FIXTURES = sorted((Path(__file__).parents[1] / "fixtures").glob("rez-*/*.json"))
SQL_BLOCK = re.compile(r"```sql\n(.*?)```", re.DOTALL)
QUERIES = SQL_BLOCK.findall(DOCS.read_text(encoding="utf-8"))


def test_docs_have_queries() -> None:
    assert len(QUERIES) >= 10


@pytest.fixture
async def populated(migrated: Pool) -> Pool:
    events = []
    for path in FIXTURES:
        fixture = json.loads(path.read_text())
        body = json.dumps(fixture["payload"]).encode()
        events.append((parse_message(body, fixture["routing_key"]), body))
    # A package last used 120 days ago, for the "unused packages" query.
    old = {
        "action": "created",
        "user": "artist",
        "host": "ws02",
        "rez_version": "3.4.0",
        "context": {
            "status": "solved",
            "package_requests": ["oldtool"],
            "resolved_packages": [{"variables": {"name": "oldtool", "version": "0.9"}}],
        },
    }
    body = json.dumps(old).encode()
    received = datetime.now(UTC) - timedelta(days=120)
    events.append((parse_message(body, "REZ.CONTEXT.CREATED", received_at=received), body))
    reject = Reject(datetime.now(UTC), "REZ.CONTEXT.CREATED", b"\xffnot json", "not valid JSON")
    await EventStore(migrated).store(events, [reject])
    return migrated


@pytest.mark.parametrize(
    "query", QUERIES, ids=[f"q{i:02d}-{q.split()[1][:20]}" for i, q in enumerate(QUERIES)]
)
async def test_query_runs_and_returns_rows(populated: Pool, query: str) -> None:
    async with populated.connection() as conn:
        cur = await conn.execute(query.encode())
        rows = await cur.fetchall()
    # Every example is written against values present in the fixtures, so an
    # empty result means the query (or the fixture data) changed meaning.
    assert rows, query
