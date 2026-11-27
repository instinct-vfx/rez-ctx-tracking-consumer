"""Parse real rez messages (see tests/fixtures/README.md)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from rez_ctx_consumer.payload import EventRow, parse_message

FIXTURES = Path(__file__).parent.parent / "fixtures"
ALL_FIXTURES = sorted(FIXTURES.glob("rez-*/*.json"))


def load(name: str, version: str = "3.5.0") -> dict[str, Any]:
    fixture: dict[str, Any] = json.loads((FIXTURES / f"rez-{version}" / f"{name}.json").read_text())
    return fixture


def parse(fixture: dict[str, Any], **kwargs: Any) -> EventRow:
    # rez publishes body=json.dumps(data)
    body = json.dumps(fixture["payload"]).encode()
    return parse_message(body, fixture["routing_key"], **kwargs)


def test_fixtures_present() -> None:
    assert len(ALL_FIXTURES) >= 5


@pytest.mark.parametrize("path", ALL_FIXTURES, ids=lambda p: f"{p.parent.name}/{p.stem}")
def test_every_fixture_parses_cleanly(path: Path) -> None:
    fixture = json.loads(path.read_text())
    row = parse(fixture)
    assert row.issues == ()
    assert row.action in ("created", "sourced")
    assert row.routing_key == f"REZ.CONTEXT.{row.action.upper()}"
    assert row.context == fixture["payload"]["context"]
    assert row.rez_version == fixture["rez_version"]


def test_created_default() -> None:
    received = datetime(2026, 11, 20, 12, 0, tzinfo=UTC)
    fixture = load("created_default")
    epoch = fixture["payload"]["context"]["timestamp"]
    assert isinstance(epoch, int)  # rez sends whole epoch seconds
    row = parse(fixture, received_at=received)

    assert row.received_at == received
    assert row.action == "created"
    assert row.host == "ws01.example.com"
    assert row.username == "artist"
    assert row.rez_version == "3.5.0"
    assert row.status == "solved"
    assert row.context_timestamp == datetime.fromtimestamp(epoch, tz=UTC)
    assert isinstance(row.solve_time, float)
    assert row.load_time == 0.0
    assert row.from_cache is False
    assert row.package_requests == ["foo"]
    assert row.implicit_packages is not None
    assert all(p.startswith("~") for p in row.implicit_packages)
    assert row.resolved_packages == ["bar-2.1.0", "baz-1.2", "foo-1.0.0"]
    assert row.extra == {}
    assert row.extra_columns == {}


def test_sourced_keeps_original_resolve_timestamp() -> None:
    created = parse(load("created_default"))
    sourced = parse(load("sourced_default"))
    assert sourced.action == "sourced"
    assert sourced.context_timestamp == created.context_timestamp
    assert sourced.resolved_packages == created.resolved_packages


def test_failed_resolve() -> None:
    row = parse(load("created_failed"))
    assert row.status == "failed"
    assert row.package_requests == ["foo", "!bar"]
    assert row.resolved_packages == []


def test_extra_fields_go_to_extra() -> None:
    row = parse(load("created_extra_fields"))
    assert row.extra == {"show": "demo"}  # rez itself dropped the unexpanded "site"
    assert row.extra_columns == {}


def test_extra_fields_mapped_to_columns() -> None:
    row = parse(load("created_extra_fields"), extra_field_columns={"show": "show_name"})
    assert row.extra == {}
    assert row.extra_columns == {"show_name": "demo"}


def test_mapped_extra_field_absent_from_message() -> None:
    row = parse(load("created_default"), extra_field_columns={"show": "show_name"})
    assert row.extra_columns == {}


def test_all_fields_promotes_same_columns() -> None:
    row = parse(load("created_all_fields"))
    assert row.status == "solved"
    assert row.resolved_packages == ["bar-2.1.0", "baz-1.2", "foo-1.0.0"]
    # Non-default context fields stay in the JSONB only.
    assert row.context["num_loaded_packages"] == 3
    assert "graph" in row.context
