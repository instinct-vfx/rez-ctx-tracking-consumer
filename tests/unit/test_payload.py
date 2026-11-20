"""Edge cases for payload parsing, beyond what real rez produces today."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import pytest

from rez_ctx_consumer.payload import (
    EVENT_COLUMNS,
    EventRow,
    PayloadError,
    parse_message,
)

ROUTING_KEY = "REZ.CONTEXT.CREATED"


def message(**overrides: Any) -> dict[str, Any]:
    msg: dict[str, Any] = {
        "action": "created",
        "host": "ws01",
        "user": "artist",
        "rez_version": "3.5.0",
        "context": {
            "status": "solved",
            "timestamp": 1700000000,
            "solve_time": 0.25,
            "load_time": 0.5,
            "from_cache": True,
            "package_requests": ["foo-1"],
            "implicit_packages": ["~platform==linux"],
            "resolved_packages": [
                {"key": "filesystem.variant", "variables": {"name": "foo", "version": "1.0"}}
            ],
        },
    }
    msg.update(overrides)
    return msg


def with_context(**fields: Any) -> dict[str, Any]:
    msg = message()
    msg["context"].update(fields)
    return msg


def parse(msg: dict[str, Any] | bytes, **kwargs: Any) -> EventRow:
    body = msg if isinstance(msg, bytes) else json.dumps(msg).encode()
    return parse_message(body, ROUTING_KEY, **kwargs)


def test_row_fields_cover_table_columns() -> None:
    # Guards against EventRow and the column list drifting apart.
    assert set(EVENT_COLUMNS) <= set(EventRow.__dataclass_fields__)


def test_happy_path() -> None:
    row = parse(message())
    assert row.issues == ()
    assert row.status == "solved"
    assert row.context_timestamp == datetime(2023, 11, 14, 22, 13, 20, tzinfo=UTC)
    assert row.solve_time == 0.25
    assert row.from_cache is True
    assert row.resolved_packages == ["foo-1.0"]


def test_received_at_defaults_to_now() -> None:
    before = datetime.now(UTC)
    row = parse(message())
    assert before <= row.received_at <= datetime.now(UTC)


def test_int_durations_become_float() -> None:
    row = parse(with_context(solve_time=0, load_time=2))
    assert row.solve_time == 0.0
    assert isinstance(row.load_time, float)


def test_float_timestamp() -> None:
    row = parse(with_context(timestamp=1700000000.5))
    assert row.context_timestamp == datetime(2023, 11, 14, 22, 13, 20, 500000, tzinfo=UTC)


def test_unversioned_package() -> None:
    row = parse(with_context(resolved_packages=[{"variables": {"name": "tool", "version": ""}}]))
    assert row.resolved_packages == ["tool"]


# --- rejected messages -----------------------------------------------------------


@pytest.mark.parametrize(
    ("body", "reason"),
    [
        (b"\xff\xfe", "UTF-8"),
        (b"{not json", "JSON"),
        (b"", "JSON"),
        (b"[1, 2]", "JSON object"),
        (b'"text"', "JSON object"),
        (json.dumps({"context": {}}).encode(), "action"),
        (json.dumps({"action": "", "context": {}}).encode(), "action"),
        (json.dumps({"action": 5, "context": {}}).encode(), "action"),
        (json.dumps({"action": "created"}).encode(), "context"),
        (json.dumps({"action": "created", "context": None}).encode(), "context"),
        (json.dumps({"action": "created", "context": []}).encode(), "context"),
    ],
)
def test_rejected(body: bytes, reason: str) -> None:
    with pytest.raises(PayloadError, match=reason):
        parse(body)


def test_minimal_message() -> None:
    row = parse({"action": "sourced", "context": {}})
    assert row.issues == ()
    assert row.action == "sourced"
    for column in ("host", "username", "rez_version", "status", "context_timestamp"):
        assert getattr(row, column) is None
    assert row.resolved_packages is None


def test_unknown_action_is_accepted() -> None:
    # A future rez may add actions; store them rather than reject.
    assert parse(message(action="bundled")).action == "bundled"


# --- lenient fields ---------------------------------------------------------------


def test_nulls_are_not_issues() -> None:
    row = parse(
        with_context(
            status=None,
            timestamp=None,
            solve_time=None,
            load_time=None,
            from_cache=None,
            package_requests=None,
            implicit_packages=None,
            resolved_packages=None,
        )
        | {"host": None, "user": None, "rez_version": None}
    )
    assert row.issues == ()
    for column in ("status", "context_timestamp", "solve_time", "from_cache", "host"):
        assert getattr(row, column) is None


@pytest.mark.parametrize(
    ("field", "value", "column"),
    [
        ("status", 3, "status"),
        ("timestamp", "2026-01-01", "context_timestamp"),
        ("timestamp", True, "context_timestamp"),
        ("timestamp", 10**20, "context_timestamp"),
        ("solve_time", "fast", "solve_time"),
        ("solve_time", False, "solve_time"),
        ("load_time", [1], "load_time"),
        ("from_cache", "yes", "from_cache"),
        ("from_cache", 1, "from_cache"),
        ("package_requests", "foo", "package_requests"),
        ("package_requests", ["foo", 1], "package_requests"),
        ("implicit_packages", {}, "implicit_packages"),
        ("resolved_packages", "foo-1", "resolved_packages"),
        ("resolved_packages", ["foo-1"], "resolved_packages"),
        ("resolved_packages", [{"key": "x"}], "resolved_packages"),
        ("resolved_packages", [{"variables": {"name": "", "version": "1"}}], "resolved_packages"),
        ("resolved_packages", [{"variables": {"name": "a", "version": 1}}], "resolved_packages"),
    ],
)
def test_bad_context_field_becomes_null(field: str, value: Any, column: str) -> None:
    msg = with_context(**{field: value})
    row = parse(msg)
    assert getattr(row, column) is None
    assert len(row.issues) == 1
    assert field in row.issues[0]
    # The original value is still stored in the context JSONB.
    assert row.context[field] == value


def test_partially_bad_resolved_packages_is_null_not_partial() -> None:
    good = {"variables": {"name": "foo", "version": "1.0"}}
    row = parse(with_context(resolved_packages=[good, {"variables": {}}]))
    assert row.resolved_packages is None
    assert "resolved_packages[1]" in row.issues[0]


@pytest.mark.parametrize(("key", "column"), [("host", "host"), ("user", "username")])
def test_bad_top_level_field_moves_to_extra(key: str, column: str) -> None:
    row = parse(message(**{key: ["a", "b"]}))
    assert getattr(row, column) is None
    assert row.extra == {key: ["a", "b"]}
    assert key in row.issues[0]


def test_nan_and_infinity_replaced() -> None:
    body = (
        b'{"action": "created", "context": {"solve_time": NaN, "load_time": Infinity,'
        b' "nested": [-Infinity]}}'
    )
    row = parse(body)
    assert row.solve_time is None
    assert row.load_time is None
    assert row.context == {"solve_time": None, "load_time": None, "nested": [None]}
    assert len(row.issues) == 3


# --- character cleaning ---------------------------------------------------------------


def test_nul_characters_removed_everywhere() -> None:
    msg = message(host="ws\x0001", show="de\x00mo")
    msg["context"]["status"] = "sol\x00ved"
    msg["context"]["key\x00"] = ["a\x00b"]
    row = parse(msg)
    assert row.host == "ws01"
    assert row.status == "solved"
    assert row.extra == {"show": "demo"}
    assert row.context["key"] == ["ab"]
    assert row.issues == ("removed NUL characters / replaced invalid surrogates",)


def test_lone_surrogates_replaced() -> None:
    # What json.dumps produces for a path decoded with surrogateescape.
    body = b'{"action": "created", "context": {"path": "/mnt/caf\\udce9"}}'
    row = parse(body)
    assert row.context["path"] == "/mnt/caf�"
    assert len(row.issues) == 1


def test_valid_surrogate_pairs_kept() -> None:
    body = b'{"action": "created", "context": {"note": "\\ud83c\\udfac"}}'
    row = parse(body)
    assert row.context["note"] == "\N{CLAPPER BOARD}"
    assert row.issues == ()


def test_non_ascii_utf8_body() -> None:
    body = json.dumps(message(user="jürgen"), ensure_ascii=False).encode()
    assert parse(body).username == "jürgen"
