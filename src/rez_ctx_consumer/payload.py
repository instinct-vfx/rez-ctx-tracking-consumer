"""Turn a rez context-tracking message into a row for ``rez_context_event``.

rez publishes JSON like::

    {"action": "created", "host": "...", "user": "...", "rez_version": "3.5.0",
     "context": {"status": "solved", "timestamp": 1795214613, ...},
     <context_tracking_extra_fields...>}

Parsing is strict only about what makes a message meaningless: the body must
be a JSON object with a non-empty string ``action`` and an object ``context``.
Everything else is lenient. A field that is missing or JSON ``null`` becomes
NULL. A field with an unexpected type also becomes NULL and is reported in
``EventRow.issues``. The full ``context`` is always stored unchanged, so nothing
is lost when a column can't be filled.
"""

from __future__ import annotations

import json
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Final

# Top-level keys rez itself sets; everything else comes from
# ``context_tracking_extra_fields``.
ACTION_KEY: Final = "action"
CONTEXT_KEY: Final = "context"
TOP_LEVEL_COLUMNS: Final[Mapping[str, str]] = {
    "host": "host",
    "user": "username",
    "rez_version": "rez_version",
}
RESERVED_PAYLOAD_KEYS: Final = frozenset({ACTION_KEY, CONTEXT_KEY, *TOP_LEVEL_COLUMNS})

# Every column of rez_context_event that the consumer fills in.
EVENT_COLUMNS: Final = (
    "received_at",
    "routing_key",
    "action",
    "host",
    "username",
    "rez_version",
    "status",
    "context_timestamp",
    "solve_time",
    "load_time",
    "from_cache",
    "package_requests",
    "implicit_packages",
    "resolved_packages",
    "context",
    "extra",
)
# Columns that EXTRA_FIELD_COLUMNS must not map onto.
RESERVED_COLUMNS: Final = frozenset({"id", *EVENT_COLUMNS})

# NUL is not allowed in Postgres text/jsonb. Lone UTF-16 surrogates (e.g. from
# paths decoded with surrogateescape on the publishing host) cannot be encoded
# as UTF-8. json.loads already joins valid surrogate pairs, so any surrogate
# left in a str is a lone one.
_INVALID_CHARS = re.compile("[\x00\ud800-\udfff]")


def clean_text(text: str) -> str:
    """Make a string storable in Postgres text: drop NUL, replace lone surrogates."""
    if _INVALID_CHARS.search(text) is None:
        return text
    return _INVALID_CHARS.sub(lambda m: "" if m.group() == "\x00" else "�", text)


class PayloadError(ValueError):
    """The message can't be stored as an event; it goes to the rejects table."""


@dataclass(frozen=True, slots=True)
class EventRow:
    received_at: datetime
    routing_key: str
    action: str
    host: str | None
    username: str | None
    rez_version: str | None
    status: str | None
    context_timestamp: datetime | None
    solve_time: float | None
    load_time: float | None
    from_cache: bool | None
    package_requests: list[str] | None
    implicit_packages: list[str] | None
    resolved_packages: list[str] | None
    context: dict[str, Any]
    extra: dict[str, Any]
    # Extra fields mapped to their own columns via EXTRA_FIELD_COLUMNS: column -> value.
    extra_columns: dict[str, Any]
    # Problems that were worked around (value stored as NULL, characters replaced).
    issues: tuple[str, ...] = ()


def parse_message(
    body: bytes,
    routing_key: str,
    *,
    received_at: datetime | None = None,
    extra_field_columns: Mapping[str, str] | None = None,
) -> EventRow:
    """Parse one message body. Raises PayloadError if it can't be stored at all."""
    issues: list[str] = []

    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise PayloadError(f"body is not valid UTF-8: {exc}") from exc
    try:
        data = json.loads(text, parse_constant=_reject_constant(issues))
    except json.JSONDecodeError as exc:
        raise PayloadError(f"body is not valid JSON: {exc}") from exc

    if not isinstance(data, dict):
        raise PayloadError(f"body must be a JSON object, got {type(data).__name__}")

    data = _clean(data, issues)

    action = data.pop(ACTION_KEY, None)
    if not isinstance(action, str) or not action:
        raise PayloadError(f"'action' must be a non-empty string, got {action!r}")
    context = data.pop(CONTEXT_KEY, None)
    if not isinstance(context, dict):
        raise PayloadError(f"'context' must be an object, got {type(context).__name__}")

    top: dict[str, str | None] = {}
    for key, column in TOP_LEVEL_COLUMNS.items():
        value = data.pop(key, None)
        if value is None or isinstance(value, str):
            top[column] = value
        else:
            # Keep the odd value in ``extra`` rather than losing it.
            data[key] = value
            top[column] = None
            issues.append(f"{key}: expected string, got {type(value).__name__}")

    extra_columns: dict[str, Any] = {}
    for key, column in (extra_field_columns or {}).items():
        if key in data:
            extra_columns[column] = data.pop(key)

    return EventRow(
        received_at=received_at or datetime.now(UTC),
        routing_key=clean_text(routing_key),
        action=action,
        host=top["host"],
        username=top["username"],
        rez_version=top["rez_version"],
        status=_str_field(context, "status", issues),
        context_timestamp=_timestamp_field(context, "timestamp", issues),
        solve_time=_float_field(context, "solve_time", issues),
        load_time=_float_field(context, "load_time", issues),
        from_cache=_bool_field(context, "from_cache", issues),
        package_requests=_str_list_field(context, "package_requests", issues),
        implicit_packages=_str_list_field(context, "implicit_packages", issues),
        resolved_packages=_resolved_packages_field(context, issues),
        context=context,
        extra=data,
        extra_columns=extra_columns,
        issues=tuple(issues),
    )


# --- cleaning ------------------------------------------------------------------


def _reject_constant(issues: list[str]) -> Any:
    # Python's json.dumps writes NaN/Infinity, which Postgres jsonb rejects.
    def parse_constant(name: str) -> None:
        issues.append(f"replaced non-standard JSON constant {name} with null")
        return None

    return parse_constant


def _clean(value: Any, issues: list[str]) -> Any:
    """Recursively replace characters Postgres can't store (keys included)."""
    replaced = False

    def fix(s: str) -> str:
        nonlocal replaced
        cleaned = clean_text(s)
        if cleaned is not s:
            replaced = True
        return cleaned

    def walk(v: Any) -> Any:
        if isinstance(v, str):
            return fix(v)
        if isinstance(v, dict):
            return {fix(k): walk(item) for k, item in v.items()}
        if isinstance(v, list):
            return [walk(item) for item in v]
        return v

    cleaned = walk(value)
    if replaced:
        issues.append("removed NUL characters / replaced invalid surrogates")
    return cleaned


# --- context fields ---------------------------------------------------------------


def _type_issue(issues: list[str], key: str, expected: str, value: Any) -> None:
    issues.append(f"context.{key}: expected {expected}, got {type(value).__name__}")


def _is_number(value: Any) -> bool:
    # bool is an int subclass; True is not a valid duration or timestamp.
    return isinstance(value, int | float) and not isinstance(value, bool)


def _str_field(context: Mapping[str, Any], key: str, issues: list[str]) -> str | None:
    value = context.get(key)
    if value is None or isinstance(value, str):
        return value
    _type_issue(issues, key, "string", value)
    return None


def _float_field(context: Mapping[str, Any], key: str, issues: list[str]) -> float | None:
    value = context.get(key)
    if value is None:
        return None
    if _is_number(value) and math.isfinite(value):
        return float(value)
    _type_issue(issues, key, "finite number", value)
    return None


def _bool_field(context: Mapping[str, Any], key: str, issues: list[str]) -> bool | None:
    value = context.get(key)
    if value is None or isinstance(value, bool):
        return value
    _type_issue(issues, key, "boolean", value)
    return None


def _timestamp_field(context: Mapping[str, Any], key: str, issues: list[str]) -> datetime | None:
    value = context.get(key)
    if value is None:
        return None
    if _is_number(value):
        try:
            return datetime.fromtimestamp(value, tz=UTC)
        except (OverflowError, OSError, ValueError):
            issues.append(f"context.{key}: epoch value {value!r} out of range")
            return None
    _type_issue(issues, key, "epoch seconds", value)
    return None


def _str_list_field(context: Mapping[str, Any], key: str, issues: list[str]) -> list[str] | None:
    value = context.get(key)
    if value is None:
        return None
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return value
    _type_issue(issues, key, "list of strings", value)
    return None


def _resolved_packages_field(context: Mapping[str, Any], issues: list[str]) -> list[str] | None:
    """``[{"key": ..., "variables": {"name": "foo", "version": "1.0", ...}}]`` -> ``["foo-1.0"]``.

    The format matches rez's qualified package name; packages without a version
    are written as just their name.
    """
    value = context.get("resolved_packages")
    if value is None:
        return None
    if not isinstance(value, list):
        _type_issue(issues, "resolved_packages", "list", value)
        return None

    names: list[str] = []
    for i, handle in enumerate(value):
        variables = handle.get("variables") if isinstance(handle, dict) else None
        name = variables.get("name") if isinstance(variables, dict) else None
        version = variables.get("version", "") if isinstance(variables, dict) else None
        if not isinstance(name, str) or not name or not isinstance(version, str):
            # A partial list would make "who used X" queries quietly wrong.
            issues.append(
                f"context.resolved_packages[{i}]: expected a variant handle with "
                "string variables.name/version"
            )
            return None
        names.append(f"{name}-{version}" if version else name)
    return names
