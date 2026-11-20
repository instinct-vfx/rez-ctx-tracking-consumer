"""Logging setup: structured JSON (default, for containers) or plain text."""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime
from typing import Literal

# Attributes every LogRecord has; anything else was passed via ``extra=``.
_STANDARD_ATTRS = frozenset(
    vars(logging.LogRecord("", 0, "", 0, "", None, None)).keys() | {"message", "asctime"}
)


def extra_fields(record: logging.LogRecord) -> dict[str, object]:
    """The fields passed to a log call via ``extra=``."""
    return {
        key: value
        for key, value in vars(record).items()
        if key not in _STANDARD_ATTRS and not key.startswith("_")
    }


class JsonFormatter(logging.Formatter):
    """One JSON object per line, including any ``extra=`` fields."""

    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, object] = {
            "ts": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        entry.update(extra_fields(record))
        if record.exc_info:
            entry["exc"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str)


class TextFormatter(logging.Formatter):
    """Human-readable lines for local use; ``extra=`` fields appended as key=value."""

    def __init__(self) -> None:
        super().__init__("%(asctime)s %(levelname)-8s %(name)s: %(message)s")

    def formatMessage(self, record: logging.LogRecord) -> str:  # noqa: N802 - stdlib name
        line = super().formatMessage(record)
        extras = " ".join(f"{k}={v!r}" for k, v in extra_fields(record).items())
        return f"{line} [{extras}]" if extras else line


def configure_logging(level: str, fmt: Literal["json", "text"]) -> None:
    handler = logging.StreamHandler(sys.stdout)
    if fmt == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(TextFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)
    # aiormq/aio-pika are chatty at INFO during reconnects; keep them quieter.
    for noisy in ("aiormq", "aio_pika"):
        logging.getLogger(noisy).setLevel(max(logging.getLevelName(level), logging.WARNING))
