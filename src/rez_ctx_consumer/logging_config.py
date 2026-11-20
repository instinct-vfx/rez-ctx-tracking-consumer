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


class JsonFormatter(logging.Formatter):
    """One JSON object per line, including any ``extra=`` fields."""

    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, object] = {
            "ts": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for key, value in vars(record).items():
            if key not in _STANDARD_ATTRS and not key.startswith("_"):
                entry[key] = value
        if record.exc_info:
            entry["exc"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str)


def configure_logging(level: str, fmt: Literal["json", "text"]) -> None:
    handler = logging.StreamHandler(sys.stdout)
    if fmt == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(name)s: %(message)s"))
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)
    # aiormq/aio-pika are chatty at INFO during reconnects; keep them quieter.
    for noisy in ("aiormq", "aio_pika"):
        logging.getLogger(noisy).setLevel(max(logging.getLevelName(level), logging.WARNING))
