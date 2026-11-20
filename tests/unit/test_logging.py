from __future__ import annotations

import json
import logging
import sys

from rez_ctx_consumer.logging_config import JsonFormatter


def make_record(**extra: object) -> logging.LogRecord:
    record = logging.LogRecord("test.logger", logging.INFO, __file__, 1, "hello %s", ("x",), None)
    for key, value in extra.items():
        setattr(record, key, value)
    return record


def test_json_formatter_basic_fields() -> None:
    entry = json.loads(JsonFormatter().format(make_record()))
    assert entry["level"] == "INFO"
    assert entry["logger"] == "test.logger"
    assert entry["msg"] == "hello x"
    assert entry["ts"].endswith("+00:00")


def test_json_formatter_includes_extra() -> None:
    entry = json.loads(JsonFormatter().format(make_record(batch=12, queue="q")))
    assert entry["batch"] == 12
    assert entry["queue"] == "q"
    assert "args" not in entry


def test_json_formatter_exception() -> None:
    try:
        raise ValueError("boom")
    except ValueError:
        record = make_record()
        record.exc_info = sys.exc_info()
    entry = json.loads(JsonFormatter().format(record))
    assert "ValueError: boom" in entry["exc"]
