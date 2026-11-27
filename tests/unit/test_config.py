from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from rez_ctx_consumer.config import Settings


def load() -> Settings:
    return Settings()


def test_defaults(env: pytest.MonkeyPatch) -> None:
    s = load()
    assert s.amqp_exchange == "rez.context"
    assert s.amqp_exchange_type == "topic"
    assert s.amqp_queue == "rez.context.tracking"
    assert s.amqp_binding_keys == ["REZ.CONTEXT.*"]
    assert not s.default_exchange_mode
    assert s.extra_field_columns == {}
    assert s.batch_size == 200
    assert s.amqp_prefetch == 500


@pytest.mark.parametrize("missing", ["AMQP_URL", "PG_DSN"])
def test_required(env: pytest.MonkeyPatch, missing: str) -> None:
    env.delenv(missing)
    with pytest.raises(ValidationError, match=missing.lower()):
        load()


def test_csv_lists(env: pytest.MonkeyPatch) -> None:
    env.setenv("AMQP_BINDING_KEYS", " REZ.CONTEXT.CREATED , REZ.CONTEXT.SOURCED,")
    assert load().amqp_binding_keys == ["REZ.CONTEXT.CREATED", "REZ.CONTEXT.SOURCED"]


def test_default_exchange_mode(env: pytest.MonkeyPatch) -> None:
    env.setenv("AMQP_EXCHANGE", "")
    s = load()
    assert s.default_exchange_mode
    assert s.amqp_default_exchange_queues == ["REZ.CONTEXT.CREATED", "REZ.CONTEXT.SOURCED"]


def test_default_exchange_mode_needs_queues(env: pytest.MonkeyPatch) -> None:
    env.setenv("AMQP_EXCHANGE", "")
    env.setenv("AMQP_DEFAULT_EXCHANGE_QUEUES", "")
    with pytest.raises(ValidationError, match="AMQP_DEFAULT_EXCHANGE_QUEUES"):
        load()


def test_exchange_mode_needs_binding_keys(env: pytest.MonkeyPatch) -> None:
    env.setenv("AMQP_BINDING_KEYS", "")
    with pytest.raises(ValidationError, match="AMQP_BINDING_KEYS"):
        load()


def test_prefetch_must_cover_batch(env: pytest.MonkeyPatch) -> None:
    env.setenv("AMQP_PREFETCH", "10")
    env.setenv("BATCH_SIZE", "50")
    with pytest.raises(ValidationError, match="AMQP_PREFETCH"):
        load()


def test_extra_field_columns(env: pytest.MonkeyPatch) -> None:
    env.setenv("EXTRA_FIELD_COLUMNS", '{"show": "show", "site": "site_name"}')
    assert load().extra_field_columns == {"show": "show", "site": "site_name"}


@pytest.mark.parametrize(
    "mapping",
    [
        '{"show": "Show"}',
        '{"show": "x; DROP TABLE y"}',
        '{"show": "1show"}',
        '{"a": "col", "b": "col"}',
        '{"user": "user_name"}',
        '{"show": "status"}',
        '{"show": "id"}',
    ],
)
def test_extra_field_columns_rejected(env: pytest.MonkeyPatch, mapping: str) -> None:
    env.setenv("EXTRA_FIELD_COLUMNS", mapping)
    with pytest.raises(ValidationError):
        load()


def test_log_level_case_insensitive(env: pytest.MonkeyPatch) -> None:
    env.setenv("LOG_LEVEL", "debug")
    assert load().log_level == "DEBUG"


def test_redacted_hides_secrets(env: pytest.MonkeyPatch) -> None:
    dumped = json.dumps(load().redacted())
    assert "amqp-secret" not in dumped
    assert "pg-secret" not in dumped
    assert "rez.context.tracking" in dumped
