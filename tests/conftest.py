from __future__ import annotations

import os

import pytest

from rez_ctx_consumer.config import Settings

# Env vars read by Settings; cleared so a developer's shell can't leak into tests.
_SETTINGS_ENV = [name.upper() for name in Settings.model_fields]


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    """Clean environment with the required settings present."""
    for name in _SETTINGS_ENV:
        if name in os.environ:
            monkeypatch.delenv(name)
    monkeypatch.setenv("AMQP_URL", "amqp://user:amqp-secret@broker:5672/")
    monkeypatch.setenv("PG_DSN", "postgresql://user:pg-secret@db:5432/rez")
    return monkeypatch
