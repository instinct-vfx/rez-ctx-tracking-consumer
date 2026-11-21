from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

import pytest

from rez_ctx_consumer.config import Settings

if sys.platform == "win32":
    # psycopg's async mode doesn't support the default Proactor loop.
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

INTEGRATION_DIR = Path(__file__).parent / "integration"
E2E_DIR = Path(__file__).parent / "e2e"

# Env vars read by Settings; cleared so a developer's shell can't leak into tests.
_SETTINGS_ENV = [name.upper() for name in Settings.model_fields]


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        if INTEGRATION_DIR in item.path.parents:
            item.add_marker(pytest.mark.integration)
        elif E2E_DIR in item.path.parents:
            item.add_marker(pytest.mark.e2e)


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    """Clean environment with the required settings present."""
    for name in _SETTINGS_ENV:
        if name in os.environ:
            monkeypatch.delenv(name)
    monkeypatch.setenv("AMQP_URL", "amqp://user:amqp-secret@broker:5672/")
    monkeypatch.setenv("PG_DSN", "postgresql://user:pg-secret@db:5432/rez")
    monkeypatch.setenv("METRICS_PORT", "0")  # tests that need the server pick a free port
    return monkeypatch
