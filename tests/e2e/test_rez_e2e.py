"""Real rez -> RabbitMQ -> consumer (Docker image) -> Postgres.

Runs the stack in tests/e2e/compose.yml (RabbitMQ, Postgres, the consumer image)
plus a rez client container (tests/e2e/rez/),
which runs a fixed set of rez commands (run.sh). Then checks that exactly the
expected events were stored. Opt-in because it builds images: ``pytest -m e2e``.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psycopg
import pytest
from psycopg.rows import dict_row

REPO = Path(__file__).resolve().parents[2]
if sys.platform == "win32":
    # buildx bake (used by compose build) refuses a lowercase drive letter as
    # "outside the project"; normalise e.g. c:\dev -> C:\dev.
    REPO = Path(str(REPO)[0].upper() + str(REPO)[1:])

COMPOSE_FILES = ["tests/e2e/compose.yml"]
CREDENTIALS = {"POSTGRES_DB": "rez", "POSTGRES_USER": "rez", "POSTGRES_PASSWORD": "rez"}


@dataclass
class Stack:
    project: str
    env: dict[str, str]
    pg_dsn: str

    def compose(self, *args: str, timeout: float = 600) -> subprocess.CompletedProcess[str]:
        files = [arg for f in COMPOSE_FILES for arg in ("-f", f)]
        cmd = ["docker", "compose", "-p", self.project, *files, *args]
        return subprocess.run(
            cmd,
            cwd=REPO,
            env=self.env,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port: int = s.getsockname()[1]
        return port


def _docker_available() -> bool:
    try:
        return subprocess.run(["docker", "info"], capture_output=True, timeout=30).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


@pytest.fixture(scope="module")
def stack(request: pytest.FixtureRequest) -> Iterator[Stack]:
    if not _docker_available():
        if os.environ.get("CI"):
            pytest.fail("Docker is required for the e2e tests")
        pytest.skip("Docker not available")

    ports = {
        name: str(_free_port())
        for name in ("POSTGRES_PORT", "RABBITMQ_PORT", "RABBITMQ_MANAGEMENT_PORT", "METRICS_PORT")
    }
    # Explicit values win over a developer's .env in the repository.
    env = os.environ | CREDENTIALS | ports | {"LOG_FORMAT": "text"}
    stack = Stack(
        project=f"rcc-e2e-{uuid.uuid4().hex[:8]}",
        env=env,
        pg_dsn=f"postgresql://rez:rez@127.0.0.1:{ports['POSTGRES_PORT']}/rez",
    )
    failed_before = request.session.testsfailed
    try:
        up = stack.compose("up", "--detach", "--build", "--wait", "--wait-timeout", "300")
        assert up.returncode == 0, f"compose up failed:\n{up.stdout}\n{up.stderr}"
        yield stack
    finally:
        if request.session.testsfailed > failed_before:
            logs = stack.compose("logs", "--no-color", "consumer", timeout=60)
            print(f"\n--- consumer logs ---\n{logs.stdout}{logs.stderr}")
        stack.compose("down", "--volumes", "--remove-orphans", timeout=180)


def fetch_events(dsn: str, expected: int, timeout: float = 30) -> list[dict[str, Any]]:
    """Wait until ``expected`` rows are stored (the consumer batches for 200 ms)."""
    deadline = time.monotonic() + timeout
    while True:
        with psycopg.connect(dsn, row_factory=dict_row) as conn:
            rows = conn.execute("SELECT * FROM rez_context_event ORDER BY id").fetchall()
        if len(rows) >= expected or time.monotonic() > deadline:
            return rows
        time.sleep(0.5)


# Includes building both images on a cold cache.
@pytest.mark.timeout(900)
def test_rez_env_events_are_stored(stack: Stack) -> None:
    run = stack.compose("run", "--rm", "--build", "rez")
    assert run.returncode == 0, f"rez commands failed:\n{run.stdout}\n{run.stderr}"

    rows = fetch_events(stack.pg_dsn, expected=4)
    # run.sh: rez-env (created), rez-env --output (created),
    # rez-env --input (sourced), conflicting rez-env (created, failed).
    assert [(r["action"], r["status"]) for r in rows] == [
        ("created", "solved"),
        ("created", "solved"),
        ("sourced", "solved"),
        ("created", "failed"),
    ]

    resolved = ["bar-2.1.0", "baz-1.2", "foo-1.0.0"]
    for row in rows:
        assert row["routing_key"] == f"REZ.CONTEXT.{row['action'].upper()}"
        assert row["username"] == "artist"
        assert row["host"]
        assert row["rez_version"] == "3.5.0"
        assert row["extra"] == {"show": "e2e_show"}  # context_tracking_extra_fields
        assert row["context_timestamp"] is not None
        assert row["solve_time"] is not None
        assert row["from_cache"] is False
        assert row["implicit_packages"]
        assert row["resolved_packages"] == (resolved if row["status"] == "solved" else [])

    saved, sourced = rows[1], rows[2]
    assert sourced["context_timestamp"] == saved["context_timestamp"]  # same resolve
    assert sourced["package_requests"] == ["foo"]
    assert rows[3]["package_requests"] == ["foo", "!bar"]

    with psycopg.connect(stack.pg_dsn) as conn:
        rejects = conn.execute("SELECT count(*) FROM rez_context_event_reject").fetchone()
    assert rejects == (0,)
