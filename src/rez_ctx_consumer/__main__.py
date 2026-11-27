"""Entrypoint: ``rez-ctx-consumer`` (or ``python -m rez_ctx_consumer``).

``rez-ctx-consumer`` runs the service; ``rez-ctx-consumer healthcheck`` queries
the running service's ``/healthz`` and exits 0 if it is healthy (used by the
Docker HEALTHCHECK, as the slim image has no curl).
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import urllib.error
import urllib.request

from pydantic import ValidationError

from rez_ctx_consumer.app import run
from rez_ctx_consumer.config import Settings
from rez_ctx_consumer.health import HEALTH_PATH
from rez_ctx_consumer.logging_config import configure_logging

log = logging.getLogger("rez_ctx_consumer")

USAGE = "usage: rez-ctx-consumer [healthcheck]"


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    if args == ["healthcheck"]:
        return healthcheck()
    if args:
        print(USAGE, file=sys.stderr)
        return 2
    return serve()


def serve() -> int:
    try:
        settings = Settings()
    except ValidationError as exc:
        print(f"Invalid configuration:\n{exc}", file=sys.stderr)
        return 2

    configure_logging(settings.log_level, settings.log_format)
    # psycopg's async mode doesn't support Windows' default Proactor loop.
    loop_factory = asyncio.SelectorEventLoop if sys.platform == "win32" else None
    try:
        asyncio.run(run(settings), loop_factory=loop_factory)
    except Exception:
        log.exception("fatal error")
        return 1
    return 0


def healthcheck(timeout_s: float = 3.0) -> int:
    # Read METRICS_PORT directly: the check shouldn't need the full config.
    port = int(os.environ.get("METRICS_PORT", "9100"))
    if port == 0:
        print("health endpoint disabled (METRICS_PORT=0)", file=sys.stderr)
        return 1
    url = f"http://127.0.0.1:{port}{HEALTH_PATH}"
    try:
        with urllib.request.urlopen(url, timeout=timeout_s) as response:
            print(response.read().decode().strip())
            return 0
    except urllib.error.HTTPError as exc:  # 503: answered, but unhealthy
        print(exc.read().decode().strip())
        return 1
    except OSError as exc:
        print(f"health endpoint not reachable: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
