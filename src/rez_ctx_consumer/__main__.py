"""Entrypoint: ``python -m rez_ctx_consumer`` or ``rez-ctx-consumer``."""

from __future__ import annotations

import asyncio
import logging
import sys

from pydantic import ValidationError

from rez_ctx_consumer.app import run
from rez_ctx_consumer.config import Settings
from rez_ctx_consumer.logging_config import configure_logging

log = logging.getLogger("rez_ctx_consumer")


def main() -> int:
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


if __name__ == "__main__":
    sys.exit(main())
