"""Entrypoint: ``python -m rez_ctx_consumer`` or ``rez-ctx-consumer``."""

from __future__ import annotations

import asyncio
import sys

from pydantic import ValidationError

from rez_ctx_consumer.app import run
from rez_ctx_consumer.config import Settings
from rez_ctx_consumer.logging_config import configure_logging


def main() -> int:
    try:
        settings = Settings()
    except ValidationError as exc:
        print(f"Invalid configuration:\n{exc}", file=sys.stderr)
        return 2

    configure_logging(settings.log_level, settings.log_format)
    asyncio.run(run(settings))
    return 0


if __name__ == "__main__":
    sys.exit(main())
