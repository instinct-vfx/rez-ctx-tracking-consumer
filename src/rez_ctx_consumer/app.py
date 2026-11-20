"""Service lifecycle: start components, wait for a stop signal, shut down cleanly."""

from __future__ import annotations

import asyncio
import logging
import signal

from rez_ctx_consumer import __version__
from rez_ctx_consumer.config import Settings

log = logging.getLogger(__name__)


def install_signal_handlers(stop: asyncio.Event) -> None:
    """Set ``stop`` on SIGTERM/SIGINT."""
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # Windows event loops (local dev only)
            signal.signal(sig, lambda *_: loop.call_soon_threadsafe(stop.set))


async def run(settings: Settings, stop: asyncio.Event | None = None) -> None:
    """Run the service until ``stop`` is set (by a signal unless supplied)."""
    if stop is None:
        stop = asyncio.Event()
        install_signal_handlers(stop)

    log.info("starting", extra={"version": __version__, "settings": settings.redacted()})
    # The DB pool, migrations, AMQP consumer and health server get wired in here
    # in the following phases (see PLAN.md).
    await stop.wait()
    log.info("stopped")
