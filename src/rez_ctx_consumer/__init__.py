"""Consume rez context tracking events from AMQP and store them in PostgreSQL."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("rez-ctx-tracking-consumer")
except PackageNotFoundError:  # pragma: no cover - running from a source tree
    __version__ = "0.0.0+unknown"
