"""Service configuration, read from environment variables."""

from __future__ import annotations

import re
from typing import Annotated, Literal, Self

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

from rez_ctx_consumer.payload import RESERVED_COLUMNS, RESERVED_PAYLOAD_KEYS

# Postgres identifiers we are willing to create from configuration.
_COLUMN_NAME_RE = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")

# A list read from a comma-separated env var (e.g. "A,B") instead of JSON.
CsvList = Annotated[list[str], NoDecode]

LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
ExchangeType = Literal["topic", "direct", "fanout", "headers"]


class Settings(BaseSettings):
    """All runtime settings. Field names map to upper-case env vars."""

    model_config = SettingsConfigDict(extra="ignore", frozen=True)

    # --- AMQP ---------------------------------------------------------------
    amqp_url: SecretStr
    # Empty string means: use rez's default exchange (routing key == queue name)
    # and consume from ``amqp_default_exchange_queues``.
    amqp_exchange: str = "rez.context"
    amqp_exchange_type: ExchangeType = "topic"
    amqp_queue: str = "rez.context.tracking"
    amqp_binding_keys: CsvList = Field(default_factory=lambda: ["REZ.CONTEXT.*"])
    amqp_default_exchange_queues: CsvList = Field(
        default_factory=lambda: ["REZ.CONTEXT.CREATED", "REZ.CONTEXT.SOURCED"]
    )
    amqp_prefetch: int = Field(default=500, ge=1)

    # --- Postgres -------------------------------------------------------------
    pg_dsn: SecretStr
    pg_pool_size: int = Field(default=4, ge=1)

    # --- Batching -------------------------------------------------------------
    batch_size: int = Field(default=200, ge=1)
    batch_max_wait_ms: int = Field(default=1000, ge=1)

    # --- Payload mapping ------------------------------------------------------
    # Top-level payload key -> table column. Unmapped extra keys go to ``extra``.
    extra_field_columns: dict[str, str] = Field(default_factory=dict)

    # --- Operations -----------------------------------------------------------
    log_level: LogLevel = "INFO"
    log_format: Literal["json", "text"] = "json"
    metrics_port: int = Field(default=9100, ge=0, le=65535)  # 0 disables

    @field_validator("amqp_binding_keys", "amqp_default_exchange_queues", mode="before")
    @classmethod
    def _split_csv(cls, value: object) -> object:
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        return value

    @field_validator("log_level", mode="before")
    @classmethod
    def _upper_log_level(cls, value: object) -> object:
        return value.upper() if isinstance(value, str) else value

    @field_validator("extra_field_columns")
    @classmethod
    def _check_column_names(cls, value: dict[str, str]) -> dict[str, str]:
        invalid = sorted(col for col in value.values() if not _COLUMN_NAME_RE.match(col))
        if invalid:
            raise ValueError(f"invalid column name(s): {', '.join(invalid)}")
        if len(set(value.values())) != len(value):
            raise ValueError("several payload keys map to the same column")
        reserved_keys = sorted(RESERVED_PAYLOAD_KEYS & value.keys())
        if reserved_keys:
            raise ValueError(f"rez's own payload key(s) can't be remapped: {reserved_keys}")
        reserved_cols = sorted(RESERVED_COLUMNS & set(value.values()))
        if reserved_cols:
            raise ValueError(f"column name(s) already used by the table: {reserved_cols}")
        return value

    @model_validator(mode="after")
    def _check_consistency(self) -> Self:
        if self.amqp_prefetch < self.batch_size:
            raise ValueError(
                f"AMQP_PREFETCH ({self.amqp_prefetch}) must be >= BATCH_SIZE "
                f"({self.batch_size}), otherwise batches can never fill up"
            )
        if self.default_exchange_mode:
            if not self.amqp_default_exchange_queues:
                raise ValueError("AMQP_DEFAULT_EXCHANGE_QUEUES must not be empty")
        elif not self.amqp_binding_keys:
            raise ValueError("AMQP_BINDING_KEYS must not be empty")
        return self

    @property
    def default_exchange_mode(self) -> bool:
        """True when rez publishes to the AMQP default exchange."""
        return self.amqp_exchange == ""

    def redacted(self) -> dict[str, object]:
        """Settings safe to log: secrets are masked."""
        return self.model_dump(mode="json")
