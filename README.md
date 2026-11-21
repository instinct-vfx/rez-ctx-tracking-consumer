# rez-ctx-tracking-consumer

A long-running service that consumes [rez](https://github.com/AcademySoftwareFoundation/rez)
context tracking events from an AMQP broker (e.g. RabbitMQ) and stores them in PostgreSQL.

> **Status:** early development, but working end to end. See [PLAN.md](PLAN.md)
> for the design and roadmap.

Requires PostgreSQL 15 or newer. The consumer creates its schema and monthly
partitions itself; no extensions are needed.

## Quick start

```sh
docker compose up -d --build     # PostgreSQL, RabbitMQ and the consumer
docker compose up -d postgres    # or: just a database for testing
```

Copy `.env.example` to `.env` to change credentials or host ports. Then point rez
at the broker (e.g. in a `rezconfig.py` referenced by `REZ_CONFIG_FILE`):

```python
context_tracking_host = "localhost:5672"
context_tracking_amqp = {
    "userid": "rez",
    "password": "rez",
    "connect_timeout": 10,
    "exchange_name": "rez.context",  # the exchange the consumer declares
    "exchange_routing_key": "REZ.CONTEXT",
    "message_delivery_mode": 2,  # persistent messages
}
```

Every `rez-env` (and every load of an `.rxt` file) now produces a row:

```sh
docker compose exec postgres psql -U rez -c   "SELECT received_at, action, username, host, status, resolved_packages
     FROM rez_context_event ORDER BY received_at DESC LIMIT 10"
```

The RabbitMQ management UI is at <http://localhost:15672> (rez / rez).

## Health and metrics

The consumer serves two endpoints on `METRICS_PORT` (default 9100; `0` disables both):

- `/healthz`: `200` when it is consuming and storing, `503` otherwise, with a JSON
  body saying why (not ready yet, broker disconnected, or storing batches has failed
  for over a minute). An idle consumer is healthy. The Docker image's `HEALTHCHECK`
  uses it via `rez-ctx-consumer healthcheck`.
- `/metrics`: Prometheus metrics (`rez_ctx_consumer_*`): messages received, events
  stored, rejects by stage, store errors, batch sizes and durations, pending messages,
  broker connection state, time of the last stored batch.

## Development

Requires [uv](https://docs.astral.sh/uv/).

```sh
uv sync                    # create .venv with runtime + dev dependencies
uv run pytest              # all tests (integration tests need Docker)
uv run pytest -m "not integration"            # unit tests only
POSTGRES_IMAGE=postgres:15 uv run pytest -m integration   # other Postgres version
uv run ruff check          # lint
uv run ruff format         # format
uv run mypy                # type-check
docker build -t rez-ctx-tracking-consumer .
```

## Configuration

Configuration comes from environment variables. `AMQP_URL` and `PG_DSN` are required;
everything else has defaults. See [PLAN.md](PLAN.md#configuration-environment) for the
full list.

## License

Apache-2.0, see [LICENSE](LICENSE).
