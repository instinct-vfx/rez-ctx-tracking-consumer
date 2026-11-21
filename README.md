# rez-ctx-tracking-consumer

A long-running service that stores [rez](https://github.com/AcademySoftwareFoundation/rez)
context tracking events in PostgreSQL. rez can publish an event to an AMQP broker
(e.g. RabbitMQ) whenever a context is created or loaded. This service consumes those
events and writes one row per event, which makes questions like these simple SQL:

- Who still uses `maya-2024`, and on which machines?
- Which packages haven't been used in 90 days?
- How often do resolves fail, and for which requests?
- How long do resolves take, and how well does the resolve cache work?

Top-level message fields and rez's default context fields (status, timestamps,
solve and load time, requested and resolved packages) become typed, indexed columns.
The full context is kept as JSONB.

Requires PostgreSQL 15 or newer and an AMQP 0-9-1 broker (tested with RabbitMQ 4).
The consumer creates its own schema and monthly partitions; no extensions needed.

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
docker compose exec postgres psql -U rez -c "
  SELECT received_at, action, username, host, status, resolved_packages
  FROM rez_context_event ORDER BY received_at DESC LIMIT 10"
```

The RabbitMQ management UI is at <http://localhost:15672> (rez / rez), and the
consumer's health and metrics are at <http://localhost:9100/healthz> and
<http://localhost:9100/metrics>.

## Documentation

- [docs/queries.md](docs/queries.md): example queries (package usage, failed
  resolves, resolve performance, users and hosts, extra fields, JSONB).
- [docs/operations.md](docs/operations.md): deploying, the configuration
  reference, rez setup, delivery guarantees, metrics and alerts, sizing, upgrades,
  removing old data, mapping extra fields to columns, troubleshooting.
- [PLAN.md](PLAN.md): design decisions and how the project was built.

## Development

Requires [uv](https://docs.astral.sh/uv/) and, for integration and end-to-end
tests, Docker.

```sh
uv sync                    # create .venv with runtime + dev dependencies
uv run pytest              # unit + integration tests
uv run pytest -m "not integration"            # unit tests only
POSTGRES_IMAGE=postgres:15 uv run pytest -m integration   # other Postgres version
uv run pytest -m e2e       # real rez against the compose stack (builds images)
uv run ruff check          # lint
uv run ruff format         # format
uv run mypy                # type-check
docker build -t rez-ctx-tracking-consumer .
```

`tests/fixtures/` holds real messages captured from rez; see its README to
capture them again for a new rez version.

## License

Apache-2.0, see [LICENSE](LICENSE).
