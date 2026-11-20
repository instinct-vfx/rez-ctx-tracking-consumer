# rez-ctx-tracking-consumer

A long-running service that consumes [rez](https://github.com/AcademySoftwareFoundation/rez)
context tracking events from an AMQP broker (e.g. RabbitMQ) and stores them in PostgreSQL.

> **Status:** early development. Message parsing and the database layer (schema,
> migrations, monthly partitions, batch writes) are done; consuming from AMQP is next.
> See [PLAN.md](PLAN.md) for the design and roadmap.

Requires PostgreSQL 15 or newer. The consumer creates its schema and monthly
partitions itself; no extensions are needed.

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
