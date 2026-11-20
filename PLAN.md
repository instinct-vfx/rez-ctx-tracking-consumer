# Implementation plan

A long-running Docker service that consumes rez context tracking events from an
AMQP broker and stores them in PostgreSQL. The message's top-level fields and the
default context fields become typed columns, and the full context is kept as JSONB.

## Background: how rez publishes

Taken from `ResolvedContext._track_context`, `rez/utils/amqp.py` and `rezconfig.py`.

- Two actions: `created` (new resolve, or one served from the resolve cache) and
  `sourced` (an existing context loaded again, e.g. from an `.rxt` file).
- The routing key is `{exchange_routing_key}.{ACTION}`, e.g. `REZ.CONTEXT.SOURCED`.
- The body is JSON (`content_type=application/json`). Its top-level keys are
  `action`, `host`, `user`, `rez_version`, `context`, plus any keys from
  `context_tracking_extra_fields`.
- `context` holds only the fields listed in `context_tracking_context_fields`.
  The default list is `status`, `timestamp` (epoch int), `solve_time`,
  `load_time`, `from_cache`, `package_requests`, `implicit_packages` and
  `resolved_packages`.
- rez never declares an exchange. With rez's default `exchange_name: ''`, the
  routing key is used directly as the queue name, and messages with no matching
  queue are dropped.
- rez's default `message_delivery_mode` is 1 (non-persistent).
- rez sets no AMQP timestamp and no message id. For `sourced` events,
  `context.timestamp` is the time of the original resolve, so the consumer's
  receive time is the best event time available.

## Decisions

| Topic | Decision |
|---|---|
| AMQP library | **aio-pika**. `connect_robust()` restores channels, queues and consumers after a reconnect, and the asyncio timers make it easy to flush batches by size *or* age while heartbeats keep running. |
| Postgres driver | **psycopg 3** with an async connection pool. |
| Broker topology | The consumer declares a durable topic exchange `rez.context` and a durable queue `rez.context.tracking`, bound with `REZ.CONTEXT.*`. A fallback for rez's default exchange (`AMQP_EXCHANGE=""`) consumes from the queues `REZ.CONTEXT.CREATED` and `REZ.CONTEXT.SOURCED`. |
| Delivery semantics | At-least-once. Messages are acked only after the DB commit. Rare duplicates are accepted, because rez sends no id that could be used to dedupe safely. |
| Extra fields | None are mapped by default. Unknown top-level keys go into `extra` (JSONB). `EXTRA_FIELD_COLUMNS` can map them to real columns later, together with a matching migration. |
| Context columns | Extracted in Python at insert time. Missing or invalid values become NULL and are logged and counted, and the row is still stored. The full `context` JSONB is kept unchanged. |
| Partitioning | Monthly range partitions on `received_at`, created by the consumer (current and next month at startup and once a day) with a DEFAULT partition as a safety net. No pg_partman. |
| Retention | None. Removing a month later is a manual `DETACH`/`DROP PARTITION`. |
| Migrations | Plain numbered SQL files, applied at startup under `pg_advisory_lock`. |
| Target Postgres | Self-managed, 15+. CI tests 15, 17 and 18; 13 and 14 are end-of-life. |

## Schema

```sql
CREATE TABLE rez_context_event (
  id                 bigint GENERATED ALWAYS AS IDENTITY,
  received_at        timestamptz NOT NULL DEFAULT now(),
  routing_key        text NOT NULL,
  -- message top-level
  action             text NOT NULL,          -- created | sourced
  host               text,
  username           text,                   -- payload "user"
  rez_version        text,
  -- promoted from context (NULL if absent/invalid)
  status             text,
  context_timestamp  timestamptz,            -- resolve time, not event time
  solve_time         double precision,       -- seconds
  load_time          double precision,       -- seconds
  from_cache         boolean,
  package_requests   text[],
  implicit_packages  text[],
  resolved_packages  text[],                 -- "name-version", built from the variant handles
  -- full payload parts
  context            jsonb NOT NULL,
  extra              jsonb NOT NULL DEFAULT '{}',
  PRIMARY KEY (id, received_at)
) PARTITION BY RANGE (received_at);
```

Indexes:

- `(received_at)` and `(action, received_at)`
- `(username)` and `(host)`
- a partial index on `(status)` where `status <> 'solved'`, for looking at failures
- a GIN index on `resolved_packages`

`rez_context_event_reject (received_at, routing_key, raw bytea, error text)`
stores messages that can't be parsed. They are acked rather than retried forever.

## Consumer behaviour

- Prefetch about 500. Each batch is flushed in one transaction at `BATCH_SIZE`
  (200) messages or `BATCH_MAX_WAIT_MS` (1000), whichever comes first, then acked
  with `multiple=True`.
- DB outage: nothing is acked, consuming pauses, and the consumer reconnects with
  exponential backoff. Messages wait in the durable queue meanwhile.
- `\u0000` is removed from strings, because JSONB rejects it.
- On SIGTERM/SIGINT: stop consuming, flush and ack the current batch, close all
  connections.
- Several replicas can consume the same queue; the broker spreads messages
  between them.
- Logs are structured JSON on stdout. `/healthz` and `/metrics` (Prometheus)
  report messages consumed/stored/rejected, batch latency and DB errors.

## Configuration (environment)

| Variable | Default |
|---|---|
| `AMQP_URL` | required |
| `AMQP_EXCHANGE` | `rez.context` (`""` = use rez's default exchange) |
| `AMQP_EXCHANGE_TYPE` | `topic` |
| `AMQP_QUEUE` | `rez.context.tracking` |
| `AMQP_BINDING_KEYS` | `REZ.CONTEXT.*` (comma separated) |
| `AMQP_DEFAULT_EXCHANGE_QUEUES` | `REZ.CONTEXT.CREATED,REZ.CONTEXT.SOURCED` |
| `AMQP_PREFETCH` | `500` |
| `PG_DSN` | required |
| `PG_POOL_SIZE` | `4` |
| `BATCH_SIZE` | `200` |
| `BATCH_MAX_WAIT_MS` | `1000` |
| `EXTRA_FIELD_COLUMNS` | `{}` (JSON object: payload key → column) |
| `LOG_LEVEL` | `INFO` |
| `LOG_FORMAT` | `json` (`text` for local use) |
| `METRICS_PORT` | `9100` (`0` disables) |
| `SHUTDOWN_TIMEOUT_S` | `8` (time to store and ack what was received on SIGTERM; keep it below the container's stop grace period) |

## Recommended rezconfig

```python
context_tracking_host = "rabbitmq.example:5672"
context_tracking_amqp = {
    "userid": "rez",
    "password": "...",
    "connect_timeout": 10,
    "exchange_name": "rez.context",
    "exchange_routing_key": "REZ.CONTEXT",
    "message_delivery_mode": 2,  # persistent
}
```

## Layout

```
pyproject.toml                 # uv, ruff, mypy, pytest
Dockerfile                     # python:3.13-slim, multi-stage uv build, non-root
docker-compose.yml             # postgres:17, rabbitmq:4-management, consumer
.env.example
src/rez_ctx_consumer/
  __main__.py                  # entrypoint
  app.py                       # task wiring, signal handling
  config.py                    # Settings
  logging_config.py            # JSON/text logging
  payload.py                   # parse/validate -> EventRow (+ reject reasons)
  amqp.py                      # robust connection, topology, consume
  batcher.py                   # batching, store with retry, acks (library-independent)
  db.py                        # pool, batch insert with row-by-row fallback, rejects
  migrations.py                # runner for sql/NNNN_*.sql (advisory lock, checksums)
  partitions.py                # monthly partitions, moving rows out of DEFAULT
  health.py                    # /healthz, /metrics
  sql/0001_init.sql
tests/
  fixtures/rez-<version>/      # real rez messages, captured by tools/
  unit/
  integration/                 # testcontainers: RabbitMQ + Postgres
  e2e/                         # real rez -> broker -> consumer -> Postgres
tools/
  capture_rez_fixtures.py      # records rez's actual message bodies (needs rez installed)
```

## Phases

1. **Scaffold.** pyproject, uv lock, ruff, mypy, pytest, settings and logging,
   entrypoint skeleton, Dockerfile, CI (lint, type-check, tests, image build).
2. **Payload model** (done). Parse the top-level fields and fill the context
   columns. Unit tests cover missing, wrongly typed and NUL-containing values.
   Fixtures were captured from rez 3.5.0. Each variant handle is
   `{"key": "filesystem.variant", "variables": {"name", "version", "index",
   "location", "repository_type"}}`, and `index` is `null` for packages without
   variants. A failed resolve sends `resolved_packages: []`.
3. **DB layer** (done). Migrations runner, `0001_init.sql`, partition
   management, batch insert, rejects table, and integration tests against
   Postgres. A batch that fails because of a row's data (DataError or
   IntegrityError) is retried row by row with savepoints, and the rows that still
   fail are rejected. At startup the consumer waits for the DB (with backoff) and
   fails fast if `EXTRA_FIELD_COLUMNS` names a column that doesn't exist.
4. **AMQP consumer** (done). Topology in both modes, batching and acks, handling
   of DB outages, graceful shutdown. Integration tests cover a broker restart, a
   DB outage, and SIGTERM with an unflushed batch. Other notes:
   - The batching logic (`batcher.py`) is independent of aio-pika.
   - Messages whose channel closed before they were stored are dropped, because
     the broker redelivers them; this avoids duplicate rows.
   - `aio_pika.connect_robust()` leaks its reconnect task when the first attempt
     times out or is cancelled, so the consumer creates the `RobustConnection`
     itself and closes it when an attempt fails.
5. **Container and operations.** docker-compose (`docker compose up postgres`
   starts just a test DB), `.env.example`, healthcheck, metrics.
6. **End to end.** A container with rez installed via pip and a tiny package
   repository runs `rez-env` and sources an `.rxt`. The test checks that both a
   `created` and a `sourced` row land in Postgres.
7. **Docs.** README with the rezconfig snippet, example queries, and how to
   remove old partitions by hand.
