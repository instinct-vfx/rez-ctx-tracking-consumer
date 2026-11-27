# Operations

How to run the consumer in production: setup, configuration, what can go
wrong, and routine maintenance. For example queries see [queries.md](queries.md).

- [How it works](#how-it-works)
- [Deploying](#deploying)
- [Configuration reference](#configuration-reference)
- [Configuring rez](#configuring-rez)
- [Delivery guarantees](#delivery-guarantees)
- [Health checks, metrics and alerts](#health-checks-metrics-and-alerts)
- [Scaling and sizing](#scaling-and-sizing)
- [Upgrades](#upgrades)
- [Partitions and removing old data](#partitions-and-removing-old-data)
- [Mapping extra fields to columns](#mapping-extra-fields-to-columns)
- [Rejected messages](#rejected-messages)
- [Troubleshooting](#troubleshooting)

## How it works

```
rez (every rez-env / .rxt load)
  --> RabbitMQ exchange "rez.context" (topic)
  --> durable queue "rez.context.tracking" (bound with REZ.CONTEXT.*)
  --> consumer: parse, batch, INSERT, then ack
  --> Postgres: rez_context_event (monthly partitions), rez_context_event_reject
```

At startup the consumer:
1. waits for Postgres;
2. applies its schema migrations;
3. creates the partitions for this month and next;
4. waits for RabbitMQ;
5. declares the exchange, queue and binding;
6. starts consuming.

It retries until both the database and the broker are reachable, so the start
order doesn't matter.

## Deploying

The image is built from the repository's `Dockerfile` (releases are published as
`ghcr.io/instinct-vfx/rez-ctx-tracking-consumer`) and is configured only
through environment variables. `AMQP_URL` and `PG_DSN` are required:

```sh
docker run -d --name rez-ctx-consumer \
  -e AMQP_URL=amqp://rez:secret@rabbitmq.example:5672/ \
  -e PG_DSN=postgresql://rez_tracking:secret@db.example:5432/pipeline \
  -p 9100:9100 \
  --stop-timeout 15 \
  ghcr.io/instinct-vfx/rez-ctx-tracking-consumer:0.1.0
```

The repository has no compose file for a full stack with PostgreSQL, a broker
and dashboards. [rez-in-a-box](https://github.com/instinct-vfx/rez-in-a-box)
runs this image in one, with Grafana dashboards, demo data and rez installs.

### Postgres

PostgreSQL 15 or newer, no extensions needed. The consumer creates and changes
its own tables, so its role needs to own its schema. Since Postgres 15 an
ordinary role can't create tables in `public`, so give it a schema of its own:

```sql
CREATE ROLE rez_tracking LOGIN PASSWORD 'secret';
CREATE SCHEMA rez_tracking AUTHORIZATION rez_tracking;
ALTER ROLE rez_tracking SET search_path = rez_tracking;
```

The consumer uses whatever schema `search_path` points to. You can also set it
per connection in the DSN: `...?options=-csearch_path%3Drez_tracking`.

For people who only query the data, grant access to the parent tables. Reads
through the parent cover all partitions, including future ones:

```sql
CREATE ROLE rez_tracking_read;
GRANT USAGE ON SCHEMA rez_tracking TO rez_tracking_read;
GRANT SELECT ON rez_tracking.rez_context_event, rez_tracking.rez_context_event_reject
    TO rez_tracking_read;
```

The tables exist once the consumer has started for the first time.

### RabbitMQ

Any AMQP 0-9-1 broker; RabbitMQ is what's tested. The consumer declares a
durable topic exchange (`AMQP_EXCHANGE`, default `rez.context`) and a durable
queue (`AMQP_QUEUE`, default `rez.context.tracking`). The broker user needs
configure, write and read permissions on both. rez needs write permission on
the exchange.

Start the consumer once before enabling tracking in rez. rez publishes to an
exchange it doesn't create, so until the consumer has declared it, rez's
messages are rejected by the broker.

## Configuration reference

| Variable | Default | Meaning |
|---|---|---|
| `AMQP_URL` | required | e.g. `amqp://user:pass@host:5672/vhost` (`amqps://` for TLS) |
| `AMQP_EXCHANGE` | `rez.context` | Exchange to bind to. `""` = rez's default exchange (see below). |
| `AMQP_EXCHANGE_TYPE` | `topic` | Type used when declaring the exchange |
| `AMQP_QUEUE` | `rez.context.tracking` | Durable queue the consumer reads from |
| `AMQP_BINDING_KEYS` | `REZ.CONTEXT.*` | Comma-separated binding keys |
| `AMQP_DEFAULT_EXCHANGE_QUEUES` | `REZ.CONTEXT.CREATED,REZ.CONTEXT.SOURCED` | Queues consumed when `AMQP_EXCHANGE=""` |
| `AMQP_PREFETCH` | `500` | Maximum unacked messages; must be ≥ `BATCH_SIZE` |
| `PG_DSN` | required | libpq connection string or URL |
| `PG_POOL_SIZE` | `4` | Maximum database connections |
| `BATCH_SIZE` | `200` | Rows per insert transaction |
| `BATCH_MAX_WAIT_MS` | `1000` | Longest a message waits before its batch is written |
| `EXTRA_FIELD_COLUMNS` | `{}` | JSON object mapping rez extra fields to columns, see [below](#mapping-extra-fields-to-columns) |
| `LOG_LEVEL` | `INFO` | `DEBUG` logs every batch |
| `LOG_FORMAT` | `json` | `json` (one object per line) or `text` |
| `METRICS_PORT` | `9100` | Port for `/healthz` and `/metrics`; `0` disables both |
| `SHUTDOWN_TIMEOUT_S` | `8` | Time on SIGTERM to store what was received. Keep it below the container's stop timeout (Docker's default is 10 s). |

Invalid settings stop the service at startup with an explanation, e.g. an
`AMQP_PREFETCH` smaller than `BATCH_SIZE`.

## Configuring rez

In your studio `rezconfig.py`:

```python
context_tracking_host = "rabbitmq.example:5672"
context_tracking_amqp = {
    "userid": "rez",
    "password": "secret",
    "connect_timeout": 10,
    "exchange_name": "rez.context",
    "exchange_routing_key": "REZ.CONTEXT",
    "message_delivery_mode": 2,  # persistent: survives a broker restart
}
# Optional: more context fields (defaults shown), extra top-level fields.
# context_tracking_context_fields = ["status", "timestamp", "solve_time", "load_time",
#     "from_cache", "package_requests", "implicit_packages", "resolved_packages"]
# context_tracking_extra_fields = {"show": "${SHOW}", "site": "london"}
```

If you can't change `exchange_name` and rez publishes to the AMQP default
exchange (rez's default config), set `AMQP_EXCHANGE=""`. The consumer then
reads from queues named like the routing keys (`REZ.CONTEXT.CREATED`,
`REZ.CONTEXT.SOURCED`). Messages sent before those queues exist are dropped by
the broker.

Extra fields that reference an unset environment variable (e.g. `${SHOW}` when
`SHOW` isn't set) are left out of the message by rez.

**When the broker is unreachable, rez gets slower.** rez publishes in a
background thread, but waits up to 5 seconds at exit for it to finish. Measured
with rez 3.5.0:

- connection refused (broker down, host up): about 0.6 s extra per rez process,
  plus a warning;
- host unreachable (network or DNS problem): about 4 s extra per rez process.

The events are lost in both cases. A highly available broker, or a lower
`connect_timeout`, limits the impact.

## Delivery guarantees

At-least-once from the broker to Postgres. A message is acked only after its
row is committed. If the consumer, the database or the connection fails before
that, the broker delivers the message again.

Events are lost when:

- rez can't reach the broker (see above);
- `message_delivery_mode` is `1` (rez's default) and the broker restarts while
  messages are queued;
- rez publishes before the exchange or queue exists (see [RabbitMQ](#rabbitmq)).

While the consumer is stopped, events wait in the durable queue. Watch the
queue depth if the consumer is down for long.

Duplicates are possible but rare: the row is committed but the ack doesn't
reach the broker (e.g. a connection drop at exactly that moment). The metric
`rez_ctx_consumer_ack_failures_total` counts these. rez sends no message id,
so the consumer can't deduplicate. Two identical events can also be genuine,
e.g. the same `.rxt` loaded twice.

## Health checks, metrics and alerts

`GET /healthz` on `METRICS_PORT` returns `200` while the service is ready,
connected to the broker, and storing batches. It returns `503` otherwise, with
the reason in the JSON body:

```json
{"healthy": false, "ready": true, "amqp_connected": false, "store_failing_s": null, "pending": 0}
```

`store_failing_s` is how long the current batch has been failing to store.
After 60 s the service reports unhealthy. An idle consumer is healthy. The
image's `HEALTHCHECK` runs `rez-ctx-consumer healthcheck`, which calls
`/healthz`.

`GET /metrics` serves Prometheus metrics, all prefixed `rez_ctx_consumer_`:

| Metric | Type | Meaning |
|---|---|---|
| `messages_received_total` | counter | Delivered by the broker |
| `events_stored_total` | counter | Rows written |
| `messages_rejected_total{stage}` | counter | Written to the rejects table (`parse`: not a valid rez message; `insert`: refused by Postgres) |
| `field_issues_total` | counter | Stored, but a field was NULLed or cleaned (see the logs) |
| `store_errors_total` | counter | Failed attempts to store a batch (retried) |
| `dropped_closed_channel_total` | counter | Not stored because the broker connection dropped; the broker redelivers them |
| `ack_failures_total` | counter | Stored but not acked; they will be stored again |
| `partition_maintenance_errors_total` | counter | Failed partition checks |
| `batch_size` | histogram | Messages per batch |
| `batch_store_seconds` | histogram | Time to store a batch, including retries |
| `pending_messages` | gauge | Received but not yet stored |
| `amqp_connected` | gauge | 1 while connected to the broker |
| `last_flush_timestamp_seconds` | gauge | Time of the last stored batch |

Suggested alerts:

- `/healthz` failing for more than 5 minutes.
- `increase(rez_ctx_consumer_store_errors_total[10m]) > 0`: the database has
  problems; messages are queuing up in the broker.
- `increase(rez_ctx_consumer_messages_rejected_total[1h]) > 0`: look at the
  rejects table.
- `increase(rez_ctx_consumer_partition_maintenance_errors_total[1d]) > 0`.
- No traffic during working hours,
  `increase(rez_ctx_consumer_messages_received_total[1h]) == 0`. This usually
  means a rez configuration problem, which the consumer can't detect itself.
- Broker side: the depth of `rez.context.tracking`.

## Scaling and sizing

One consumer drained a backlog at about 1,400 events per second in a local test
(Docker Desktop on a workstation). For comparison, 50,000 rez processes a day
average under one per second, and even a render farm starting thousands of jobs
at once only queues a short backlog. For high availability, run two or more
replicas on the same queue. The broker
spreads messages between them, and migrations and partition creation are safe
to run concurrently.

Storage: about 2 KB per event, indexes included, with rez's default context
fields and around 15 resolved packages. For example, 50,000 rez processes a day
come to roughly 100 MB a day, or 3 GB a month. More context fields (e.g.
`graph`) make rows much larger.

## Upgrades

Deploy the new image. On startup it applies new schema migrations automatically,
in one transaction, under a lock, so several replicas can start at once.
Migrations only add things, so an older image keeps working against a newer
schema; it logs a warning. Don't edit tables the consumer manages, except as
described below (removing partitions, adding extra-field columns and indexes).

## Partitions and removing old data

`rez_context_event` is partitioned by month on `received_at` (UTC), with
partitions named `rez_context_event_yYYYYmMM`. The consumer creates the current
and next month's partition at startup and every 6 hours. Nothing is ever
deleted automatically.

`rez_context_event_default` catches rows for months without a partition. It
should stay empty. If it isn't, the consumer moves the rows into a new
partition at its next check (or restart) and logs a warning.

To list partitions and their sizes:

```sql
SELECT c.relname AS partition,
       pg_get_expr(c.relpartbound, c.oid) AS range,
       pg_size_pretty(pg_total_relation_size(c.oid)) AS size
FROM pg_inherits i
JOIN pg_class c ON c.oid = i.inhrelid
WHERE i.inhparent = 'rez_context_event'::regclass
ORDER BY c.relname;
```

To remove a month, e.g. January 2025: detach it, optionally archive it, then
drop it.

```sql
ALTER TABLE rez_context_event DETACH PARTITION rez_context_event_y2025m01;
-- now a standalone table; archive it if needed, e.g.
--   pg_dump -t rez_context_event_y2025m01 -Fc pipeline > rez_events_2025-01.dump
DROP TABLE rez_context_event_y2025m01;
```

`DETACH` briefly locks the table, so inserts wait for a moment; the consumer
copes with that. `DETACH ... CONCURRENTLY` isn't possible, because the table has
a DEFAULT partition, and Postgres refuses it in that case.

To bring an archived month back, restore the table and attach it:

```sql
ALTER TABLE rez_context_event ATTACH PARTITION rez_context_event_y2025m01
    FOR VALUES FROM ('2025-01-01 00:00+00') TO ('2025-02-01 00:00+00');
```

## Mapping extra fields to columns

By default, everything from rez's `context_tracking_extra_fields` ends up in the
`extra` JSONB column. For fields you filter on often (show, site, department),
a real column is faster and easier to query. Example for `show`:

1. Add the column (and an index if you filter by it):

   ```sql
   ALTER TABLE rez_context_event ADD COLUMN show text;
   CREATE INDEX rez_context_event_show_received_at_idx ON rez_context_event (show, received_at);
   ```

2. Restart the consumer with `EXTRA_FIELD_COLUMNS={"show": "show"}` (payload
   key → column). New events then fill `show`, and `extra` no longer contains it.
   The consumer refuses to start if the column doesn't exist.

3. Optionally move existing values over:

   ```sql
   UPDATE rez_context_event
   SET show = extra->>'show', extra = extra - 'show'
   WHERE extra ? 'show';
   ```

The value is stored as rez sent it, so the column type must fit: `text` for
strings, `jsonb` for lists and objects. A value that doesn't fit, such as text
in an `integer` column, sends that message to the rejects table. The other
messages in the batch are stored normally.

## Rejected messages

`rez_context_event_reject` keeps messages that couldn't be stored as events,
with the raw body and the reason:

- the body isn't UTF-8 JSON, isn't an object, or has no `action` or `context`
  (`stage="parse"` in the metrics);
- Postgres refused the row, e.g. a mapped extra field of the wrong type
  (`stage="insert"`).

See [queries.md](queries.md#rejected-messages) for how to look at them. Delete
them when done, e.g. `DELETE FROM rez_context_event_reject WHERE received_at < now() - interval '30 days'`.

Messages with a single odd field (e.g. a non-numeric `solve_time`) are not
rejected. They are stored with that column NULL, the original value stays in
`context`, and the log line "stored message with field issues" says which field.

## Troubleshooting

**No events arrive.**

1. Is rez sending? Set `context_tracking_host = "stdout"` (or
   `REZ_CONTEXT_TRACKING_HOST=stdout`) and run `rez-env`. rez then prints each
   message instead of sending it. `REZ_DEBUG_CONTEXT_TRACKING=1` logs connection
   problems.
2. Does it reach the broker? In the RabbitMQ management UI, check the message
   rates of the `rez.context` exchange and of the `rez.context.tracking` queue,
   and that the queue has one consumer per replica.
3. Do the names match? rez's `exchange_name` must equal `AMQP_EXCHANGE`, and
   `exchange_routing_key` + `.CREATED` / `.SOURCED` must match
   `AMQP_BINDING_KEYS`.

**`/healthz` returns 503.** The body says why:
- `ready: false`: still starting, so check the logs for "database not reachable"
  or "broker not reachable";
- `amqp_connected: false`: the broker connection dropped, reconnect in progress;
- `store_failing_s` set: inserts are failing, the log shows the error.

**The consumer exits right after starting.** It does that on errors retrying
can't fix, and logs the reason:
- invalid configuration;
- an `EXTRA_FIELD_COLUMNS` column that doesn't exist;
- a migration file that was changed after it was applied;
- an exchange that already exists with a different type.

**Rows in `rez_context_event_default`.** Partition maintenance hasn't run or
failed (see `partition_maintenance_errors_total` and the logs). Restarting the
consumer moves the rows into proper partitions.
