# Minimal consumer

[consumer.py](consumer.py) is a single-file version of this project, under 200
lines including comments: it listens for rez context tracking events on RabbitMQ and writes each
one to Postgres. Use it to see how the pieces fit together, or as a starting
point if the full service is more than you need.

```sh
pip install -r requirements.txt
export AMQP_URL=amqp://rez:rez@localhost:5672/
export PG_DSN=postgresql://rez:rez@localhost:5432/rez
python consumer.py
```

The defaults suit a local Postgres and RabbitMQ with user and password `rez`
(for example `docker run -e POSTGRES_USER=rez -e POSTGRES_PASSWORD=rez
-e POSTGRES_DB=rez -p 5432:5432 postgres:17`). The rez configuration is the same as
in the main [README](../../README.md#configuring-rez). Start the script once before rez sends
anything: it creates the queue, and until a queue exists RabbitMQ drops the
messages.

It writes the same columns as the full consumer to a `rez_context_event` table
(created if missing, not partitioned), so most of the queries in
[docs/queries.md](../../docs/queries.md) work with it as well, and so do the
example Grafana dashboards of
[rez-in-a-box](https://github.com/instinct-vfx/rez-in-a-box). The exceptions
are the queries about rejected messages and partitions.

## What it does

- Declares a durable topic exchange `rez.context` and a durable queue bound to
  `REZ.CONTEXT.*`, so events wait in the queue while the script isn't running.
- Inserts and commits each message, then acks it. If the script dies in
  between, the message is delivered again: you may get a rare duplicate, but
  you don't lose events.
- Uses the time of arrival as `received_at`. rez doesn't send an event time;
  `context.timestamp` is when the context was resolved, which for `sourced`
  events can be days earlier.
- Turns the resolved variant handles into `name-version` strings
  (`resolved_packages`), so "who used maya-2025.3" is an indexed array lookup.
  The full context is kept in a `jsonb` column, and the
  `context_tracking_extra_fields` go to `extra`.
- Reads `NaN` and `Infinity` (which Python's `json` writes, but `jsonb` rejects)
  as null.
- Logs and drops messages it can't store: invalid JSON, no `action` or
  `context`, or text Postgres can't store (a NUL character). Requeueing them
  would only fail again.
- If the database or broker goes away, it reconnects every 5 seconds. The
  message being handled stays in the queue.

## What it leaves out

The full consumer in `src/` adds:

- batched inserts, for much higher throughput;
- a rejects table that keeps undecodable messages instead of dropping them;
- cleaning of NUL characters and invalid surrogates, and checks on field types
  (a wrong type becomes NULL instead of failing the insert);
- monthly partitions, so old data can be dropped cheaply;
- configuration for rez's default exchange, extra fields mapped to their own
  columns, health checks, Prometheus metrics, and a Docker image.

Tested with Python 3.10 and pika 1.5. pika 1.5 warns that `BlockingConnection` is
deprecated in favour of its new thread-based `Connection`; it still works and
remains available until pika 2.0.
