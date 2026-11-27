"""A minimal rez context tracking consumer: RabbitMQ -> Postgres.

One message, one row, one commit. No batching, metrics or partitions; see the
main project for those. Writes the same columns as the main consumer, so the
example Grafana dashboards of rez-in-a-box work with this table too.

    pip install pika psycopg2-binary
    AMQP_URL=amqp://rez:rez@localhost:5672/ PG_DSN=postgresql://rez:rez@localhost/rez \
        python consumer.py

rez config (rezconfig.py) to match:

    context_tracking_host = "localhost:5672"
    context_tracking_amqp = {
        "userid": "rez",
        "password": "rez",
        "exchange_name": "rez.context",
        "exchange_routing_key": "REZ.CONTEXT",
        "message_delivery_mode": 2,  # persistent
    }
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import pika
import psycopg2
from psycopg2.extras import Json

AMQP_URL = os.environ.get("AMQP_URL", "amqp://rez:rez@localhost:5672/")
PG_DSN = os.environ.get("PG_DSN", "postgresql://rez:rez@localhost:5432/rez")
EXCHANGE = "rez.context"  # context_tracking_amqp["exchange_name"]
QUEUE = "rez.context.tracking"
BINDING_KEY = "REZ.CONTEXT.*"  # rez sends REZ.CONTEXT.CREATED / REZ.CONTEXT.SOURCED

SCHEMA = """
CREATE TABLE IF NOT EXISTS rez_context_event (
    id                 bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    received_at        timestamptz NOT NULL,
    routing_key        text NOT NULL,
    action             text NOT NULL,
    host               text,
    username           text,
    rez_version        text,
    status             text,
    context_timestamp  timestamptz,
    solve_time         double precision,
    load_time          double precision,
    from_cache         boolean,
    package_requests   text[],
    implicit_packages  text[],
    resolved_packages  text[],
    context            jsonb NOT NULL,
    extra              jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS rez_context_event_received_at_idx
    ON rez_context_event (received_at);
CREATE INDEX IF NOT EXISTS rez_context_event_resolved_packages_idx
    ON rez_context_event USING gin (resolved_packages);
"""

INSERT = """
INSERT INTO rez_context_event (
    received_at, routing_key, action, host, username, rez_version, status,
    context_timestamp, solve_time, load_time, from_cache, package_requests,
    implicit_packages, resolved_packages, context, extra
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

log = logging.getLogger("rez-consumer")


def to_row(routing_key, body):
    # Postgres jsonb rejects NaN/Infinity (Python's json writes them), so read them as null.
    data = json.loads(body, parse_constant=lambda name: None)
    action = data.pop("action")
    context = data.pop("context")
    timestamp = context.get("timestamp")  # when the context was resolved, epoch seconds
    # Variant handles -> "name-version", which makes "who used maya-2025.3" an
    # indexed array lookup. The full handles stay in the context column.
    resolved = [
        "-".join(filter(None, (p["variables"]["name"], p["variables"].get("version"))))
        for p in context.get("resolved_packages") or []
    ]
    return (
        # rez sends no event time; the time of arrival is the best there is.
        # (context.timestamp is the resolve time: for "sourced" events that is
        # when the saved context was originally resolved.)
        datetime.now(timezone.utc),
        routing_key,
        action,
        data.pop("host", None),
        data.pop("user", None),
        data.pop("rez_version", None),
        context.get("status"),
        datetime.fromtimestamp(timestamp, timezone.utc) if timestamp else None,
        context.get("solve_time"),
        context.get("load_time"),
        context.get("from_cache"),
        context.get("package_requests"),
        context.get("implicit_packages"),
        resolved,
        Json(context),
        Json(data),  # whatever is left: context_tracking_extra_fields, e.g. {"show": ...}
    )


def consume(db, connection):
    channel = connection.channel()
    # Durable exchange and queue: rez's messages wait in the queue while this
    # script is not running. Start it once before rez sends anything, though:
    # an exchange without a bound queue drops messages.
    channel.exchange_declare(EXCHANGE, exchange_type="topic", durable=True)
    channel.queue_declare(QUEUE, durable=True)
    channel.queue_bind(QUEUE, EXCHANGE, routing_key=BINDING_KEY)
    channel.basic_qos(prefetch_count=100)

    def on_message(ch, method, properties, body):
        try:
            row = to_row(method.routing_key, body)
            with db.cursor() as cur:
                cur.execute(INSERT, row)
            db.commit()
        except Exception:
            if db.closed:
                raise  # lost the database: stop without ack, the message stays queued
            # Roll back only on a live connection; on a closed one that would raise
            # and hide the original error.
            db.rollback()
            # Not JSON, no "action"/"context", a NUL character in a string, ...
            # Acked anyway: requeueing would just fail again, forever.
            log.exception("dropping message: %r", body[:500])
        # Ack only after the commit: a crash before this means the message is
        # delivered again (at least once; a rare duplicate rather than a loss).
        ch.basic_ack(method.delivery_tag)

    channel.basic_consume(QUEUE, on_message)
    log.info("consuming from %s", QUEUE)
    channel.start_consuming()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("pika").setLevel(logging.WARNING)  # pika logs every connect step at INFO
    while True:
        db = connection = None
        try:
            db = psycopg2.connect(PG_DSN)
            with db, db.cursor() as cur:
                cur.execute(SCHEMA)
            connection = pika.BlockingConnection(pika.URLParameters(AMQP_URL))
            consume(db, connection)
        except (pika.exceptions.AMQPError, psycopg2.Error) as exc:
            log.warning("connection lost (%r), retrying in 5 s", exc)
            time.sleep(5)
        except KeyboardInterrupt:
            break
        finally:
            for conn in (connection, db):
                try:
                    if conn is not None:
                        conn.close()
                except Exception:
                    pass  # already closed or broken


if __name__ == "__main__":
    main()
