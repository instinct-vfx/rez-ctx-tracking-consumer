-- rez context tracking events, one row per AMQP message.
-- Partitioned by month on received_at. The consumer creates the monthly
-- partitions (see partitions.py); the DEFAULT partition only catches rows
-- whose month has no partition yet.

CREATE TABLE rez_context_event (
    id                 bigint GENERATED ALWAYS AS IDENTITY,
    received_at        timestamptz NOT NULL DEFAULT now(),
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
    extra              jsonb NOT NULL DEFAULT '{}',

    PRIMARY KEY (id, received_at)
) PARTITION BY RANGE (received_at);

CREATE TABLE rez_context_event_default PARTITION OF rez_context_event DEFAULT;

CREATE INDEX rez_context_event_received_at_idx
    ON rez_context_event (received_at);
CREATE INDEX rez_context_event_action_received_at_idx
    ON rez_context_event (action, received_at);
CREATE INDEX rez_context_event_username_received_at_idx
    ON rez_context_event (username, received_at);
CREATE INDEX rez_context_event_host_received_at_idx
    ON rez_context_event (host, received_at);
CREATE INDEX rez_context_event_unsolved_idx
    ON rez_context_event (status, received_at) WHERE status <> 'solved';
CREATE INDEX rez_context_event_resolved_packages_idx
    ON rez_context_event USING gin (resolved_packages);

COMMENT ON TABLE rez_context_event IS
    'rez context tracking events (rez config: context_tracking_*), one row per message';
COMMENT ON COLUMN rez_context_event.received_at IS
    'When the consumer received the message; the closest thing to event time rez provides';
COMMENT ON COLUMN rez_context_event.routing_key IS
    'AMQP routing key, e.g. REZ.CONTEXT.CREATED';
COMMENT ON COLUMN rez_context_event.action IS
    'created (new resolve or resolve-cache hit) or sourced (existing context loaded, e.g. from .rxt)';
COMMENT ON COLUMN rez_context_event.username IS
    'Payload field "user"';
COMMENT ON COLUMN rez_context_event.rez_version IS
    'rez version that sent the event (may differ from the one that created the context)';
COMMENT ON COLUMN rez_context_event.status IS
    'context.status, e.g. solved or failed';
COMMENT ON COLUMN rez_context_event.context_timestamp IS
    'context.timestamp: when the context was resolved. For sourced events this is the original resolve time';
COMMENT ON COLUMN rez_context_event.solve_time IS
    'context.solve_time in seconds';
COMMENT ON COLUMN rez_context_event.load_time IS
    'context.load_time in seconds';
COMMENT ON COLUMN rez_context_event.resolved_packages IS
    'context.resolved_packages as name-version strings; the full variant handles are in context';
COMMENT ON COLUMN rez_context_event.context IS
    'The context as sent by rez (fields per context_tracking_context_fields)';
COMMENT ON COLUMN rez_context_event.extra IS
    'Top-level payload keys from context_tracking_extra_fields that are not mapped to their own column';


-- Messages that could not be stored as events (not JSON, missing action, ...).
CREATE TABLE rez_context_event_reject (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    received_at  timestamptz NOT NULL DEFAULT now(),
    routing_key  text NOT NULL,
    body         bytea NOT NULL,
    error        text NOT NULL
);

CREATE INDEX rez_context_event_reject_received_at_idx
    ON rez_context_event_reject (received_at);

COMMENT ON TABLE rez_context_event_reject IS
    'Messages the consumer could not store as rez_context_event rows, with the raw body';
