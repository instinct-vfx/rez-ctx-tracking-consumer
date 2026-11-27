# Example queries

Starting points for questions people ask about rez usage. All queries run
against `rez_context_event`; the time filters on `received_at` keep them cheap,
because the table is partitioned by month on that column.

Every SQL block on this page is run against real rez messages by
`tests/integration/test_docs.py`, so the queries are known to work.

- [The table at a glance](#the-table-at-a-glance)
- [Package usage](#package-usage)
- [Failed resolves](#failed-resolves)
- [Resolve performance](#resolve-performance)
- [Users, hosts and rez versions](#users-hosts-and-rez-versions)
- [Extra fields](#extra-fields)
- [The full context (JSONB)](#the-full-context-jsonb)
- [Rejected messages](#rejected-messages)

## The table at a glance

| Column | Meaning |
|---|---|
| `received_at` | When the consumer received the event. Use this as the event time. |
| `action` | `created`: a new context (resolved, or taken from the resolve cache). `sourced`: an existing context loaded again, e.g. `rez-env --input ctx.rxt`. |
| `username`, `host` | Who and where. |
| `rez_version` | The rez version that sent the event. |
| `status` | `solved`, `failed`, … |
| `context_timestamp` | When the context was resolved. For `sourced` events this is the original resolve time, which can be days earlier. |
| `solve_time`, `load_time` | Seconds. |
| `from_cache` | The resolve came from rez's resolve cache (memcached). |
| `package_requests` | What was asked for, e.g. `{maya-2025, mtoa}`. |
| `implicit_packages` | The implicit requests, e.g. `{~platform==linux, …}`. |
| `resolved_packages` | What was resolved, as `name-version`, e.g. `{maya-2025.1, mtoa-5.4.0}`. Empty for a failed resolve. |
| `context` | The full context as rez sent it (JSONB). |
| `extra` | Fields from `context_tracking_extra_fields` that aren't mapped to their own column. |

The typed columns are NULL when rez didn't send that field (see
`context_tracking_context_fields`) or sent a value of an unexpected type. The
original value is always kept in `context`.

## Package usage

Who used an exact package version in the last 30 days. `@>` ("contains") uses
the GIN index on `resolved_packages`:

```sql
SELECT username, host, count(*) AS contexts, max(received_at) AS last_used
FROM rez_context_event
WHERE resolved_packages @> ARRAY['foo-1.0.0']
  AND received_at > now() - interval '30 days'
GROUP BY username, host
ORDER BY last_used DESC;
```

Which versions of a package are in use. rez package names can't contain `-`,
so the part before the first `-` is the name:

```sql
SELECT substr(pkg, length(split_part(pkg, '-', 1)) + 2) AS version,
       count(*) AS contexts,
       count(DISTINCT username) AS users,
       max(received_at) AS last_used
FROM rez_context_event, unnest(resolved_packages) AS pkg
WHERE split_part(pkg, '-', 1) = 'foo'
  AND received_at > now() - interval '30 days'
GROUP BY version
ORDER BY contexts DESC;
```

The most used packages this week:

```sql
SELECT split_part(pkg, '-', 1) AS package,
       count(*) AS contexts,
       count(DISTINCT username) AS users
FROM rez_context_event, unnest(resolved_packages) AS pkg
WHERE received_at > now() - interval '7 days'
GROUP BY package
ORDER BY contexts DESC
LIMIT 25;
```

Packages that haven't been used for 90 days, among those seen in the last year.
These are candidates for cleanup:

```sql
SELECT split_part(pkg, '-', 1) AS package, max(received_at) AS last_used
FROM rez_context_event, unnest(resolved_packages) AS pkg
WHERE received_at > now() - interval '1 year'
GROUP BY package
HAVING max(received_at) < now() - interval '90 days'
ORDER BY last_used;
```

What was requested, as opposed to resolved:

```sql
SELECT request, count(*) AS contexts
FROM rez_context_event, unnest(package_requests) AS request
WHERE action = 'created'
  AND received_at > now() - interval '7 days'
GROUP BY request
ORDER BY contexts DESC
LIMIT 25;
```

## Failed resolves

Recent failures with what was requested. The reason is only included if
`failure_description` is listed in rez's `context_tracking_context_fields`; it
isn't by default.

```sql
SELECT received_at, username, host, package_requests,
       context->>'failure_description' AS reason
FROM rez_context_event
WHERE status <> 'solved'
  AND received_at > now() - interval '7 days'
ORDER BY received_at DESC
LIMIT 50;
```

The failure rate per day:

```sql
SELECT date_trunc('day', received_at) AS day,
       count(*) FILTER (WHERE status <> 'solved') AS failed,
       count(*) AS total,
       round(100.0 * count(*) FILTER (WHERE status <> 'solved') / count(*), 1) AS failed_pct
FROM rez_context_event
WHERE action = 'created'
  AND received_at > now() - interval '30 days'
GROUP BY day
ORDER BY day DESC;
```

## Resolve performance

Solve times and resolve-cache hit rate per day. Only `created` events involve a
resolve:

```sql
SELECT date_trunc('day', received_at) AS day,
       count(*) AS resolves,
       round(avg(solve_time)::numeric, 3) AS avg_s,
       round((percentile_cont(0.95) WITHIN GROUP (ORDER BY solve_time))::numeric, 3) AS p95_s,
       round(avg(from_cache::int), 2) AS cache_hit_ratio
FROM rez_context_event
WHERE action = 'created'
  AND status = 'solved'
  AND received_at > now() - interval '30 days'
GROUP BY day
ORDER BY day DESC;
```

The slowest requests:

```sql
SELECT package_requests,
       count(*) AS resolves,
       round(avg(solve_time)::numeric, 3) AS avg_s,
       round(max(solve_time)::numeric, 3) AS max_s
FROM rez_context_event
WHERE action = 'created'
  AND from_cache IS NOT TRUE
  AND received_at > now() - interval '7 days'
GROUP BY package_requests
ORDER BY avg_s DESC NULLS LAST
LIMIT 20;
```

## Users, hosts and rez versions

Activity per day:

```sql
SELECT date_trunc('day', received_at) AS day,
       count(*) FILTER (WHERE action = 'created') AS created,
       count(*) FILTER (WHERE action = 'sourced') AS sourced,
       count(DISTINCT username) AS users,
       count(DISTINCT host) AS hosts
FROM rez_context_event
WHERE received_at > now() - interval '30 days'
GROUP BY day
ORDER BY day DESC;
```

Which rez versions are deployed where:

```sql
SELECT rez_version, count(DISTINCT host) AS hosts, max(received_at) AS last_seen
FROM rez_context_event
WHERE received_at > now() - interval '7 days'
GROUP BY rez_version
ORDER BY hosts DESC;
```

A user's recent environments:

```sql
SELECT received_at, action, host, package_requests, resolved_packages
FROM rez_context_event
WHERE username = 'artist'
ORDER BY received_at DESC
LIMIT 20;
```

## Extra fields

Fields from `context_tracking_extra_fields` are in `extra` unless they are
mapped to their own column (see [operations.md](operations.md#mapping-extra-fields-to-columns)).
For example, with `context_tracking_extra_fields = {"show": "${SHOW}"}`:

```sql
SELECT extra->>'show' AS show, count(*) AS contexts, count(DISTINCT username) AS users
FROM rez_context_event
WHERE extra ? 'show'
  AND received_at > now() - interval '7 days'
GROUP BY show
ORDER BY contexts DESC;
```

## The full context (JSONB)

`context` holds everything rez sent, including the full variant handles of the
resolved packages. Guard against unexpected shapes with `jsonb_typeof`; the
consumer stores whatever rez sent, so a malformed message can't break a
query. For example, which package repositories are in use:

```sql
SELECT pkg->'variables'->>'location' AS repository,
       count(*) AS uses,
       count(DISTINCT pkg->'variables'->>'name') AS packages
FROM rez_context_event,
     jsonb_array_elements(
         CASE WHEN jsonb_typeof(context->'resolved_packages') = 'array'
              THEN context->'resolved_packages' ELSE '[]'::jsonb END
     ) AS pkg
WHERE received_at > now() - interval '7 days'
GROUP BY repository
ORDER BY uses DESC;
```

Fields beyond the defaults (if added to `context_tracking_context_fields` in
rez), e.g. how many packages each resolve had to load:

```sql
SELECT received_at, username, (context->>'num_loaded_packages')::int AS loaded
FROM rez_context_event
WHERE context ? 'num_loaded_packages'
ORDER BY received_at DESC
LIMIT 20;
```

## Rejected messages

Messages that couldn't be stored as events (not JSON, no `action`, …), with the
first part of the raw body. `encode(..., 'escape')` works even when the body
isn't valid UTF-8:

```sql
SELECT received_at, routing_key, error,
       encode(substring(body FROM 1 FOR 200), 'escape') AS body_start
FROM rez_context_event_reject
ORDER BY received_at DESC
LIMIT 20;
```
