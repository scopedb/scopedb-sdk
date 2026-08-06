# Rust HTTP API reference

This document describes the HTTP surface modeled by the Rust SDK and the
delivery rules that its higher-level helpers preserve. Public endpoints are
rooted at `/v1`.

## REST catalog API

Catalog endpoints are read-only. Every user-provided database, schema, and
table name occupies one URL path segment; the SDK percent-encodes those segments.

### Databases

```text
GET /v1/databases
GET /v1/databases/{database}
```

### Schemas

```text
GET /v1/databases/{database}/schemas
GET /v1/databases/{database}/schemas/{schema}
```

### Tables

```text
GET /v1/databases/{database}/schemas/{schema}/tables
GET /v1/databases/{database}/schemas/{schema}/tables/{table}
```

### Pagination

All list endpoints accept:

- `page_size`: optional integer from 1 through 1000; the default is 100
- `page_token`: optional opaque token returned by the previous page

The Rust SDK exposes these as `CatalogListOptions`. A list response has the same
shape for every resource type:

```json
{
  "items": [],
  "next_page_token": "opaque-token"
}
```

`next_page_token` is omitted when there is no next page. Clients must pass it
back unchanged and must not parse or synthesize it.

`Client::iterate_databases`, `iterate_schemas`, and `iterate_tables` follow the
token automatically, fetch pages lazily, and reject a repeated token instead of
looping forever. The `list_*` methods expose one explicit page when an
application needs page boundaries.

### Resource shapes

Database:

```json
{
  "name": "scopedb",
  "comment": "optional description"
}
```

Schema:

```json
{
  "database": "scopedb",
  "name": "public",
  "comment": "optional description"
}
```

Table list endpoints return summaries:

```json
{
  "database": "scopedb",
  "schema": "public",
  "name": "events",
  "comment": "optional description"
}
```

Fetching one table returns its full public specification:

```json
{
  "database": "scopedb",
  "schema": "public",
  "name": "events",
  "columns": [
    {
      "name": "occurred_at",
      "data_type": "timestamp",
      "comment": null
    }
  ],
  "partition_by": [],
  "cluster_by": [],
  "distinct_on": {
    "on": [],
    "by": []
  },
  "data_retention_days": null,
  "comment": null
}
```

## Streaming write API

The streaming write API appends rows that already match an existing table. It
supports NDJSON only.

### `POST /v1/databases/{database}/schemas/{schema}/tables/{table}/rows`

Required request header:

```http
Content-Type: application/x-ndjson
```

Request body:

```ndjson
{"id":1,"name":"first"}
{"id":2,"name":"second"}
```

Each line is one complete JSON row object. A JSON array is not a valid
replacement for multiple NDJSON lines. The body must not be empty. One request
is limited to 16 MiB and 200,000 rows.

A committed response is:

```json
{
  "append_state": "committed",
  "num_rows_inserted": 2
}
```

The Rust SDK accepts success only when `append_state` is `committed` and the
inserted row count is valid. A malformed or contradictory success response has
an unknown commit outcome.

### Structured append errors

An append failure uses this payload when the outcome is known:

```json
{
  "message": "row validation failed",
  "append_state": "rejected",
  "row_errors": [
    {
      "row_index": 0,
      "column": "id",
      "message": "invalid value"
    }
  ],
  "row_errors_truncated": false
}
```

`append_state` has three meanings:

- `committed`: all request rows committed
- `rejected`: no request row committed
- `unknown`: the commit outcome cannot be determined

`row_index` is zero-based within the submitted NDJSON request. The server may
truncate the row-error list; `row_errors_truncated` preserves that fact.

Transport errors, response-body read failures, attempt timeouts, and malformed
responses are classified as `unknown`, because the request may have reached the
commit path. Replaying an unknown payload may insert duplicates.

The asynchronous append stream retries only the same HTTP batch when both of
these conditions hold:

1. The structured response explicitly says `append_state: "rejected"`.
2. The HTTP failure is temporary.

It never automatically retries an unknown batch. Direct `Table::append` and
`Client::append_rows` return the structured error to the caller and do not own a
retry loop.

### Client-side batching and barriers

`Table::append_stream` is a client-side batching layer over the rows endpoint;
it is not a separate HTTP endpoint.

- Every accepted Rust value is serialized to exactly one NDJSON line.
- `send` and `send_all` wait for local admission capacity, not a remote commit.
- `try_send` attempts local admission immediately.
- `target_batch_bytes`, `max_batch_rows`, and the flush interval seal batches.
- `max_concurrent_batches` bounds concurrent append requests.
- `max_buffered_bytes` bounds accepted serialized data that has not settled.
- `flush` settles all rows accepted before its barrier.
- `shutdown` closes admission and settles the final accepted prefix.

The older `batch_bytes`, `max_in_flight_requests`, and `max_pending_bytes`
builder names remain source-compatible deprecated aliases.

With the default `Stop` failure policy, a failed batch makes the stream terminal
and barriers return an error. With `Continue`, rejected and unknown batches are
accounted for and released so later batches can proceed. Continue-mode barriers
return an `AppendDeliveryReport`; they do not imply that every row committed.

Concurrent batches do not have a defined commit order. Set the in-flight limit
to one when requests must be submitted serially. Neither policy provides a
stream-wide transaction, rollback, durable replay queue, or idempotency.

## Statement API

### `POST /v1/statements`

Submits a statement for execution.

```json
{
  "statement_id": "uuid-v7-or-user-provided",
  "statement": "SELECT 1",
  "exec_timeout": "PT1S",
  "max_parallelism": 16,
  "format": "json"
}
```

Request fields:

- `statement_id`: optional from the SDK perspective
- `statement`: required
- `exec_timeout`: optional
- `max_parallelism`: optional
- `format`: `json` for the public Rust SDK

The response is a tagged statement-state payload: `pending`, `running`,
`finished`, `failed`, or `cancelled`. Statement failure and cancellation are
in-band states, so HTTP success does not imply statement success.

### `GET /v1/statements/{statement_id}?format=json`

Fetches the latest state for a submitted statement and returns the same state
payload family as statement submission. `StatementHandle::status().await`
performs at most one fetch and updates the snapshot returned by
`StatementHandle::last_status`; terminal snapshots are returned without another
request. `StatementHandle::wait` polls with bounded exponential delay until a
terminal state. The older `fetch_once` and `fetch` names remain deprecated
aliases for `status` and `wait`, respectively.

### `POST /v1/statements/{statement_id}/cancel`

Cancels a pending or running statement. The response contains the post-cancel
terminal status view:

```json
{
  "statement_id": "uuid",
  "status": "finished|failed|cancelled",
  "message": "statement is ...",
  "created_at": "timestamp"
}
```

### Statement result shape

A finished statement contains a JSON result set:

```json
{
  "status": "finished",
  "statement_id": "uuid",
  "created_at": "timestamp",
  "progress": {},
  "result_set": {
    "metadata": {
      "fields": [
        { "name": "col", "data_type": "string" }
      ],
      "num_rows": 1
    },
    "format": "json",
    "rows": [["value"]]
  }
}
```

## Transform-oriented ingest API

### `POST /v1/ingest`

Ingests JSON lines through a transformation statement.

```json
{
  "type": "committed",
  "data": {
    "format": "json",
    "rows": "{\"k\":1}\n{\"k\":2}"
  },
  "statement": "SELECT ... INSERT INTO target_table"
}
```

The Rust SDK uses committed ingest with JSON-line data. A successful response is:

```json
{
  "num_rows_inserted": 2
}
```

This endpoint is useful when each input record needs a SQL transform. For rows
already shaped like a table, use the streaming write API.

## Generic error responses

Non-append non-2xx responses generally use:

```json
{
  "message": "..."
}
```

The SDK distinguishes transport or deserialization errors, non-2xx server
errors, structured append outcomes, and in-band statement terminal states.
Server messages remain unchanged in `Error::message()`. When available,
`http_status()`, `request_id()`, and `retry_after()` expose response metadata;
`is_retryable()` includes an explicit `retryable` value from direct or nested
error envelopes before falling back to HTTP status classification.

`Retry-After` accepts delta seconds and HTTP dates. Streaming writes use it only
for a temporary append explicitly reported as `rejected`, and cap the delay at
the configured maximum backoff. Unknown append outcomes are never retried.
