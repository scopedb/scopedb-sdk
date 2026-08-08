# ScopeDB Go SDK examples

Run these examples from the `go/` directory in a source checkout. They use only
the public SDK API.

## Read-only paths

| Example | Shows | Run |
| --- | --- | --- |
| [`statement`](statement) | Query conversion, an optional caller-provided ID and execution timeout, and the asynchronous statement handle lifecycle | `go run ./examples/statement` |
| [`catalog`](catalog) | Lazy REST catalog pagination and table metadata | `go run ./examples/catalog` |

The shared helper reads:

- `SCOPEDB_ENDPOINT` (defaults to `http://127.0.0.1:6543`)
- `SCOPEDB_API_KEY` (falls back to `SCOPEDB_TOKEN`)
- `SCOPEDB_DATABASE` (defaults to `scopedb`)
- `SCOPEDB_SCHEMA` (defaults to `public`)

ScopeQL is documented in the [quickstart], [query guide], and [language
reference].

[quickstart]: https://docs.scopedb.io/guides/quickstart
[query guide]: https://docs.scopedb.io/guides/query-events
[language reference]: https://docs.scopedb.io/reference/

## Before running a write example

Every write example refuses to start unless `SCOPEDB_WRITE_TABLE` names an
existing, unqualified destination table. Configure its database and schema
separately. The examples never create or drop a table. Use a disposable table
and confirm the configured database and schema before running them:

```sh
export SCOPEDB_WRITE_TABLE=sdk_example_events
```

The example rows use the following columns so all write journeys can target the
same table:

| Column | Value used by examples |
| --- | --- |
| `id` | integer |
| `event_id` | string |
| `occurred_at` | timestamp |
| `name` | string |
| `attributes` | object |

## Choose a write journey

For most application writes, start with `append_stream`. It accepts typed rows
and owns their encoding, batching, backpressure, and request concurrency.

| Example | Choose it when | Run |
| --- | --- | --- |
| [`append_stream`](append_stream) | Typed application rows need asynchronous batching with strict delivery | `go run ./examples/append_stream` |
| [`bulk_append`](patterns/bulk_append) | A backfill needs bounded memory and concurrent strict batches | `go run ./examples/patterns/bulk_append` |
| [`telemetry`](patterns/telemetry) | Logs or events need non-blocking, observable best-effort delivery | `go run ./examples/patterns/telemetry` |
| [`append_ndjson`](append_ndjson) | The caller already owns one exact raw NDJSON request body | `go run ./examples/append_ndjson` |

## Delivery contract

- `Table.AppendNDJSON` sends one caller-encoded raw NDJSON body: one JSON object
  per non-empty line, not a JSON array.
- `Table.AppendStream` accepts typed rows and owns their JSON encoding, batching,
  and concurrent request scheduling.
- `AppendStream.Send` and `AppendStream.TrySend` confirm local admission only.
  They do not confirm a remote commit.
- Append stream admission is safe for concurrent producers; use a fixed worker
  pool instead of starting one goroutine per row.
- `TrySend` does not wait for stream capacity. Use it for latency-sensitive
  logs and telemetry and monitor `Stats().DroppedByReason`.
- `Flush` settles the prefix accepted before its barrier. `Shutdown` closes
  admission and settles all accepted rows.
- A successful strict append-stream barrier confirms its accepted prefix
  committed. A continue-mode barrier is settlement; inspect every delivery
  report for failed, unknown, and locally dropped rows.
- The append stream retries only an exact temporary HTTP batch explicitly
  reported as rejected. A timeout, transport failure, or invalid success
  response is unknown and is not automatically retried.
- Unknown rows may already be committed. Never blindly replay the same payload;
  reconcile it or use an application-owned durable outbox.
- Each background write request has a finite 30-second timeout by default. A
  timeout makes that request's commit outcome unknown.
- Concurrent append batches have no defined commit order. Set
  `MaxConcurrentBatches: 1` when request submission must be serial.

## Advanced: server-side transformation

Use [`ingest_transform`](ingest_transform) only when source JSON specifically
needs a ScopeQL transformation before it can match the destination table. For
normal typed events, use `Table.AppendStream`.

```sh
go run ./examples/ingest_transform
```

`IngestStream.Send` confirms local admission only. An ingest result returned
with an error counts only earlier confirmed batches; it is not a safe offset
for replaying the failing batch. Reconcile an unknown outcome before replaying
records.

## Compile every example

```sh
go test ./...
```
