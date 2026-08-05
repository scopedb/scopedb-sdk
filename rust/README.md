# ScopeDB SDK for Rust

`scopedb-client` is an async Rust client for ScopeDB. It supports statements,
read-only REST catalog discovery, direct NDJSON table appends, bounded concurrent
streaming writes, and transform-oriented JSON ingest.

## Installation

```sh
cargo add scopedb-client serde_json
cargo add tokio --features macros,rt-multi-thread
```

## Create a client

The SDK accepts a configured HTTP client, so applications can own TLS, timeouts,
authentication headers, and connection pooling. Use the compatible reqwest
version re-exported as `scopedb_client::reqwest`; a separate direct reqwest
dependency is not needed.

```rust
use scopedb_client::Client;

let client = Client::new(
    "http://127.0.0.1:6543",
    scopedb_client::reqwest::Client::new(),
)?;
# Ok::<(), scopedb_client::Error>(())
```

The runnable examples read authentication from `SCOPEDB_API_KEY`. For backward
compatibility, they fall back to `SCOPEDB_TOKEN` when the API key variable is
unset or empty. The shared helper marks the resulting authorization header as
sensitive so standard header and request `Debug` formatting redacts the
credential.

## ScopeQL

This SDK sends ScopeQL statements to ScopeDB; the language is documented
separately. Use these canonical entry points:

- [Quickstart](https://docs.scopedb.io/guides/quickstart)
- [Query guide](https://docs.scopedb.io/guides/query-events)
- [Language reference](https://docs.scopedb.io/reference/)

## Run a statement

```rust
# async fn demo() -> Result<(), scopedb_client::Error> {
# let client = scopedb_client::Client::new("http://127.0.0.1:6543", scopedb_client::reqwest::Client::new())?;
let result = client.statement("SELECT 1".to_string()).execute().await?;
let rows = result.into_values()?;
println!("{rows:?}");
# Ok(())
# }
```

## Browse the catalog

Catalog list methods return one page and preserve the opaque continuation token.
Fetch methods return the full database, schema, or table resource.

```rust
use scopedb_client::CatalogListOptions;

# async fn demo() -> Result<(), scopedb_client::Error> {
# let client = scopedb_client::Client::new("http://127.0.0.1:6543", scopedb_client::reqwest::Client::new())?;
let databases = client
    .list_databases(CatalogListOptions {
        page_size: Some(100),
        page_token: None,
    })
    .await?;

let database = client.fetch_database("scopedb").await?;
let schemas = client
    .list_schemas("scopedb", CatalogListOptions::default())
    .await?;
let schema = client.fetch_schema("scopedb", "public").await?;
let tables = client
    .list_tables("scopedb", "public", CatalogListOptions::default())
    .await?;
let table = client
    .fetch_table("scopedb", "public", "events")
    .await?;

println!("{} {} {}", database.name, schema.name, table.name);
println!("first page: {} databases, {} schemas, {} tables", databases.items.len(), schemas.items.len(), tables.items.len());
# Ok(())
# }
```

See [`examples/catalog.rs`][catalog-example] for complete database pagination
and table metadata discovery.

## Streaming writes with NDJSON

The table write API accepts newline-delimited JSON only: each line is one JSON
row object. It does not accept a JSON array. The destination table must already
exist. `Table` uses `scopedb` and `public` when the database or schema is not
specified.

### Direct append

Use direct append when the caller already owns one exact NDJSON request boundary.

```rust
use scopedb_client::Client;

# async fn demo() -> Result<(), scopedb_client::Error> {
# let client = Client::new("http://127.0.0.1:6543", scopedb_client::reqwest::Client::new())?;
let table = client
    .table("events")
    .with_database("scopedb")
    .with_schema("public");

let ndjson = [
    serde_json::json!({"id": 1, "name": "first"}),
    serde_json::json!({"id": 2, "name": "second"}),
]
.iter()
.map(serde_json::to_string)
.collect::<Result<Vec<_>, _>>()
.expect("example rows serialize")
.join("\n");

let result = table.append(ndjson).await?;
println!("committed remotely: {} rows", result.num_rows_inserted);
# Ok(())
# }
```

A failed append exposes structured details through `Error::append_details()`:

```rust
use scopedb_client::AppendState;
use scopedb_client::ErrorKind;

# fn inspect(error: &scopedb_client::Error) {
if error.kind() == ErrorKind::AppendRowsFailed {
    if let Some(details) = error.append_details() {
        match details.append_state {
            AppendState::Rejected => {
                eprintln!("request rejected: {:?}", details.row_errors);
            }
            AppendState::Unknown => {
                eprintln!("rows may have committed; reconcile before replaying");
            }
            AppendState::Committed => {}
        }
    }
}
# }
```

`Unknown` is deliberately different from a rejection: replaying the same rows
may insert duplicates.

### Asynchronous append stream

Use `append_stream()` for continuous or large producers. The stream serializes
records to NDJSON, batches by size or time, bounds pending bytes, and sends a
bounded number of HTTP append requests concurrently.

```rust
use std::time::Duration;

# async fn demo() -> Result<(), scopedb_client::Error> {
# let client = scopedb_client::Client::new("http://127.0.0.1:6543", scopedb_client::reqwest::Client::new())?;
let table = client.table("events").with_schema("public");
let stream = table
    .append_stream()
    .batch_bytes(4 * 1024 * 1024)
    .flush_interval(Duration::from_secs(1))
    .max_in_flight_requests(4)
    .max_pending_bytes(64 * 1024 * 1024)
    .build()?;

let admitted = stream
    .send_all([
        serde_json::json!({"id": 1, "name": "first"}),
        serde_json::json!({"id": 2, "name": "second"}),
    ])
    .await?;
println!("accepted locally: {} rows", admitted.accepted_rows);

// Remote delivery barrier for every row accepted before this call.
let report = stream.flush().await?;
println!("committed remotely: {} rows", report.committed_rows);

// Closes admission, flushes remaining rows, and settles in-flight requests.
stream.shutdown().await?;
# Ok(())
# }
```

`send()` and `send_all()` wait for local admission capacity only; they do not
wait for a remote commit. Feed an iterator sequentially instead of spawning one
task per row, which would move the unbounded backlog outside the stream.
`flush()` and `shutdown()` are remote delivery barriers.

Once a `flush()` future has enqueued its barrier, dropping that future does not
cancel remote settlement. Keep it alive to receive the interval report; if a
task is cancelled, inspect `stats().last_report` and lifetime counters before
deciding how to reconcile the covered rows.

The default `AppendFailurePolicy::Stop` is strict: the first failed batch stops
admission, and a successful barrier confirms that its accepted prefix committed.
Use `max_in_flight_requests(1)` if request submission order matters; concurrent
batches have no defined commit order.

### Best-effort telemetry and logs

Telemetry producers often cannot wait for admission or stop permanently after
one unavailable batch. Opt into `Continue`, use `try_send()` on the hot path,
and inspect settlement reports and lifetime stats.

```rust
use std::time::Duration;
use scopedb_client::AppendDeliveryOutcome;
use scopedb_client::AppendFailurePolicy;

# async fn demo() -> Result<(), scopedb_client::Error> {
# let client = scopedb_client::Client::new("http://127.0.0.1:6543", scopedb_client::reqwest::Client::new())?;
let telemetry = client
    .table("events")
    .append_stream()
    .failure_policy(AppendFailurePolicy::Continue)
    .flush_interval(Duration::from_secs(1))
    .on_batch_failure(|event| eprintln!("append failure: {event:?}"))
    .build()?;

if let Err(error) = telemetry.try_send(&serde_json::json!({
    "name": "request.completed",
    "status": 200,
})) {
    // Send this diagnostic to a different sink.
    eprintln!("telemetry row dropped locally: {error}");
}

let report = telemetry.shutdown().await?;
if report.outcome != AppendDeliveryOutcome::Ok {
    eprintln!("telemetry loss or ambiguity: {report:?}");
}
# Ok(())
# }
```

`Ok(())` from `try_send()` still means local admission, not remote commit. It
returns immediately with an error when serialization fails, the row is invalid
or too large, the buffer is full, the circuit is open, or the stream is closed.
Continue mode releases failed batches after accounting for them; it is not an
in-memory retry queue.

The circuit breaker rejects non-blocking `try_send()` calls while open. The
backpressured `send()` path can still admit rows within the configured memory
budget; their dispatch waits for the circuit probe, so use `try_send()` on
latency-sensitive logging and telemetry paths.

A continue-mode report classifies rows as committed, failed, unknown, or locally
dropped. For accepted rows in a completed report:

```text
accepted_rows = committed_rows + failed_rows + unknown_rows
```

`AppendDeliveryOutcome::Partial` means at least one row committed while another
row failed, was dropped, or remains unknown. With no committed rows, the outcome
is `Unknown` when any batch may have committed and `Failed` otherwise. Only a
loss-free report is `Ok`.

The stream retries only the exact temporary HTTP batch explicitly reported as
`Rejected`. A timeout, transport failure, or invalid success response is
`Unknown` and is never automatically retried. An in-memory stream is not a
durable queue; use an outbox when payloads must survive process failure or be
available for reconciliation.

### Choose a delivery path

| Workload | Admission and delivery | Example |
| --- | --- | --- |
| One exact NDJSON payload | Caller owns the request boundary | [`append.rs`][append-example] |
| Basic asynchronous batching | SDK owns batches; strict barriers | [`append_stream.rs`][append-stream-example] |
| Backfill or file import | Bounded producer memory and concurrent strict batches | [`bulk_append.rs`][bulk-append-example] |
| Long-running logs and events | Non-blocking continue mode with observable loss | [`telemetry.rs`][telemetry-example] |
| SQL transformation before insert | Transform-oriented ingest stream | [`ingest_transform.rs`][ingest-transform-example] |

## Table helper

```rust
# async fn demo() -> Result<(), scopedb_client::Error> {
# let client = scopedb_client::Client::new("http://127.0.0.1:6543", scopedb_client::reqwest::Client::new())?;
let table = client.table("events").with_schema("public");
println!("identifier = {}", table.identifier());

let schema = table.table_schema().await?;
println!("fields = {}", schema.fields().len());
# Ok(())
# }
```

## Transform-oriented ingest

`IngestStream` remains useful when input JSON needs a SQL transform before
insertion. Prefer `Table::append` or `Table::append_stream` when records already
match the destination table.

```rust
# async fn demo() -> Result<(), scopedb_client::Error> {
# let client = scopedb_client::Client::new("http://127.0.0.1:6543", scopedb_client::reqwest::Client::new())?;
let stream = client
    .ingest_stream(
        r#"
        SELECT
            $0["ts"]::timestamp as occurred_at,
            $0["name"]::string as name
        INSERT INTO public.events (occurred_at, name)
        "#,
    )
    .build();

stream
    .send(&serde_json::json!({
        "ts": "2026-03-13T12:00:00Z",
        "name": "ScopeDB",
    }))
    .await?;
stream.shutdown().await?;
# Ok(())
# }
```

## Examples and development

The [example guide][example-guide] includes setup, safety guards, delivery
contracts, and runnable commands.

```sh
cargo check --examples
cargo test
cargo clippy --all-targets --all-features
```

The wire-level endpoint and payload reference is in
[`docs/rust-http-api.md`][rust-http-api].
Release history and the maintainer runbook are in
[`CHANGELOG.md`][changelog] and [`RELEASE.md`][release].

[append-example]: https://github.com/scopedb/scopedb-sdk/blob/main/rust/examples/append.rs
[append-stream-example]: https://github.com/scopedb/scopedb-sdk/blob/main/rust/examples/append_stream.rs
[bulk-append-example]: https://github.com/scopedb/scopedb-sdk/blob/main/rust/examples/bulk_append.rs
[catalog-example]: https://github.com/scopedb/scopedb-sdk/blob/main/rust/examples/catalog.rs
[changelog]: https://github.com/scopedb/scopedb-sdk/blob/main/rust/CHANGELOG.md
[example-guide]: https://github.com/scopedb/scopedb-sdk/blob/main/rust/examples/README.md
[ingest-transform-example]: https://github.com/scopedb/scopedb-sdk/blob/main/rust/examples/ingest_transform.rs
[release]: https://github.com/scopedb/scopedb-sdk/blob/main/rust/RELEASE.md
[rust-http-api]: https://github.com/scopedb/scopedb-sdk/blob/main/docs/rust-http-api.md
[telemetry-example]: https://github.com/scopedb/scopedb-sdk/blob/main/rust/examples/telemetry.rs
