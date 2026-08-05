# ScopeDB Rust SDK examples

Start with a quickstart, then choose a write pattern whose delivery tradeoffs
match the workload. Every example uses only the public `scopedb-client` API.

The commands below run from the [`rust/`](../) directory in a source checkout.

## Read-only discovery

These examples can run against a reachable ScopeDB endpoint without modifying
data.

| Example | Shows | Run |
| --- | --- | --- |
| [`statement.rs`](statement.rs) | Statement execution and typed result values | `cargo run --example statement` |
| [`catalog.rs`](catalog.rs) | REST catalog pagination and full table metadata | `cargo run --example catalog` |
| [`table.rs`](table.rs) | Quoted table identifiers and the table schema helper | `cargo run --example table` |

## Before running a write example

Every write example refuses to start unless `SCOPEDB_TABLE` names an existing,
disposable table. Do not point these examples at production unless the writes
are intentional.

The append, stream, telemetry, and transform examples can share this schema:

```sql
CREATE TABLE public.sdk_example_events (
  id int,
  event_id string,
  occurred_at timestamp,
  name string,
  attributes object
);
```

Configuration comes from:

- `SCOPEDB_ENDPOINT` (defaults to `http://127.0.0.1:6543`)
- `SCOPEDB_TOKEN` (optional Bearer token)
- `SCOPEDB_DATABASE` (defaults to `scopedb`)
- `SCOPEDB_SCHEMA` (defaults to `public`)
- `SCOPEDB_TABLE` (required for writes)

Set the disposable destination once before running a write example:

```sh
export SCOPEDB_TABLE=sdk_example_events
```

```powershell
$env:SCOPEDB_TABLE = "sdk_example_events"
```

## Choose a write journey

| Example | Choose it when | Run |
| --- | --- | --- |
| [`append.rs`](append.rs) | The caller owns one exact NDJSON request boundary | `cargo run --example append` |
| [`append_stream.rs`](append_stream.rs) | The SDK should asynchronously batch rows with strict delivery | `cargo run --example append_stream` |
| [`bulk_append.rs`](bulk_append.rs) | A backfill needs bounded memory and concurrent strict batches | `cargo run --example bulk_append` |
| [`telemetry.rs`](telemetry.rs) | Logs or events need non-blocking, observable best-effort delivery | `cargo run --example telemetry` |
| [`ingest_transform.rs`](ingest_transform.rs) | JSON records need a SQL transform before insertion | `cargo run --example ingest_transform` |

`append.rs` sends exactly one NDJSON request. `append_stream.rs` uses the
default `Stop` policy: `send()` and `send_all()` wait only for local admission,
while a successful `flush()` or `shutdown()` is a remote commit barrier for the
accepted prefix.

`bulk_append.rs` keeps producer memory bounded and sends multiple HTTP batches
concurrently. It does not add durable resume, idempotency, transactionality, or
whole-job rollback. Earlier concurrent batches may have committed even when a
later batch fails.

`telemetry.rs` opts into `AppendFailurePolicy::Continue` and uses `try_send()`
on the request path. It keeps working after a failed batch, but does not retain
the batch for replay. The shutdown report makes rejected, ambiguous, and local
loss observable.

When the continue-mode circuit is open, `try_send()` rejects immediately while
the backpressured `send()` path can queue within the configured byte budget and
wait for a later probe.

## Delivery contract

- The table append API accepts NDJSON only: one JSON row object per line,
  not a JSON array.
- `send()`, `send_all()`, and `Ok(())` from `try_send()` mean local admission;
  they do not confirm a remote commit.
- A successful strict barrier confirms that its accepted prefix committed.
- A continue-mode barrier is settlement. Always inspect its
  `AppendDeliveryReport`.
- The stream retries only an exact temporary HTTP batch explicitly reported as
  `rejected`.
- A timeout, transport failure, or invalid success response is `unknown`. The
  rows may already exist remotely, so never blindly replay that payload.
- `shutdown()` closes admission and settles accepted rows. It is not an abort or
  rollback; stop and join producer tasks before calling it.
- Dropping an enqueued `flush()` future does not cancel its remote settlement;
  keep the future alive to receive the interval report and use `stats()` for
  post-cancellation diagnostics.
- Concurrent batches do not have a defined commit order. Use
  `max_in_flight_requests(1)` when requests must be submitted serially.
- An in-memory stream is not a durable queue. Audit or billing writes need an
  application-owned outbox and a reconciliation path for unknown outcomes.

## Check the examples

Compile every example without running it:

```sh
cargo check --examples
```
