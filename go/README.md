# ScopeDB SDK for Go

[![Apache License, Version 2.0](https://img.shields.io/:license-Apache%202-brightgreen.svg)](https://www.apache.org/licenses/LICENSE-2.0.txt)
[![Go Reference](https://pkg.go.dev/badge/github.com/scopedb/scopedb-sdk/go.svg)](https://pkg.go.dev/github.com/scopedb/scopedb-sdk/go)

The ScopeDB Go SDK supports ScopeQL statements, REST catalog discovery, and
bounded asynchronous streaming writes. For application writes, start with
`Table.AppendStream`.

## Runtime and installation

The SDK requires Go 1.24 or later.

```sh
go get github.com/scopedb/scopedb-sdk/go@latest
```

## Create a client

Pass the ScopeDB endpoint and API key through application configuration. Keep
API keys out of source control.

```go
client, err := scopedb.NewClient(scopedb.Config{
	Endpoint: os.Getenv("SCOPEDB_ENDPOINT"),
	APIKey:   os.Getenv("SCOPEDB_API_KEY"),
})
if err != nil {
	return err
}
defer client.Close()
```

`NewClient` validates the endpoint and returns an error for invalid
configuration. Set `Config.HTTPClient` when the application needs to own HTTP
timeouts, proxies, TLS, or connection pooling. `Client.Close` closes idle
connections only for the HTTP client created by the SDK; it never closes a
caller-provided client.

Statement, transform-ingest, and `AppendStream` request bodies use zstd
compression by default. Set `Config.Compression` to `CompressionGzip` when gzip
is required. Direct caller-encoded `AppendNDJSON` requests remain
identity-encoded.

## ScopeQL documentation

The SDK executes ScopeQL but does not define the language. Start with the
language documentation:

- [Quickstart](https://docs.scopedb.io/guides/quickstart)
- [Query guide](https://docs.scopedb.io/guides/query-events)
- [Language reference](https://docs.scopedb.io/reference/)

## Query and results

`Query` submits a statement and waits for its result:

```go
result, err := client.Query(ctx, "SELECT 1 AS ready")
if err != nil {
	return err
}

rows, err := result.ToObjects()
if err != nil {
	return err
}
fmt.Println(rows)
```

Use `RawRows` for unconverted wire values, `ToValues` for positional values,
`ToObjects` for values keyed by column name, or `First` for an optional first
row.

For a detached or long-running statement, keep its handle and choose between a
local status snapshot, one remote status request, or waiting for the result:

```go
handle, err := client.Statement("SELECT 1 AS ready").Submit(ctx)
if err != nil {
	return err
}

fmt.Println("statement ID:", handle.ID())
if cached := handle.LastStatus(); cached != nil {
	fmt.Println("cached status:", *cached) // No network request.
}

latest, err := handle.Status(ctx) // Fetches one remote snapshot while active.
if err != nil {
	return err
}
fmt.Println("latest status:", latest)

result, err = handle.Wait(ctx) // Polls until the statement terminates.
if err != nil {
	return err
}
```

Once a handle has a terminal status, `Status` returns the cached status without
another request. Store `handle.ID()` and use `client.StatementHandle(id)` to
resume the lifecycle in another process. `Cancel` returns the statement ID,
creation time, status, and server message. If cancellation finds that a
statement already finished or failed, `Wait` fetches the complete statement
response needed for its result or structured failure details. A cancelled
outcome uses the cancellation message directly.

`Statement.ID` and `Statement.ExecTimeout` are the only optional statement
settings. Provide an ID when the application needs to choose the statement ID;
otherwise ScopeDB generates one. `StatementHandle.ID()` always returns the ID
confirmed by ScopeDB:

```go
statement := client.Statement("FROM events")
statementID := uuid.New()
statement.ID = &statementID // Optional.
statement.ExecTimeout = "30s"
handle, err := statement.Submit(ctx)
if err != nil {
	return err
}
fmt.Println("statement ID:", handle.ID())
```

When `Wait` or `Execute` returns a `*scopedb.Error` with kind
`ErrorKindStatementFailed`, `StatementDetails` preserves the server's
structured error code, message, and code-specific JSON details. The outer error
message remains the server's top-level statement message:

```go
var scopeErr *scopedb.Error
if errors.As(err, &scopeErr) && scopeErr.StatementDetails != nil {
	fmt.Println("statement error code:", scopeErr.StatementDetails.Code)
	fmt.Println("statement error:", scopeErr.StatementDetails.Message)
	fmt.Println("details:", string(scopeErr.StatementDetails.Details))
}
```

## Browse the REST catalog

List methods expose one explicit page. Iterators lazily request later pages and
are the simpler choice for discovery:

```go
for database, err := range client.IterateDatabases(ctx, scopedb.CatalogListOptions{
	PageSize: 100,
}) {
	if err != nil {
		return err
	}
	fmt.Println(database.Name)
}

for table, err := range client.IterateTables(
	ctx,
	"scopedb",
	"public",
	scopedb.CatalogListOptions{PageSize: 100},
) {
	if err != nil {
		return err
	}
	fmt.Println(table.Name)
}
```

Use `ListDatabases`, `ListSchemas`, or `ListTables` when the application owns
page boundaries. Use `FetchDatabase`, `FetchSchema`, or `FetchTable` for a full
resource.

## Describe a table

The table helper defaults to database `scopedb` and schema `public`. Set both
explicitly when the destination is application-configured:

```go
table := client.Table("events")
table.Database = "scopedb"
table.Schema = "public"

description, err := table.Describe(ctx)
if err != nil {
	return err
}
fmt.Println(description.Columns)
```

## Streaming writes

Table appends write rows to an existing destination table. For most
applications, `AppendStream` is the recommended path: the SDK accepts typed
rows and owns their encoding, bounded batching, backpressure, and request
concurrency. Its current wire encoding is NDJSON, but callers do not construct
the wire payload. Evaluate the examples against an explicitly selected
disposable table before using a production destination.

### Recommended: asynchronous append stream

Use `AppendStream` for normal application writes, including continuous and
large producers. `Send` accepts typed rows and uses `encoding/json` to encode
each value as one top-level JSON object. Standard JSON tags and custom
`MarshalJSON` methods apply. The stream batches those objects by size or time,
bounds pending bytes, and sends a bounded number of append requests
concurrently. The zero-value options use bounded defaults; override them only
when the workload needs a different delivery policy or resource bound.

Each `AppendStream` request contains at most 8 MiB of uncompressed NDJSON and
200,000 rows. The stream splits automatically at either limit.

```go
type Event struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

stream, err := table.AppendStream(scopedb.AppendStreamOptions{})
if err != nil {
	return err
}

for _, event := range []Event{
	{ID: 1, Name: "first"},
	{ID: 2, Name: "second"},
} {
	if err := stream.Send(ctx, event); err != nil {
		_, _ = stream.Shutdown(ctx)
		return err
	}
}

report, err := stream.Flush(ctx)
if err != nil {
	_, _ = stream.Shutdown(ctx)
	return err
}
fmt.Println("flush committed:", report.CommittedRows)

// Permanently closes admission and settles all remaining accepted rows.
_, err = stream.Shutdown(ctx)
return err
```

`Send` waits for bounded local admission capacity. A nil error means only that
the row entered the local stream; it does not confirm a remote commit. Feed
large sources one row at a time instead of starting one goroutine per row,
which would move an unbounded backlog outside the stream.

JSON serialization validates only that each value encodes as an object. ScopeDB
validates that object's fields and types against the destination table when it
processes the batch. With the default stop policy, `Flush` or `Shutdown` returns
the server error and structured row details. Continue mode reports failed rows
through the barrier report and `Stats().LastFailure`. An earlier successful
`Send` does not imply schema compatibility.

`Send` and `TrySend` are safe for concurrent producers. When source-side work
benefits from parallelism, use a fixed worker pool; the
[`append_stream`](examples/append_stream) example uses four producers. Remote
batch commits are still unordered when `MaxConcurrentBatches` is greater than
one.

`Flush` settles every row accepted before its barrier. `Shutdown` permanently
closes admission and settles all accepted rows. Canceling either call's
context stops that caller's wait after an enqueued barrier, but does not cancel
remote settlement. Inspect `Stats().LastReport` if the wait is interrupted.

The default `AppendFailureStop` policy is strict: the first failed batch stops
admission, and a successful barrier confirms that its accepted prefix
committed. Concurrent batches have no defined commit order; set
`MaxConcurrentBatches: 1` when request submission must be serial.

### Best-effort logs and telemetry

Logs and telemetry often cannot block a request path or stop forever after one
remote failure. Opt into `AppendFailureContinue`, use `TrySend`, and inspect the
settlement report and lifetime statistics:

```go
telemetry, err := table.AppendStream(scopedb.AppendStreamOptions{
	FailurePolicy: scopedb.AppendFailureContinue,
	FlushInterval: time.Second,
})
if err != nil {
	return err
}

if err := telemetry.TrySend(map[string]any{
	"name":   "request.completed",
	"status": 200,
}); err != nil {
	// Send this diagnostic to a different sink.
	log.Printf("telemetry row dropped locally: %v", err)
}

report, err := telemetry.Shutdown(ctx)
if err != nil {
	return err
}
if report.Outcome != scopedb.AppendDeliveryOK {
	log.Printf("telemetry loss or ambiguity: %+v", report)
}
fmt.Printf("lifetime stats: %+v\n", telemetry.Stats())
```

`TrySend` does not wait for stream capacity. A nil error still means local
admission only; an error can indicate invalid input, an oversized row, a full
buffer, or a closed stream. `Stats().DroppedByReason` separates local loss
causes.

Continue mode accounts for a failed batch and continues with later rows. A
completed report separates committed, failed, unknown, and locally dropped
rows. `Stats().LastFailure` preserves the latest HTTP status, request ID,
retry metadata, and structured row errors for diagnostics. It is a settlement
report, not a commit receipt for every row. The stream retries only an exact
temporary batch that the server explicitly marks `rejected`. A timeout,
transport failure, or malformed success response is `unknown` and is never
automatically retried. Rows with an unknown outcome may already exist remotely,
so never blindly replay them.

An in-memory stream is not a durable queue. Use an application-owned outbox and
a reconciliation path when payloads must survive process failure or unknown
outcomes.

### Low-level: direct NDJSON append

Use `AppendNDJSON` only when the caller already owns one exact raw NDJSON body
and its request boundary. The body contains one JSON object per non-empty line,
not a JSON array:

```go
ndjson := []byte("{\"id\":1,\"name\":\"first\"}\n{\"id\":2,\"name\":\"second\"}")
result, err := table.AppendNDJSON(ctx, ndjson)
if err != nil {
	return err
}
fmt.Println("committed rows:", result.NumRowsInserted)
```

One request is limited to 16 MiB and 200,000 rows.

### Choose a delivery path

| Workload | Admission and delivery | Example |
| --- | --- | --- |
| Normal typed application writes | SDK owns encoding and batches; strict barriers | [`append_stream`](examples/append_stream) |
| Backfill or file import | Sequential producer admission and bounded concurrent batches | [`bulk_append`](examples/patterns/bulk_append) |
| Long-running logs and events | Non-blocking continue mode with observable loss | [`telemetry`](examples/patterns/telemetry) |
| One exact raw NDJSON payload | Caller encodes the body and owns the request boundary | [`append_ndjson`](examples/append_ndjson) |

## Advanced: transform before writing

Use `Client.IngestStream` only when source JSON specifically needs a server-side
ScopeQL transformation before it can match the destination table. For normal
typed events, shape the row in the producer and use `Table.AppendStream`. See
the guarded [`ingest_transform`](examples/ingest_transform) example for the
advanced path.

This path is sequential and fail-fast. `IngestStream.Send` confirms local
admission only, while `Flush` and `Shutdown` wait for the accepted prefix to
settle when they succeed. If a remote ingest request returns an error, its
commit outcome may be unknown. A nonzero result returned with that error counts
only earlier confirmed batches; it is not a safe replay offset. Reconcile the
failing batch before replaying records.

## Structured errors

Server error messages pass through unchanged. `scopedb.Error` adds structured
diagnostics without requiring applications to parse the message:

```go
var scopeErr *scopedb.Error
if errors.As(err, &scopeErr) {
	log.Printf("kind=%s status=%d request_id=%s retryable=%t retry_after=%s",
		scopeErr.Kind,
		scopeErr.HTTPStatus,
		scopeErr.RequestID,
		scopeErr.Retryable,
		scopeErr.RetryAfter,
	)

	if details := scopeErr.AppendDetails; details != nil &&
		details.AppendState == scopedb.AppendStateUnknown {
		log.Print("append may have committed; reconcile before replaying")
	}
}
```

The main kinds are `ErrorKindConfigInvalid`, `ErrorKindStatementFailed`,
`ErrorKindAppendRowsFailed`, and `ErrorKindUnexpected`. Transport and decoding
causes support `errors.Is` and `errors.As` through `Unwrap`. A direct append
context canceled before its request starts is returned directly.

## Examples and development

The [examples guide](examples/README.md) contains read-only discovery, guarded
write examples, delivery contracts, and runnable commands.

```sh
go test ./...
go test -race ./...
go vet ./...
```

Release notes and the maintainer runbook are in [CHANGELOG.md](CHANGELOG.md) and
[RELEASE.md](RELEASE.md). See [CONTRIBUTING.md](CONTRIBUTING.md) to contribute.

## License

This software is licensed under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0.txt).
