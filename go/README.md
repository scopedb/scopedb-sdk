# ScopeDB SDK for Go

[![Apache License, Version 2.0](https://img.shields.io/:license-Apache%202-brightgreen.svg)](https://www.apache.org/licenses/LICENSE-2.0.txt)
[![Go Reference](https://pkg.go.dev/badge/github.com/scopedb/scopedb-sdk/go.svg)](https://pkg.go.dev/github.com/scopedb/scopedb-sdk/go)

The ScopeDB Go SDK supports ScopeQL statements, REST catalog discovery, direct
NDJSON table appends, bounded asynchronous streaming writes, and
transform-oriented JSON ingest.

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
resume the lifecycle in another process. Set `Statement.ID`, `ExecTimeout`, or
`MaxParallelism` before `Submit` when a query needs those controls. `Cancel`
returns the statement ID, creation time, status, and server message.

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

## Streaming writes with NDJSON

Table writes accept newline-delimited JSON only: one JSON object per non-empty
line, not a JSON array. The destination table must already exist. Evaluate the
examples against an explicitly selected disposable table before using a
production destination.

### Direct append

Use `Append` when the caller owns one exact NDJSON request boundary:

```go
ndjson := []byte("{\"id\":1,\"name\":\"first\"}\n{\"id\":2,\"name\":\"second\"}")
result, err := table.Append(ctx, ndjson)
if err != nil {
	return err
}
fmt.Println("committed rows:", result.NumRowsInserted)
```

One request is limited to 16 MiB and 200,000 rows.

### Asynchronous append stream

Use `AppendStream` for continuous or large producers. It serializes each Go
value as one JSON object, batches by size or time, bounds pending bytes, and
sends a bounded number of append requests concurrently.

```go
stream, err := table.AppendStream(scopedb.AppendStreamOptions{
	TargetBatchBytes:     4 * 1024 * 1024,
	MaxBatchRows:         10_000,
	FlushInterval:        time.Second,
	MaxBufferedBytes:     64 * 1024 * 1024,
	MaxConcurrentBatches: 4,
	AttemptTimeout:       30 * time.Second,
})
if err != nil {
	return err
}

for _, row := range []any{
	map[string]any{"id": 1, "name": "first"},
	map[string]any{"id": 2, "name": "second"},
} {
	if err := stream.Send(ctx, row); err != nil {
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

### Choose a delivery path

| Workload | Admission and delivery | Example |
| --- | --- | --- |
| One exact NDJSON payload | Caller owns the request boundary | [`append`](examples/append) |
| Basic asynchronous batching | SDK owns batches; strict barriers | [`append_stream`](examples/append_stream) |
| Backfill or file import | Sequential producer admission and bounded concurrent batches | [`bulk_append`](examples/patterns/bulk_append) |
| Long-running logs and events | Non-blocking continue mode with observable loss | [`telemetry`](examples/patterns/telemetry) |
| ScopeQL transformation before insert | Sequential transform-oriented ingest | [`ingest_transform`](examples/ingest_transform) |

## Transform-oriented ingest

`IngestStream` is the secondary write path for JSON records that need a ScopeQL
transformation before insertion. Prefer `Table.Append` or `Table.AppendStream`
when records already match the destination table.

```go
stream, err := client.IngestStream(`
	SELECT $0["ts"]::timestamp, $0["name"]::string
	INSERT INTO public.events (occurred_at, name)
`, scopedb.IngestStreamOptions{
	AttemptTimeout: 30 * time.Second,
})
if err != nil {
	return err
}

if err := stream.Send(ctx, map[string]any{
	"ts":   "2026-08-08T12:00:00Z",
	"name": "example",
}); err != nil {
	_, _ = stream.Shutdown(ctx)
	return err
}

result, err := stream.Shutdown(ctx)
if err != nil {
	return err
}
fmt.Println("inserted rows:", result.NumRowsInserted)
```

`IngestStream.Send` also confirms local admission only. `Flush` and `Shutdown`
wait for the accepted prefix to settle when they succeed. This path is
sequential and fail-fast. If a remote ingest request returns an error, its
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
