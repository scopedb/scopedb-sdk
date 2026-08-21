# CHANGELOG

All significant changes to this project will be documented in this file.

## Unreleased

## v0.6.2 (2026-08-22)

### Changed

* `AppendStream` now caps each uncompressed NDJSON request at 8 MiB.

## v0.6.1 (2026-08-20)

### Changed

* `AppendStream` request batches now use the client's configured compression,
  which defaults to zstd. Direct `AppendNDJSON` requests remain
  identity-encoded.

## v0.6.0 (2026-08-08)

### Breaking Changes

* Redesigned `NewClient` to accept `Config` by value and return `(*Client, error)`; invalid endpoints now fail during construction.
* Replaced the old data cable API with bounded `AppendStream` and `IngestStream` write paths.
* Redesigned asynchronous statements around `StatementHandle.ID`, optional `LastStatus`, value-returning `Status`, and `Wait`, while retaining optional caller-provided IDs through `Statement.ID`.
* Removed public result-format configuration; query results use the supported JSON wire format internally.
* Replaced table schema discovery with `Table.Describe`, which returns the complete REST catalog resource.
* Renamed the table helper's destination field from `Table` to `Name`.

### New Features

* Added `Client.Query` and expanded result conversion with `RawRows`, `ToObjects`, and `First` alongside `ToValues`.
* Added structured statement failure details, complete pruning progress, and complete cancellation results.
* Added paged and lazy-iterator REST catalog APIs for databases, schemas, and tables.
* Added direct raw NDJSON table append through `Table.AppendNDJSON`, with committed, rejected, and unknown outcomes.
* Added bounded concurrent `AppendStream` batching with strict and continue failure policies, local admission backpressure, delivery reports, and lifetime statistics.
* Added bounded sequential `IngestStream` batching for transform-oriented JSON ingest.
* Added finite, configurable per-attempt timeouts to both background write streams.
* Added caller-provided HTTP client support while preserving caller ownership.
* Added structured API errors with HTTP status, request ID, retry metadata, append details, and wrapped causes.

### Bug Fixes

* Added the missing binary and null data type constants.
* Fixed `ResultSet.ToValues` conversion for binary and interval columns.
* Fixed result field decoding for the `data_type` wire field.
* Fixed statement polling and terminal failure propagation.
* Fixed cancellation races so cached finished status does not hide the result from `Wait`.
* Prevented blocked `IngestStream` producers from being admitted after a terminal batch failure.
* Preserved configured endpoint base paths and percent-encoded individual REST resource identifiers.
* Kept REST and ScopeQL table destinations aligned when only a database is configured.

## v0.5.0 (2026-04-23)

### Breaking Changes

* POST requests now use `zstd` compression by default.
  * Set `Compression: scopedb.CompressionGzip` when talking to older ScopeDB deployments that do not support `zstd` yet.

## v0.4.0 (2026-04-22)

### Bug Fixes

* Added API key client configuration support for authenticated requests.

## v0.3.0 (2025-07-03)

### Breaking Changes

* Reworked the Go SDK to catch up with the latest ScopeDB APIs.
* Updated the statement lifecycle API to follow the latest server changes.
* Renamed task-related APIs to job-related APIs.
* Renamed variant-related APIs to `any`.
* Renamed `VariantBatchCable` to `RawDataBatchCable`.
* `ArrowBatchCable` now takes ownership of Arrow batches.
* Removed the inaccurate `nanos_to_finish` progress field from statement responses.

### New Features

* Added `ResultSet.ToValues`.
* Added the `Table` helper.
* Improved statement response handling.

### Bug Fixes

* Fixed array aliasing between goroutines in concurrent code paths.

### Improvements

* Improved error handling.
* Caught up new response and schema types from ScopeDB.

## v0.2.6 (2025-03-04)

### New Features

* Added support for caller-provided statement IDs.

## v0.2.5 (2025-02-11)

### New Features

* Added the `X-ScopeDB-Uncompressed-Content-Length` header for POST requests.

## v0.2.4 (2025-01-22)

### New Features

* Added support for `exec_timeout`.
* Added support for gzip-compressed requests.

### Bug Fixes

* Fixed a typo in `StatementStatus`.

## v0.2.3 (2025-01-10)

### New Features

* Added richer error types and cancel responses.

## v0.2.2 (2025-01-04)

### New Features

* Added statement cancellation and progress APIs.
* Added support for ingest formats.

### Improvements

* The client now respects caller-provided context timeouts.

## v0.2.1 (2024-11-30)

### Breaking Changes

* Dropped ingest channel helper functions.

## v0.2.0 (2024-11-27)

### New Features

* Added support for the ingest v2 one-for-all API.

## v0.1.0 (2024-11-20)

### New Features

* Initial release of the ScopeDB Go SDK.
