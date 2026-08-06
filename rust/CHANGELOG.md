# Changelog

All significant changes to the ScopeDB Rust SDK are documented in this file.

## Unreleased

### Breaking Changes

* Raised the minimum supported Rust version from 1.85.0 to 1.91.0 so fresh
  consumers can resolve the current dependency graph without an inherited
  lockfile.
* Renamed the synchronous `StatementHandle::status()` snapshot accessor to
  `last_status()` and use the asynchronous `status().await` operation for one
  remote status check. The previous synchronous name cannot remain as an alias
  because Rust does not support overloading it with the asynchronous operation.

### New Features

* Added an API-key client builder, `Client::query`, full table descriptions,
  lazy catalog iterators, and `StatementHandle::status` / `wait` application
  paths. The released `fetch_once` and `fetch` methods remain as deprecated
  aliases for `status` and `wait`.
* Added raw, typed-value, keyed-object, and first-row result conversions.
  Object conversion rejects duplicate output column names instead of silently
  dropping a value.
* Added streaming-write builder names centered on batches and buffering, plus a
  configurable `max_batch_rows` below the protocol ceiling. Existing builder
  names remain compatible deprecated aliases.

### Reliability

* Preserve HTTP status, request ID, `Retry-After`, and explicit server retry
  classification on errors while keeping server messages unchanged.
* Honor delta-seconds and HTTP-date `Retry-After` values for safe rejected-batch
  retries, capped by the configured maximum backoff.
* Return typed errors instead of panicking when result metadata and row shapes
  disagree, and reject repeated catalog page tokens.

### Documentation

* Reworked runnable examples around read-only discovery, direct append,
  asynchronous batching, bulk imports, and best-effort telemetry journeys.

## v0.2.2 (2026-08-05)

### Bug Fixes

* Removed `Client::health_check`, which targeted an internal liveness endpoint
  and could report non-success HTTP responses as healthy.
* Decode error messages from both direct and nested API error envelopes,
  including unknown-outcome append failures.

### Documentation

* Prefer `SCOPEDB_API_KEY` in examples while retaining `SCOPEDB_TOKEN` as a
  backward-compatible fallback, and mark authorization headers as sensitive.

## v0.2.1 (2026-08-05)

### Bug Fixes

* Re-exported the SDK's compatible reqwest version and corrected the installation
  guide so new consumers do not resolve an incompatible reqwest major.

## v0.2.0 (2026-08-05)

### Breaking Changes

* Replaced the `Connection` and Arrow record-batch API with the async `Client`,
  `Statement`, `StatementHandle`, and `Table` APIs.
* Replaced Arrow results with the SDK's typed JSON result and value model.
* Set the minimum supported Rust version to 1.85.0.

### New Features

* Added typed statement lifecycle, cancellation, and progress APIs.
* Added JSON ingest with transform support and a bounded asynchronous ingest
  stream.
* Added read-only REST catalog list and fetch APIs for databases, schemas, and
  tables.
* Added direct NDJSON table appends and a bounded, concurrent asynchronous
  append stream.
* Added strict and best-effort delivery policies, settlement reports, retry and
  unknown-outcome handling, circuit breaking, and local drop statistics.
* Added runnable examples for catalog discovery, direct appends, bulk writes,
  telemetry, and transform-oriented ingest.

### Reliability

* Preserved structured append rejection details and kept ambiguous commit
  outcomes distinct from safe-to-retry rejections.
* Added backpressure for queued rows and bytes, bounded HTTP concurrency, and
  delivery barriers for flushing and shutdown.

## v0.1.1 (2025-02-21)

### New Features

* Added Arrow record-batch insertion with a custom transform.

## v0.1.0 (2025-02-20)

* Initial crates.io release.
