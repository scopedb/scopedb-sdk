# Rust HTTP API Reference

This document describes the public HTTP API surface used by the Rust SDK.
It is intended as a maintenance reference for the SDK implementation and for
examples in this repository.

## Base URL

All public endpoints are rooted at `/v1`.

## Endpoints

### `GET /v1/health`

Returns a plain-text health response:

```text
OK
```

This endpoint is suitable for connectivity and liveness checks.

### `GET /v1/version`

Returns a JSON version payload for the service.

The Rust SDK does not currently model this response as a typed API.

### `POST /v1/statements`

Submits a statement for execution.

Request body:

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
- `format`: always `json` for the current public Rust SDK

The service may support additional wire encodings, but the Rust SDK does not
expose them as public request options.

Response body:

- `pending`
- `running`
- `finished`
- `failed`
- `cancelled`

All of the above are represented as tagged statement-state payloads.

Important behavior:

- Statement failure and cancellation are represented in-band as statement-state payloads.
- Transport-level success does not imply statement-level success.
- Request validation and transport failures are returned as non-2xx responses.

### `GET /v1/statements/{statement_id}?format=...`

Fetches the latest state for a submitted statement.

Query params:

- `format`: always `json` for the current public Rust SDK

Response behavior:

- Returns the same statement-state payload family as `POST /v1/statements`
- Returns a non-2xx response if the statement does not exist

### `POST /v1/statements/{statement_id}/cancel`

Cancels a pending or running statement.

Response body:

```json
{
  "statement_id": "uuid",
  "status": "finished|failed|cancelled",
  "message": "statement is ...",
  "created_at": "timestamp"
}
```

Notes:

- The response returns the post-cancel terminal status view
- A missing statement is returned as a non-2xx response

### `POST /v1/ingest`

Ingests rows through a transform statement.

Request body:

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

Supported data payloads used by the Rust SDK:

- `{"format":"json","rows":"...json lines..."}`

Supported ingest type values:

- `committed`

Response body:

```json
{
  "num_rows_inserted": 2
}
```

## Error Response Shape

For non-2xx request failures, the response body is generally shaped as:

```json
{
  "message": "..."
}
```

The Rust SDK should therefore distinguish:

- transport or deserialization errors
- non-2xx server error payloads
- in-band statement failures represented as `failed` or `cancelled` statement states

## Statement Result Shape

A finished statement contains a `result_set` payload:

```json
{
  "status": "finished",
  "statement_id": "uuid",
  "created_at": "timestamp",
  "progress": { "...": "..." },
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

The public Rust SDK currently requests JSON results only, so the main
high-level row-conversion path is JSON-oriented.

## SDK Notes

- A higher-level polling helper must inspect statement status rather than rely only on HTTP status codes.
- A higher-level ingest helper may treat record serialization as an SDK concern while keeping the HTTP payload in JSON-lines form.
