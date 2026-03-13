# ScopeDB HTTP API Spec for Rust Client

This document is a temporary implementation reference for the Rust client.
It is derived from the current `scopedb` server code in `/home/leiysky/work/scopedb`, mainly:

- `crates/server/src/http/mod.rs`
- `crates/server/src/http/handshake.rs`
- `crates/server/src/http/statements.rs`
- `crates/server/src/http/ingest.rs`
- `crates/protos/src/http/statement.rs`
- `crates/protos/src/http/ingest.rs`

It reflects the server code as read on 2026-03-12.

## Base URL

All public endpoints are under `/v1`.

## Endpoints

### `GET /v1/health`

- Response body: plain text `"OK"`
- Used for a simple liveness check

### `GET /v1/version`

- Response body: JSON `version::build_info()`
- Not yet modeled in the Rust client

### `POST /v1/statements`

Submit a statement for execution.

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

Request notes:

- `statement_id` is optional from the client's perspective. The server defaults it to a random UUID v7 if omitted.
- `statement` is required.
- `exec_timeout` is optional. The server default is `DEFAULT_EXEC_TIMEOUT` from `protos::http`.
- `max_parallelism` is optional.
- `format` is required and currently supports:
  - `json`
  - `arrow`
  - `result_set`

Response body:

- Success or in-band execution state is always a tagged `StatementResponse`:
  - `pending`
  - `running`
  - `finished`
  - `failed`
  - `cancelled`

Important behavior:

- A statement that fails during planning or execution is returned as HTTP `200 OK` with a JSON body whose `status` is `failed`.
- A cancelled statement is likewise returned as HTTP `200 OK` with `status = "cancelled"`.
- Validation / transport / request-shape failures are returned as non-2xx with JSON error payloads.

### `GET /v1/statements/{statement_id}?format=...`

Fetch the latest statement state.

Query params:

- `format`: same enum as submit

Response behavior:

- Same `StatementResponse` tagged union as submit
- If the statement is not found, the server returns a non-2xx error response

### `POST /v1/statements/{statement_id}/cancel`

Cancel a running or pending statement.

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

- The server returns the terminal state after cancellation handling.
- If the statement does not exist, the server returns a non-2xx error response.

### `POST /v1/ingest`

Ingest rows through a transform statement.

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

Supported data payloads in the current server code:

- `{"format":"json","rows":"...json lines..."}`
- `{"format":"arrow","rows":"...base64 encoded arrow record batch..."}`
- `{"format":"result_set","fields":[...],"rows":"...encoded rowset..."}`

Current ingest type in the checked server code:

- `committed`

Response body:

```json
{
  "num_rows_inserted": 2
}
```

## Error Shape

For non-2xx request failures, the server generally returns:

```json
{
  "message": "..."
}
```

The Rust client should therefore distinguish:

- transport / deserialization errors
- non-2xx server error payloads
- in-band statement failures represented as `StatementResponse::Failed` or `StatementResponse::Cancelled`

## Result Set Shape

`StatementResponse::Finished` contains:

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

For `result_set.format`:

- `json` => `rows: Vec<Vec<Option<String>>>`
- `arrow` => `rows: String`
- `result_set` => `rows: String`

The current Rust high-level row decoding only needs to support `json` for typed `Value` conversion.

## Current Server Caveats Relevant to the SDK

- Public HTTP routes in the checked `scopedb` tree do not currently require auth headers.
- `GET /v1/version` exists but is not yet surfaced by the Rust client.
- Statement failures can be represented as successful HTTP transport responses carrying a failed statement state, so client polling helpers must inspect the statement payload instead of relying only on status codes.
