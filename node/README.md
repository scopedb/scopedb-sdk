# ScopeDB SDK for Node.js

This package provides a TypeScript-first client for ScopeDB on Node.js.

## Installation

```sh
npm install scopedb-client
```

## Create a Client

```ts
import { Client } from "scopedb-client";

const client = new Client("http://127.0.0.1:6543");
```

## Run a Statement

```ts
import { Client } from "scopedb-client";

const client = new Client("http://127.0.0.1:6543");

const result = await client.statement("SELECT 1").execute();
console.log(result.intoValues());
```

## Table Helper

```ts
import { Client } from "scopedb-client";

const client = new Client("http://127.0.0.1:6543");

const table = client.table("events").withSchema("public");
console.log(table.identifier());

const schema = await table.tableSchema();
console.log(schema.fields().length);
```

## Batched JSON Ingest

```ts
import { Client } from "scopedb-client";

const client = new Client("http://127.0.0.1:6543");

const stream = client
  .ingestStream(`
    SELECT
      $0["ts"]::timestamp AS ts,
      $0["name"]::string AS name
    INSERT INTO public.events (ts, name)
  `)
  .build();

await stream.send({
  ts: "2026-03-13T12:00:00Z",
  name: "scopedb",
});

await stream.flush();
await stream.shutdown();
```

## Examples

See the TypeScript examples under [`examples/`](examples/):

- `examples/statement.ts`
- `examples/table.ts`
- `examples/batch.ts`

These examples import from `src/` directly so they stay close to the in-repo SDK surface while the
package is still evolving.

## Development

```sh
npm test
npm run build
npm run check
```

## Delivery Notes

- The package is TypeScript-first and emits declarations from `src/index.ts`.
- Generated artifacts should stay out of git; `dist/`, `dist-test/`, and `node_modules/` are
  ignored in [`node/.gitignore`](.gitignore).
- A broader package-delivery checklist lives in [`node/DELIVERY.md`](DELIVERY.md).
