/*
 * Copyright 2024 ScopeDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { Client } from "../src/client.js";
import { Table } from "../src/table.js";
import { emptyResultSet, finishedStatus, jsonResponse, makeFetchStub } from "./helpers.js";

describe("Table.identifier — ScopeQL quoting", () => {
  function makeTable(name: string): Table {
    // fetch stub is irrelevant for identifier(); just needs a client
    const { fn } = makeFetchStub([]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    return new Table(client, name);
  }

  it("wraps simple table name in backticks", () => {
    assert.equal(makeTable("events").identifier(), "`events`");
  });

  it("escapes backtick in table name", () => {
    assert.equal(makeTable("my`table").identifier(), "`my\\`table`");
  });

  it("includes database and schema when set", () => {
    const { fn } = makeFetchStub([]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const table = new Table(client, "events").withDatabase("mydb").withSchema("myschema");
    assert.equal(table.identifier(), "`mydb`.`myschema`.`events`");
  });

  it("includes only schema when database is not set", () => {
    const { fn } = makeFetchStub([]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const table = new Table(client, "events").withSchema("logs");
    assert.equal(table.identifier(), "`logs`.`events`");
  });

  it("includes only database when schema is not set", () => {
    const { fn } = makeFetchStub([]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const table = new Table(client, "events").withDatabase("mydb");
    assert.equal(table.identifier(), "`mydb`.`events`");
  });

  it("escapes tab, newline, carriage return in table name", () => {
    assert.equal(makeTable("tab\there").identifier(), "`tab\\there`");
    assert.equal(makeTable("new\nline").identifier(), "`new\\nline`");
    assert.equal(makeTable("cr\rhere").identifier(), "`cr\\rhere`");
  });

  it("escapes backslash in table name", () => {
    assert.equal(makeTable("back\\slash").identifier(), "`back\\\\slash`");
  });

  it("escapes control characters using hex notation", () => {
    // 0x01 should become \x01
    const name = "ctrl\x01char";
    const id = makeTable(name).identifier();
    assert.ok(id.includes("\\x01"), `expected \\x01 in: ${id}`);
  });
});

describe("Table.drop", () => {
  it("executes DROP TABLE with the correct identifier", async () => {
    // drop() calls statement(...).execute(), which does POST /v1/statements then GET
    const finished = finishedStatus(emptyResultSet());
    const { fn, calls } = makeFetchStub([
      jsonResponse(200, { status: "pending", statement_id: "s1", created_at: "2024-01-01T00:00:00Z", progress: { total_percentage: 0, nanos_from_submitted: 0, nanos_from_started: 0, total_stages: 0, total_partitions: 0, total_rows: 0, total_compressed_bytes: 0, total_uncompressed_bytes: 0, scanned_stages: 0, scanned_partitions: 0, scanned_rows: 0, scanned_compressed_bytes: 0, scanned_uncompressed_bytes: 0, skipped_partitions: 0, skipped_rows: 0, skipped_compressed_bytes: 0, skipped_uncompressed_bytes: 0 } }),
      jsonResponse(200, finished),
    ]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const table = new Table(client, "my_table");

    await table.drop({ initialDelayMs: 0, maxDelayMs: 0 } as Parameters<typeof table.drop>[0]);

    // First call is POST /v1/statements; verify the statement contains the quoted identifier
    const body = JSON.parse((calls[0]!.init as RequestInit).body as string) as Record<string, unknown>;
    assert.ok(
      (body["statement"] as string).includes("`my_table`"),
      `statement was: ${body["statement"]}`,
    );
    assert.ok(
      (body["statement"] as string).toLowerCase().includes("drop table"),
      `statement was: ${body["statement"]}`,
    );
  });
});
