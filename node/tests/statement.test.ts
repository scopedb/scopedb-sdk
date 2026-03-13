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
import { ScopeDBError } from "../src/errors.js";
import { StatementHandle } from "../src/statement.js";
import {
  cancelledStatus,
  emptyResultSet,
  failedStatus,
  finishedStatus,
  jsonResponse,
  makeFetchStub,
  pendingStatus,
  runningStatus,
} from "./helpers.js";

// Use zero delays in tests to avoid slow polling
const noDelay = { initialDelayMs: 0, maxDelayMs: 0 };

describe("Statement.submit", () => {
  it("submits statement and returns a handle with correct statementId", async () => {
    const status = pendingStatus("stmt-abc");
    const { fn } = makeFetchStub([jsonResponse(200, status)]);
    const client = new Client("http://localhost:8080", { fetch: fn });

    const handle = await client.statement("SELECT 1").submit();

    assert.equal(handle.statementId, "stmt-abc");
    assert.equal(handle.status()?.status, "pending");
  });

  it("forwards optional fields from Statement builder", async () => {
    const status = pendingStatus();
    const { fn, calls } = makeFetchStub([jsonResponse(200, status)]);
    const client = new Client("http://localhost:8080", { fetch: fn });

    await client
      .statement("SELECT 1")
      .withStatementId("custom-id")
      .withExecTimeout("60s")
      .withMaxParallelism(8)
      .submit();

    const body = JSON.parse((calls[0]!.init as RequestInit).body as string) as Record<string, unknown>;
    assert.equal(body["statement_id"], "custom-id");
    assert.equal(body["exec_timeout"], "60s");
    assert.equal(body["max_parallelism"], 8);
    assert.equal(body["format"], "json");
  });
});

describe("StatementHandle.fetchOnce", () => {
  it("fetches status from server and updates internal state", async () => {
    const status = runningStatus("stmt-1");
    const { fn, calls } = makeFetchStub([jsonResponse(200, status)]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1");

    await handle.fetchOnce();

    assert.equal(calls.length, 1);
    assert.equal(handle.status()?.status, "running");
  });

  it("skips the request when already finished", async () => {
    const finished = finishedStatus(emptyResultSet());
    const { fn, calls } = makeFetchStub([]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1", "json", finished);

    await handle.fetchOnce();

    assert.equal(calls.length, 0);
  });

  it("skips the request when already failed", async () => {
    const failed = failedStatus("something went wrong");
    const { fn, calls } = makeFetchStub([]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1", "json", failed);

    await handle.fetchOnce();

    assert.equal(calls.length, 0);
  });

  it("skips the request when already cancelled", async () => {
    const cancelled = cancelledStatus("user cancelled");
    const { fn, calls } = makeFetchStub([]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1", "json", cancelled);

    await handle.fetchOnce();

    assert.equal(calls.length, 0);
  });
});

describe("StatementHandle.fetch", () => {
  it("returns ResultSet immediately when first poll returns finished", async () => {
    const resultSet = {
      metadata: { fields: [{ name: "x", data_type: "int" as const }], num_rows: 1 },
      format: "json" as const,
      rows: [["42"]],
    };
    const finished = finishedStatus(resultSet);
    const { fn, calls } = makeFetchStub([jsonResponse(200, finished)]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1");

    const rs = await handle.fetch(noDelay);

    assert.equal(calls.length, 1);
    assert.equal(rs.numRows(), 1);
  });

  it("polls through pending→running→finished and returns ResultSet", async () => {
    const finished = finishedStatus(emptyResultSet());
    const { fn, calls } = makeFetchStub([
      jsonResponse(200, pendingStatus()),
      jsonResponse(200, runningStatus()),
      jsonResponse(200, finished),
    ]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1");

    const rs = await handle.fetch(noDelay);

    assert.equal(calls.length, 3);
    assert.equal(rs.numRows(), 0);
  });

  it("throws ScopeDBError when statement fails with in-band status", async () => {
    const { fn } = makeFetchStub([jsonResponse(200, failedStatus("query error"))]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1");

    await assert.rejects(
      () => handle.fetch(noDelay),
      (err: unknown) => {
        assert.ok(err instanceof ScopeDBError);
        assert.ok(err.message.includes("query error"), `message was: ${err.message}`);
        return true;
      },
    );
  });

  it("throws ScopeDBError when statement is cancelled with in-band status", async () => {
    const { fn } = makeFetchStub([jsonResponse(200, cancelledStatus("cancelled by user"))]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1");

    await assert.rejects(
      () => handle.fetch(noDelay),
      (err: unknown) => {
        assert.ok(err instanceof ScopeDBError);
        assert.ok(err.message.includes("cancelled by user"), `message was: ${err.message}`);
        return true;
      },
    );
  });

  it("respects AbortSignal and throws on abort", async () => {
    // pending forever — the signal will abort it
    const { fn } = makeFetchStub([
      jsonResponse(200, pendingStatus()),
      jsonResponse(200, pendingStatus()),
      jsonResponse(200, pendingStatus()),
    ]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1");
    const controller = new AbortController();

    const promise = handle.fetch({ initialDelayMs: 5, maxDelayMs: 10, signal: controller.signal });

    // Abort after first fetch completes
    setTimeout(() => controller.abort(), 1);

    await assert.rejects(promise);
  });
});

describe("StatementHandle.cancel", () => {
  it("sends POST cancel when statement is still pending", async () => {
    const cancelResult = {
      statement_id: "stmt-1",
      created_at: "2024-01-01T00:00:00Z",
      status: "cancelled" as const,
      message: "cancelled",
    };
    const pending = pendingStatus();
    const { fn, calls } = makeFetchStub([jsonResponse(200, cancelResult)]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1", "json", pending);

    const result = await handle.cancel();

    assert.equal(calls.length, 1);
    assert.ok(calls[0]!.url.endsWith("/cancel"), `unexpected URL: ${calls[0]!.url}`);
    assert.equal(result.status, "cancelled");
  });

  it("sends POST cancel when statement is running", async () => {
    const cancelResult = {
      statement_id: "stmt-1",
      created_at: "2024-01-01T00:00:00Z",
      status: "cancelled" as const,
      message: "cancelled",
    };
    const { fn, calls } = makeFetchStub([jsonResponse(200, cancelResult)]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1", "json", runningStatus());

    await handle.cancel();

    assert.equal(calls.length, 1);
  });

  it("returns synthetic result without HTTP call when already finished", async () => {
    const finished = finishedStatus(emptyResultSet());
    const { fn, calls } = makeFetchStub([]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1", "json", finished);

    const result = await handle.cancel();

    assert.equal(calls.length, 0);
    assert.equal(result.status, "finished");
  });

  it("returns synthetic result without HTTP call when already failed", async () => {
    const failed = failedStatus("something went wrong");
    const { fn, calls } = makeFetchStub([]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1", "json", failed);

    const result = await handle.cancel();

    assert.equal(calls.length, 0);
    assert.equal(result.status, "failed");
  });

  it("returns synthetic result without HTTP call when already cancelled", async () => {
    const cancelled = cancelledStatus("already cancelled");
    const { fn, calls } = makeFetchStub([]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1", "json", cancelled);

    const result = await handle.cancel();

    assert.equal(calls.length, 0);
    assert.equal(result.status, "cancelled");
  });

  it("updates internal status after cancel returns failed", async () => {
    const cancelResult = {
      statement_id: "stmt-1",
      created_at: "2024-01-01T00:00:00Z",
      status: "failed" as const,
      message: "execution failed",
    };
    const { fn } = makeFetchStub([jsonResponse(200, cancelResult)]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1", "json", pendingStatus());

    await handle.cancel();

    assert.equal(handle.status()?.status, "failed");
  });

  it("updates internal status after cancel returns cancelled", async () => {
    const cancelResult = {
      statement_id: "stmt-1",
      created_at: "2024-01-01T00:00:00Z",
      status: "cancelled" as const,
      message: "done",
    };
    const { fn } = makeFetchStub([jsonResponse(200, cancelResult)]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const handle = new StatementHandle(client, "stmt-1", "json", runningStatus());

    await handle.cancel();

    assert.equal(handle.status()?.status, "cancelled");
  });
});
