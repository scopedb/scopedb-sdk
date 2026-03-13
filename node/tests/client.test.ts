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
import {
  emptyResultSet,
  finishedStatus,
  jsonResponse,
  makeFetchStub,
  pendingStatus,
  textResponse,
} from "./helpers.js";

describe("Client.submitStatement", () => {
  it("sends POST /v1/statements with correct body", async () => {
    const status = pendingStatus();
    const { fn, calls } = makeFetchStub([jsonResponse(200, status)]);
    const client = new Client("http://localhost:8080", { fetch: fn });

    await client.submitStatement({ statement: "SELECT 1", format: "json" });

    assert.equal(calls.length, 1);
    const call = calls[0]!;
    assert.ok(call.url.endsWith("/v1/statements"));
    assert.equal((call.init as RequestInit).method, "POST");
    const body = JSON.parse((call.init as RequestInit).body as string) as Record<string, unknown>;
    assert.equal(body["statement"], "SELECT 1");
    assert.equal(body["format"], "json");
  });

  it("includes optional fields when set", async () => {
    const status = pendingStatus();
    const { fn, calls } = makeFetchStub([jsonResponse(200, status)]);
    const client = new Client("http://localhost:8080", { fetch: fn });

    await client.submitStatement({
      statement: "SELECT 1",
      format: "json",
      statement_id: "my-id",
      exec_timeout: "30s",
      max_parallelism: 4,
    });

    const body = JSON.parse((calls[0]!.init as RequestInit).body as string) as Record<string, unknown>;
    assert.equal(body["statement_id"], "my-id");
    assert.equal(body["exec_timeout"], "30s");
    assert.equal(body["max_parallelism"], 4);
  });

  it("returns the statement status from the response", async () => {
    const status = pendingStatus("stmt-42");
    const { fn } = makeFetchStub([jsonResponse(200, status)]);
    const client = new Client("http://localhost:8080", { fetch: fn });

    const result = await client.submitStatement({ statement: "SELECT 1", format: "json" });

    assert.equal(result.statement_id, "stmt-42");
    assert.equal(result.status, "pending");
  });
});

describe("Client.fetchStatement", () => {
  it("sends GET /v1/statements/{id}?format=json", async () => {
    const status = finishedStatus(emptyResultSet(), "stmt-99");
    const { fn, calls } = makeFetchStub([jsonResponse(200, status)]);
    const client = new Client("http://localhost:8080", { fetch: fn });

    await client.fetchStatement("stmt-99");

    assert.equal(calls.length, 1);
    const call = calls[0]!;
    assert.ok(call.url.includes("/v1/statements/stmt-99"), `unexpected URL: ${call.url}`);
    assert.ok(call.url.includes("format=json"), `missing format=json in URL: ${call.url}`);
    assert.equal((call.init as RequestInit).method, "GET");
  });

  it("returns the statement status from the response", async () => {
    const status = finishedStatus(emptyResultSet(), "stmt-99");
    const { fn } = makeFetchStub([jsonResponse(200, status)]);
    const client = new Client("http://localhost:8080", { fetch: fn });

    const result = await client.fetchStatement("stmt-99");
    assert.equal(result.status, "finished");
    assert.equal(result.statement_id, "stmt-99");
  });
});

describe("Client.cancelStatement", () => {
  it("sends POST /v1/statements/{id}/cancel", async () => {
    const cancelResult = {
      statement_id: "stmt-1",
      created_at: "2024-01-01T00:00:00Z",
      status: "cancelled",
      message: "cancelled by user",
    };
    const { fn, calls } = makeFetchStub([jsonResponse(200, cancelResult)]);
    const client = new Client("http://localhost:8080", { fetch: fn });

    await client.cancelStatement("stmt-1");

    assert.equal(calls.length, 1);
    const call = calls[0]!;
    assert.ok(call.url.endsWith("/v1/statements/stmt-1/cancel"), `unexpected URL: ${call.url}`);
    assert.equal((call.init as RequestInit).method, "POST");
  });
});

describe("Client error mapping", () => {
  it("throws permanent ScopeDBError on 404 with JSON body", async () => {
    const { fn } = makeFetchStub([jsonResponse(404, { message: "not found" })]);
    const client = new Client("http://localhost:8080", { fetch: fn });

    await assert.rejects(
      () => client.fetchStatement("no-such-id"),
      (err: unknown) => {
        assert.ok(err instanceof ScopeDBError);
        assert.ok(err.message.includes("not found"), `message was: ${err.message}`);
        assert.ok(err.isPermanent(), `expected permanent, got ${err.status()}`);
        return true;
      },
    );
  });

  it("throws temporary ScopeDBError on 503", async () => {
    const { fn } = makeFetchStub([jsonResponse(503, { message: "service unavailable" })]);
    const client = new Client("http://localhost:8080", { fetch: fn });

    await assert.rejects(
      () => client.healthCheck(),
      (err: unknown) => {
        assert.ok(err instanceof ScopeDBError);
        assert.ok(err.isTemporary(), `expected temporary, got ${err.status()}`);
        return true;
      },
    );
  });

  it("throws temporary ScopeDBError on 429", async () => {
    const { fn } = makeFetchStub([jsonResponse(429, { message: "rate limited" })]);
    const client = new Client("http://localhost:8080", { fetch: fn });

    await assert.rejects(
      () => client.healthCheck(),
      (err: unknown) => {
        assert.ok(err instanceof ScopeDBError);
        assert.ok(err.isTemporary(), `expected temporary, got ${err.status()}`);
        return true;
      },
    );
  });

  it("falls back to plain-text body when response is not JSON", async () => {
    const { fn } = makeFetchStub([textResponse(500, "internal server error")]);
    const client = new Client("http://localhost:8080", { fetch: fn });

    await assert.rejects(
      () => client.healthCheck(),
      (err: unknown) => {
        assert.ok(err instanceof ScopeDBError);
        assert.ok(
          err.message.includes("internal server error"),
          `message was: ${err.message}`,
        );
        return true;
      },
    );
  });

  it("throws ScopeDBError on transport failure", async () => {
    const { fn } = makeFetchStub([]);
    const client = new Client("http://localhost:8080", { fetch: fn });

    await assert.rejects(
      () => client.healthCheck(),
      (err: unknown) => {
        assert.ok(err instanceof ScopeDBError);
        return true;
      },
    );
  });
});

describe("Client.healthCheck", () => {
  it("sends GET /v1/health", async () => {
    const { fn, calls } = makeFetchStub([jsonResponse(200, {})]);
    const client = new Client("http://localhost:8080", { fetch: fn });

    await client.healthCheck();

    assert.equal(calls.length, 1);
    const call = calls[0]!;
    assert.ok(call.url.endsWith("/v1/health"), `unexpected URL: ${call.url}`);
    assert.equal((call.init as RequestInit).method, "GET");
  });
});
