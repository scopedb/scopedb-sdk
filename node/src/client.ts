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

import { ScopeDBError, asScopeDBError } from "./errors.js";
import { IngestStreamBuilder } from "./ingest-stream.js";
import type {
  ErrorPayload,
  IngestRequest,
  IngestResult,
  StatementCancelResult,
  StatementRequest,
  StatementStatus,
} from "./protocol.js";
import type { ResultSet } from "./result.js";
import { Statement, StatementHandle } from "./statement.js";
import type { FetchOptions } from "./statement.js";
import { Table } from "./table.js";

export interface RequestOptions {
  signal?: AbortSignal;
}

export interface ClientOptions {
  fetch?: typeof globalThis.fetch;
  /** Default headers sent with every request. */
  headers?: HeadersInit;
  /**
   * Bearer token for authentication.
   * Equivalent to `headers: { Authorization: 'Bearer <token>' }`.
   * If both `token` and `headers.Authorization` are provided, `token` wins.
   */
  token?: string;
}

export class Client {
  private readonly endpoint: URL;
  private readonly fetchFn: typeof globalThis.fetch;
  private readonly defaultHeaders: Headers;

  constructor(endpoint: string | URL, options: ClientOptions = {}) {
    try {
      this.endpoint = normalizeEndpoint(endpoint);
    } catch (cause) {
      throw new ScopeDBError("ConfigInvalid", "failed to parse endpoint", { cause });
    }

    this.fetchFn = options.fetch ?? globalThis.fetch;
    this.defaultHeaders = new Headers(options.headers);
    if (options.token !== undefined) {
      this.defaultHeaders.set("Authorization", `Bearer ${options.token}`);
    }
  }

  statement(statement: string): Statement {
    return new Statement(this, statement);
  }

  statementHandle(statementId: string): StatementHandle {
    return new StatementHandle(this, statementId);
  }

  table(table: string): Table {
    return new Table(this, table);
  }

  ingestStream(statement: string): IngestStreamBuilder {
    return new IngestStreamBuilder(this, statement);
  }

  /**
   * Executes a ScopeQL statement and returns all rows.
   *
   * Shorthand for `client.statement(sql).execute(options)`.
   *
   * @example
   * const result = await client.query("SELECT * FROM events LIMIT 10");
   * for (const row of result.intoObjects()) {
   *   console.log(row);
   * }
   */
  async query(sql: string, options: FetchOptions = {}): Promise<ResultSet> {
    return this.statement(sql).execute(options);
  }

  async healthCheck(options: RequestOptions = {}): Promise<void> {
    await this.request("v1/health", {
      method: "GET",
      signal: options.signal,
    });
  }

  async insert(
    rows: string,
    transform: string,
    options: RequestOptions = {},
  ): Promise<IngestResult> {
    return this.ingest(
      {
        type: "committed",
        data: { format: "json", rows },
        statement: transform,
      },
      options,
    );
  }

  async submitStatement(
    request: StatementRequest,
    options: RequestOptions = {},
  ): Promise<StatementStatus> {
    return this.requestJson("v1/statements", {
      method: "POST",
      body: JSON.stringify(request),
      signal: options.signal,
    });
  }

  async fetchStatement(
    statementId: string,
    options: RequestOptions = {},
  ): Promise<StatementStatus> {
    const url = this.makeUrl(`v1/statements/${statementId}`);
    url.searchParams.set("format", "json");
    return this.requestJson(url, {
      method: "GET",
      signal: options.signal,
    });
  }

  async cancelStatement(
    statementId: string,
    options: RequestOptions = {},
  ): Promise<StatementCancelResult> {
    return this.requestJson(`v1/statements/${statementId}/cancel`, {
      method: "POST",
      signal: options.signal,
    });
  }

  async ingest(
    request: IngestRequest,
    options: RequestOptions = {},
  ): Promise<IngestResult> {
    return this.requestJson("v1/ingest", {
      method: "POST",
      body: JSON.stringify(request),
      signal: options.signal,
    });
  }

  private async requestJson<T>(
    path: string | URL,
    init: RequestInit,
  ): Promise<T> {
    const response = await this.request(path, init);
    try {
      return (await response.json()) as T;
    } catch (cause) {
      throw new ScopeDBError("Unexpected", "failed to parse response body", {
        cause,
      });
    }
  }

  private async request(path: string | URL, init: RequestInit): Promise<Response> {
    const headers = new Headers(this.defaultHeaders);
    headers.set("Accept", "application/json");
    if (init.body !== undefined) {
      headers.set("Content-Type", "application/json");
    }
    const providedHeaders = new Headers(init.headers);
    providedHeaders.forEach((value, key) => headers.set(key, value));

    const url = path instanceof URL ? path : this.makeUrl(path);

    let response: Response;
    try {
      response = await this.fetchFn(url, { ...init, headers });
    } catch (cause) {
      throw asScopeDBError("Unexpected", `failed to send request to ${url}`, cause).setTemporary();
    }

    if (response.ok) {
      return response;
    }

    throw await responseToError(response);
  }

  private makeUrl(path: string): URL {
    return new URL(path, this.endpoint);
  }
}

function normalizeEndpoint(endpoint: string | URL): URL {
  const url = new URL(endpoint.toString());
  if (!url.pathname.endsWith("/")) {
    url.pathname = `${url.pathname}/`;
  }
  return url;
}

async function responseToError(response: Response): Promise<ScopeDBError> {
  const body = await response.text();
  let message = body;
  try {
    const payload = JSON.parse(body) as Partial<ErrorPayload>;
    if (typeof payload.message === "string" && payload.message.length > 0) {
      message = payload.message;
    }
  } catch {
    // Fall back to the raw response body.
  }

  const error = new ScopeDBError(
    "Unexpected",
    `${response.status} ${response.statusText}: ${message}`,
  );
  if (
    response.status === 429 ||
    response.status === 502 ||
    response.status === 503 ||
    response.status === 504 ||
    response.status >= 500
  ) {
    return error.setTemporary();
  }
  return error.setPermanent();
}
