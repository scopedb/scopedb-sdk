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

import type { Client, RequestOptions } from "./client.js";
import { ScopeDBError } from "./errors.js";
import type {
  ResultFormat,
  StatementCancelResult,
  StatementEstimatedProgress,
  StatementStatus,
} from "./protocol.js";
import { statementIsFinished, statementIsTerminated } from "./protocol.js";
import type { Value } from "./result.js";
import { ResultSet } from "./result.js";

export interface FetchOptions extends RequestOptions {
  /**
   * Initial polling delay in milliseconds.
   * The delay doubles on each poll up to `maxDelayMs`.
   * @default 5
   */
  initialDelayMs?: number;
  /**
   * Maximum polling delay in milliseconds.
   * @default 1000
   */
  maxDelayMs?: number;
}

export class Statement {
  private statementIdValue?: string;
  private execTimeoutValue?: string;
  private maxParallelismValue?: number;
  private readonly format: ResultFormat = "json";

  constructor(
    private readonly client: Client,
    private readonly statementText: string,
  ) {}

  withStatementId(statementId: string): this {
    this.statementIdValue = statementId;
    return this;
  }

  withExecTimeout(execTimeout: string): this {
    this.execTimeoutValue = execTimeout;
    return this;
  }

  withMaxParallelism(maxParallelism: number): this {
    this.maxParallelismValue = maxParallelism;
    return this;
  }

  async submit(options: RequestOptions = {}): Promise<StatementHandle> {
    const status = await this.client.submitStatement(
      {
        statement: this.statementText,
        statement_id: this.statementIdValue,
        exec_timeout: this.execTimeoutValue,
        max_parallelism: this.maxParallelismValue,
        format: this.format,
      },
      options,
    );

    return new StatementHandle(this.client, status.statement_id, this.format, status);
  }

  async execute(options: FetchOptions = {}): Promise<ResultSet> {
    const handle = await this.submit(options);
    return handle.fetch(options);
  }

  /**
   * Executes the statement and returns the first row as a plain object, or
   * `null` if the result set is empty.
   *
   * Useful for point lookups and aggregate queries that return at most one row.
   *
   * @example
   * const row = await client.statement("SELECT count(*) AS n FROM events").executeOne();
   * console.log(row?.["n"]); // bigint
   */
  async executeOne(options: FetchOptions = {}): Promise<Record<string, Value> | null> {
    return (await this.execute(options)).first();
  }
}

export class StatementHandle {
  constructor(
    private readonly client: Client,
    readonly statementId: string,
    private readonly format: ResultFormat = "json",
    private currentStatus?: StatementStatus,
  ) {}

  status(): StatementStatus | undefined {
    return this.currentStatus;
  }

  progress(): StatementEstimatedProgress | undefined {
    return this.currentStatus?.progress;
  }

  resultSet(): ResultSet | null {
    if (this.currentStatus === undefined || !statementIsFinished(this.currentStatus)) {
      return null;
    }
    return ResultSet.fromStatementResultSet(this.currentStatus.result_set);
  }

  async fetchOnce(options: RequestOptions = {}): Promise<void> {
    if (this.currentStatus !== undefined && statementIsTerminated(this.currentStatus)) {
      return;
    }

    this.currentStatus = await this.client.fetchStatement(this.statementId, options);
  }

  async fetch(options: FetchOptions = {}): Promise<ResultSet> {
    let delayMs = options.initialDelayMs ?? 5;
    const maxDelayMs = options.maxDelayMs ?? 1000;

    for (;;) {
      await this.fetchOnce(options);

      if (this.currentStatus === undefined) {
        throw new ScopeDBError("Unexpected", "statement fetch returned no status");
      }

      switch (this.currentStatus.status) {
        case "finished":
          return ResultSet.fromStatementResultSet(this.currentStatus.result_set);
        case "failed":
        case "cancelled":
          throw new ScopeDBError("StatementFailed", this.currentStatus.message);
        case "pending":
        case "running":
          await sleep(delayMs, options.signal);
          if (delayMs < maxDelayMs) {
            delayMs = Math.min(delayMs * 2, maxDelayMs);
          }
          break;
      }
    }
  }

  async cancel(options: RequestOptions = {}): Promise<StatementCancelResult> {
    if (this.currentStatus !== undefined) {
      switch (this.currentStatus.status) {
        case "finished":
          return {
            statement_id: this.currentStatus.statement_id,
            created_at: this.currentStatus.created_at,
            status: "finished",
            message: "statement is finished",
          };
        case "failed":
          return {
            statement_id: this.currentStatus.statement_id,
            created_at: this.currentStatus.created_at,
            status: "failed",
            message: "statement is failed",
          };
        case "cancelled":
          return {
            statement_id: this.currentStatus.statement_id,
            created_at: this.currentStatus.created_at,
            status: "cancelled",
            message: "statement is cancelled",
          };
        case "pending":
        case "running":
          break;
      }
    }

    const result = await this.client.cancelStatement(this.statementId, options);
    if (result.status === "failed") {
      this.currentStatus = {
        status: "failed",
        statement_id: result.statement_id,
        created_at: result.created_at,
        progress: emptyProgress(),
        message: result.message,
      };
    } else if (result.status === "cancelled") {
      this.currentStatus = {
        status: "cancelled",
        statement_id: result.statement_id,
        created_at: result.created_at,
        progress: emptyProgress(),
        message: result.message,
      };
    }
    return result;
  }
}

function emptyProgress(): StatementEstimatedProgress {
  return {
    total_percentage: 0,
    nanos_from_submitted: 0,
    nanos_from_started: 0,
    total_stages: 0,
    total_partitions: 0,
    total_rows: 0,
    total_compressed_bytes: 0,
    total_uncompressed_bytes: 0,
    scanned_stages: 0,
    scanned_partitions: 0,
    scanned_rows: 0,
    scanned_compressed_bytes: 0,
    scanned_uncompressed_bytes: 0,
    skipped_partitions: 0,
    skipped_rows: 0,
    skipped_compressed_bytes: 0,
    skipped_uncompressed_bytes: 0,
  };
}

async function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  if (signal?.aborted) {
    throw signal.reason ?? new Error("aborted");
  }

  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(resolve, ms);
    const onAbort = () => {
      clearTimeout(timer);
      reject(signal?.reason ?? new Error("aborted"));
    };

    signal?.addEventListener("abort", onAbort, { once: true });
  });
}
