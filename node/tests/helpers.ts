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

import type { StatementEstimatedProgress, StatementResultSet, StatementStatus } from "../src/protocol.js";

export type FetchCall = { url: string; init?: RequestInit };

export type FetchStub = {
  fn: typeof globalThis.fetch;
  calls: FetchCall[];
};

export function makeFetchStub(responses: Response[]): FetchStub {
  const calls: FetchCall[] = [];
  let index = 0;

  const fn: typeof globalThis.fetch = async (input, init) => {
    const url =
      input instanceof URL
        ? input.toString()
        : typeof input === "string"
          ? input
          : (input as Request).url;
    calls.push({ url, init });

    const response = responses[index++];
    if (response === undefined) {
      throw new Error(
        `Unexpected fetch call #${index} — only ${responses.length} response(s) configured`,
      );
    }
    return response;
  };

  return { fn, calls };
}

export function jsonResponse(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

export function textResponse(status: number, text: string): Response {
  return new Response(text, { status });
}

export function emptyProgress(): StatementEstimatedProgress {
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

export function pendingStatus(statementId = "stmt-1"): StatementStatus {
  return { status: "pending", statement_id: statementId, created_at: "2024-01-01T00:00:00Z", progress: emptyProgress() };
}

export function runningStatus(statementId = "stmt-1"): StatementStatus {
  return { status: "running", statement_id: statementId, created_at: "2024-01-01T00:00:00Z", progress: emptyProgress() };
}

export function finishedStatus(resultSet: StatementResultSet, statementId = "stmt-1"): StatementStatus {
  return { status: "finished", statement_id: statementId, created_at: "2024-01-01T00:00:00Z", progress: emptyProgress(), result_set: resultSet };
}

export function failedStatus(message: string, statementId = "stmt-1"): StatementStatus {
  return { status: "failed", statement_id: statementId, created_at: "2024-01-01T00:00:00Z", progress: emptyProgress(), message };
}

export function cancelledStatus(message: string, statementId = "stmt-1"): StatementStatus {
  return { status: "cancelled", statement_id: statementId, created_at: "2024-01-01T00:00:00Z", progress: emptyProgress(), message };
}

export function emptyResultSet(): StatementResultSet {
  return {
    metadata: { fields: [], num_rows: 0 },
    format: "json",
    rows: [],
  };
}
