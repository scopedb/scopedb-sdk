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

export type DataType =
  | "int"
  | "uint"
  | "float"
  | "timestamp"
  | "interval"
  | "boolean"
  | "string"
  | "binary"
  | "array"
  | "object"
  | "any"
  | "null";

export interface FieldSchemaPayload {
  name: string;
  data_type: DataType;
}

export interface ResultSetMetadata {
  fields: FieldSchemaPayload[];
  num_rows: number;
}

export type ResultFormat = "json";

export interface StatementResultSet {
  metadata: ResultSetMetadata;
  format: ResultFormat;
  rows: Array<Array<string | null>>;
}

export interface StatementProgress {
  total_stages: number;
  total_partitions: number;
  total_rows: number;
  total_compressed_bytes: number;
  total_uncompressed_bytes: number;
  scanned_stages: number;
  scanned_partitions: number;
  scanned_rows: number;
  scanned_compressed_bytes: number;
  scanned_uncompressed_bytes: number;
  skipped_partitions: number;
  skipped_rows: number;
  skipped_compressed_bytes: number;
  skipped_uncompressed_bytes: number;
}

export interface StatementEstimatedProgress extends StatementProgress {
  total_percentage: number;
  nanos_from_submitted: number;
  nanos_from_started: number;
}

interface StatementStatusBase {
  statement_id: string;
  created_at: string;
  progress: StatementEstimatedProgress;
}

export interface StatementStatusPending extends StatementStatusBase {
  status: "pending";
}

export interface StatementStatusRunning extends StatementStatusBase {
  status: "running";
}

export interface StatementStatusFinished extends StatementStatusBase {
  status: "finished";
  result_set: StatementResultSet;
}

export interface StatementStatusFailed extends StatementStatusBase {
  status: "failed";
  message: string;
}

export interface StatementStatusCancelled extends StatementStatusBase {
  status: "cancelled";
  message: string;
}

export type StatementStatus =
  | StatementStatusPending
  | StatementStatusRunning
  | StatementStatusFinished
  | StatementStatusFailed
  | StatementStatusCancelled;

export interface StatementRequest {
  statement: string;
  statement_id?: string;
  exec_timeout?: string;
  max_parallelism?: number;
  format: ResultFormat;
}

export interface StatementCancelResult {
  statement_id: string;
  status: "finished" | "failed" | "cancelled";
  message: string;
  created_at: string;
}

export type IngestType = "committed" | "buffered";

export interface IngestRequest {
  type: IngestType;
  data: {
    format: "json";
    rows: string;
  };
  statement: string;
}

export interface IngestResult {
  num_rows_inserted: number;
}

export interface ErrorPayload {
  message: string;
}

export function statementIsFinished(status: StatementStatus): status is StatementStatusFinished {
  return status.status === "finished";
}

export function statementIsTerminated(status: StatementStatus): boolean {
  return (
    status.status === "finished" ||
    status.status === "failed" ||
    status.status === "cancelled"
  );
}
