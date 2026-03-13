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
import type { DataType } from "./protocol.js";
import { FieldSchema, Schema } from "./result.js";

export class Table {
  private databaseName?: string;
  private schemaName?: string;

  constructor(
    private readonly client: Client,
    private readonly tableName: string,
  ) {}

  withDatabase(database: string): this {
    this.databaseName = database;
    return this;
  }

  withSchema(schema: string): this {
    this.schemaName = schema;
    return this;
  }

  identifier(): string {
    const parts: string[] = [];
    if (this.databaseName !== undefined) {
      parts.push(quoteIdent(this.databaseName, "`"));
    }
    if (this.schemaName !== undefined) {
      parts.push(quoteIdent(this.schemaName, "`"));
    }
    parts.push(quoteIdent(this.tableName, "`"));
    return parts.join(".");
  }

  async drop(options: RequestOptions = {}): Promise<void> {
    await this.client.statement(`DROP TABLE ${this.identifier()}`).execute(options);
  }

  async tableSchema(options: RequestOptions = {}): Promise<Schema> {
    const databaseName = this.databaseName ?? "scopedb";
    const schemaName = this.schemaName ?? "public";
    const statement = `
            FROM scopedb.system.columns
            WHERE table_name = ${quoteStringLiteral(this.tableName)}
              AND schema_name = ${quoteStringLiteral(schemaName)}
              AND database_name = ${quoteStringLiteral(databaseName)}
            SELECT column_name, data_type
            `;

    const rows = await this.client.statement(statement).execute(options);
    const values = rows.intoValues();

    return new Schema(
      values.map((row) => {
        if (row.length !== 2) {
          throw new ScopeDBError(
            "Unexpected",
            `expected 2 columns in table schema row, got ${row.length}`,
          );
        }

        const [columnName, dataType] = row;
        if (typeof columnName !== "string") {
          throw new ScopeDBError("Unexpected", `expected string column name, got ${columnName}`);
        }
        if (typeof dataType !== "string") {
          throw new ScopeDBError("Unexpected", `expected string data type, got ${dataType}`);
        }

        return new FieldSchema(columnName, dataType as DataType);
      }),
    );
  }
}

function quoteIdent(input: string, quote: string): string {
  return quoteScopeQL(input, quote);
}

function quoteStringLiteral(input: string): string {
  return quoteScopeQL(input, "'");
}

function quoteScopeQL(input: string, quote: string): string {
  let out = quote;
  for (const ch of input) {
    switch (ch) {
      case "\t":
        out += "\\t";
        break;
      case "\n":
        out += "\\n";
        break;
      case "\r":
        out += "\\r";
        break;
      case "\\":
        out += "\\\\";
        break;
      default:
        if (ch === quote) {
          out += `\\${ch}`;
        } else if (ch < " ") {
          out += `\\x${ch.charCodeAt(0).toString(16).padStart(2, "0")}`;
        } else {
          out += ch;
        }
    }
  }
  out += quote;
  return out;
}
