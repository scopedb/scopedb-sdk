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

import { ScopeDBError } from "./errors.js";
import type { DataType, StatementResultSet } from "./protocol.js";

export type Value = bigint | number | boolean | string | Date | null;

export class FieldSchema {
  constructor(
    private readonly fieldName: string,
    private readonly fieldDataType: DataType,
  ) {}

  name(): string {
    return this.fieldName;
  }

  dataType(): DataType {
    return this.fieldDataType;
  }
}

export class Schema {
  constructor(private readonly schemaFields: FieldSchema[]) {}

  fields(): readonly FieldSchema[] {
    return this.schemaFields;
  }
}

export class ResultSet {
  constructor(
    private readonly resultSchema: Schema,
    private readonly totalRows: number,
    private readonly rows: Array<Array<string | null>>,
  ) {}

  numRows(): number {
    return this.totalRows;
  }

  schema(): Schema {
    return this.resultSchema;
  }

  jsonRows(): ReadonlyArray<ReadonlyArray<string | null>> {
    return this.rows;
  }

  intoValues(): Value[][] {
    return this.rows.map((row) => {
      const fields = this.resultSchema.fields();
      if (row.length !== fields.length) {
        throw new ScopeDBError(
          "Unexpected",
          `row field count mismatch: expected ${fields.length}, got ${row.length}`,
        );
      }
      return row.map((cell, index) => parseCell(cell, fields[index]!.dataType()));
    });
  }

  static fromStatementResultSet(resultSet: StatementResultSet): ResultSet {
    return new ResultSet(
      new Schema(
        resultSet.metadata.fields.map(
          (field) => new FieldSchema(field.name, field.data_type),
        ),
      ),
      resultSet.metadata.num_rows,
      resultSet.rows,
    );
  }
}

function parseCell(cell: string | null, dataType: DataType): Value {
  if (cell === null) {
    return null;
  }

  switch (dataType) {
    case "int":
    case "uint":
      try {
        return BigInt(cell);
      } catch (cause) {
        throw new ScopeDBError("Unexpected", `failed to parse integer value: ${cell}`, {
          cause,
        });
      }
    case "float": {
      const value = Number(cell);
      if (Number.isNaN(value)) {
        throw new ScopeDBError("Unexpected", `failed to parse float value: ${cell}`);
      }
      return value;
    }
    case "timestamp": {
      const value = new Date(cell);
      if (Number.isNaN(value.getTime())) {
        throw new ScopeDBError("Unexpected", `failed to parse timestamp value: ${cell}`);
      }
      return value;
    }
    case "boolean":
      if (cell === "true") {
        return true;
      }
      if (cell === "false") {
        return false;
      }
      throw new ScopeDBError("Unexpected", `failed to parse boolean value: ${cell}`);
    case "interval":
    case "string":
    case "binary":
    case "array":
    case "object":
    case "any":
    case "null":
      return cell;
  }
}
