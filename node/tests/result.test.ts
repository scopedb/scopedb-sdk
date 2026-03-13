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
import type { DataType } from "../src/protocol.js";
import { FieldSchema, ResultSet, Schema } from "../src/result.js";
import { ScopeDBError } from "../src/errors.js";

function makeResultSet(
  fields: Array<{ name: string; data_type: DataType }>,
  rows: Array<Array<string | null>>,
): ResultSet {
  return ResultSet.fromStatementResultSet({
    metadata: { fields, num_rows: rows.length },
    format: "json",
    rows,
  });
}

describe("ResultSet.fromStatementResultSet", () => {
  it("builds schema from field metadata", () => {
    const rs = makeResultSet(
      [{ name: "id", data_type: "int" }, { name: "name", data_type: "string" }],
      [],
    );
    const fields = rs.schema().fields();
    assert.equal(fields.length, 2);
    assert.equal(fields[0]!.name(), "id");
    assert.equal(fields[0]!.dataType(), "int");
    assert.equal(fields[1]!.name(), "name");
    assert.equal(fields[1]!.dataType(), "string");
  });

  it("reports correct numRows", () => {
    const rs = makeResultSet(
      [{ name: "x", data_type: "int" }],
      [["1"], ["2"], ["3"]],
    );
    assert.equal(rs.numRows(), 3);
  });

  it("preserves raw JSON rows via jsonRows()", () => {
    const rs = makeResultSet(
      [{ name: "x", data_type: "int" }, { name: "y", data_type: "string" }],
      [["1", "hello"], ["2", null]],
    );
    const rows = rs.jsonRows();
    assert.deepEqual(rows[0], ["1", "hello"]);
    assert.deepEqual(rows[1], ["2", null]);
  });
});

describe("ResultSet.intoValues — type parsing", () => {
  it("parses int as BigInt", () => {
    const rs = makeResultSet([{ name: "n", data_type: "int" }], [["42"]]);
    assert.equal(rs.intoValues()[0]![0], 42n);
  });

  it("parses uint as BigInt", () => {
    const rs = makeResultSet([{ name: "n", data_type: "uint" }], [["100"]]);
    assert.equal(rs.intoValues()[0]![0], 100n);
  });

  it("parses u_int (backward-compat alias) as BigInt", () => {
    const rs = makeResultSet([{ name: "n", data_type: "u_int" }], [["99"]]);
    assert.equal(rs.intoValues()[0]![0], 99n);
  });

  it("parses float as number", () => {
    const rs = makeResultSet([{ name: "f", data_type: "float" }], [["3.14"]]);
    const val = rs.intoValues()[0]![0];
    assert.ok(typeof val === "number");
    assert.ok(Math.abs((val as number) - 3.14) < 1e-9);
  });

  it("parses boolean true", () => {
    const rs = makeResultSet([{ name: "b", data_type: "boolean" }], [["true"]]);
    assert.equal(rs.intoValues()[0]![0], true);
  });

  it("parses boolean false", () => {
    const rs = makeResultSet([{ name: "b", data_type: "boolean" }], [["false"]]);
    assert.equal(rs.intoValues()[0]![0], false);
  });

  it("parses timestamp as Date", () => {
    const rs = makeResultSet([{ name: "t", data_type: "timestamp" }], [["2024-06-01T12:00:00Z"]]);
    const val = rs.intoValues()[0]![0];
    assert.ok(val instanceof Date);
    assert.equal((val as Date).toISOString(), "2024-06-01T12:00:00.000Z");
  });

  it("returns string as-is for string type", () => {
    const rs = makeResultSet([{ name: "s", data_type: "string" }], [["hello"]]);
    assert.equal(rs.intoValues()[0]![0], "hello");
  });

  it("returns string as-is for interval type", () => {
    const rs = makeResultSet([{ name: "d", data_type: "interval" }], [["P1D"]]);
    assert.equal(rs.intoValues()[0]![0], "P1D");
  });

  it("returns null for null cell regardless of type", () => {
    const types: DataType[] = ["int", "uint", "float", "boolean", "timestamp", "string"];
    for (const dt of types) {
      const rs = makeResultSet([{ name: "v", data_type: dt }], [[null]]);
      assert.equal(rs.intoValues()[0]![0], null, `expected null for type ${dt}`);
    }
  });

  it("throws ScopeDBError on invalid integer value", () => {
    const rs = makeResultSet([{ name: "n", data_type: "int" }], [["not-a-number"]]);
    assert.throws(() => rs.intoValues(), ScopeDBError);
  });

  it("throws ScopeDBError on invalid float value", () => {
    const rs = makeResultSet([{ name: "f", data_type: "float" }], [["NaN"]]);
    assert.throws(() => rs.intoValues(), ScopeDBError);
  });

  it("throws ScopeDBError on invalid boolean value", () => {
    const rs = makeResultSet([{ name: "b", data_type: "boolean" }], [["yes"]]);
    assert.throws(() => rs.intoValues(), ScopeDBError);
  });

  it("throws ScopeDBError on invalid timestamp value", () => {
    const rs = makeResultSet([{ name: "t", data_type: "timestamp" }], [["not-a-date"]]);
    assert.throws(() => rs.intoValues(), ScopeDBError);
  });

  it("throws ScopeDBError when row field count mismatches schema", () => {
    const rs = makeResultSet(
      [{ name: "a", data_type: "string" }, { name: "b", data_type: "string" }],
      [["only-one-cell"]],
    );
    assert.throws(() => rs.intoValues(), ScopeDBError);
  });

  it("handles multiple rows with mixed types correctly", () => {
    const rs = makeResultSet(
      [
        { name: "id", data_type: "int" },
        { name: "score", data_type: "float" },
        { name: "label", data_type: "string" },
        { name: "active", data_type: "boolean" },
      ],
      [
        ["1", "9.5", "alpha", "true"],
        ["2", "3.0", "beta", "false"],
      ],
    );
    const rows = rs.intoValues();
    assert.equal(rows[0]![0], 1n);
    assert.equal(rows[1]![2], "beta");
    assert.equal(rows[1]![3], false);
  });
});

describe("Schema and FieldSchema", () => {
  it("returns fields in order", () => {
    const schema = new Schema([
      new FieldSchema("a", "int"),
      new FieldSchema("b", "string"),
      new FieldSchema("c", "boolean"),
    ]);
    const fields = schema.fields();
    assert.equal(fields.length, 3);
    assert.equal(fields[2]!.name(), "c");
    assert.equal(fields[2]!.dataType(), "boolean");
  });
});
