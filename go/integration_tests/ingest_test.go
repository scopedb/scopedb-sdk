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

package integration_tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/gkampitakis/go-snaps/snaps"
	"github.com/google/uuid"
	scopedb "github.com/scopedb/scopedb-sdk/go"
	testkit "github.com/scopedb/scopedb-sdk/go/integration_tests/internal"
	"github.com/stretchr/testify/require"
)

func TestReadAfterWrite(t *testing.T) {
	tk := testkit.NewTestKit(t)
	if tk == nil {
		t.Skip("nil testkit")
	}
	defer tk.Close()

	ctx := context.Background()

	tableName := tk.RandomName()
	statement := fmt.Sprintf("CREATE TABLE %s (a INT, v VARIANT)", tableName)
	tk.NewTable(ctx, tableName, statement)

	// 1. Simple ingest and verify the result
	schema := makeSchema()
	records := makeRecords(schema)
	resp := tk.IngestArrowBatch(ctx, records, fmt.Sprintf("INSERT INTO %s", tableName))
	require.Equal(t, resp.NumRowsInserted, 2)
	require.Equal(t, resp.NumRowsUpdated, 0)
	require.Equal(t, resp.NumRowsDeleted, 0)

	statement = fmt.Sprintf("FROM %s", tableName)
	rs := tk.QueryAsArrowBatch(ctx, &scopedb.StatementRequest{
		Statement: statement,
		Format:    scopedb.ArrowJSONFormat,
	})
	snaps.MatchSnapshot(t, rs.Metadata)
	snaps.MatchSnapshot(t, fmt.Sprintf("%v", rs.Records))

	// 2. Merge data and verify the result
	mergeRecords := makeMergeRecords(schema)
	resp = tk.IngestArrowBatch(ctx, mergeRecords, fmt.Sprintf(`
	MERGE INTO %s
	ON %s.a = $0
	WHEN MATCHED THEN UPDATE ALL
	`, tableName, tableName))
	require.Equal(t, resp.NumRowsInserted, 0)
	require.Equal(t, resp.NumRowsUpdated, 1)
	require.Equal(t, resp.NumRowsDeleted, 0)

	id, err := uuid.NewRandom()
	require.NoError(t, err)

	rs = tk.QueryAsArrowBatch(ctx, &scopedb.StatementRequest{
		StatementId: &id,
		Statement:   statement,
		Format:      scopedb.ArrowJSONFormat,
	})
	snaps.MatchSnapshot(t, rs.Metadata)
	snaps.MatchSnapshot(t, fmt.Sprintf("%v", rs.Records))
}

func makeSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "v", Type: arrow.StructOf(arrow.Field{
			Name: "int", Type: arrow.PrimitiveTypes.Int64,
			Nullable: true,
		})},
	}, nil)
}

func makeRecords(schema *arrow.Schema) []arrow.Record {
	// Data:
	// a:int64 | v:struct<int:int64>
	// --------+--------------------
	// 1       | { int: 1 }
	// 2       | { int: 2 }
	pool := memory.NewGoAllocator()
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).Append(1)
	b.Field(0).(*array.Int64Builder).Append(2)
	b.Field(1).(*array.StructBuilder).FieldBuilder(0).(*array.Int64Builder).Append(1)
	b.Field(1).(*array.StructBuilder).Append(true)
	b.Field(1).(*array.StructBuilder).FieldBuilder(0).(*array.Int64Builder).Append(2)
	b.Field(1).(*array.StructBuilder).Append(true)
	return []arrow.Record{b.NewRecord()}
}

func makeMergeRecords(schema *arrow.Schema) []arrow.Record {
	// Merge data:
	// a:int64 | s:struct<int:int64>
	// --------+--------------------
	// 1       | { int: 2 }
	pool := memory.NewGoAllocator()
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).Append(1)
	b.Field(1).(*array.StructBuilder).FieldBuilder(0).(*array.Int64Builder).Append(2)
	b.Field(1).(*array.StructBuilder).Append(true)
	return []arrow.Record{b.NewRecord()}
}
