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

package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/gkampitakis/go-snaps/snaps"
	scopedb "github.com/scopedb/scopedb-sdk/go"
	"github.com/stretchr/testify/require"
)

func TestReadAfterWrite(t *testing.T) {
	ctx := context.Background()

	config := LoadConfig()
	if config == nil {
		t.Skip("Connection config is not set")
	}

	tableName, err := GenerateTableName()
	require.NoError(t, err)
	t.Logf("With tableName: %s", tableName)

	conn := scopedb.Open(config)

	statement := fmt.Sprintf("CREATE TABLE %s (a INT, v VARIANT)", tableName)
	err = conn.Execute(ctx, &scopedb.StatementRequest{
		Statement: statement,
		Format:    scopedb.ArrowJSONFormat,
	})
	require.NoError(t, err)
	defer func() {
		err := DropTable(ctx, conn, tableName)
		require.NoError(t, err)
	}()

	// 1. Simple ingest and verify the result
	schema := makeSchema()
	records := makeRecords(schema)
	ingester, err := scopedb.NewIngester(ctx, conn)
	require.NoError(t, err)
	require.NoError(t, ingester.IngestData(ctx, records))
	require.NoError(t, ingester.Commit(ctx, fmt.Sprintf("INSERT INTO %s", tableName)))

	statement = fmt.Sprintf("FROM %s", tableName)
	rs, err := conn.QueryAsArrowBatch(ctx, &scopedb.StatementRequest{
		Statement: statement,
		Format:    scopedb.ArrowJSONFormat,
	})
	require.NoError(t, err)
	snaps.MatchSnapshot(t, rs.Metadata)
	snaps.MatchSnapshot(t, fmt.Sprintf("%v", rs.Records))

	// 2. Merge data and verify the result
	mergeRecords := makeMergeRecords(schema)
	ingester, err = scopedb.NewIngester(ctx, conn)
	require.NoError(t, err)
	require.NoError(t, ingester.IngestData(ctx, mergeRecords))
	require.NoError(t, ingester.Commit(ctx, fmt.Sprintf(`
    MERGE INTO %s
    ON %s.a = $0
    WHEN MATCHED THEN UPDATE ALL
    `, tableName, tableName)))

	rs, err = conn.QueryAsArrowBatch(ctx, &scopedb.StatementRequest{
		Statement: statement,
		Format:    scopedb.ArrowJSONFormat,
	})
	require.NoError(t, err)
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
