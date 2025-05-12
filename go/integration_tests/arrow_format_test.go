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
	scopedb "github.com/scopedb/scopedb-sdk/go"
	"github.com/stretchr/testify/require"
)

func TestResultFormatArrow(t *testing.T) {
	ctx := context.Background()
	client := scopedb.NewClient(&scopedb.Config{
		Endpoint: "http://localhost:6543",
	})
	defer client.Close()

	_, err := client.Statement("DROP TABLE IF EXISTS arrows").Execute(ctx)
	require.NoError(t, err)
	_, err = client.Statement("CREATE TABLE arrows (a INT, v VARIANT)").Execute(ctx)
	require.NoError(t, err)

	schema := makeArrowSchema()
	record := makeArrowRecord(schema)

	cable := client.ArrowBatchCable(schema, "INSERT INTO arrows")
	cable.BatchSize = 0 // immediately flush
	cable.Start(ctx)
	defer cable.Close()

	require.NoError(t, <-cable.Send(record))

	s := client.Statement("FROM arrows")
	s.ResultFormat = scopedb.ResultFormatArrow
	result, err := s.Execute(ctx)
	require.NoError(t, err)

	records, err := result.ToArrowBatch()
	require.NoError(t, err)

	fmt.Printf("%v\n", records)
}

func makeArrowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "v", Type: arrow.StructOf(arrow.Field{
			Name: "int", Type: arrow.PrimitiveTypes.Int64,
			Nullable: true,
		})},
	}, nil)
}

func makeArrowRecord(schema *arrow.Schema) arrow.Record {
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
	return b.NewRecord()
}
