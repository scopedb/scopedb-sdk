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
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/brianvoe/gofakeit/v7"
	scopedb "github.com/scopedb/scopedb-sdk/go"
	"github.com/stretchr/testify/require"

	testkit "github.com/scopedb/scopedb-sdk/go/integration_tests/internal"
)

const (
	BatchSize  = 1024
	BatchCount = 1
)

func BenchmarkLargeVariantSchema(b *testing.B) {
	tk := testkit.NewTestKit(b)
	if tk == nil {
		b.Skip("nil testkit")
	}
	defer tk.Close()

	ctx := context.Background()
	tk.NewTable(ctx, "bench_vars", "CREATE TABLE bench_vars (b int, n int, var variant)")

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "b", Type: arrow.PrimitiveTypes.Int64},
		{Name: "n", Type: arrow.PrimitiveTypes.Int64},
		{Name: "var", Type: arrow.BinaryTypes.String},
	}, nil)

	for i := 0; i < BatchCount; i++ {
		func() {
			builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
			defer builder.Release()

			for n := 0; n < BatchSize; n++ {
				value, err := gofakeit.JSON(&gofakeit.JSONOptions{
					Type:   "object",
					Indent: false,
					Fields: []gofakeit.Field{
						{Name: "first_name", Function: "firstname"},
						{Name: "last_name", Function: "lastname"},
						{Name: "address", Function: "address"},
						{Name: "password", Function: "password", Params: gofakeit.MapParams{"special": {"false"}}},
					},
				})
				require.NoError(b, err)

				builder.Field(0).(*array.Int64Builder).Append(int64(i))
				builder.Field(1).(*array.Int64Builder).Append(int64(n))
				builder.Field(2).(*array.StringBuilder).Append(string(value))
			}

			rec := builder.NewRecord()
			defer rec.Release()
			tk.IngestArrowBatch(ctx, []arrow.Record{rec}, "SELECT $0, $1, PARSE_JSON($2) INSERT INTO bench_vars")
		}()
	}

	tk.QueryAsArrowBatch(ctx, &scopedb.StatementRequest{
		Statement: "OPTIMIZE TABLE bench_vars",
		Format:    scopedb.ArrowJSONFormat,
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tk.QueryAsArrowBatch(ctx, &scopedb.StatementRequest{
			Statement: "FROM bench_vars AGGREGATE OBJECT_SCHEMA(var)",
			Format:    scopedb.ArrowJSONFormat,
		})
	}
	b.StopTimer()
}
