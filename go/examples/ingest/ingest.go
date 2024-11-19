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

package main

import (
	"context"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	scopedb "github.com/scopedb/scopedb-sdk/go"
)

// prepareData creates a slice of arrow.Record
//
// a:int64 | s:struct<int:int64>
// 1       | {1}
func prepareData() []arrow.Record {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "s", Type: arrow.StructOf(arrow.Field{
			Name: "int", Type: arrow.PrimitiveTypes.Int64,
			Nullable: true,
		})},
	}, nil)
	pool := memory.NewGoAllocator()
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).Append(1)
	b.Field(1).(*array.StructBuilder).FieldBuilder(0).(*array.Int64Builder).Append(1)
	b.Field(1).(*array.StructBuilder).Append(true)
	rec := b.NewRecord()
	return []arrow.Record{rec}
}

func ingest(data []arrow.Record) error {
	conn := scopedb.Open(&scopedb.Config{
		Endpoint: "http://localhost:6543",
	})

	ctx := context.Background()

	// Create an ingest channel
	ingester, err := scopedb.NewIngester(ctx, conn)
	if err != nil {
		return err
	}

	// Ingest data
	if err := ingester.IngestData(ctx, data); err != nil {
		return err
	}

	// Commit the ingest channel
	if err := ingester.CommitIngest(ctx, "INSERT INTO database.schema.table"); err != nil {
		return err
	}

	return nil
}

func ingestWithMerge(data []arrow.Record) error {
	conn := scopedb.Open(&scopedb.Config{
		Endpoint: "http://localhost:6543",
	})

	ctx := context.Background()

	// Create an ingest channel
	ingester, err := scopedb.NewIngester(ctx, conn)
	if err != nil {
		return err
	}

	// Ingest data
	if err := ingester.IngestData(ctx, data); err != nil {
		return err
	}

	// Commit the ingest channel
	if err := ingester.CommitIngest(ctx, `
    MERGE INTO table
    ON table.a = $0
    WHEN MATCHED AND table.a < $0 THEN UPDATE ALL
    WHEN NOT MATCHED THEN INSERT ALL
    `); err != nil {
		return err
	}

	return nil
}

func main() {
	data := prepareData()
	if err := ingest(data); err != nil {
		panic(err)
	}
	if err := ingestWithMerge(data); err != nil {
		panic(err)
	}
}
