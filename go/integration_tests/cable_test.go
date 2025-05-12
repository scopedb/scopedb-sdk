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

	scopedb "github.com/scopedb/scopedb-sdk/go"
	"github.com/stretchr/testify/require"
)

func TestVariantBatchCable(t *testing.T) {
	c := NewClient()
	if c == nil {
		t.Skip("nil test client")
	}
	defer c.Close()

	ctx := context.Background()
	tbl := c.Table(RandomName(t))
	_, err := c.Statement(fmt.Sprintf(`CREATE TABLE %s (ts TIMESTAMP, v VARIANT)`, tbl.Identifier())).Execute(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, tbl.Drop(ctx))
	}()

	cable := c.VariantBatchCable(fmt.Sprintf(`
		SELECT $0["ts"], $0["v"]
		INSERT INTO %s (ts, v)
	`, tbl.Identifier()))
	cable.BatchSize = 0 // immediately flush
	cable.Start(ctx)
	defer cable.Close()

	require.NoError(t, <-cable.Send(struct {
		TS int64 `json:"ts"`
		V  any   `json:"v"`
	}{
		TS: -1024,
		V:  "scopedb",
	}))

	require.NoError(t, <-cable.Send(struct {
		TS int64 `json:"ts"`
		V  any   `json:"v"`
	}{
		TS: 1024,
		V:  42.1,
	}))

	s := c.Statement(fmt.Sprintf(`FROM %s ORDER BY ts`, tbl.Identifier()))
	result, err := s.Execute(ctx)
	require.NoError(t, err)

	records, err := result.ToValues()
	require.NoError(t, err)

	snaps.MatchSnapshot(t, result.Schema)
	snaps.MatchSnapshot(t, records)
}

func TestArrowBatchCable(t *testing.T) {
	c := NewClient()
	if c == nil {
		t.Skip("nil test client")
	}
	defer c.Close()

	ctx := context.Background()

	tbl := c.Table(RandomName(t))
	_, err := c.Statement(fmt.Sprintf(`CREATE TABLE %s (a INT, v VARIANT)`, tbl.Identifier())).Execute(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, tbl.Drop(ctx))
	}()

	schema := makeArrowSchema()
	record := makeArrowRecord(schema)

	cable := c.ArrowBatchCable(schema, fmt.Sprintf(`INSERT INTO %s`, tbl.Identifier()))
	cable.BatchSize = 0 // immediately flush
	cable.Start(ctx)
	defer cable.Close()

	require.NoError(t, <-cable.Send(record))

	s := c.Statement(fmt.Sprintf(`FROM %s`, tbl.Identifier()))
	s.ResultFormat = scopedb.ResultFormatArrow
	result, err := s.Execute(ctx)
	require.NoError(t, err)

	records, err := result.ToArrowBatch()
	require.NoError(t, err)

	snaps.MatchSnapshot(t, result.Schema)
	snaps.MatchSnapshot(t, fmt.Sprintf("%v", records))
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
