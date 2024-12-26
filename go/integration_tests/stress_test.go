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
	"crypto/rand"
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	scopedb "github.com/scopedb/scopedb-sdk/go"
	testkit "github.com/scopedb/scopedb-sdk/go/integration_tests/internal"
	"github.com/stretchr/testify/require"
)

const (
	IngestDataBatch = 10000
	TaskParallelism = 8
	TaskInterval    = 50 * time.Millisecond
	TaskDuration    = 10 * time.Second
)

type stressSuite struct {
	t  testing.TB
	tk *testkit.TestKit

	idGen          *atomic.Int64
	tableName      string
	logTableName   string
	stageTableName string
}

func newStressSuite(t testing.TB, tk *testkit.TestKit) *stressSuite {
	tableName := tk.RandomName()

	return &stressSuite{
		t:              t,
		tk:             tk,
		idGen:          &atomic.Int64{},
		tableName:      tableName,
		logTableName:   fmt.Sprintf("%s_log", tableName),
		stageTableName: fmt.Sprintf("%s_stage", tableName),
	}
}

func (suite *stressSuite) init(ctx context.Context) {
	stmt := fmt.Sprintf(`CREATE TABLE %s (id INT, message STRING, var VARIANT)`, suite.logTableName)
	suite.tk.NewTable(ctx, suite.logTableName, stmt)

	stmt = fmt.Sprintf(`CREATE TABLE %s (id INT, message STRING, var VARIANT)`, suite.stageTableName)
	suite.tk.NewTable(ctx, suite.stageTableName, stmt)

	taskName := fmt.Sprintf(`%s_compact_log`, suite.tableName)
	stmt = fmt.Sprintf(`CREATE TASK %s
		SCHEDULE = '* * * * * Asia/Shanghai'
		NODEGROUP = 'default' AS
		OPTIMIZE TABLE scopedb.public.%s`, taskName, suite.logTableName)
	suite.tk.NewTask(ctx, taskName, stmt)

	taskName = fmt.Sprintf(`%s_compact_stage`, suite.tableName)
	stmt = fmt.Sprintf(`CREATE TASK %[1]v
		SCHEDULE = '* * * * * Asia/Shanghai'
		NODEGROUP = 'default' AS
		BEGIN
			FROM %[3]v MERGE INTO %[2]v
				ON %[3]v.id = %[2]v.id
				WHEN MATCHED THEN UPDATE ALL
				WHEN NOT MATCHED THEN INSERT ALL;
			DELETE FROM %[3]v;
		END`, taskName, suite.logTableName, suite.stageTableName)
	suite.tk.NewTask(ctx, taskName, stmt)
}

func (suite *stressSuite) queryColumns(ctx context.Context) {
	start := time.Now()
	_ = suite.tk.QueryAsArrowBatch(ctx, &scopedb.StatementRequest{
		Statement:   "FROM system.columns",
		WaitTimeout: "60s",
		Format:      scopedb.ArrowJSONFormat,
	})
	suite.t.Logf("Queried columns in %s", time.Since(start))
}

func (suite *stressSuite) queryTables(ctx context.Context) {
	start := time.Now()
	_ = suite.tk.QueryAsArrowBatch(ctx, &scopedb.StatementRequest{
		Statement:   "FROM system.tables",
		WaitTimeout: "60s",
		Format:      scopedb.ArrowJSONFormat,
	})
	suite.t.Logf("Queried tables in %s", time.Since(start))
}

func (suite *stressSuite) ingestLogs(ctx context.Context) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "message", Type: arrow.BinaryTypes.String},
		{Name: "var", Type: arrow.BinaryTypes.String},
	}, nil)

	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()

	msg := suite.tk.RandomString(1024)
	idStart := suite.idGen.Load()
	for i := 0; i < IngestDataBatch; i++ {
		b.Field(0).(*array.Int64Builder).Append(int64(i) + idStart)
		b.Field(1).(*array.StringBuilder).Append("[INFO] 2024/02/02 00:00:00 path/to/file.go:123 - " + msg)
		b.Field(2).(*array.StringBuilder).Append(fmt.Sprintf(`{"%d": 1, "k_%d": 1 , "v_%d": 1 }`, i, i, i))
	}

	rec := b.NewRecord()
	defer rec.Release()

	stmt := fmt.Sprintf(`SELECT $0, $1, PARSE_JSON($2) INSERT INTO scopedb.public.%s`, suite.logTableName)
	suite.tk.IngestArrowBatch(ctx, []arrow.Record{rec}, stmt)
	suite.idGen.Add(IngestDataBatch)
	suite.t.Logf("Ingested %d logs into %s", IngestDataBatch, suite.logTableName)
}

func (suite *stressSuite) ingestStageLog(ctx context.Context) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "message", Type: arrow.BinaryTypes.String},
		{Name: "var", Type: arrow.BinaryTypes.String},
	}, nil)

	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()

	msg := suite.tk.RandomString(1024)
	idStart := suite.idGen.Load()
	for i := 0; i < IngestDataBatch; i++ {
		b.Field(0).(*array.Int64Builder).Append(int64(i) + idStart)
		b.Field(1).(*array.StringBuilder).Append("[INFO] 2024/02/02 00:00:00 path/to/file.go:123 - " + msg)
		b.Field(2).(*array.StringBuilder).Append(fmt.Sprintf(`{"%d": 1, "k_%d": 1 , "v_%d": 1 }`, i, i, i))
	}

	rec := b.NewRecord()
	defer rec.Release()

	stmt := fmt.Sprintf(`SELECT $0, $1, PARSE_JSON($2) INSERT INTO scopedb.public.%s`, suite.stageTableName)
	suite.tk.IngestArrowBatch(ctx, []arrow.Record{rec}, stmt)
	suite.idGen.Add(IngestDataBatch)
	suite.t.Logf("Ingested %d logs into %s", IngestDataBatch, suite.stageTableName)
}

func BenchmarkStressHeavyReadWrite(b *testing.B) {
	tk := testkit.NewTestKit(b)
	if tk == nil {
		b.Skip("nil testkit")
	}
	defer tk.Close()

	ctx := context.Background()
	suite := newStressSuite(b, tk)
	suite.init(ctx)

	wg := sync.WaitGroup{}
	tasks := make(chan func(), 1024)
	for i := 0; i < TaskParallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range tasks {
				task()
			}
		}()
	}

	c := time.After(TaskDuration)

	for {
		select {
		case <-c:
			close(tasks)
			wg.Wait()
			fmt.Println("Ingested:", suite.idGen.Load())
			fmt.Println("Shutting down...")
			return
		default:
			tasks <- func() {
				n, err := rand.Int(rand.Reader, big.NewInt(4))
				require.NoError(b, err)
				switch n.Int64() {
				case 0:
					suite.ingestLogs(ctx)
				case 1:
					suite.queryTables(ctx)
				case 2:
					suite.ingestStageLog(ctx)
				case 3:
					suite.queryColumns(ctx)
				}
			}
			time.Sleep(TaskInterval)
		}
	}
}
