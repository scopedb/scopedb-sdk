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

package scopedb_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"math/big"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	scopedb "github.com/scopedb/scopedb-sdk/go"
	"github.com/stretchr/testify/require"
)

func initDatabase(t *testing.T, ctx context.Context, conn *scopedb.Connection, tableName string) {
	logTableName := fmt.Sprintf("%s_log", tableName)
	stageTableName := fmt.Sprintf("%s_stage", tableName)

	stmt := fmt.Sprintf(`CREATE TABLE %s (id INT, message STRING, var VARIANT)`, logTableName)
	err := conn.Execute(ctx, &scopedb.StatementRequest{
		Statement:   stmt,
		Format:      scopedb.ArrowJSONFormat,
		WaitTimeout: "60s",
	})
	require.NoError(t, err)

	stmt = fmt.Sprintf(`CREATE TABLE %s (id INT, message STRING, var VARIANT)`, stageTableName)
	err = conn.Execute(ctx, &scopedb.StatementRequest{
		Statement:   stmt,
		Format:      scopedb.ArrowJSONFormat,
		WaitTimeout: "30s",
	})
	require.NoError(t, err)

	stmt = fmt.Sprintf(`CREATE TASK %s_compact_log
		SCHEDULE = '* * * * * Asia/Shanghai'
		NODEGROUP = 'default' AS
		OPTIMIZE TABLE scopedb.public.%s`, tableName, logTableName)
	err = conn.Execute(ctx, &scopedb.StatementRequest{
		Statement:   stmt,
		Format:      scopedb.ArrowJSONFormat,
		WaitTimeout: "30s",
	})
	require.NoError(t, err)

	stmt = fmt.Sprintf(`CREATE TASK %[1]v_merge_stage
		SCHEDULE = '* * * * * Asia/Shanghai'
		NODEGROUP = 'default' AS
		BEGIN
			FROM %[3]v MERGE INTO %[2]v
				ON %[3]v.id = %[2]v.id
				WHEN MATCHED THEN UPDATE ALL
				WHEN NOT MATCHED THEN INSERT ALL;
			DELETE FROM %[3]v;
		END`, tableName, logTableName, stageTableName)
	err = conn.Execute(ctx, &scopedb.StatementRequest{
		Statement:   stmt,
		Format:      scopedb.ArrowJSONFormat,
		WaitTimeout: "30s",
	})
	require.NoError(t, err)
}

func generateRandomString(length int) (string, error) {
	if length <= 0 {
		return "", fmt.Errorf("length must be greater than 0")
	}

	bytes := make([]byte, length)
	_, err := rand.Read(bytes)
	if err != nil {
		return "", err
	}

	return hex.EncodeToString(bytes)[:length], nil
}

func ingestLogs(t *testing.T, conn *scopedb.Connection, tableName string, batchSize int, idStart int64) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "message", Type: arrow.BinaryTypes.String},
		{Name: "var", Type: arrow.BinaryTypes.String},
	}, nil)

	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()

	msg, err := generateRandomString(1024)
	require.NoError(t, err)

	for i := 0; i < batchSize; i++ {
		b.Field(0).(*array.Int64Builder).Append(int64(i) + idStart)
		b.Field(1).(*array.StringBuilder).Append("[INFO] 2024/02/02 00:00:00 path/to/file.go:123 - " + msg)
		b.Field(2).(*array.StringBuilder).Append(fmt.Sprintf(`{"%d": 1, "k_%d": 1 , "v_%d": 1 }`, i, i, i))
	}

	rec := b.NewRecord()
	defer rec.Release()

	_, err = conn.IngestArrowBatch(context.Background(), []arrow.Record{
		rec,
	}, fmt.Sprintf(`SELECT $0, $1, PARSE_JSON($2) INSERT INTO scopedb.public.%s`, tableName))
	require.NoError(t, err)

	log.Printf("Ingested %d logs into %s", batchSize, tableName)
}

func ingestStageLog(t *testing.T, conn *scopedb.Connection, tableName string, batchSize int, idStart int64) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "message", Type: arrow.BinaryTypes.String},
		{Name: "var", Type: arrow.BinaryTypes.String},
	}, nil)

	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()

	msg, err := generateRandomString(1024)
	require.NoError(t, err)

	for i := 0; i < batchSize; i++ {
		b.Field(0).(*array.Int64Builder).Append(int64(i) + idStart)
		b.Field(1).(*array.StringBuilder).Append("[INFO] 2024/02/02 00:00:00 path/to/file.go:123 - " + msg)
		b.Field(2).(*array.StringBuilder).Append(fmt.Sprintf(`{"%d": 1, "k_%d": 1 , "v_%d": 1 }`, i, i, i))
	}

	rec := b.NewRecord()
	defer rec.Release()

	_, err = conn.IngestArrowBatch(context.Background(), []arrow.Record{
		rec,
	}, fmt.Sprintf(`SELECT $0, $1, PARSE_JSON($2) INSERT INTO scopedb.public.%s`, tableName))
	require.NoError(t, err)

	log.Printf("Ingested %d logs into %s", batchSize, tableName)
}

func queryTables(t *testing.T, conn *scopedb.Connection) {
	stmt := "FROM system.tables"

	start := time.Now()
	_, err := conn.QueryAsArrowBatch(context.Background(), &scopedb.StatementRequest{
		Statement:   stmt,
		Format:      scopedb.ArrowJSONFormat,
		WaitTimeout: "30s",
	})
	require.NoError(t, err)

	log.Printf("Queried tables in %s", time.Since(start))
}

func queryColumns(t *testing.T, conn *scopedb.Connection) {
	stmt := "FROM system.columns"

	start := time.Now()
	_, err := conn.QueryAsArrowBatch(context.Background(), &scopedb.StatementRequest{
		Statement:   stmt,
		Format:      scopedb.ArrowJSONFormat,
		WaitTimeout: "30s",
	})
	require.NoError(t, err)

	log.Printf("Queried columns in %s", time.Since(start))
}

const (
	IngestDataBatch = 10000
	IngestDataMax   = 1000000
)

func TestStressHeavyReadWrite(t *testing.T) {
	ctx := context.Background()
	idGen := &atomic.Int64{}

	config := LoadConfig()
	if config == nil {
		t.Skip("Connection config is not set")
	}

	tableName, err := GenerateTableName()
	logTableName := fmt.Sprintf("%s_log", tableName)
	stageTableName := fmt.Sprintf("%s_stage", tableName)
	require.NoError(t, err)
	t.Logf("With tableName: %s", tableName)

	conn := scopedb.Open(config)
	initDatabase(t, ctx, conn, tableName)

	ingestLogs(t, conn, logTableName, IngestDataBatch, idGen.Load())
	idGen.Add(IngestDataBatch)

	ingestStageLog(t, conn, stageTableName, IngestDataBatch, idGen.Load())
	idGen.Add(IngestDataBatch)

	wg := sync.WaitGroup{}
	jobs := make(chan func(), 100)
	for i := 0; i < 8; i++ {
		go func() {
			for job := range jobs {
				wg.Add(1)
				job()
				wg.Done()
			}
		}()
	}

	c := time.After(1 * time.Minute)

	for {
		select {
		case <-c:
			wg.Wait()
			fmt.Println("Ingested:", idGen.Load())
			fmt.Println("Shutting down...")
			return
		default:
			jobs <- func() {
				n, err := rand.Int(rand.Reader, big.NewInt(4))
				require.NoError(t, err)
				switch n.Int64() {
				case 0:
					if idGen.Load() < IngestDataMax {
						ingestLogs(t, conn, logTableName, IngestDataBatch, idGen.Load())
						idGen.Add(IngestDataBatch)
						break
					}
					fallthrough
				case 1:
					if idGen.Load() < IngestDataMax {
						ingestStageLog(t, conn, stageTableName, IngestDataBatch, idGen.Load())
						idGen.Add(IngestDataBatch)
						break
					}
					fallthrough
				case 2:
					queryTables(t, conn)
				case 3:
					queryColumns(t, conn)
				}
			}
			time.Sleep(1 * time.Second)
		}
	}
}
