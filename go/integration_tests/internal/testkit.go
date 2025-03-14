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

package testkit

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/lucasepe/codename"
	scopedb "github.com/scopedb/scopedb-sdk/go"
	"github.com/stretchr/testify/require"
)

type TestKit struct {
	t testing.TB

	conn *scopedb.Connection

	tables []string
	tasks  []string
}

func NewTestKit(t testing.TB) *TestKit {
	endpoint := os.Getenv("SCOPEDB_ENDPOINT")
	if endpoint == "" {
		return nil
	}

	return &TestKit{
		t: t,
		conn: scopedb.Open(&scopedb.Config{
			Endpoint: endpoint,
		}),
	}
}

func (tk *TestKit) Close() {
	ctx := context.Background()

	for _, table := range tk.tables {
		err := tk.conn.Execute(ctx, &scopedb.StatementRequest{
			Statement: fmt.Sprintf(`DROP TABLE %s`, table),
			Format:    scopedb.ArrowJSONFormat,
		})
		require.NoError(tk.t, err)
	}

	for _, tasks := range tk.tasks {
		err := tk.conn.Execute(ctx, &scopedb.StatementRequest{
			Statement: fmt.Sprintf(`DROP TASK %s`, tasks),
			Format:    scopedb.ArrowJSONFormat,
		})
		require.NoError(tk.t, err)
	}

	tk.conn.Close()
}

// RandomName generates a random name.
func (tk *TestKit) RandomName() string {
	rng, err := codename.DefaultRNG()
	require.NoError(tk.t, err)
	return strings.ReplaceAll(codename.Generate(rng, 10), "-", "_")
}

// RandomString generates a random string of n bytes.
func (tk *TestKit) RandomString(n int) string {
	require.Greater(tk.t, n, 0)

	bytes := make([]byte, n)
	_, err := rand.Read(bytes)
	require.NoError(tk.t, err)

	return hex.EncodeToString(bytes)[:n]
}

// NewTable creates a new table and track it for close.
func (tk *TestKit) NewTable(ctx context.Context, tableName string, createTableStatement string) {
	err := tk.conn.Execute(ctx, &scopedb.StatementRequest{
		Statement: createTableStatement,
		Format:    scopedb.ArrowJSONFormat,
	})
	require.NoError(tk.t, err)
	tk.tables = append(tk.tables, tableName)
}

// NewTask creates a new task and track it for close.
func (tk *TestKit) NewTask(ctx context.Context, taskName string, createTaskStatement string) {
	err := tk.conn.Execute(ctx, &scopedb.StatementRequest{
		Statement: createTaskStatement,
		Format:    scopedb.ArrowJSONFormat,
	})
	require.NoError(tk.t, err)
	tk.tasks = append(tk.tasks, taskName)
}

func (tk *TestKit) IngestArrowBatch(ctx context.Context, batches []arrow.Record, statement string) *scopedb.IngestResponse {
	resp, err := tk.conn.IngestArrowBatch(ctx, batches, statement)
	require.NoError(tk.t, err)
	return resp
}

func (tk *TestKit) QueryAsArrowBatch(ctx context.Context, req *scopedb.StatementRequest) *scopedb.ArrowResultSet {
	rs, err := tk.conn.QueryAsArrowBatch(ctx, req)
	require.NoError(tk.t, err)
	return rs
}
