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

package scopedb

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTableDescribeUsesDefaultLocation(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodGet, r.Method)
		require.Equal(t, "/v1/databases/scopedb/schemas/public/tables/events", r.URL.Path)
		writeTestJSON(t, w, `{
			"database":"scopedb",
			"schema":"public",
			"name":"events",
			"columns":[{"name":"message","data_type":"string","comment":null}],
			"partition_by":[],
			"cluster_by":[],
			"distinct_on":{"on":[],"by":[]},
			"data_retention_days":null,
			"comment":"application events"
		}`)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	description, err := client.Table("events").Describe(context.Background())
	require.NoError(t, err)
	require.Equal(t, "scopedb", description.Database)
	require.Equal(t, "public", description.Schema)
	require.Equal(t, "events", description.Name)
	require.Equal(t, []TableColumnSpec{{
		Name:     "message",
		DataType: StringDataType,
	}}, description.Columns)
	require.NotNil(t, description.Comment)
	require.Equal(t, "application events", *description.Comment)
}

func TestTableAppendNDJSONDelegatesRequest(t *testing.T) {
	t.Parallel()

	ndjson := []byte("{\"id\":1}\n{\"id\":2}")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "/v1/databases/analytics/schemas/events/tables/logs/rows", r.URL.Path)
		require.Equal(t, "application/x-ndjson", r.Header.Get("Content-Type"))
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, ndjson, body)
		writeTestJSON(t, w, `{"append_state":"committed","num_rows_inserted":2}`)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	table := client.Table("logs")
	table.Database = "analytics"
	table.Schema = "events"
	result, err := table.AppendNDJSON(context.Background(), ndjson)
	require.NoError(t, err)
	require.Equal(t, AppendStateCommitted, result.AppendState)
	require.Equal(t, int64(2), result.NumRowsInserted)
}

func TestTableIdentifierQuotesEveryConfiguredPart(t *testing.T) {
	t.Parallel()

	table := &Table{
		Database: "analytics`archive",
		Schema:   "event\nlogs",
		Name:     "request\\completed",
	}

	require.Equal(
		t,
		"`analytics\\`archive`.`event\\nlogs`.`request\\\\completed`",
		table.Identifier(),
	)
}

func TestTableIdentifierUsesDefaultSchemaWithDatabase(t *testing.T) {
	t.Parallel()

	table := &Table{Database: "analytics", Name: "events"}
	require.Equal(t, "`analytics`.`public`.`events`", table.Identifier())
}
