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
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCatalogListAndFetchResources(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Bearer catalog-key", r.Header.Get("Authorization"))
		switch r.URL.EscapedPath() {
		case "/base/v1/databases":
			writeJSONValue(t, w, CatalogPage[DatabaseResource]{
				Items: []DatabaseResource{{Name: "analytics"}},
			})
		case "/base/v1/databases/analytics":
			writeJSONValue(t, w, DatabaseResource{Name: "analytics"})
		case "/base/v1/databases/analytics/schemas":
			writeJSONValue(t, w, CatalogPage[SchemaResource]{
				Items: []SchemaResource{{Database: "analytics", Name: "public"}},
			})
		case "/base/v1/databases/analytics/schemas/public":
			writeJSONValue(t, w, SchemaResource{Database: "analytics", Name: "public"})
		case "/base/v1/databases/analytics/schemas/public/tables":
			writeJSONValue(t, w, CatalogPage[TableResourceSummary]{
				Items: []TableResourceSummary{{Database: "analytics", Schema: "public", Name: "events"}},
			})
		case "/base/v1/databases/analytics/schemas/public/tables/events":
			retention := int32(30)
			writeJSONValue(t, w, TableResource{
				Database: "analytics",
				Schema:   "public",
				Name:     "events",
				Columns: []TableColumnSpec{{
					Name:     "id",
					DataType: IntDataType,
				}},
				PartitionBy:       []string{},
				ClusterBy:         []string{},
				DistinctOn:        TableDistinctSpec{On: []string{"id"}, By: []string{}},
				DataRetentionDays: &retention,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := NewClient(Config{
		Endpoint: server.URL + "/base/",
		APIKey:   "catalog-key",
	})
	require.NoError(t, err)
	t.Cleanup(client.Close)
	ctx := context.Background()

	databases, err := client.ListDatabases(ctx, CatalogListOptions{})
	require.NoError(t, err)
	require.Equal(t, "analytics", databases.Items[0].Name)
	database, err := client.FetchDatabase(ctx, "analytics")
	require.NoError(t, err)
	require.Equal(t, "analytics", database.Name)

	schemas, err := client.ListSchemas(ctx, "analytics", CatalogListOptions{})
	require.NoError(t, err)
	require.Equal(t, "public", schemas.Items[0].Name)
	schema, err := client.FetchSchema(ctx, "analytics", "public")
	require.NoError(t, err)
	require.Equal(t, "public", schema.Name)

	tables, err := client.ListTables(ctx, "analytics", "public", CatalogListOptions{})
	require.NoError(t, err)
	require.Equal(t, "events", tables.Items[0].Name)
	table, err := client.FetchTable(ctx, "analytics", "public", "events")
	require.NoError(t, err)
	require.Equal(t, "events", table.Name)
	require.Equal(t, IntDataType, table.Columns[0].DataType)
	require.Equal(t, int32(30), *table.DataRetentionDays)
	require.Equal(t, table.Comment, table.Spec().Comment)
}

func TestCatalogRejectsInvalidSuccessShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		body    string
		call    func(*Client) error
		message string
		cause   string
	}{
		{
			name: "empty database resource",
			body: `{}`,
			call: func(client *Client) error {
				_, err := client.FetchDatabase(context.Background(), "analytics")
				return err
			},
			cause: "database resource is missing name",
		},
		{
			name: "null database resource",
			body: `null`,
			call: func(client *Client) error {
				_, err := client.FetchDatabase(context.Background(), "analytics")
				return err
			},
			cause: "database resource is missing name",
		},
		{
			name: "null page",
			body: `null`,
			call: func(client *Client) error {
				_, err := client.ListDatabases(context.Background(), CatalogListOptions{})
				return err
			},
			cause: "catalog page must be a JSON object",
		},
		{
			name: "page missing items",
			body: `{}`,
			call: func(client *Client) error {
				_, err := client.ListDatabases(context.Background(), CatalogListOptions{})
				return err
			},
			cause: "catalog page is missing items",
		},
		{
			name: "null items",
			body: `{"items":null}`,
			call: func(client *Client) error {
				_, err := client.ListDatabases(context.Background(), CatalogListOptions{})
				return err
			},
			cause: "catalog page items must be an array",
		},
		{
			name: "null item",
			body: `{"items":[null]}`,
			call: func(client *Client) error {
				_, err := client.ListDatabases(context.Background(), CatalogListOptions{})
				return err
			},
			cause: "catalog page item 0 must be an object",
		},
		{
			name: "invalid page token type",
			body: `{"items":[],"next_page_token":42}`,
			call: func(client *Client) error {
				_, err := client.ListDatabases(context.Background(), CatalogListOptions{})
				return err
			},
			message: "failed to decode catalog response",
			cause:   "cannot unmarshal number into Go struct field",
		},
		{
			name: "schema missing identity",
			body: `{"database":"analytics","comment":null}`,
			call: func(client *Client) error {
				_, err := client.FetchSchema(context.Background(), "analytics", "public")
				return err
			},
			cause: "schema resource is missing name",
		},
		{
			name: "table summary missing identity",
			body: `{"items":[{"database":"analytics","schema":"public","comment":null}]}`,
			call: func(client *Client) error {
				_, err := client.ListTables(
					context.Background(),
					"analytics",
					"public",
					CatalogListOptions{},
				)
				return err
			},
			cause: "table resource summary is missing name",
		},
		{
			name: "table missing identity",
			body: `{
				"schema":"public",
				"name":"events",
				"columns":[],
				"partition_by":[],
				"cluster_by":[],
				"distinct_on":{"on":[],"by":[]}
			}`,
			call: func(client *Client) error {
				_, err := client.FetchTable(context.Background(), "analytics", "public", "events")
				return err
			},
			cause: "table resource is missing database",
		},
		{
			name: "table missing spec",
			body: `{"database":"analytics","schema":"public","name":"events"}`,
			call: func(client *Client) error {
				_, err := client.FetchTable(context.Background(), "analytics", "public", "events")
				return err
			},
			cause: "table resource is missing columns",
		},
		{
			name: "table missing nested spec shape",
			body: `{
				"database":"analytics",
				"schema":"public",
				"name":"events",
				"columns":[],
				"partition_by":[],
				"cluster_by":[],
				"distinct_on":{"on":[]}
			}`,
			call: func(client *Client) error {
				_, err := client.FetchTable(context.Background(), "analytics", "public", "events")
				return err
			},
			cause: "table resource is missing distinct_on.by",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("X-Request-ID", "invalid-catalog-shape")
				_, _ = w.Write([]byte(test.body))
			}))
			defer server.Close()

			client, err := NewClient(Config{Endpoint: server.URL})
			require.NoError(t, err)
			defer client.Close()

			err = test.call(client)
			var scopeErr *Error
			require.ErrorAs(t, err, &scopeErr)
			require.Equal(t, ErrorKindUnexpected, scopeErr.Kind)
			expectedMessage := test.message
			if expectedMessage == "" {
				expectedMessage = "invalid catalog response"
			}
			require.Equal(t, expectedMessage, scopeErr.Error())
			require.Equal(t, http.StatusOK, scopeErr.HTTPStatus)
			require.Equal(t, "invalid-catalog-shape", scopeErr.RequestID)
			require.ErrorContains(t, scopeErr.Unwrap(), test.cause)
		})
	}
}

func TestCatalogAcceptsEmptyTableSpecSlices(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, err := w.Write([]byte(`{
			"database":"analytics",
			"schema":"public",
			"name":"empty_table",
			"columns":[],
			"partition_by":[],
			"cluster_by":[],
			"distinct_on":{"on":[],"by":[]}
		}`))
		require.NoError(t, err)
	}))
	defer server.Close()

	client, err := NewClient(Config{Endpoint: server.URL})
	require.NoError(t, err)
	t.Cleanup(client.Close)

	table, err := client.FetchTable(context.Background(), "analytics", "public", "empty_table")
	require.NoError(t, err)
	require.NotNil(t, table.Columns)
	require.NotNil(t, table.PartitionBy)
	require.NotNil(t, table.ClusterBy)
	require.NotNil(t, table.DistinctOn.On)
	require.NotNil(t, table.DistinctOn.By)
	require.Nil(t, table.DataRetentionDays)
	require.Nil(t, table.Comment)
}

func TestCatalogAcceptsNullNextPageToken(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, err := w.Write([]byte(`{"items":[],"next_page_token":null}`))
		require.NoError(t, err)
	}))
	defer server.Close()

	client, err := NewClient(Config{Endpoint: server.URL})
	require.NoError(t, err)
	t.Cleanup(client.Close)

	page, err := client.ListDatabases(context.Background(), CatalogListOptions{})
	require.NoError(t, err)
	require.NotNil(t, page.Items)
	require.Empty(t, page.NextPageToken)
}

func TestTableResourceSpecClonesSlices(t *testing.T) {
	t.Parallel()

	columnComment := "identifier"
	retention := int32(30)
	tableComment := "events"
	resource := TableResource{
		Columns: []TableColumnSpec{{
			Name:     "id",
			DataType: IntDataType,
			Comment:  &columnComment,
		}},
		PartitionBy: []string{"day"},
		ClusterBy:   []string{"service"},
		DistinctOn: TableDistinctSpec{
			On: []string{"trace_id"},
			By: []string{"timestamp"},
		},
		DataRetentionDays: &retention,
		Comment:           &tableComment,
	}
	spec := resource.Spec()
	spec.Columns[0].Name = "changed"
	spec.PartitionBy[0] = "changed"
	spec.ClusterBy[0] = "changed"
	spec.DistinctOn.On[0] = "changed"
	spec.DistinctOn.By[0] = "changed"
	*spec.Columns[0].Comment = "changed"
	*spec.DataRetentionDays = 1
	*spec.Comment = "changed"

	require.Equal(t, "id", resource.Columns[0].Name)
	require.Equal(t, "day", resource.PartitionBy[0])
	require.Equal(t, "service", resource.ClusterBy[0])
	require.Equal(t, "trace_id", resource.DistinctOn.On[0])
	require.Equal(t, "timestamp", resource.DistinctOn.By[0])
	require.Equal(t, "identifier", *resource.Columns[0].Comment)
	require.Equal(t, int32(30), *resource.DataRetentionDays)
	require.Equal(t, "events", *resource.Comment)
}

func TestCatalogPreservesBasePathAndEscapesEachSegment(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.EscapedPath() {
		case "/proxy%2Froot/v1/databases/analytics%2F2026/schemas/events%20archive/tables":
			require.Equal(t, "7", r.URL.Query().Get("page_size"))
			require.Equal(t, "token +/?", r.URL.Query().Get("page_token"))
			writeJSONValue(t, w, CatalogPage[TableResourceSummary]{Items: []TableResourceSummary{}})
		case "/proxy%2Froot/v1/databases/analytics%2F2026/schemas/events%20archive/tables/events%3F%23%2Fraw":
			writeJSONValue(t, w, TableResource{
				Database:    "analytics/2026",
				Schema:      "events archive",
				Name:        "events?#/raw",
				Columns:     []TableColumnSpec{},
				PartitionBy: []string{},
				ClusterBy:   []string{},
				DistinctOn: TableDistinctSpec{
					On: []string{},
					By: []string{},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := NewClient(Config{Endpoint: server.URL + "/proxy%2Froot"})
	require.NoError(t, err)
	t.Cleanup(client.Close)

	_, err = client.ListTables(
		context.Background(),
		"analytics/2026",
		"events archive",
		CatalogListOptions{PageSize: 7, PageToken: "token +/?"},
	)
	require.NoError(t, err)
	_, err = client.FetchTable(
		context.Background(),
		"analytics/2026",
		"events archive",
		"events?#/raw",
	)
	require.NoError(t, err)
}

func TestCatalogRejectsInvalidPageSizeBeforeRequest(t *testing.T) {
	t.Parallel()

	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client, err := NewClient(Config{Endpoint: server.URL})
	require.NoError(t, err)
	t.Cleanup(client.Close)

	for _, pageSize := range []int{-1, 1001} {
		_, err := client.ListDatabases(context.Background(), CatalogListOptions{PageSize: pageSize})
		var scopeErr *Error
		require.ErrorAs(t, err, &scopeErr)
		require.Equal(t, ErrorKindConfigInvalid, scopeErr.Kind)
	}
	require.Zero(t, requests)
}

func TestCatalogIteratorFetchesPagesLazily(t *testing.T) {
	t.Parallel()

	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		switch r.URL.Query().Get("page_token") {
		case "":
			writeJSONValue(t, w, CatalogPage[DatabaseResource]{
				Items:         []DatabaseResource{{Name: "first"}},
				NextPageToken: "second-page",
			})
		case "second-page":
			writeJSONValue(t, w, CatalogPage[DatabaseResource]{
				Items: []DatabaseResource{{Name: "second"}},
			})
		default:
			http.Error(w, "unexpected token", http.StatusBadRequest)
		}
	}))
	defer server.Close()

	client, err := NewClient(Config{Endpoint: server.URL})
	require.NoError(t, err)
	t.Cleanup(client.Close)

	sequence := client.IterateDatabases(context.Background(), CatalogListOptions{PageSize: 1})
	require.Zero(t, requests)

	var names []string
	for database, err := range sequence {
		require.NoError(t, err)
		names = append(names, database.Name)
	}
	require.Equal(t, []string{"first", "second"}, names)
	require.Equal(t, 2, requests)
}

func TestCatalogIteratorRejectsRepeatedPageToken(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSONValue(t, w, CatalogPage[DatabaseResource]{
			Items:         []DatabaseResource{{Name: "only"}},
			NextPageToken: "same-token",
		})
	}))
	defer server.Close()

	client, err := NewClient(Config{Endpoint: server.URL})
	require.NoError(t, err)
	t.Cleanup(client.Close)

	var iterationErr error
	for _, err := range client.IterateDatabases(
		context.Background(),
		CatalogListOptions{PageToken: "same-token"},
	) {
		if err != nil {
			iterationErr = err
		}
	}
	var scopeErr *Error
	require.ErrorAs(t, iterationErr, &scopeErr)
	require.Equal(t, ErrorKindUnexpected, scopeErr.Kind)
	require.Equal(t, "catalog returned a repeated page token", scopeErr.Error())
}

func TestCatalogAPIErrorPreservesServerMetadata(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-Request-ID", "header-request")
		w.Header().Set("Retry-After", "2")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, err := w.Write([]byte(`{
			"message":"catalog temporarily unavailable",
			"request_id":"body-request",
			"retryable":false
		}`))
		require.NoError(t, err)
	}))
	defer server.Close()

	client, err := NewClient(Config{Endpoint: server.URL})
	require.NoError(t, err)
	t.Cleanup(client.Close)

	_, err = client.ListDatabases(context.Background(), CatalogListOptions{})
	var scopeErr *Error
	require.ErrorAs(t, err, &scopeErr)
	require.Equal(t, "catalog temporarily unavailable", scopeErr.Error())
	require.Equal(t, ErrorKindUnexpected, scopeErr.Kind)
	require.Equal(t, http.StatusServiceUnavailable, scopeErr.HTTPStatus)
	require.Equal(t, "body-request", scopeErr.RequestID)
	require.Equal(t, 2*time.Second, scopeErr.RetryAfter)
	require.False(t, scopeErr.Retryable)
}

func TestCatalogAPIErrorParsesNestedEnvelope(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, err := w.Write([]byte(`{
			"request_id":"outer-request",
			"error":{
				"message":"nested server message",
				"request_id":"nested-request",
				"retryable":true
			}
		}`))
		require.NoError(t, err)
	}))
	defer server.Close()

	client, err := NewClient(Config{Endpoint: server.URL})
	require.NoError(t, err)
	t.Cleanup(client.Close)

	_, err = client.FetchDatabase(context.Background(), "missing")
	var scopeErr *Error
	require.True(t, errors.As(err, &scopeErr))
	require.Equal(t, "nested server message", scopeErr.Error())
	require.Equal(t, "nested-request", scopeErr.RequestID)
	require.True(t, scopeErr.Retryable)
}

func TestParseRetryAfterHTTPDate(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 8, 12, 0, 0, 0, time.UTC)
	retryAt := now.Add(7 * time.Second).Format(http.TimeFormat)
	require.Equal(t, 7*time.Second, parseRetryAfter(retryAt, now))
}

func writeJSONValue(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	require.NoError(t, json.NewEncoder(w).Encode(value))
}
