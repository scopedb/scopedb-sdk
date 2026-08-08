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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAppendNDJSONSendsRawPayload(t *testing.T) {
	t.Parallel()

	ndjson := []byte("{\"id\":1}\n\n  \n{\"id\":2}\n")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(
			t,
			"/proxy/v1/databases/analytics%2F2026/schemas/events%20archive/tables/events%3F%23%2Fraw/rows",
			r.URL.EscapedPath(),
		)
		require.Equal(t, "Bearer append-key", r.Header.Get("Authorization"))
		require.Equal(t, "application/x-ndjson", r.Header.Get("Content-Type"))
		require.Empty(t, r.Header.Get("Content-Encoding"))
		require.Empty(t, r.Header.Get("X-ScopeDB-Uncompressed-Content-Length"))
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, ndjson, body)
		writeJSONValue(t, w, AppendRowsResult{
			AppendState:     AppendStateCommitted,
			NumRowsInserted: 2,
		})
	}))
	defer server.Close()

	client, err := NewClient(Config{
		Endpoint: server.URL + "/proxy",
		APIKey:   "append-key",
	})
	require.NoError(t, err)
	t.Cleanup(client.Close)

	result, err := client.appendNDJSON(
		context.Background(),
		"analytics/2026",
		"events archive",
		"events?#/raw",
		ndjson,
	)
	require.NoError(t, err)
	require.Equal(t, AppendStateCommitted, result.AppendState)
	require.Equal(t, int64(2), result.NumRowsInserted)
}

func TestAppendNDJSONRejectsLocalLimitsBeforeRequest(t *testing.T) {
	t.Parallel()

	requests := 0
	client, err := NewClient(Config{
		Endpoint: "https://example.com",
		HTTPClient: &http.Client{Transport: appendRoundTripFunc(func(*http.Request) (*http.Response, error) {
			requests++
			return nil, errors.New("unexpected request")
		})},
	})
	require.NoError(t, err)

	for _, ndjson := range [][]byte{
		bytes.Repeat([]byte{'x'}, maxAppendBodyBytes+1),
		bytes.Repeat([]byte("{}\n"), maxAppendRows+1),
	} {
		_, err := client.appendNDJSON(context.Background(), "db", "schema", "table", ndjson)
		var scopeErr *Error
		require.ErrorAs(t, err, &scopeErr)
		require.Equal(t, ErrorKindAppendRowsFailed, scopeErr.Kind)
		require.Equal(t, AppendStateRejected, scopeErr.AppendDetails.AppendState)
		require.False(t, scopeErr.Retryable)
	}
	require.Zero(t, requests)
}

func TestAppendNDJSONPreservesRejectedDetails(t *testing.T) {
	t.Parallel()

	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		w.Header().Set("X-Request-ID", "header-request")
		w.Header().Set("Retry-After", "3")
		w.WriteHeader(http.StatusBadRequest)
		_, err := w.Write([]byte(`{
			"message":"column id is invalid",
			"request_id":"append-request",
			"retryable":true,
			"append_state":"rejected",
			"row_errors":[{"row_index":1,"column":"id","message":"expected int"}],
			"row_errors_truncated":false
		}`))
		require.NoError(t, err)
	}))
	defer server.Close()

	client, err := NewClient(Config{Endpoint: server.URL})
	require.NoError(t, err)
	t.Cleanup(client.Close)

	_, err = client.appendNDJSON(context.Background(), "db", "schema", "table", []byte("{\"id\":\"x\"}"))
	var scopeErr *Error
	require.ErrorAs(t, err, &scopeErr)
	require.Equal(t, "column id is invalid", scopeErr.Error())
	require.Equal(t, ErrorKindAppendRowsFailed, scopeErr.Kind)
	require.Equal(t, http.StatusBadRequest, scopeErr.HTTPStatus)
	require.Equal(t, "append-request", scopeErr.RequestID)
	require.Equal(t, AppendStateRejected, scopeErr.AppendDetails.AppendState)
	require.Equal(t, []AppendRowError{{
		RowIndex: 1,
		Column:   "id",
		Message:  "expected int",
	}}, scopeErr.AppendDetails.RowErrors)
	require.True(t, scopeErr.Retryable)
	require.Equal(t, 1, requests)
}

func TestAppendNDJSONNeverRetriesUnknownOutcome(t *testing.T) {
	t.Parallel()

	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		w.WriteHeader(http.StatusServiceUnavailable)
		_, err := w.Write([]byte(`{
			"message":"commit outcome is unavailable",
			"retryable":true,
			"append_state":"unknown",
			"row_errors":[],
			"row_errors_truncated":false
		}`))
		require.NoError(t, err)
	}))
	defer server.Close()

	client, err := NewClient(Config{Endpoint: server.URL})
	require.NoError(t, err)
	t.Cleanup(client.Close)

	_, err = client.appendNDJSON(context.Background(), "db", "schema", "table", []byte("{}"))
	var scopeErr *Error
	require.ErrorAs(t, err, &scopeErr)
	require.Equal(t, "commit outcome is unavailable", scopeErr.Error())
	require.Equal(t, AppendStateUnknown, scopeErr.AppendDetails.AppendState)
	require.False(t, scopeErr.Retryable)
	require.Equal(t, 1, requests)
}

func TestAppendNDJSONTreatsInvalidSuccessAsUnknown(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
	}{
		{name: "malformed", body: "not-json"},
		{name: "contradictory state", body: `{"append_state":"rejected","num_rows_inserted":0}`},
		{name: "missing row count", body: `{"append_state":"committed"}`},
		{name: "null row count", body: `{"append_state":"committed","num_rows_inserted":null}`},
		{name: "negative row count", body: `{"append_state":"committed","num_rows_inserted":-1}`},
		{name: "row count mismatch", body: `{"append_state":"committed","num_rows_inserted":2}`},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("X-Request-ID", "success-request")
				_, err := io.WriteString(w, test.body)
				require.NoError(t, err)
			}))
			defer server.Close()

			client, err := NewClient(Config{Endpoint: server.URL})
			require.NoError(t, err)
			t.Cleanup(client.Close)
			_, err = client.appendNDJSON(context.Background(), "db", "schema", "table", []byte("{}"))

			var scopeErr *Error
			require.ErrorAs(t, err, &scopeErr)
			require.Equal(t, ErrorKindAppendRowsFailed, scopeErr.Kind)
			require.Equal(t, AppendStateUnknown, scopeErr.AppendDetails.AppendState)
			require.Equal(t, http.StatusOK, scopeErr.HTTPStatus)
			require.Equal(t, "success-request", scopeErr.RequestID)
			require.False(t, scopeErr.Retryable)
			if test.name == "malformed" {
				var syntaxErr *json.SyntaxError
				require.ErrorAs(t, scopeErr, &syntaxErr)
			}
		})
	}
}

func TestAppendNDJSONValidatesZeroRowSuccess(t *testing.T) {
	t.Parallel()

	t.Run("explicit zero", func(t *testing.T) {
		t.Parallel()

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, err := io.WriteString(w, `{"append_state":"committed","num_rows_inserted":0}`)
			require.NoError(t, err)
		}))
		defer server.Close()

		client, err := NewClient(Config{Endpoint: server.URL})
		require.NoError(t, err)
		t.Cleanup(client.Close)

		result, err := client.appendNDJSON(context.Background(), "db", "schema", "table", []byte("\n  \n"))
		require.NoError(t, err)
		require.Equal(t, AppendStateCommitted, result.AppendState)
		require.Zero(t, result.NumRowsInserted)
	})

	t.Run("missing row count", func(t *testing.T) {
		t.Parallel()

		requests := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			requests++
			w.Header().Set("X-Request-ID", "missing-count-request")
			_, err := io.WriteString(w, `{"append_state":"committed"}`)
			require.NoError(t, err)
		}))
		defer server.Close()

		client, err := NewClient(Config{Endpoint: server.URL})
		require.NoError(t, err)
		t.Cleanup(client.Close)

		_, err = client.appendNDJSON(context.Background(), "db", "schema", "table", []byte("\n  \n"))
		var scopeErr *Error
		require.ErrorAs(t, err, &scopeErr)
		require.Equal(t, ErrorKindAppendRowsFailed, scopeErr.Kind)
		require.Equal(t, AppendStateUnknown, scopeErr.AppendDetails.AppendState)
		require.Equal(t, "missing-count-request", scopeErr.RequestID)
		require.False(t, scopeErr.Retryable)
		require.Equal(t, 1, requests)
	})
}

func TestAppendNDJSONUnstructuredHTTPErrorIsUnknown(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, err := io.WriteString(w, "upstream disconnected")
		require.NoError(t, err)
	}))
	defer server.Close()

	client, err := NewClient(Config{Endpoint: server.URL})
	require.NoError(t, err)
	t.Cleanup(client.Close)

	_, err = client.appendNDJSON(context.Background(), "db", "schema", "table", []byte("{}"))
	var scopeErr *Error
	require.ErrorAs(t, err, &scopeErr)
	require.Equal(t, "upstream disconnected", scopeErr.Error())
	require.Equal(t, AppendStateUnknown, scopeErr.AppendDetails.AppendState)
	require.False(t, scopeErr.Retryable)
}

func TestAppendNDJSONTransportAndReadErrorsPreserveCause(t *testing.T) {
	t.Parallel()

	t.Run("transport", func(t *testing.T) {
		t.Parallel()

		transportErr := errors.New("network unavailable")
		client, err := NewClient(Config{
			Endpoint: "https://example.com",
			HTTPClient: &http.Client{Transport: appendRoundTripFunc(func(*http.Request) (*http.Response, error) {
				return nil, transportErr
			})},
		})
		require.NoError(t, err)

		_, err = client.appendNDJSON(context.Background(), "db", "schema", "table", []byte("{}"))
		var scopeErr *Error
		require.ErrorAs(t, err, &scopeErr)
		require.ErrorIs(t, scopeErr, transportErr)
		require.Contains(t, scopeErr.Error(), transportErr.Error())
		require.Equal(t, AppendStateUnknown, scopeErr.AppendDetails.AppendState)
	})

	t.Run("response body", func(t *testing.T) {
		t.Parallel()

		readErr := errors.New("response stream interrupted")
		client, err := NewClient(Config{
			Endpoint: "https://example.com",
			HTTPClient: &http.Client{Transport: appendRoundTripFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     make(http.Header),
					Body:       errorReadCloser{err: readErr},
				}, nil
			})},
		})
		require.NoError(t, err)

		_, err = client.appendNDJSON(context.Background(), "db", "schema", "table", []byte("{}"))
		var scopeErr *Error
		require.ErrorAs(t, err, &scopeErr)
		require.Equal(t, readErr.Error(), scopeErr.Error())
		require.ErrorIs(t, scopeErr, readErr)
		require.Equal(t, AppendStateUnknown, scopeErr.AppendDetails.AppendState)
	})
}

func TestAppendNDJSONReturnsPreCancelledContextUnchanged(t *testing.T) {
	t.Parallel()

	requests := 0
	client, err := NewClient(Config{
		Endpoint: "https://example.com",
		HTTPClient: &http.Client{Transport: appendRoundTripFunc(func(*http.Request) (*http.Response, error) {
			requests++
			return nil, errors.New("unexpected request")
		})},
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = client.appendNDJSON(ctx, "db", "schema", "table", []byte("{}"))
	require.Equal(t, context.Canceled, err)
	require.Zero(t, requests)
}

type appendRoundTripFunc func(*http.Request) (*http.Response, error)

func (f appendRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type errorReadCloser struct {
	err error
}

func (r errorReadCloser) Read([]byte) (int, error) {
	return 0, r.err
}

func (errorReadCloser) Close() error {
	return nil
}
