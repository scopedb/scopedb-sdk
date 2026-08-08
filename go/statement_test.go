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
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestClientQueryExecutesAndReturnsRows(t *testing.T) {
	t.Parallel()

	statementID := uuid.New()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "/v1/statements", r.URL.Path)
		var request struct {
			Format string `json:"format"`
		}
		body, err := decodeCompressedRequestBody(r)
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(body, &request))
		require.Equal(t, "json", request.Format)
		writeTestJSON(t, w, `{
			"statement_id":"`+statementID.String()+`",
			"status":"finished",
			"created_at":"2026-08-08T00:00:00Z",
			"progress":{},
			"result_set":{
				"metadata":{"fields":[{"name":"ready","data_type":"u_int"}],"num_rows":1},
				"format":"json",
				"rows":[["1"]]
			}
		}`)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	result, err := client.Query(context.Background(), "SELECT 1 AS ready")
	require.NoError(t, err)
	row, ok, err := result.First()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(1), row["ready"])
}

func TestStatementSubmitSendsOnlySupportedConfiguration(t *testing.T) {
	t.Parallel()

	providedID := uuid.MustParse("01989a4e-4ee2-7e63-87a5-65ac3b5161dc")
	tests := []struct {
		name        string
		statementID *uuid.UUID
		execTimeout string
		wantBody    string
	}{
		{
			name:     "default",
			wantBody: `{"statement":"FROM events","format":"json"}`,
		},
		{
			name:        "execution timeout",
			execTimeout: "1h",
			wantBody:    `{"statement":"FROM events","exec_timeout":"1h","format":"json"}`,
		},
		{
			name:        "provided statement ID",
			statementID: &providedID,
			wantBody:    `{"statement_id":"01989a4e-4ee2-7e63-87a5-65ac3b5161dc","statement":"FROM events","format":"json"}`,
		},
		{
			name:        "provided statement ID and execution timeout",
			statementID: &providedID,
			execTimeout: "1h",
			wantBody:    `{"statement_id":"01989a4e-4ee2-7e63-87a5-65ac3b5161dc","statement":"FROM events","exec_timeout":"1h","format":"json"}`,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			responseID := uuid.New()
			if test.statementID != nil {
				responseID = *test.statementID
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := decodeCompressedRequestBody(r)
				require.NoError(t, err)
				require.JSONEq(t, test.wantBody, string(body))
				writeTestJSON(t, w, `{
					"statement_id":"`+responseID.String()+`",
					"status":"running",
					"created_at":"2026-08-08T00:00:00Z",
					"progress":{}
				}`)
			}))
			defer server.Close()

			client := newTestClient(t, server.URL)
			statement := client.Statement("FROM events")
			statement.ID = test.statementID
			statement.ExecTimeout = test.execTimeout
			handle, err := statement.Submit(context.Background())
			require.NoError(t, err)
			require.Equal(t, responseID, handle.ID())
		})
	}
}

func TestStatementHandleStatusFetchesOnceAndCachesTerminalStatus(t *testing.T) {
	t.Parallel()

	statementID := uuid.New()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		require.Equal(t, http.MethodGet, r.Method)
		require.Equal(t, "/v1/statements/"+statementID.String(), r.URL.Path)
		require.Equal(t, "json", r.URL.Query().Get("format"))
		writeTestJSON(t, w, `{
			"statement_id":"`+statementID.String()+`",
			"status":"finished",
			"created_at":"2026-08-08T00:00:00Z",
			"progress":{},
			"result_set":{"metadata":{"fields":[],"num_rows":0},"format":"json","rows":[]}
		}`)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	handle := client.StatementHandle(statementID)
	require.Equal(t, statementID, handle.ID())
	require.Nil(t, handle.LastStatus())
	require.Zero(t, requests.Load(), "LastStatus must not make a request")

	status, err := handle.Status(context.Background())
	require.NoError(t, err)
	require.Equal(t, StatementStatusFinished, status)
	require.Equal(t, int32(1), requests.Load())

	status, err = handle.Status(context.Background())
	require.NoError(t, err)
	require.Equal(t, StatementStatusFinished, status)
	require.Equal(t, int32(1), requests.Load(), "terminal status must be cached")
	require.Equal(t, StatementStatusFinished, *handle.LastStatus())
}

func TestStatementHandleWaitPollsToFinished(t *testing.T) {
	t.Parallel()

	statementID := uuid.New()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		request := requests.Add(1)
		if request == 1 {
			writeTestJSON(t, w, `{
				"statement_id":"`+statementID.String()+`",
				"status":"running",
				"created_at":"2026-08-08T00:00:00Z",
				"progress":{}
			}`)
			return
		}
		writeTestJSON(t, w, `{
			"statement_id":"`+statementID.String()+`",
			"status":"finished",
			"created_at":"2026-08-08T00:00:00Z",
			"progress":{},
			"result_set":{
				"metadata":{"fields":[{"name":"value","data_type":"string"}],"num_rows":1},
				"format":"json",
				"rows":[["done"]]
			}
		}`)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	result, err := client.StatementHandle(statementID).Wait(context.Background())
	require.NoError(t, err)
	require.Equal(t, int32(2), requests.Load())
	row, ok, err := result.First()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "done", row["value"])
}

func TestStatementHandleWaitReturnsStatementFailure(t *testing.T) {
	t.Parallel()

	statementID := uuid.New()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeTestJSON(t, w, `{
			"statement_id":"`+statementID.String()+`",
			"status":"failed",
			"created_at":"2026-08-08T00:00:00Z",
			"progress":{},
			"message":"invalid ScopeQL"
		}`)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	_, err := client.StatementHandle(statementID).Wait(context.Background())
	require.Error(t, err)
	var scopeDBError *Error
	require.True(t, errors.As(err, &scopeDBError))
	require.Equal(t, ErrorKindStatementFailed, scopeDBError.Kind)
	require.Equal(t, "invalid ScopeQL", scopeDBError.Message)
}

func TestStatementHandleWaitPreservesStructuredStatementFailure(t *testing.T) {
	t.Parallel()

	statementID := uuid.New()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeTestJSON(t, w, `{
			"statement_id":"`+statementID.String()+`",
			"status":"failed",
			"created_at":"2026-08-08T00:00:00Z",
			"progress":{},
			"message":"outer server message",
			"error":{
				"code":"row_limit_exceeded",
				"message":"structured server message",
				"details":{"total_rows":42,"max_total_rows":0}
			}
		}`)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	_, err := client.StatementHandle(statementID).Wait(context.Background())
	var scopeDBError *Error
	require.ErrorAs(t, err, &scopeDBError)
	require.Equal(t, ErrorKindStatementFailed, scopeDBError.Kind)
	require.Equal(t, "outer server message", scopeDBError.Message)
	require.Equal(t, &StatementErrorDetails{
		Code:    StatementErrorCodeRowLimitExceeded,
		Message: "structured server message",
		Details: json.RawMessage(`{"total_rows":42,"max_total_rows":0}`),
	}, scopeDBError.StatementDetails)
}

func TestStatementHandleCancelResumesWithoutSnapshot(t *testing.T) {
	t.Parallel()

	statementID := uuid.New()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "/v1/statements/"+statementID.String()+"/cancel", r.URL.Path)
		writeTestJSON(t, w, `{
			"statement_id":"`+statementID.String()+`",
			"created_at":"2026-08-08T00:00:00Z",
			"status":"cancelled",
			"message":"statement is cancelled"
		}`)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	handle := client.StatementHandle(statementID)
	result, err := handle.Cancel(context.Background())
	require.NoError(t, err)
	require.Equal(t, statementID, result.StatementID)
	require.Equal(t, time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC), result.CreatedAt)
	require.Equal(t, StatementStatusCancelled, result.Status)
	require.Equal(t, "statement is cancelled", result.Message)
	require.Equal(t, StatementStatusCancelled, *handle.LastStatus())
	require.Equal(t, int32(1), requests.Load())

	again, err := handle.Cancel(context.Background())
	require.NoError(t, err)
	require.Equal(t, result, again)
	require.Equal(t, int32(1), requests.Load(), "terminal cancel result must be cached")
}

func TestStatementHandleCancelReturnsCompleteCachedTerminalResult(t *testing.T) {
	t.Parallel()

	statementID := uuid.New()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		require.Equal(t, http.MethodGet, r.Method)
		writeTestJSON(t, w, `{
			"statement_id":"`+statementID.String()+`",
			"status":"finished",
			"created_at":"2026-08-08T00:00:00Z",
			"progress":{},
			"result_set":{"metadata":{"fields":[],"num_rows":0},"format":"json","rows":[]}
		}`)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	handle := client.StatementHandle(statementID)
	_, err := handle.Status(context.Background())
	require.NoError(t, err)
	result, err := handle.Cancel(context.Background())
	require.NoError(t, err)
	require.Equal(t, StatementCancelResult{
		StatementID: statementID,
		CreatedAt:   time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC),
		Status:      StatementStatusFinished,
		Message:     "statement is finished",
	}, result)
	require.Equal(t, int32(1), requests.Load(), "cached terminal cancel must not make a request")
}

func TestStatementHandleWaitFetchesResultAfterCancelReportsFinished(t *testing.T) {
	t.Parallel()

	statementID := uuid.New()
	var cancelRequests atomic.Int32
	var statusRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/statements/"+statementID.String()+"/cancel":
			cancelRequests.Add(1)
			writeTestJSON(t, w, `{
				"statement_id":"`+statementID.String()+`",
				"created_at":"2026-08-08T00:00:00Z",
				"status":"finished",
				"message":"statement already finished"
			}`)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/statements/"+statementID.String():
			statusRequests.Add(1)
			writeTestJSON(t, w, `{
				"statement_id":"`+statementID.String()+`",
				"status":"finished",
				"created_at":"2026-08-08T00:00:00Z",
				"progress":{},
				"result_set":{
					"metadata":{"fields":[{"name":"value","data_type":"string"}],"num_rows":1},
					"format":"json",
					"rows":[["done"]]
				}
			}`)
		default:
			http.Error(w, "unexpected request", http.StatusNotFound)
		}
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	handle := client.StatementHandle(statementID)
	cancelResult, err := handle.Cancel(context.Background())
	require.NoError(t, err)
	require.Equal(t, StatementStatusFinished, cancelResult.Status)
	require.Equal(t, StatementStatusFinished, *handle.LastStatus())
	require.Equal(t, int32(1), cancelRequests.Load())
	require.Zero(t, statusRequests.Load())

	status, err := handle.Status(context.Background())
	require.NoError(t, err)
	require.Equal(t, StatementStatusFinished, status)
	require.Zero(t, statusRequests.Load(), "Status must use the cached terminal outcome")

	result, err := handle.Wait(context.Background())
	require.NoError(t, err)
	require.Equal(t, int32(1), statusRequests.Load(), "Wait must fetch the missing result set")
	row, found, err := result.First()
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "done", row["value"])

	status, err = handle.Status(context.Background())
	require.NoError(t, err)
	require.Equal(t, StatementStatusFinished, status)
	require.Equal(t, int32(1), statusRequests.Load())
}

func TestStatementHandleWaitFetchesFailureAfterCancelReportsFailed(t *testing.T) {
	t.Parallel()

	statementID := uuid.New()
	var cancelRequests atomic.Int32
	var statusRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/statements/"+statementID.String()+"/cancel":
			cancelRequests.Add(1)
			writeTestJSON(t, w, `{
				"statement_id":"`+statementID.String()+`",
				"created_at":"2026-08-08T00:00:00Z",
				"status":"failed",
				"message":"statement already failed"
			}`)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/statements/"+statementID.String():
			request := statusRequests.Add(1)
			if request == 1 {
				writeTestJSON(t, w, `{
					"statement_id":"`+statementID.String()+`",
					"status":"running",
					"created_at":"2026-08-08T00:00:00Z",
					"progress":{"scanned_rows":11}
				}`)
				return
			}
			writeTestJSON(t, w, `{
				"statement_id":"`+statementID.String()+`",
				"status":"failed",
				"created_at":"2026-08-08T00:00:00Z",
				"progress":{"scanned_rows":12,"skipped_rows":3},
				"message":"row limit reached",
				"error":{
					"code":"row_limit_exceeded",
					"message":"total rows exceeds limit",
					"details":{"total_rows":12,"max_total_rows":10}
				}
			}`)
		default:
			http.Error(w, "unexpected request", http.StatusNotFound)
		}
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	handle := client.StatementHandle(statementID)
	status, err := handle.Status(context.Background())
	require.NoError(t, err)
	require.Equal(t, StatementStatusRunning, status)
	require.Equal(t, int64(11), handle.Progress().ScannedRows)

	cancelResult, err := handle.Cancel(context.Background())
	require.NoError(t, err)
	require.Equal(t, StatementStatusFailed, cancelResult.Status)
	require.Equal(t, StatementStatusFailed, *handle.LastStatus())
	require.Equal(t, int64(11), handle.Progress().ScannedRows, "cancel must preserve progress")
	require.Equal(t, int32(1), cancelRequests.Load())
	require.Equal(t, int32(1), statusRequests.Load())

	status, err = handle.Status(context.Background())
	require.NoError(t, err)
	require.Equal(t, StatementStatusFailed, status)
	require.Equal(t, int32(1), statusRequests.Load(), "Status must use the cached terminal outcome")

	_, err = handle.Wait(context.Background())
	var scopeDBError *Error
	require.ErrorAs(t, err, &scopeDBError)
	require.Equal(t, ErrorKindStatementFailed, scopeDBError.Kind)
	require.Equal(t, "row limit reached", scopeDBError.Message)
	require.Equal(t, &StatementErrorDetails{
		Code:    StatementErrorCodeRowLimitExceeded,
		Message: "total rows exceeds limit",
		Details: json.RawMessage(`{"total_rows":12,"max_total_rows":10}`),
	}, scopeDBError.StatementDetails)
	require.Equal(t, int32(2), statusRequests.Load(), "Wait must fetch the complete failure once")
	require.Equal(t, int64(12), handle.Progress().ScannedRows)
	require.Equal(t, int64(3), handle.Progress().SkippedRows)

	status, err = handle.Status(context.Background())
	require.NoError(t, err)
	require.Equal(t, StatementStatusFailed, status)
	require.Equal(t, int32(2), statusRequests.Load())
}

func TestStatementProgressIncludesSkippedWork(t *testing.T) {
	t.Parallel()

	statementID := uuid.New()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeTestJSON(t, w, `{
			"statement_id":"`+statementID.String()+`",
			"status":"running",
			"created_at":"2026-08-08T00:00:00Z",
			"progress":{
				"skipped_partitions":1,
				"skipped_rows":2,
				"skipped_compressed_bytes":3,
				"skipped_uncompressed_bytes":4
			}
		}`)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	handle := client.StatementHandle(statementID)
	_, err := handle.Status(context.Background())
	require.NoError(t, err)
	require.Equal(t, &StatementProgress{
		SkippedPartitions:        1,
		SkippedRows:              2,
		SkippedCompressedBytes:   3,
		SkippedUncompressedBytes: 4,
	}, handle.Progress())
}

func TestStatementStatusRejectsMalformedStructuredFailures(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		"non-object error":           `[]`,
		"missing code":               `{"message":"failed"}`,
		"missing message":            `{"code":"execute_error"}`,
		"missing limit details":      `{"code":"row_limit_exceeded","message":"failed"}`,
		"non-object limit details":   `{"code":"row_limit_exceeded","message":"failed","details":[]}`,
		"missing limit detail field": `{"code":"row_limit_exceeded","message":"failed","details":{"total_rows":1}}`,
		"invalid limit detail field": `{"code":"scan_limit_exceeded","message":"failed","details":{"scanned_uncompressed_bytes":1,"max_scanned_uncompressed_bytes":-1}}`,
	}
	for name, statementError := range tests {
		name, statementError := name, statementError
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			statementID := uuid.New()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				writeTestJSON(t, w, `{
					"statement_id":"`+statementID.String()+`",
					"status":"failed",
					"created_at":"2026-08-08T00:00:00Z",
					"progress":{},
					"message":"failed",
					"error":`+statementError+`
				}`)
			}))
			defer server.Close()

			client := newTestClient(t, server.URL)
			_, err := client.StatementHandle(statementID).Status(context.Background())
			var scopeDBError *Error
			require.ErrorAs(t, err, &scopeDBError)
			require.Equal(t, ErrorKindUnexpected, scopeDBError.Kind)
		})
	}
}

func TestStatementStatusRejectsMalformedFinishedResultsWithoutPanicking(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		"missing result set": ``,
		"missing metadata":   `,"result_set":{"format":"json","rows":[]}`,
		"unsupported format": `,"result_set":{"metadata":{"fields":[],"num_rows":0},"format":"arrow","rows":[]}`,
		"null field":         `,"result_set":{"metadata":{"fields":[null],"num_rows":0},"format":"json","rows":[]}`,
		"missing data type":  `,"result_set":{"metadata":{"fields":[{"name":"v"}],"num_rows":0},"format":"json","rows":[]}`,
		"unknown data type":  `,"result_set":{"metadata":{"fields":[{"name":"v","data_type":"unsigned"}],"num_rows":0},"format":"json","rows":[]}`,
	}
	for name, resultSet := range tests {
		name, resultSet := name, resultSet
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			statementID := uuid.New()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				writeTestJSON(t, w, `{
					"statement_id":"`+statementID.String()+`",
					"status":"finished",
					"created_at":"2026-08-08T00:00:00Z",
					"progress":{}`+resultSet+`
				}`)
			}))
			defer server.Close()

			client := newTestClient(t, server.URL)
			require.NotPanics(t, func() {
				_, err := client.StatementHandle(statementID).Status(context.Background())
				var scopeDBError *Error
				require.ErrorAs(t, err, &scopeDBError)
				require.Equal(t, ErrorKindUnexpected, scopeDBError.Kind)
			})
		})
	}
}

func TestMalformedFinishedRowsReturnResultErrorsWithoutPanicking(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		metadata  string
		rows      string
		assertErr func(*testing.T, *ResultSet)
	}{
		{
			name:     "malformed rows",
			metadata: `{"fields":[],"num_rows":0}`,
			rows:     `{}`,
			assertErr: func(t *testing.T, result *ResultSet) {
				_, err := result.RawRows()
				require.Error(t, err)
			},
		},
		{
			name:     "null rows",
			metadata: `{"fields":[],"num_rows":0}`,
			rows:     `null`,
			assertErr: func(t *testing.T, result *ResultSet) {
				_, err := result.RawRows()
				require.ErrorContains(t, err, "must be a JSON array")
			},
		},
		{
			name:     "row count mismatch",
			metadata: `{"fields":[],"num_rows":1}`,
			rows:     `[]`,
			assertErr: func(t *testing.T, result *ResultSet) {
				_, err := result.RawRows()
				require.ErrorContains(t, err, "result row count mismatch")
			},
		},
		{
			name:     "field count mismatch",
			metadata: `{"fields":[{"name":"v","data_type":"int"}],"num_rows":1}`,
			rows:     `[[]]`,
			assertErr: func(t *testing.T, result *ResultSet) {
				_, err := result.ToValues()
				require.ErrorContains(t, err, "schema length does not match record length")
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			statementID := uuid.New()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				writeTestJSON(t, w, `{
					"statement_id":"`+statementID.String()+`",
					"status":"finished",
					"created_at":"2026-08-08T00:00:00Z",
					"progress":{},
					"result_set":{"metadata":`+test.metadata+`,"format":"json","rows":`+test.rows+`}
				}`)
			}))
			defer server.Close()

			client := newTestClient(t, server.URL)
			require.NotPanics(t, func() {
				handle := client.StatementHandle(statementID)
				_, err := handle.Status(context.Background())
				require.NoError(t, err)
				result := handle.ResultSet()
				require.NotNil(t, result)
				test.assertErr(t, result)
			})
		})
	}
}

func newTestClient(t *testing.T, endpoint string) *Client {
	t.Helper()
	client, err := NewClient(Config{Endpoint: endpoint})
	require.NoError(t, err)
	t.Cleanup(client.Close)
	return client
}

func writeTestJSON(t *testing.T, w http.ResponseWriter, body string) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, err := w.Write([]byte(body))
	require.NoError(t, err)
}
