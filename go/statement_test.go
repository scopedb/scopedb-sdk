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

func TestStatementSubmitSendsMaxParallelism(t *testing.T) {
	t.Parallel()

	statementID := uuid.New()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			MaxParallelism int    `json:"max_parallelism"`
			Format         string `json:"format"`
		}
		body, err := decodeCompressedRequestBody(r)
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(body, &request))
		require.Equal(t, 8, request.MaxParallelism)
		require.Equal(t, "json", request.Format)
		writeTestJSON(t, w, `{
			"statement_id":"`+statementID.String()+`",
			"status":"running",
			"created_at":"2026-08-08T00:00:00Z",
			"progress":{}
		}`)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	statement := client.Statement("FROM events")
	statement.MaxParallelism = 8
	handle, err := statement.Submit(context.Background())
	require.NoError(t, err)
	require.Equal(t, statementID, handle.ID())
}

func TestStatementRejectsNegativeMaxParallelismLocally(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	statement := client.Statement("FROM events")
	statement.MaxParallelism = -1
	_, err := statement.Submit(context.Background())
	var scopeDBError *Error
	require.ErrorAs(t, err, &scopeDBError)
	require.Equal(t, ErrorKindConfigInvalid, scopeDBError.Kind)
	require.Zero(t, requests.Load())
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
