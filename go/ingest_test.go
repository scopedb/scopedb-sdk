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
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIngestResponseRequiresExplicitNonnegativeInsertedRows(t *testing.T) {
	t.Parallel()

	for name, body := range map[string]string{
		"missing":     `{}`,
		"null body":   `null`,
		"null field":  `{"num_rows_inserted":null}`,
		"wrong type":  `{"num_rows_inserted":"1"}`,
		"wrong shape": `[]`,
		"negative":    `{"num_rows_inserted":-1}`,
		"malformed":   `{`,
	} {
		name, body := name, body
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("X-Request-ID", "ingest-request")
				w.WriteHeader(http.StatusOK)
				_, err := w.Write([]byte(body))
				require.NoError(t, err)
			}))
			defer server.Close()

			client := newTestClient(t, server.URL)
			response, err := client.ingest(context.Background(), testIngestRequest())
			require.Nil(t, response)
			assertUnknownIngestOutcome(t, err, http.StatusOK, "ingest-request")
		})
	}
}

func TestIngestResponseAcceptsPresentNonnegativeInsertedRows(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		body     string
		expected int
	}{
		{body: `{"num_rows_inserted":0}`, expected: 0},
		{body: `{"num_rows_inserted":7}`, expected: 7},
	} {
		test := test
		t.Run(test.body, func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				writeTestJSON(t, w, test.body)
			}))
			defer server.Close()

			client := newTestClient(t, server.URL)
			response, err := client.ingest(context.Background(), testIngestRequest())
			require.NoError(t, err)
			require.NotNil(t, response)
			require.Equal(t, test.expected, response.NumRowsInserted)
		})
	}
}

func TestIngestHTTPErrorPreservesServerMessageButDisablesRetry(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-Request-ID", "rejected-request")
		w.Header().Set("Retry-After", "10")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, err := w.Write([]byte(`{"message":"ingest overloaded","retryable":true}`))
		require.NoError(t, err)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	_, err := client.ingest(context.Background(), testIngestRequest())
	var scopeErr *Error
	require.ErrorAs(t, err, &scopeErr)
	require.Equal(t, ErrorKindUnexpected, scopeErr.Kind)
	require.Equal(t, "ingest overloaded", scopeErr.Message)
	require.Equal(t, http.StatusServiceUnavailable, scopeErr.HTTPStatus)
	require.Equal(t, "rejected-request", scopeErr.RequestID)
	require.Equal(t, 10*time.Second, scopeErr.RetryAfter)
	require.False(t, scopeErr.Retryable)
	require.Nil(t, scopeErr.AppendDetails)
}

func TestIngestTransportAndSuccessBodyReadFailuresAreUnknown(t *testing.T) {
	t.Parallel()

	t.Run("transport", func(t *testing.T) {
		t.Parallel()

		transportErr := errors.New("connection reset after request write")
		client, err := NewClient(Config{
			Endpoint: "https://example.com",
			HTTPClient: &http.Client{Transport: ingestRoundTripFunc(func(*http.Request) (*http.Response, error) {
				return nil, transportErr
			})},
		})
		require.NoError(t, err)

		_, err = client.ingest(context.Background(), testIngestRequest())
		assertUnknownIngestOutcome(t, err, 0, "")
		require.ErrorIs(t, err, transportErr)
	})

	t.Run("response body", func(t *testing.T) {
		t.Parallel()

		readErr := errors.New("response stream interrupted")
		client, err := NewClient(Config{
			Endpoint: "https://example.com",
			HTTPClient: &http.Client{Transport: ingestRoundTripFunc(func(*http.Request) (*http.Response, error) {
				header := make(http.Header)
				header.Set("X-Request-ID", "read-request")
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     header,
					Body:       errorReadCloser{err: readErr},
				}, nil
			})},
		})
		require.NoError(t, err)

		_, err = client.ingest(context.Background(), testIngestRequest())
		assertUnknownIngestOutcome(t, err, http.StatusOK, "read-request")
		require.ErrorIs(t, err, readErr)
	})
}

func TestIngestPreCanceledContextDoesNotClaimUnknownOutcome(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	client, err := NewClient(Config{
		Endpoint: "https://example.com",
		HTTPClient: &http.Client{Transport: ingestRoundTripFunc(func(*http.Request) (*http.Response, error) {
			requests.Add(1)
			return nil, errors.New("unexpected request")
		})},
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = client.ingest(ctx, testIngestRequest())
	require.ErrorIs(t, err, context.Canceled)
	require.NotEqual(t, ingestCommitOutcomeUnknownMessage, err.Error())
	require.Zero(t, requests.Load())
}

func TestIngestStreamFailsOnMalformedSuccessResponse(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = readIngestStreamRequest(t, r)
		writeTestJSON(t, w, `{}`)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	stream, err := client.IngestStream("SELECT $0 INSERT INTO events", IngestStreamOptions{
		MaxBatchRows:  1,
		FlushInterval: time.Hour,
	})
	require.NoError(t, err)
	require.NoError(t, stream.Send(context.Background(), map[string]any{"id": 1}))

	result, err := stream.Shutdown(context.Background())
	require.Zero(t, result.NumRowsInserted)
	assertUnknownIngestOutcome(t, err, http.StatusOK, "")
}

func assertUnknownIngestOutcome(
	t *testing.T,
	err error,
	httpStatus int,
	requestID string,
) {
	t.Helper()
	var scopeErr *Error
	require.ErrorAs(t, err, &scopeErr)
	require.Equal(t, ErrorKindUnexpected, scopeErr.Kind)
	require.Equal(t, ingestCommitOutcomeUnknownMessage, scopeErr.Message)
	require.Equal(t, httpStatus, scopeErr.HTTPStatus)
	require.Equal(t, requestID, scopeErr.RequestID)
	require.False(t, scopeErr.Retryable)
	require.Nil(t, scopeErr.AppendDetails)
	require.Error(t, scopeErr.Unwrap())
}

func testIngestRequest() *ingestRequest {
	return &ingestRequest{
		Data: ingestData{
			Format: writeFormatJSON,
			Rows:   `{"id":1}`,
		},
		Statement: "SELECT $0 INSERT INTO events",
	}
}

type ingestRoundTripFunc func(*http.Request) (*http.Response, error)

func (f ingestRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
