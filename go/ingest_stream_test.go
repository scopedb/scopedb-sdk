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
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/stretchr/testify/require"
)

func TestIngestStreamBatchesRowsAndReportsBarrierInterval(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var payloads []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := readIngestStreamRequest(t, r)
		mu.Lock()
		payloads = append(payloads, request.Data.Rows)
		mu.Unlock()
		writeIngestStreamResult(t, w, countNDJSONRows(request.Data.Rows))
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	stream, err := client.IngestStream("SELECT $0 INSERT INTO events", IngestStreamOptions{
		MaxBatchRows:  2,
		FlushInterval: time.Hour,
	})
	require.NoError(t, err)

	require.NoError(t, stream.Send(context.Background(), map[string]any{"id": 1}))
	require.NoError(t, stream.Send(context.Background(), map[string]any{"id": 2}))
	require.NoError(t, stream.Send(context.Background(), map[string]any{"id": 3}))

	result, err := stream.Flush(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(3), result.NumRowsInserted)

	final, err := stream.Shutdown(context.Background())
	require.NoError(t, err)
	require.Zero(t, final.NumRowsInserted)

	mu.Lock()
	require.Equal(t, []string{
		"{\"id\":1}\n{\"id\":2}",
		"{\"id\":3}",
	}, payloads)
	mu.Unlock()
}

func TestIngestStreamBatchesByTargetBytes(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var payloads []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := readIngestStreamRequest(t, r)
		mu.Lock()
		payloads = append(payloads, request.Data.Rows)
		mu.Unlock()
		writeIngestStreamResult(t, w, countNDJSONRows(request.Data.Rows))
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	stream, err := client.IngestStream("SELECT $0 INSERT INTO events", IngestStreamOptions{
		TargetBatchBytes: len(`{"id":1}`) + len(`{"id":2}`),
		FlushInterval:    time.Hour,
	})
	require.NoError(t, err)
	require.NoError(t, stream.Send(context.Background(), map[string]any{"id": 1}))
	require.NoError(t, stream.Send(context.Background(), map[string]any{"id": 2}))

	result, err := stream.Shutdown(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(2), result.NumRowsInserted)
	mu.Lock()
	require.Equal(t, []string{`{"id":1}`, `{"id":2}`}, payloads)
	mu.Unlock()
}

func TestIngestStreamFlushesOnTimer(t *testing.T) {
	t.Parallel()

	requestSeen := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := readIngestStreamRequest(t, r)
		requestSeen <- struct{}{}
		writeIngestStreamResult(t, w, countNDJSONRows(request.Data.Rows))
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	stream, err := client.IngestStream("SELECT $0 INSERT INTO events", IngestStreamOptions{
		FlushInterval: 10 * time.Millisecond,
	})
	require.NoError(t, err)
	require.NoError(t, stream.Send(context.Background(), map[string]any{"id": 1}))

	select {
	case <-requestSeen:
	case <-time.After(time.Second):
		t.Fatal("timed ingest batch was not dispatched")
	}
	result, err := stream.Flush(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(1), result.NumRowsInserted)
	_, err = stream.Shutdown(context.Background())
	require.NoError(t, err)
}

func TestIngestStreamAttemptTimeoutStopsStalledRequest(t *testing.T) {
	t.Parallel()

	requestStarted := make(chan struct{})
	requestCanceled := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		_ = readIngestStreamRequest(t, r)
		close(requestStarted)
		<-r.Context().Done()
		requestCanceled <- r.Context().Err()
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	stream, err := client.IngestStream("SELECT $0 INSERT INTO events", IngestStreamOptions{
		MaxBatchRows:   1,
		FlushInterval:  time.Hour,
		AttemptTimeout: 25 * time.Millisecond,
	})
	require.NoError(t, err)
	require.NoError(t, stream.Send(context.Background(), map[string]any{"id": 1}))
	<-requestStarted

	waitCtx, cancelWait := context.WithTimeout(context.Background(), time.Second)
	defer cancelWait()
	result, err := stream.Shutdown(waitCtx)
	require.Zero(t, result.NumRowsInserted)
	assertUnknownIngestOutcome(t, err, 0, "")
	require.ErrorIs(t, err, context.DeadlineExceeded)

	select {
	case serverErr := <-requestCanceled:
		require.ErrorIs(t, serverErr, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("stalled ingest request context was not canceled")
	}
}

func TestIngestStreamBackpressureHonorsSendContext(t *testing.T) {
	t.Parallel()

	requestStarted := make(chan struct{})
	releaseRequest := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := readIngestStreamRequest(t, r)
		close(requestStarted)
		<-releaseRequest
		writeIngestStreamResult(t, w, countNDJSONRows(request.Data.Rows))
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	stream, err := client.IngestStream("SELECT $0 INSERT INTO events", IngestStreamOptions{
		MaxBatchRows:     1,
		MaxBufferedBytes: len(`{"id":1}`) + 1,
		FlushInterval:    time.Hour,
	})
	require.NoError(t, err)
	require.NoError(t, stream.Send(context.Background(), map[string]any{"id": 1}))
	<-requestStarted

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err = stream.Send(ctx, map[string]any{"id": 2})
	require.ErrorIs(t, err, context.DeadlineExceeded)

	close(releaseRequest)
	result, err := stream.Shutdown(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(1), result.NumRowsInserted)
}

func TestIngestStreamBarrierLinearizesConcurrentAdmission(t *testing.T) {
	t.Parallel()

	firstRequestStarted := make(chan struct{})
	releaseFirstRequest := make(chan struct{})
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := readIngestStreamRequest(t, r)
		if requests.Add(1) == 1 {
			close(firstRequestStarted)
			<-releaseFirstRequest
		}
		writeIngestStreamResult(t, w, countNDJSONRows(request.Data.Rows))
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	stream, err := client.IngestStream("SELECT $0 INSERT INTO events", IngestStreamOptions{
		FlushInterval: time.Hour,
	})
	require.NoError(t, err)
	require.NoError(t, stream.Send(context.Background(), map[string]any{"id": 1}))

	flushDone := make(chan ingestOutcome, 1)
	go func() {
		result, err := stream.Flush(context.Background())
		flushDone <- ingestOutcome{result: result, err: err}
	}()
	<-firstRequestStarted

	// The first barrier is already being processed, so this row belongs to the
	// next interval even though the first remote request has not completed.
	require.NoError(t, stream.Send(context.Background(), map[string]any{"id": 2}))
	close(releaseFirstRequest)
	first := <-flushDone
	require.NoError(t, first.err)
	require.Equal(t, int64(1), first.result.NumRowsInserted)

	final, err := stream.Shutdown(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(1), final.NumRowsInserted)
	require.Equal(t, int32(2), requests.Load())
}

func TestIngestStreamShutdownContinuesAfterContextCancellationAndIsIdempotent(t *testing.T) {
	t.Parallel()

	requestStarted := make(chan struct{})
	releaseRequest := make(chan struct{})
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := readIngestStreamRequest(t, r)
		requests.Add(1)
		close(requestStarted)
		<-releaseRequest
		writeIngestStreamResult(t, w, countNDJSONRows(request.Data.Rows))
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	stream, err := client.IngestStream("SELECT $0 INSERT INTO events", IngestStreamOptions{
		FlushInterval: time.Hour,
	})
	require.NoError(t, err)
	require.NoError(t, stream.Send(context.Background(), map[string]any{"id": 1}))

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = stream.Shutdown(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	<-requestStarted
	close(releaseRequest)

	result, err := stream.Shutdown(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(1), result.NumRowsInserted)
	again, err := stream.Shutdown(context.Background())
	require.NoError(t, err)
	require.Equal(t, result, again)
	require.Equal(t, int32(1), requests.Load())

	err = stream.Send(context.Background(), map[string]any{"id": 2})
	require.ErrorContains(t, err, "ingest stream is closed")
}

func TestIngestStreamRejectsInvalidRowsLocally(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		writeIngestStreamResult(t, w, 0)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	stream, err := client.IngestStream("SELECT $0 INSERT INTO events", IngestStreamOptions{})
	require.NoError(t, err)

	for _, row := range []any{nil, 1, "value", []int{1, 2}} {
		err := stream.Send(context.Background(), row)
		require.ErrorContains(t, err, "must serialize to JSON objects")
	}
	err = stream.Send(context.Background(), map[string]any{"invalid": make(chan int)})
	require.ErrorContains(t, err, "failed to serialize ingest stream row")

	result, err := stream.Shutdown(context.Background())
	require.NoError(t, err)
	require.Zero(t, result.NumRowsInserted)
	require.Zero(t, requests.Load())
}

func TestIngestStreamSupportsConcurrentSenders(t *testing.T) {
	t.Parallel()

	var inserted atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := readIngestStreamRequest(t, r)
		rows := countNDJSONRows(request.Data.Rows)
		inserted.Add(int64(rows))
		writeIngestStreamResult(t, w, rows)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	stream, err := client.IngestStream("SELECT $0 INSERT INTO events", IngestStreamOptions{
		MaxBatchRows:  17,
		FlushInterval: time.Hour,
	})
	require.NoError(t, err)

	const rows = 100
	var wg sync.WaitGroup
	errorsCh := make(chan error, rows)
	for id := 0; id < rows; id++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errorsCh <- stream.Send(context.Background(), map[string]any{"id": id})
		}()
	}
	wg.Wait()
	close(errorsCh)
	for err := range errorsCh {
		require.NoError(t, err)
	}

	result, err := stream.Shutdown(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(rows), result.NumRowsInserted)
	require.Equal(t, int64(rows), inserted.Load())
}

func TestIngestStreamFailureWakesBlockedAdmissionsWithoutLeaking(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	requestStarted := make(chan struct{})
	releaseRequest := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = readIngestStreamRequest(t, r)
		close(requestStarted)
		<-releaseRequest
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, err := w.Write([]byte(`{"message":"ingest unavailable"}`))
		require.NoError(t, err)
	}))
	defer server.Close()

	client, err := NewClient(Config{Endpoint: server.URL})
	require.NoError(t, err)
	defer client.Close()
	stream, err := client.IngestStream("SELECT $0 INSERT INTO events", IngestStreamOptions{
		MaxBatchRows:     1,
		MaxBufferedBytes: len(`{"id":1}`) + 1,
		FlushInterval:    time.Hour,
	})
	require.NoError(t, err)
	require.NoError(t, stream.Send(context.Background(), map[string]any{"id": 1}))
	<-requestStarted

	blocked := make(chan error, 1)
	go func() {
		blocked <- stream.Send(context.Background(), map[string]any{"id": 2})
	}()
	close(releaseRequest)
	err = <-blocked
	require.ErrorContains(t, err, "ingest unavailable")

	_, err = stream.Flush(context.Background())
	require.ErrorContains(t, err, "ingest unavailable")
	_, err = stream.Shutdown(context.Background())
	require.ErrorContains(t, err, "ingest unavailable")
}

func TestIngestStreamFailurePreservesCommittedPrefix(t *testing.T) {
	t.Parallel()

	secondRequestStarted := make(chan struct{})
	releaseSecondRequest := make(chan struct{})
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := readIngestStreamRequest(t, r)
		if requests.Add(1) == 1 {
			writeIngestStreamResult(t, w, countNDJSONRows(request.Data.Rows))
			return
		}
		close(secondRequestStarted)
		<-releaseSecondRequest
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, err := w.Write([]byte(`{"message":"ingest unavailable"}`))
		require.NoError(t, err)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	stream, err := client.IngestStream("SELECT $0 INSERT INTO events", IngestStreamOptions{
		MaxBatchRows:  1,
		FlushInterval: time.Hour,
	})
	require.NoError(t, err)
	require.NoError(t, stream.Send(context.Background(), map[string]any{"id": 1}))
	require.NoError(t, stream.Send(context.Background(), map[string]any{"id": 2}))
	<-secondRequestStarted

	flushDone := make(chan ingestOutcome, 1)
	go func() {
		result, err := stream.Flush(context.Background())
		flushDone <- ingestOutcome{result: result, err: err}
	}()
	require.Eventually(t, func() bool {
		return len(stream.inner.commands) > 0
	}, time.Second, time.Millisecond)
	close(releaseSecondRequest)

	flushed := <-flushDone
	require.Equal(t, int64(1), flushed.result.NumRowsInserted)
	require.ErrorContains(t, flushed.err, "ingest unavailable")
	terminalFlush, err := stream.Flush(context.Background())
	require.Equal(t, int64(1), terminalFlush.NumRowsInserted)
	require.ErrorContains(t, err, "ingest unavailable")

	final, err := stream.Shutdown(context.Background())
	require.Equal(t, int64(1), final.NumRowsInserted)
	require.ErrorContains(t, err, "ingest unavailable")
	again, err := stream.Shutdown(context.Background())
	require.Equal(t, final, again)
	require.ErrorContains(t, err, "ingest unavailable")
}

func TestIngestStreamPreCanceledSendSkipsSerialization(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, "http://127.0.0.1:6543")
	stream, err := client.IngestStream("SELECT $0 INSERT INTO events", IngestStreamOptions{})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = stream.Send(ctx, make(chan int))
	require.ErrorIs(t, err, context.Canceled)
	_, err = stream.Shutdown(context.Background())
	require.NoError(t, err)
}

func TestIngestStreamValidatesOptions(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, "http://127.0.0.1:6543")
	tests := []struct {
		name      string
		statement string
		options   IngestStreamOptions
	}{
		{name: "empty statement"},
		{name: "batch bytes", statement: "SELECT $0", options: IngestStreamOptions{TargetBatchBytes: -1}},
		{name: "batch rows", statement: "SELECT $0", options: IngestStreamOptions{MaxBatchRows: -1}},
		{name: "flush interval", statement: "SELECT $0", options: IngestStreamOptions{FlushInterval: -1}},
		{name: "buffer bytes", statement: "SELECT $0", options: IngestStreamOptions{MaxBufferedBytes: -1}},
		{name: "attempt timeout", statement: "SELECT $0", options: IngestStreamOptions{AttemptTimeout: -1}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := client.IngestStream(test.statement, test.options)
			require.Error(t, err)
		})
	}
}

func TestIngestStreamDefaultsAttemptTimeout(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, "http://127.0.0.1:6543")
	stream, err := client.IngestStream("SELECT $0", IngestStreamOptions{})
	require.NoError(t, err)
	require.Equal(t, 30*time.Second, stream.inner.config.attemptTimeout)
	_, err = stream.Shutdown(context.Background())
	require.NoError(t, err)
}

type ingestStreamRequestPayload struct {
	Statement string `json:"statement"`
	Data      struct {
		Format string `json:"format"`
		Rows   string `json:"rows"`
	} `json:"data"`
}

func readIngestStreamRequest(t *testing.T, r *http.Request) ingestStreamRequestPayload {
	t.Helper()
	require.Equal(t, http.MethodPost, r.Method)
	require.Equal(t, "/v1/ingest", r.URL.Path)
	body, err := decodeCompressedRequestBody(r)
	require.NoError(t, err)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(body, &fields))
	require.Len(t, fields, 2)
	require.Contains(t, fields, "data")
	require.Contains(t, fields, "statement")
	require.NotContains(t, fields, "type")
	var request ingestStreamRequestPayload
	require.NoError(t, json.Unmarshal(body, &request))
	require.Equal(t, "json", request.Data.Format)
	require.Equal(t, "SELECT $0 INSERT INTO events", request.Statement)
	return request
}

func writeIngestStreamResult(t *testing.T, w http.ResponseWriter, rows int) {
	t.Helper()
	writeTestJSON(t, w, `{"num_rows_inserted":`+strconv.Itoa(rows)+`}`)
}

func countNDJSONRows(rows string) int {
	count := 0
	for _, line := range strings.Split(rows, "\n") {
		if strings.TrimSpace(line) != "" {
			count++
		}
	}
	return count
}
