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
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAppendStreamBatchesByRowsAndBarrier(t *testing.T) {
	var mu sync.Mutex
	var bodies []string
	table := newAppendStreamTestTable(t, func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		mu.Lock()
		bodies = append(bodies, string(body))
		mu.Unlock()
		writeAppendStreamSuccess(t, w, countAppendRows(body))
	})
	stream, err := table.AppendStream(AppendStreamOptions{
		MaxBatchRows:         2,
		FlushInterval:        time.Hour,
		MaxConcurrentBatches: 1,
	})
	require.NoError(t, err)

	for id := 1; id <= 3; id++ {
		require.NoError(t, stream.Send(context.Background(), map[string]any{"id": id}))
	}
	report, err := stream.Shutdown(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(3), report.AcceptedRows)
	require.Equal(t, uint64(3), report.CommittedRows)
	require.Equal(t, uint64(2), report.CommittedBatches)
	require.Equal(t, AppendDeliveryOK, report.Outcome)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{
		"{\"id\":1}\n{\"id\":2}",
		"{\"id\":3}",
	}, bodies)
}

func TestAppendStreamSerializesTypedStructRows(t *testing.T) {
	type event struct {
		EventID    string    `json:"event_id"`
		OccurredAt time.Time `json:"occurred_at"`
		Name       string    `json:"name"`
		Ignored    string    `json:"-"`
	}

	received := make(chan string, 1)
	table := newAppendStreamTestTable(t, func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		received <- string(body)
		writeAppendStreamSuccess(t, w, countAppendRows(body))
	})
	stream, err := table.AppendStream(AppendStreamOptions{
		MaxBatchRows:         2,
		FlushInterval:        time.Hour,
		MaxConcurrentBatches: 1,
	})
	require.NoError(t, err)

	occurredAt := time.Date(2026, time.August, 8, 12, 34, 56, 123456789, time.UTC)
	first := event{
		EventID:    "evt-1",
		OccurredAt: occurredAt,
		Name:       "checkout.completed",
		Ignored:    "not-on-the-wire",
	}
	second := first
	second.EventID = "evt-2"
	require.NoError(t, stream.Send(context.Background(), first))
	require.NoError(t, stream.Send(context.Background(), &second))

	report, err := stream.Shutdown(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(2), report.CommittedRows)
	require.Equal(t,
		"{\"event_id\":\"evt-1\",\"occurred_at\":\"2026-08-08T12:34:56.123456789Z\",\"name\":\"checkout.completed\"}\n"+
			"{\"event_id\":\"evt-2\",\"occurred_at\":\"2026-08-08T12:34:56.123456789Z\",\"name\":\"checkout.completed\"}",
		<-received,
	)
}

func TestAppendStreamFlushInterval(t *testing.T) {
	requestStarted := make(chan struct{}, 1)
	table := newAppendStreamTestTable(t, func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		requestStarted <- struct{}{}
		writeAppendStreamSuccess(t, w, countAppendRows(body))
	})
	stream, err := table.AppendStream(AppendStreamOptions{
		FlushInterval: 10 * time.Millisecond,
	})
	require.NoError(t, err)
	require.NoError(t, stream.Send(context.Background(), map[string]int{"id": 1}))
	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		t.Fatal("timer did not dispatch the buffered row")
	}
	report, err := stream.Shutdown(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(1), report.CommittedRows)
}

func TestAppendStreamBoundsConcurrency(t *testing.T) {
	started := make(chan struct{}, 8)
	release := make(chan struct{})
	var inFlight atomic.Int64
	var maximum atomic.Int64
	table := newAppendStreamTestTable(t, func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		current := inFlight.Add(1)
		for {
			seen := maximum.Load()
			if current <= seen || maximum.CompareAndSwap(seen, current) {
				break
			}
		}
		started <- struct{}{}
		<-release
		inFlight.Add(-1)
		writeAppendStreamSuccess(t, w, countAppendRows(body))
	})
	stream, err := table.AppendStream(AppendStreamOptions{
		TargetBatchBytes:     1,
		FlushInterval:        time.Hour,
		MaxConcurrentBatches: 2,
	})
	require.NoError(t, err)
	for id := 0; id < 4; id++ {
		require.NoError(t, stream.Send(context.Background(), map[string]int{"id": id}))
	}
	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("expected two concurrent append requests")
		}
	}
	select {
	case <-started:
		t.Fatal("started more than two concurrent append requests")
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	report, err := stream.Shutdown(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(4), report.CommittedRows)
	require.Equal(t, int64(2), maximum.Load())
}

func TestAppendStreamBackpressureAndTrySend(t *testing.T) {
	requestStarted := make(chan struct{}, 1)
	release := make(chan struct{})
	table := newAppendStreamTestTable(t, func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		requestStarted <- struct{}{}
		<-release
		writeAppendStreamSuccess(t, w, countAppendRows(body))
	})
	payload, err := marshalAppendRecord(map[string]int{"id": 1})
	require.NoError(t, err)
	stream, err := table.AppendStream(AppendStreamOptions{
		TargetBatchBytes:     1,
		FlushInterval:        time.Hour,
		MaxBufferedBytes:     len(payload) + 1,
		MaxConcurrentBatches: 1,
	})
	require.NoError(t, err)
	require.NoError(t, stream.Send(context.Background(), map[string]int{"id": 1}))
	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		t.Fatal("append request did not start")
	}
	require.ErrorIs(t, stream.TrySend(map[string]int{"id": 2}), ErrAppendStreamFull)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, stream.Send(ctx, map[string]int{"id": 2}), context.DeadlineExceeded)
	stats := stream.Stats()
	require.Equal(t, uint64(1), stats.AcceptedRows)
	require.Equal(t, uint64(1), stats.DroppedByReason.BufferFull)
	require.Equal(t, 1, stats.InFlightBatches)

	close(release)
	_, err = stream.Shutdown(context.Background())
	require.NoError(t, err)
}

func TestAppendStreamTrySendReportsConcurrentClose(t *testing.T) {
	table := newAppendStreamTestTable(t, func(http.ResponseWriter, *http.Request) {
		t.Fatal("a row rejected during shutdown must not reach the server")
	})
	stream, err := table.AppendStream(AppendStreamOptions{})
	require.NoError(t, err)
	row := blockingAppendStreamRow{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	result := make(chan error, 1)
	go func() {
		result <- stream.TrySend(row)
	}()
	<-row.started

	_, err = stream.Shutdown(context.Background())
	require.NoError(t, err)
	close(row.release)
	require.ErrorIs(t, <-result, ErrAppendStreamClosed)
	stats := stream.Stats()
	require.Equal(t, uint64(1), stats.DroppedByReason.Closed)
	require.Zero(t, stats.DroppedByReason.BufferFull)
}

func TestAppendStreamRejectsNonObjectRows(t *testing.T) {
	table := newAppendStreamTestTable(t, func(_ http.ResponseWriter, _ *http.Request) {
		t.Fatal("invalid rows must not reach the server")
	})
	stream, err := table.AppendStream(AppendStreamOptions{})
	require.NoError(t, err)

	require.ErrorIs(t, stream.TrySend([]int{1}), ErrAppendRowInvalid)
	require.ErrorIs(t, stream.TrySend(nil), ErrAppendRowInvalid)
	sendErr := stream.Send(context.Background(), "scalar")
	require.ErrorIs(t, sendErr, ErrAppendRowInvalid)
	var scopeErr *Error
	require.ErrorAs(t, sendErr, &scopeErr)
	require.Equal(t, ErrorKindAppendRowsFailed, scopeErr.Kind)

	tooLargeRow := map[string]string{"payload": strings.Repeat("x", maxAppendStreamBodyBytes)}
	require.ErrorIs(t, stream.TrySend(tooLargeRow), ErrAppendRowTooLarge)
	tooLargeErr := stream.Send(context.Background(), tooLargeRow)
	require.ErrorIs(t, tooLargeErr, ErrAppendRowTooLarge)
	require.ErrorAs(t, tooLargeErr, &scopeErr)
	require.Equal(t, ErrorKindAppendRowsFailed, scopeErr.Kind)

	report, err := stream.Shutdown(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(0), report.AcceptedRows)
	require.Equal(t, uint64(3), report.DroppedRows)
	require.Equal(t, AppendDeliveryFailed, report.Outcome)
	require.Equal(t, uint64(2), stream.Stats().DroppedByReason.InvalidRow)
	require.Equal(t, uint64(1), stream.Stats().DroppedByReason.RowTooLarge)
}

func TestAppendStreamFlushIsPrefixBarrier(t *testing.T) {
	requests := make(chan string, 2)
	releaseFirst := make(chan struct{})
	var call atomic.Int64
	table := newAppendStreamTestTable(t, func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		requests <- string(body)
		if call.Add(1) == 1 {
			<-releaseFirst
		}
		writeAppendStreamSuccess(t, w, countAppendRows(body))
	})
	stream, err := table.AppendStream(AppendStreamOptions{
		TargetBatchBytes:     1,
		MaxConcurrentBatches: 1,
	})
	require.NoError(t, err)
	require.NoError(t, stream.Send(context.Background(), map[string]int{"id": 1}))
	select {
	case <-requests:
	case <-time.After(time.Second):
		t.Fatal("first append did not start")
	}

	stream.commandSlots <- struct{}{}
	stream.admissionMu.Lock()
	barrier := &appendBarrier{startedAt: time.Now(), ack: make(chan appendBarrierResult, 1)}
	stream.shared.mu.Lock()
	barrier.acceptedRows = stream.shared.counters.acceptedRows
	barrier.droppedRows = stream.shared.counters.droppedRows
	stream.shared.mu.Unlock()
	stream.commands <- appendCommand{barrier: barrier}
	stream.admissionMu.Unlock()
	require.NoError(t, stream.Send(context.Background(), map[string]int{"id": 2}))
	close(releaseFirst)

	first := <-barrier.ack
	require.NoError(t, first.err)
	require.Equal(t, uint64(1), first.report.AcceptedRows)
	require.Equal(t, uint64(1), first.report.CommittedRows)
	select {
	case <-requests:
		t.Fatal("a row admitted after the barrier was dispatched before it completed")
	default:
	}

	final, err := stream.Shutdown(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(1), final.AcceptedRows)
	require.Equal(t, uint64(1), final.CommittedRows)
	select {
	case body := <-requests:
		require.Equal(t, "{\"id\":2}", body)
	case <-time.After(time.Second):
		t.Fatal("second append did not run after the barrier")
	}
}

func TestAppendStreamStopFailureKeepsBarrierPrefix(t *testing.T) {
	requestStarted := make(chan struct{}, 1)
	releaseFirst := make(chan struct{})
	var calls atomic.Int64
	table := newAppendStreamTestTable(t, func(w http.ResponseWriter, r *http.Request) {
		_, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		calls.Add(1)
		requestStarted <- struct{}{}
		<-releaseFirst
		writeAppendStreamFailure(t, w, http.StatusUnprocessableEntity, AppendStateRejected, false)
	})
	stream, err := table.AppendStream(AppendStreamOptions{
		TargetBatchBytes:     1,
		MaxConcurrentBatches: 1,
	})
	require.NoError(t, err)
	require.NoError(t, stream.Send(context.Background(), map[string]int{"id": 1}))
	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		t.Fatal("first append did not start")
	}

	stream.commandSlots <- struct{}{}
	stream.admissionMu.Lock()
	barrier := &appendBarrier{startedAt: time.Now(), ack: make(chan appendBarrierResult, 1)}
	stream.shared.mu.Lock()
	barrier.acceptedRows = stream.shared.counters.acceptedRows
	barrier.droppedRows = stream.shared.counters.droppedRows
	stream.shared.mu.Unlock()
	stream.commands <- appendCommand{barrier: barrier}
	stream.admissionMu.Unlock()
	require.NoError(t, stream.Send(context.Background(), map[string]int{"id": 2}))
	close(releaseFirst)

	first := <-barrier.ack
	require.Error(t, first.err)
	require.Equal(t, uint64(1), first.report.AcceptedRows)
	require.Equal(t, uint64(1), first.report.FailedRows)

	final, err := stream.Shutdown(context.Background())
	require.Error(t, err)
	require.Equal(t, uint64(1), final.AcceptedRows)
	require.Equal(t, uint64(1), final.FailedRows)
	require.Equal(t, int64(1), calls.Load())
}

func TestAppendStreamFailurePolicies(t *testing.T) {
	t.Run("stop", func(t *testing.T) {
		var calls atomic.Int64
		table := newAppendStreamTestTable(t, func(w http.ResponseWriter, _ *http.Request) {
			calls.Add(1)
			writeAppendStreamFailure(t, w, http.StatusUnprocessableEntity, AppendStateRejected, false)
		})
		stream, err := table.AppendStream(AppendStreamOptions{
			TargetBatchBytes:     1,
			MaxConcurrentBatches: 1,
		})
		require.NoError(t, err)
		require.NoError(t, stream.Send(context.Background(), map[string]int{"id": 1}))
		require.NoError(t, stream.Send(context.Background(), map[string]int{"id": 2}))
		report, err := stream.Shutdown(context.Background())
		require.Error(t, err)
		require.Equal(t, AppendDeliveryFailed, report.Outcome)
		require.Equal(t, uint64(2), report.AcceptedRows)
		require.Equal(t, uint64(2), report.FailedRows)
		require.Equal(t, int64(1), calls.Load())
		require.Error(t, stream.TrySend(map[string]int{"id": 3}))
		require.Equal(t, AppendStreamFailed, stream.Stats().State)
	})

	t.Run("continue", func(t *testing.T) {
		var calls atomic.Int64
		table := newAppendStreamTestTable(t, func(w http.ResponseWriter, r *http.Request) {
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			if calls.Add(1) == 1 {
				writeAppendStreamFailure(t, w, http.StatusUnprocessableEntity, AppendStateRejected, false)
				return
			}
			writeAppendStreamSuccess(t, w, countAppendRows(body))
		})
		stream, err := table.AppendStream(AppendStreamOptions{
			FailurePolicy:        AppendFailureContinue,
			TargetBatchBytes:     1,
			MaxConcurrentBatches: 1,
		})
		require.NoError(t, err)
		require.NoError(t, stream.Send(context.Background(), map[string]int{"id": 1}))
		require.NoError(t, stream.Send(context.Background(), map[string]int{"id": 2}))
		report, err := stream.Shutdown(context.Background())
		require.NoError(t, err)
		require.Equal(t, AppendDeliveryPartial, report.Outcome)
		require.Equal(t, uint64(2), report.AcceptedRows)
		require.Equal(t, uint64(1), report.CommittedRows)
		require.Equal(t, uint64(1), report.FailedRows)
		require.Equal(t, AppendStreamClosed, stream.Stats().State)
	})
}

func TestAppendStreamContinuePreservesStructuredLastFailure(t *testing.T) {
	var calls atomic.Int64
	table := newAppendStreamTestTable(t, func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		if calls.Add(1) == 1 {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Request-ID", "append-request-123")
			w.Header().Set("Retry-After", "2")
			w.WriteHeader(http.StatusUnprocessableEntity)
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
				"message":      "invalid timestamp",
				"append_state": AppendStateRejected,
				"retryable":    false,
				"row_errors": []map[string]any{{
					"row_index": 0,
					"column":    "occurred_at",
					"message":   "invalid timestamp",
				}},
				"row_errors_truncated": true,
			}))
			return
		}
		writeAppendStreamSuccess(t, w, countAppendRows(body))
	})
	stream, err := table.AppendStream(AppendStreamOptions{
		FailurePolicy:        AppendFailureContinue,
		TargetBatchBytes:     1,
		MaxConcurrentBatches: 1,
	})
	require.NoError(t, err)
	require.NoError(t, stream.Send(context.Background(), map[string]int{"id": 1}))
	require.NoError(t, stream.Send(context.Background(), map[string]int{"id": 2}))

	report, err := stream.Shutdown(context.Background())
	require.NoError(t, err)
	require.Equal(t, AppendDeliveryPartial, report.Outcome)
	failure := stream.Stats().LastFailure
	require.NotNil(t, failure)
	require.Equal(t, "invalid timestamp", failure.Message)
	require.Equal(t, AppendStateRejected, failure.AppendState)
	require.Equal(t, http.StatusUnprocessableEntity, failure.HTTPStatus)
	require.Equal(t, "append-request-123", failure.RequestID)
	require.Equal(t, 2*time.Second, failure.RetryAfter)
	require.False(t, failure.Retryable)
	require.True(t, failure.RowErrorsTruncated)
	require.Equal(t, []AppendRowError{{
		RowIndex: 0,
		Column:   "occurred_at",
		Message:  "invalid timestamp",
	}}, failure.RowErrors)

	// Stats snapshots must not share caller-mutable row-error storage.
	failure.RowErrors[0].Message = "mutated"
	require.Equal(t, "invalid timestamp", stream.Stats().LastFailure.RowErrors[0].Message)
}

func TestAppendStreamRetriesOnlyRejectedTemporaryBatches(t *testing.T) {
	t.Run("rejected temporary", func(t *testing.T) {
		var calls atomic.Int64
		table := newAppendStreamTestTable(t, func(w http.ResponseWriter, r *http.Request) {
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			if calls.Add(1) == 1 {
				writeAppendStreamFailure(t, w, http.StatusServiceUnavailable, AppendStateRejected, true)
				return
			}
			writeAppendStreamSuccess(t, w, countAppendRows(body))
		})
		stream, err := table.AppendStream(AppendStreamOptions{TargetBatchBytes: 1})
		require.NoError(t, err)
		require.NoError(t, stream.Send(context.Background(), map[string]int{"id": 1}))
		report, err := stream.Shutdown(context.Background())
		require.NoError(t, err)
		require.Equal(t, int64(2), calls.Load())
		require.Equal(t, uint64(1), report.Retries)
		require.Equal(t, uint64(1), report.CommittedRows)
	})

	t.Run("unknown", func(t *testing.T) {
		var calls atomic.Int64
		table := newAppendStreamTestTable(t, func(w http.ResponseWriter, _ *http.Request) {
			calls.Add(1)
			writeAppendStreamFailure(t, w, http.StatusServiceUnavailable, AppendStateUnknown, true)
		})
		stream, err := table.AppendStream(AppendStreamOptions{
			FailurePolicy:    AppendFailureContinue,
			TargetBatchBytes: 1,
		})
		require.NoError(t, err)
		require.NoError(t, stream.Send(context.Background(), map[string]int{"id": 1}))
		report, err := stream.Shutdown(context.Background())
		require.NoError(t, err)
		require.Equal(t, int64(1), calls.Load())
		require.Zero(t, report.Retries)
		require.Equal(t, uint64(1), report.UnknownRows)
		require.Equal(t, AppendDeliveryUnknown, report.Outcome)
	})

	t.Run("attempt timeout", func(t *testing.T) {
		var calls atomic.Int64
		table := newAppendStreamTestTable(t, func(w http.ResponseWriter, _ *http.Request) {
			calls.Add(1)
			time.Sleep(50 * time.Millisecond)
			writeAppendStreamSuccess(t, w, 1)
		})
		stream, err := table.AppendStream(AppendStreamOptions{
			FailurePolicy:    AppendFailureContinue,
			TargetBatchBytes: 1,
			AttemptTimeout:   10 * time.Millisecond,
		})
		require.NoError(t, err)
		require.NoError(t, stream.Send(context.Background(), map[string]int{"id": 1}))
		report, err := stream.Shutdown(context.Background())
		require.NoError(t, err)
		require.Equal(t, int64(1), calls.Load())
		require.Zero(t, report.Retries)
		require.Equal(t, uint64(1), report.UnknownRows)
	})
}

func TestAppendStreamStopPreservesUnknownAsPrimaryFailure(t *testing.T) {
	var started atomic.Int64
	bothStarted := make(chan struct{})
	table := newAppendStreamTestTable(t, func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		if started.Add(1) == 2 {
			close(bothStarted)
		}
		<-bothStarted
		if strings.Contains(string(body), "\"id\":1") {
			writeAppendStreamFailure(t, w, http.StatusUnprocessableEntity, AppendStateRejected, false)
			return
		}
		writeAppendStreamFailure(t, w, http.StatusServiceUnavailable, AppendStateUnknown, true)
	})
	stream, err := table.AppendStream(AppendStreamOptions{
		TargetBatchBytes:     1,
		MaxConcurrentBatches: 2,
	})
	require.NoError(t, err)
	require.NoError(t, stream.Send(context.Background(), map[string]int{"id": 1}))
	require.NoError(t, stream.Send(context.Background(), map[string]int{"id": 2}))
	report, err := stream.Shutdown(context.Background())
	require.Error(t, err)
	require.Equal(t, AppendStateUnknown, appendErrorState(err))
	require.Equal(t, uint64(2), report.AcceptedRows)
	require.Equal(t, uint64(1), report.FailedRows)
	require.Equal(t, uint64(1), report.UnknownRows)
	require.Equal(t, AppendDeliveryUnknown, report.Outcome)
}

func TestAppendStreamShutdownContextAndIdempotency(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	table := newAppendStreamTestTable(t, func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		started <- struct{}{}
		<-release
		writeAppendStreamSuccess(t, w, countAppendRows(body))
	})
	stream, err := table.AppendStream(AppendStreamOptions{TargetBatchBytes: 1})
	require.NoError(t, err)
	require.NoError(t, stream.Send(context.Background(), map[string]int{"id": 1}))
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("append did not start")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	report, err := stream.Shutdown(ctx)
	require.Zero(t, report)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, AppendStreamClosing, stream.Stats().State)
	require.Error(t, stream.TrySend(map[string]int{"id": 2}))

	close(release)
	first, err := stream.Shutdown(context.Background())
	require.NoError(t, err)
	second, err := stream.Shutdown(context.Background())
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.Equal(t, uint64(1), first.CommittedRows)

	canceled, cancelAgain := context.WithCancel(context.Background())
	cancelAgain()
	terminal, err := stream.Shutdown(canceled)
	require.NoError(t, err)
	require.Equal(t, first, terminal)
}

func TestAppendStreamShutdownLinearizesWithSend(t *testing.T) {
	table := newAppendStreamTestTable(t, func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		writeAppendStreamSuccess(t, w, countAppendRows(body))
	})
	stream, err := table.AppendStream(AppendStreamOptions{
		TargetBatchBytes:     1,
		MaxConcurrentBatches: 4,
	})
	require.NoError(t, err)

	stream.admissionMu.Lock()
	const producerCount = 64
	sendResults := make(chan error, producerCount)
	for id := range producerCount {
		go func() {
			sendResults <- stream.Send(context.Background(), map[string]int{"id": id})
		}()
	}
	type shutdownResult struct {
		report AppendDeliveryReport
		err    error
	}
	shutdownResults := make(chan shutdownResult, 1)
	go func() {
		report, err := stream.Shutdown(context.Background())
		shutdownResults <- shutdownResult{report: report, err: err}
	}()
	stream.admissionMu.Unlock()

	var accepted uint64
	for range producerCount {
		err := <-sendResults
		if err == nil {
			accepted++
			continue
		}
		require.ErrorIs(t, err, ErrAppendStreamClosed)
	}
	result := <-shutdownResults
	require.NoError(t, result.err)
	require.Equal(t, accepted, result.report.AcceptedRows)
	require.Equal(t, accepted, result.report.CommittedRows)
	require.Equal(t, AppendStreamClosed, stream.Stats().State)
}

func TestAppendStreamCanceledFlushKeepsSettling(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	table := newAppendStreamTestTable(t, func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		started <- struct{}{}
		<-release
		writeAppendStreamSuccess(t, w, countAppendRows(body))
	})
	stream, err := table.AppendStream(AppendStreamOptions{TargetBatchBytes: 1})
	require.NoError(t, err)
	require.NoError(t, stream.Send(context.Background(), map[string]int{"id": 1}))
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("append did not start")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	report, err := stream.Flush(ctx)
	require.Zero(t, report)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	close(release)
	require.Eventually(t, func() bool {
		stats := stream.Stats()
		return stats.LastReport != nil && stats.LastReport.CommittedRows == 1
	}, time.Second, time.Millisecond)

	final, err := stream.Shutdown(context.Background())
	require.NoError(t, err)
	require.Zero(t, final.AcceptedRows)
	require.Equal(t, uint64(1), stream.Stats().CommittedRows)
}

func TestAppendStreamSnapshotsDestinationAndHonorsPreCanceledSend(t *testing.T) {
	var pathsMu sync.Mutex
	var paths []string
	table := newAppendStreamTestTable(t, func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		pathsMu.Lock()
		paths = append(paths, r.URL.EscapedPath())
		pathsMu.Unlock()
		writeAppendStreamSuccess(t, w, countAppendRows(body))
	})
	table.Database = "before/db"
	table.Schema = "before schema"
	stream, err := table.AppendStream(AppendStreamOptions{TargetBatchBytes: 1})
	require.NoError(t, err)
	table.Database = "after"
	table.Schema = "after"
	table.Name = "after"

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, stream.Send(canceled, map[string]int{"id": 0}), context.Canceled)
	require.Zero(t, stream.Stats().AcceptedRows)

	require.NoError(t, stream.Send(context.Background(), map[string]int{"id": 1}))
	_, err = stream.Shutdown(context.Background())
	require.NoError(t, err)
	pathsMu.Lock()
	defer pathsMu.Unlock()
	require.Len(t, paths, 1)
	require.Contains(t, paths[0], "/databases/before%2Fdb/schemas/before%20schema/tables/events/rows")
}

func TestAppendStreamConcurrentProducersAndStats(t *testing.T) {
	table := newAppendStreamTestTable(t, func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		writeAppendStreamSuccess(t, w, countAppendRows(body))
	})
	stream, err := table.AppendStream(AppendStreamOptions{
		TargetBatchBytes:     64,
		MaxConcurrentBatches: 4,
	})
	require.NoError(t, err)

	var producers sync.WaitGroup
	for id := range 100 {
		producers.Add(1)
		go func() {
			defer producers.Done()
			require.NoError(t, stream.Send(context.Background(), map[string]int{"id": id}))
		}()
	}
	statsDone := make(chan struct{})
	go func() {
		defer close(statsDone)
		for stream.Stats().AcceptedRows < 100 {
			time.Sleep(time.Microsecond)
		}
	}()
	producers.Wait()
	<-statsDone
	report, err := stream.Shutdown(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(100), report.AcceptedRows)
	require.Equal(t, uint64(100), report.CommittedRows)
	stats := stream.Stats()
	require.Zero(t, stats.PendingRows)
	require.Zero(t, stats.PendingBytes)
}

func TestAppendStreamOptionValidation(t *testing.T) {
	table := &Table{}
	invalid := []AppendStreamOptions{
		{FailurePolicy: AppendFailurePolicy(9)},
		{TargetBatchBytes: -1},
		{TargetBatchBytes: maxAppendStreamBodyBytes + 1},
		{MaxBatchRows: -1},
		{MaxBatchRows: maxAppendRows + 1},
		{FlushInterval: -1},
		{MaxBufferedBytes: -1},
		{MaxConcurrentBatches: -1},
		{MaxConcurrentBatches: maxAppendConcurrentBatches + 1},
		{AttemptTimeout: -1},
	}
	for _, options := range invalid {
		_, err := table.AppendStream(options)
		require.Error(t, err)
		var scopeErr *Error
		require.ErrorAs(t, err, &scopeErr)
		require.Equal(t, ErrorKindConfigInvalid, scopeErr.Kind)
	}
	defaults, err := normalizeAppendStreamOptions(AppendStreamOptions{})
	require.NoError(t, err)
	require.Equal(t, 8*1024*1024, defaults.targetBatchBytes)
	require.Equal(t, 64*1024*1024, defaults.maxBufferedBytes)
	require.Equal(t, defaultAppendAttemptTimeout, defaults.attemptTimeout)
	require.Equal(t, "stop", AppendFailureStop.String())
	require.Equal(t, "continue", AppendFailureContinue.String())
	require.Equal(t, "open", AppendStreamOpen.String())
	require.Equal(t, "closing", AppendStreamClosing.String())
	require.Equal(t, "closed", AppendStreamClosed.String())
	require.Equal(t, "failed", AppendStreamFailed.String())
	require.Equal(t, "ok", AppendDeliveryOK.String())
	require.Equal(t, "partial", AppendDeliveryPartial.String())
	require.Equal(t, "failed", AppendDeliveryFailed.String())
	require.Equal(t, "unknown", AppendDeliveryUnknown.String())
	require.Equal(t, 2*time.Second, appendRetryDelay(time.Second, 2*time.Second))
	require.Equal(t, defaultAppendMaxBackoff, appendRetryDelay(time.Second, time.Hour))
}

func newAppendStreamTestTable(t *testing.T, handler http.HandlerFunc) *Table {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, string(CompressionZstd), r.Header.Get("Content-Encoding"))
		body, err := decodeCompressedRequestBody(r)
		require.NoError(t, err)
		r.Body = io.NopCloser(bytes.NewReader(body))
		handler(w, r)
	}))
	t.Cleanup(server.Close)
	client, err := NewClient(Config{Endpoint: server.URL})
	require.NoError(t, err)
	t.Cleanup(client.Close)
	return client.Table("events")
}

func writeAppendStreamSuccess(t *testing.T, w http.ResponseWriter, rows int) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	require.NoError(t, json.NewEncoder(w).Encode(AppendRowsResult{
		AppendState:     AppendStateCommitted,
		NumRowsInserted: int64(rows),
	}))
}

func writeAppendStreamFailure(
	t *testing.T,
	w http.ResponseWriter,
	status int,
	state AppendState,
	retryable bool,
) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
		"message":              "append failed",
		"append_state":         state,
		"row_errors":           []any{},
		"row_errors_truncated": false,
		"retryable":            retryable,
	}))
}

type blockingAppendStreamRow struct {
	started chan struct{}
	release chan struct{}
}

func (row blockingAppendStreamRow) MarshalJSON() ([]byte, error) {
	close(row.started)
	<-row.release
	return []byte(`{"id":1}`), nil
}
