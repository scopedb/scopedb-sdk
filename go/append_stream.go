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
	"fmt"
	"slices"
	"sync"
	"time"
)

const (
	defaultAppendTargetBatchBytes     = maxAppendBodyBytes
	defaultAppendMaxBatchRows         = maxAppendRows
	defaultAppendFlushInterval        = time.Second
	defaultAppendCommandCapacity      = 1024
	defaultAppendMaxBufferedBytes     = maxAppendBodyBytes * 4
	defaultAppendMaxConcurrentBatches = 4
	maxAppendConcurrentBatches        = 1024
	defaultAppendMaxRetries           = 8
	defaultAppendInitialBackoff       = 100 * time.Millisecond
	defaultAppendMaxBackoff           = 5 * time.Second
	defaultAppendAttemptTimeout       = 30 * time.Second
)

var (
	// ErrAppendStreamFull means TrySend could not reserve bounded local capacity.
	ErrAppendStreamFull = errors.New("append stream buffer is full")
	// ErrAppendStreamClosed means the stream no longer accepts rows.
	ErrAppendStreamClosed = errors.New("append stream is closed")
	// ErrAppendRowInvalid means a row did not encode to one JSON object.
	ErrAppendRowInvalid = errors.New("append row must encode to a JSON object")
	// ErrAppendRowTooLarge means one encoded row exceeds the append protocol limit.
	ErrAppendRowTooLarge = errors.New("append row exceeds the protocol limit")
)

// AppendFailurePolicy controls what a stream does after a remote batch fails.
type AppendFailurePolicy uint8

const (
	// AppendFailureStop stops admission after the first failed batch.
	AppendFailureStop AppendFailurePolicy = iota
	// AppendFailureContinue accounts for the failed batch and processes later rows.
	AppendFailureContinue
)

func (p AppendFailurePolicy) String() string {
	switch p {
	case AppendFailureStop:
		return "stop"
	case AppendFailureContinue:
		return "continue"
	default:
		return fmt.Sprintf("AppendFailurePolicy(%d)", p)
	}
}

// AppendStreamState is the lifecycle state of an AppendStream.
type AppendStreamState uint8

const (
	// AppendStreamOpen means the stream accepts rows.
	AppendStreamOpen AppendStreamState = iota
	// AppendStreamClosing means shutdown has closed admission and is settling rows.
	AppendStreamClosing
	// AppendStreamClosed means shutdown settled all accepted rows without a fatal error.
	AppendStreamClosed
	// AppendStreamFailed means strict delivery stopped after a batch failure.
	AppendStreamFailed
)

func (s AppendStreamState) String() string {
	switch s {
	case AppendStreamOpen:
		return "open"
	case AppendStreamClosing:
		return "closing"
	case AppendStreamClosed:
		return "closed"
	case AppendStreamFailed:
		return "failed"
	default:
		return fmt.Sprintf("AppendStreamState(%d)", s)
	}
}

// AppendDeliveryOutcome summarizes one completed delivery barrier.
type AppendDeliveryOutcome uint8

const (
	// AppendDeliveryOK means the interval settled without failed, unknown, or dropped rows.
	AppendDeliveryOK AppendDeliveryOutcome = iota
	// AppendDeliveryPartial means some rows committed and some did not settle cleanly.
	AppendDeliveryPartial
	// AppendDeliveryFailed means no rows committed and at least one row failed or dropped.
	AppendDeliveryFailed
	// AppendDeliveryUnknown means no rows committed and at least one row may have committed.
	AppendDeliveryUnknown
)

func (o AppendDeliveryOutcome) String() string {
	switch o {
	case AppendDeliveryOK:
		return "ok"
	case AppendDeliveryPartial:
		return "partial"
	case AppendDeliveryFailed:
		return "failed"
	case AppendDeliveryUnknown:
		return "unknown"
	default:
		return fmt.Sprintf("AppendDeliveryOutcome(%d)", o)
	}
}

// AppendStreamOptions configures bounded asynchronous table appends.
// Zero-valued fields use SDK defaults.
type AppendStreamOptions struct {
	// FailurePolicy defaults to AppendFailureStop.
	FailurePolicy AppendFailurePolicy
	// TargetBatchBytes defaults to 16 MiB and cannot exceed the request limit.
	TargetBatchBytes int
	// MaxBatchRows defaults to 200,000 and cannot exceed the request limit.
	MaxBatchRows int
	// FlushInterval defaults to one second.
	FlushInterval time.Duration
	// MaxBufferedBytes bounds admitted rows not yet settled. It defaults to 64 MiB.
	MaxBufferedBytes int
	// MaxConcurrentBatches defaults to four, cannot exceed 1024, and may be set
	// to one for serial request submission.
	MaxConcurrentBatches int
	// AttemptTimeout bounds each HTTP attempt. It defaults to 30 seconds.
	// A timeout leaves that batch's commit outcome unknown.
	AttemptTimeout time.Duration
}

// AppendDeliveryReport contains settlement counters since the previous barrier.
type AppendDeliveryReport struct {
	// Outcome summarizes delivery for this barrier interval.
	Outcome AppendDeliveryOutcome
	// AcceptedRows is the number of rows locally admitted in the interval.
	AcceptedRows uint64
	// CommittedRows is the number of rows confirmed committed.
	CommittedRows uint64
	// FailedRows is the number of rejected or unsent accepted rows.
	FailedRows uint64
	// UnknownRows is the number of rows whose commit outcome is unknown.
	UnknownRows uint64
	// DroppedRows is the number of TrySend calls rejected locally in the interval.
	DroppedRows      uint64
	CommittedBatches uint64
	FailedBatches    uint64
	UnknownBatches   uint64
	Retries          uint64
	Duration         time.Duration
}

// AppendDroppedRows contains lifetime TrySend failures by reason.
type AppendDroppedRows struct {
	// BufferFull counts rows rejected because immediate local admission was unavailable.
	BufferFull uint64
	// InvalidRow counts values that did not encode to a JSON object.
	InvalidRow uint64
	// RowTooLarge counts values over the single-request protocol limit.
	RowTooLarge uint64
	// Closed counts attempts after shutdown or a terminal failure.
	Closed uint64
}

// AppendLastFailure describes the last remotely failed batch.
type AppendLastFailure struct {
	// At is when the client observed the failure.
	At time.Time
	// Message is the original error message.
	Message string
	// AppendState is rejected or unknown.
	AppendState AppendState
	// HTTPStatus is zero when no HTTP response was available.
	HTTPStatus int
	// RequestID is the request identifier reported by the service, when present.
	RequestID string
	// RetryAfter is the service-provided retry delay, when present.
	RetryAfter time.Duration
	// Retryable reports the service's retry classification. AppendStream retries
	// only exact rejected batches, regardless of this field alone.
	Retryable bool
	// RowErrors contains structured validation failures reported for the batch.
	RowErrors []AppendRowError
	// RowErrorsTruncated reports whether the service omitted additional row errors.
	RowErrorsTruncated bool
}

// AppendStreamStats is a race-safe lifetime snapshot.
type AppendStreamStats struct {
	// State is the current stream lifecycle state.
	State           AppendStreamState
	AcceptedRows    uint64
	CommittedRows   uint64
	FailedRows      uint64
	UnknownRows     uint64
	DroppedRows     uint64
	DroppedByReason AppendDroppedRows
	Retries         uint64
	PendingRows     uint64
	PendingBytes    int
	InFlightBatches int
	// LastFailure is nil until a remote batch fails.
	LastFailure *AppendLastFailure
	// LastReport is nil until a delivery barrier completes.
	LastReport *AppendDeliveryReport
}

type normalizedAppendStreamOptions struct {
	failurePolicy        AppendFailurePolicy
	targetBatchBytes     int
	maxBatchRows         int
	flushInterval        time.Duration
	maxBufferedBytes     int
	maxConcurrentBatches int
	attemptTimeout       time.Duration
}

// AppendStream creates a bounded asynchronous append stream for this table.
func (t *Table) AppendStream(options AppendStreamOptions) (*AppendStream, error) {
	config, err := normalizeAppendStreamOptions(options)
	if err != nil {
		return nil, err
	}

	stream := &AppendStream{
		client:        t.c,
		database:      t.databaseName(),
		schema:        t.schemaName(),
		table:         t.Name,
		config:        config,
		commands:      make(chan appendCommand, defaultAppendCommandCapacity),
		commandSlots:  make(chan struct{}, defaultAppendCommandCapacity),
		results:       make(chan appendBatchResult, config.maxConcurrentBatches),
		budget:        newAppendByteBudget(config.maxBufferedBytes),
		admissionDone: make(chan struct{}),
		terminalDone:  make(chan struct{}),
	}
	stream.shared.state = AppendStreamOpen
	go stream.run()
	return stream, nil
}

func normalizeAppendStreamOptions(options AppendStreamOptions) (normalizedAppendStreamOptions, error) {
	config := normalizedAppendStreamOptions{
		failurePolicy:        options.FailurePolicy,
		targetBatchBytes:     options.TargetBatchBytes,
		maxBatchRows:         options.MaxBatchRows,
		flushInterval:        options.FlushInterval,
		maxBufferedBytes:     options.MaxBufferedBytes,
		maxConcurrentBatches: options.MaxConcurrentBatches,
		attemptTimeout:       options.AttemptTimeout,
	}
	if config.failurePolicy != AppendFailureStop && config.failurePolicy != AppendFailureContinue {
		return config, appendStreamConfigError("failure policy is invalid")
	}
	if config.targetBatchBytes == 0 {
		config.targetBatchBytes = defaultAppendTargetBatchBytes
	}
	if config.targetBatchBytes < 0 || config.targetBatchBytes > maxAppendBodyBytes {
		return config, appendStreamConfigError(
			fmt.Sprintf("target batch bytes must be from 1 to %d", maxAppendBodyBytes),
		)
	}
	if config.maxBatchRows == 0 {
		config.maxBatchRows = defaultAppendMaxBatchRows
	}
	if config.maxBatchRows < 0 || config.maxBatchRows > maxAppendRows {
		return config, appendStreamConfigError(
			fmt.Sprintf("max batch rows must be from 1 to %d", maxAppendRows),
		)
	}
	if config.flushInterval == 0 {
		config.flushInterval = defaultAppendFlushInterval
	}
	if config.flushInterval < 0 {
		return config, appendStreamConfigError("flush interval must be greater than zero")
	}
	if config.maxBufferedBytes == 0 {
		config.maxBufferedBytes = defaultAppendMaxBufferedBytes
	}
	if config.maxBufferedBytes < 0 {
		return config, appendStreamConfigError("max buffered bytes must be greater than zero")
	}
	if config.maxConcurrentBatches == 0 {
		config.maxConcurrentBatches = defaultAppendMaxConcurrentBatches
	}
	if config.maxConcurrentBatches < 0 || config.maxConcurrentBatches > maxAppendConcurrentBatches {
		return config, appendStreamConfigError(
			fmt.Sprintf("max concurrent batches must be from 1 to %d", maxAppendConcurrentBatches),
		)
	}
	if config.attemptTimeout < 0 {
		return config, appendStreamConfigError("attempt timeout must not be negative")
	}
	if config.attemptTimeout == 0 {
		config.attemptTimeout = defaultAppendAttemptTimeout
	}
	return config, nil
}

func appendStreamConfigError(message string) error {
	return newError(ErrorKindConfigInvalid, message, nil)
}

type appendCommand struct {
	record  *appendRecord
	barrier *appendBarrier
}

type appendRecord struct {
	payload       []byte
	reservedBytes int
}

type appendBarrier struct {
	shutdown     bool
	startedAt    time.Time
	acceptedRows uint64
	droppedRows  uint64
	ack          chan appendBarrierResult
}

type appendBarrierResult struct {
	report AppendDeliveryReport
	err    error
}

type appendBatch struct {
	payload       []byte
	rows          int
	reservedBytes int
}

type appendBatchResult struct {
	batch   appendBatch
	result  AppendRowsResult
	err     error
	retries uint64
}

type appendCounters struct {
	acceptedRows     uint64
	committedRows    uint64
	failedRows       uint64
	unknownRows      uint64
	droppedRows      uint64
	committedBatches uint64
	failedBatches    uint64
	unknownBatches   uint64
	retries          uint64
}

type appendSharedState struct {
	mu sync.Mutex

	state           AppendStreamState
	counters        appendCounters
	droppedByReason AppendDroppedRows
	pendingRows     uint64
	pendingBytes    int
	inFlightBatches int
	lastFailure     *AppendLastFailure
	lastReport      *AppendDeliveryReport
	fatal           error
	terminalReport  AppendDeliveryReport
	hasTerminal     bool
}

// AppendStream asynchronously batches rows into bounded NDJSON append requests.
// Send and TrySend are safe to call from concurrent producer goroutines.
type AppendStream struct {
	client   *Client
	database string
	schema   string
	table    string
	config   normalizedAppendStreamOptions

	commands     chan appendCommand
	commandSlots chan struct{}
	results      chan appendBatchResult
	budget       *appendByteBudget

	admissionMu   sync.Mutex
	admissionDone chan struct{}
	admissionOnce sync.Once
	terminalDone  chan struct{}
	terminalOnce  sync.Once

	shared appendSharedState
}

// Send serializes row with encoding/json and admits it, waiting for bounded
// local capacity. Row may be a typed struct or any other value that encodes as
// one top-level JSON object. Success does not validate the destination table
// schema or confirm a remote commit.
func (s *AppendStream) Send(ctx context.Context, row any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.checkOpen(); err != nil {
		return err
	}
	payload, err := marshalAppendRecord(row)
	if err != nil {
		return newError(ErrorKindAppendRowsFailed, err.Error(), err)
	}
	reservedBytes := len(payload) + 1
	if reservedBytes > s.config.maxBufferedBytes {
		return newError(
			ErrorKindAppendRowsFailed,
			fmt.Sprintf("append row needs %d buffered bytes, exceeding max buffered bytes %d", reservedBytes, s.config.maxBufferedBytes),
			ErrAppendStreamFull,
		)
	}
	if err := s.budget.acquire(ctx, reservedBytes); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		return s.closedOrFatalError()
	}
	select {
	case s.commandSlots <- struct{}{}:
	case <-ctx.Done():
		s.budget.release(reservedBytes)
		return ctx.Err()
	case <-s.admissionDone:
		s.budget.release(reservedBytes)
		return s.closedOrFatalError()
	}

	s.admissionMu.Lock()
	defer s.admissionMu.Unlock()
	if err := s.checkOpen(); err != nil {
		s.releaseCommandSlot()
		s.budget.release(reservedBytes)
		return err
	}
	s.noteAccepted(reservedBytes)
	s.commands <- appendCommand{record: &appendRecord{
		payload:       payload,
		reservedBytes: reservedBytes,
	}}
	return nil
}

// TrySend serializes row with encoding/json and attempts local admission
// without waiting for capacity. Row must encode as one top-level JSON object.
// Success does not validate the destination table schema or confirm a remote
// commit.
func (s *AppendStream) TrySend(row any) error {
	if err := s.checkOpen(); err != nil {
		s.noteDrop(appendDropClosed)
		return err
	}
	payload, err := marshalAppendRecord(row)
	if err != nil {
		if errors.Is(err, ErrAppendRowTooLarge) {
			s.noteDrop(appendDropRowTooLarge)
		} else {
			s.noteDrop(appendDropInvalidRow)
		}
		return err
	}
	reservedBytes := len(payload) + 1
	if reservedBytes > s.config.maxBufferedBytes || !s.budget.tryAcquire(reservedBytes) {
		return s.trySendCapacityError()
	}
	select {
	case s.commandSlots <- struct{}{}:
	default:
		s.budget.release(reservedBytes)
		return s.trySendCapacityError()
	}
	if !s.admissionMu.TryLock() {
		s.releaseCommandSlot()
		s.budget.release(reservedBytes)
		return s.trySendCapacityError()
	}
	defer s.admissionMu.Unlock()
	if err := s.checkOpen(); err != nil {
		s.releaseCommandSlot()
		s.budget.release(reservedBytes)
		s.noteDrop(appendDropClosed)
		return err
	}
	s.noteAccepted(reservedBytes)
	s.commands <- appendCommand{record: &appendRecord{
		payload:       payload,
		reservedBytes: reservedBytes,
	}}
	return nil
}

func (s *AppendStream) trySendCapacityError() error {
	if err := s.checkOpen(); err != nil {
		s.noteDrop(appendDropClosed)
		return err
	}
	s.noteDrop(appendDropBufferFull)
	return ErrAppendStreamFull
}

// Flush dispatches all rows admitted before this barrier and waits for settlement.
// Canceling ctx stops only this wait; an already enqueued barrier keeps running.
// If ctx ends first, Flush returns a zero report and ctx.Err().
func (s *AppendStream) Flush(ctx context.Context) (AppendDeliveryReport, error) {
	select {
	case <-s.terminalDone:
		return s.waitTerminal(ctx)
	default:
	}
	if err := ctx.Err(); err != nil {
		return AppendDeliveryReport{}, err
	}
	select {
	case s.commandSlots <- struct{}{}:
	case <-ctx.Done():
		return AppendDeliveryReport{}, ctx.Err()
	case <-s.terminalDone:
		return s.waitTerminal(ctx)
	}

	s.admissionMu.Lock()
	s.shared.mu.Lock()
	state := s.shared.state
	s.shared.mu.Unlock()
	if state != AppendStreamOpen {
		s.admissionMu.Unlock()
		s.releaseCommandSlot()
		return s.waitTerminal(ctx)
	}
	barrier := &appendBarrier{
		startedAt: time.Now(),
		ack:       make(chan appendBarrierResult, 1),
	}
	s.shared.mu.Lock()
	barrier.acceptedRows = s.shared.counters.acceptedRows
	barrier.droppedRows = s.shared.counters.droppedRows
	s.shared.mu.Unlock()
	s.commands <- appendCommand{barrier: barrier}
	s.admissionMu.Unlock()
	return s.waitBarrier(ctx, barrier)
}

func (s *AppendStream) waitBarrier(ctx context.Context, barrier *appendBarrier) (AppendDeliveryReport, error) {
	select {
	case result := <-barrier.ack:
		return result.report, result.err
	default:
	}
	select {
	case result := <-barrier.ack:
		return result.report, result.err
	case <-ctx.Done():
		select {
		case result := <-barrier.ack:
			return result.report, result.err
		default:
			return AppendDeliveryReport{}, ctx.Err()
		}
	case <-s.terminalDone:
		select {
		case result := <-barrier.ack:
			return result.report, result.err
		default:
			return s.waitTerminal(ctx)
		}
	}
}

// Shutdown permanently closes admission and settles every admitted row.
// It is idempotent; canceling ctx stops only the caller's wait.
// If ctx ends first, Shutdown returns a zero report and ctx.Err().
func (s *AppendStream) Shutdown(ctx context.Context) (AppendDeliveryReport, error) {
	start := false
	s.admissionMu.Lock()
	s.shared.mu.Lock()
	if s.shared.state == AppendStreamOpen {
		s.shared.state = AppendStreamClosing
		start = true
	}
	s.shared.mu.Unlock()
	if start {
		s.closeAdmission()
		go s.enqueueShutdown()
	}
	s.admissionMu.Unlock()
	return s.waitTerminal(ctx)
}

// Stats returns a race-safe lifetime snapshot.
func (s *AppendStream) Stats() AppendStreamStats {
	s.shared.mu.Lock()
	defer s.shared.mu.Unlock()
	stats := AppendStreamStats{
		State:           s.shared.state,
		AcceptedRows:    s.shared.counters.acceptedRows,
		CommittedRows:   s.shared.counters.committedRows,
		FailedRows:      s.shared.counters.failedRows,
		UnknownRows:     s.shared.counters.unknownRows,
		DroppedRows:     s.shared.counters.droppedRows,
		DroppedByReason: s.shared.droppedByReason,
		Retries:         s.shared.counters.retries,
		PendingRows:     s.shared.pendingRows,
		PendingBytes:    s.shared.pendingBytes,
		InFlightBatches: s.shared.inFlightBatches,
	}
	if s.shared.lastFailure != nil {
		stats.LastFailure = copyAppendLastFailure(s.shared.lastFailure)
	}
	if s.shared.lastReport != nil {
		stats.LastReport = copyAppendReport(*s.shared.lastReport)
	}
	return stats
}

func (s *AppendStream) enqueueShutdown() {
	select {
	case s.commandSlots <- struct{}{}:
	case <-s.terminalDone:
		return
	}

	s.admissionMu.Lock()
	defer s.admissionMu.Unlock()
	select {
	case <-s.terminalDone:
		s.releaseCommandSlot()
		return
	default:
	}
	barrier := &appendBarrier{
		shutdown:  true,
		startedAt: time.Now(),
		ack:       make(chan appendBarrierResult, 1),
	}
	s.shared.mu.Lock()
	barrier.acceptedRows = s.shared.counters.acceptedRows
	barrier.droppedRows = s.shared.counters.droppedRows
	s.shared.mu.Unlock()
	s.commands <- appendCommand{barrier: barrier}
}

func (s *AppendStream) waitTerminal(ctx context.Context) (AppendDeliveryReport, error) {
	select {
	case <-s.terminalDone:
		s.shared.mu.Lock()
		report := s.shared.terminalReport
		err := s.shared.fatal
		s.shared.mu.Unlock()
		return report, err
	default:
	}
	select {
	case <-s.terminalDone:
		s.shared.mu.Lock()
		report := s.shared.terminalReport
		err := s.shared.fatal
		s.shared.mu.Unlock()
		return report, err
	case <-ctx.Done():
		return AppendDeliveryReport{}, ctx.Err()
	}
}

func (s *AppendStream) checkOpen() error {
	s.shared.mu.Lock()
	defer s.shared.mu.Unlock()
	if s.shared.state == AppendStreamOpen {
		return nil
	}
	if s.shared.fatal != nil {
		return s.shared.fatal
	}
	return ErrAppendStreamClosed
}

func (s *AppendStream) closedOrFatalError() error {
	s.shared.mu.Lock()
	defer s.shared.mu.Unlock()
	if s.shared.fatal != nil {
		return s.shared.fatal
	}
	return ErrAppendStreamClosed
}

func (s *AppendStream) noteAccepted(bytes int) {
	s.shared.mu.Lock()
	s.shared.counters.acceptedRows++
	s.shared.pendingRows++
	s.shared.pendingBytes += bytes
	s.shared.mu.Unlock()
}

type appendDropReason uint8

const (
	appendDropBufferFull appendDropReason = iota
	appendDropInvalidRow
	appendDropRowTooLarge
	appendDropClosed
)

func (s *AppendStream) noteDrop(reason appendDropReason) {
	s.shared.mu.Lock()
	s.shared.counters.droppedRows++
	switch reason {
	case appendDropBufferFull:
		s.shared.droppedByReason.BufferFull++
	case appendDropInvalidRow:
		s.shared.droppedByReason.InvalidRow++
	case appendDropRowTooLarge:
		s.shared.droppedByReason.RowTooLarge++
	case appendDropClosed:
		s.shared.droppedByReason.Closed++
	}
	s.shared.mu.Unlock()
}

func (s *AppendStream) closeAdmission() {
	s.admissionOnce.Do(func() {
		close(s.admissionDone)
		s.budget.close()
	})
}

func (s *AppendStream) finishTerminal(report AppendDeliveryReport, err error, state AppendStreamState) {
	s.shared.mu.Lock()
	if !s.shared.hasTerminal {
		s.shared.state = state
		s.shared.terminalReport = report
		s.shared.hasTerminal = true
		s.shared.fatal = err
		reportCopy := report
		s.shared.lastReport = &reportCopy
	}
	s.shared.mu.Unlock()
	s.closeAdmission()
	s.terminalOnce.Do(func() { close(s.terminalDone) })
}

func (s *AppendStream) releaseCommandSlot() {
	<-s.commandSlots
}

func marshalAppendRecord(row any) ([]byte, error) {
	payload, err := json.Marshal(row)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrAppendRowInvalid, err)
	}
	if len(payload) == 0 || payload[0] != '{' {
		return nil, ErrAppendRowInvalid
	}
	if len(payload) > maxAppendBodyBytes {
		return nil, fmt.Errorf("%w: encoded row is %d bytes, limit is %d", ErrAppendRowTooLarge, len(payload), maxAppendBodyBytes)
	}
	return payload, nil
}

func (s *AppendStream) run() {
	worker := appendWorker{
		stream:   s,
		baseline: appendCounters{},
	}
	worker.run()
}

type appendWorker struct {
	stream *AppendStream

	current        []appendRecord
	currentBytes   int
	ready          []appendBatch
	inFlight       int
	barrier        *appendBarrier
	failureBarrier *appendBarrier
	baseline       appendCounters
	stopping       bool
	fatal          error
	timer          *time.Timer
	timerC         <-chan time.Time
}

func (w *appendWorker) run() {
	for {
		w.dispatchReady()
		if w.stopping {
			if w.failureBarrier != nil {
				if w.inFlight == 0 {
					if w.finishFailureBarrier() {
						return
					}
					continue
				}
			} else {
				w.drainAfterFailure()
				if w.failureBarrier != nil {
					continue
				}
			}
			if w.inFlight == 0 {
				w.finishFailure()
				return
			}
		}
		if w.barrier != nil && len(w.ready) == 0 && w.inFlight == 0 {
			if w.finishBarrier() {
				return
			}
			continue
		}

		var commandC <-chan appendCommand
		if (!w.stopping && w.barrier == nil) || (w.stopping && w.failureBarrier == nil) {
			commandC = w.stream.commands
		}
		select {
		case command := <-commandC:
			w.stream.releaseCommandSlot()
			w.handleCommand(command)
		case result := <-w.stream.results:
			w.handleResult(result)
		case <-w.timerC:
			w.timerC = nil
			w.finalizeCurrent()
		}
	}
}

func (w *appendWorker) handleCommand(command appendCommand) {
	if command.record != nil {
		if w.stopping {
			w.failUnsentRecord(*command.record)
			return
		}
		w.addRecord(*command.record)
		return
	}
	if command.barrier == nil {
		return
	}
	if w.stopping {
		w.failureBarrier = command.barrier
		return
	}
	w.finalizeCurrent()
	w.barrier = command.barrier
}

func (w *appendWorker) addRecord(record appendRecord) {
	additional := len(record.payload)
	if len(w.current) > 0 {
		additional++
	}
	if len(w.current) > 0 && (w.currentBytes+additional > w.stream.config.targetBatchBytes || len(w.current) >= w.stream.config.maxBatchRows) {
		w.finalizeCurrent()
		additional = len(record.payload)
	}
	if len(w.current) == 0 {
		w.startTimer()
	}
	w.current = append(w.current, record)
	w.currentBytes += additional
	if w.currentBytes >= w.stream.config.targetBatchBytes || len(w.current) >= w.stream.config.maxBatchRows {
		w.finalizeCurrent()
	}
}

func (w *appendWorker) startTimer() {
	if w.timer == nil {
		w.timer = time.NewTimer(w.stream.config.flushInterval)
	} else {
		if !w.timer.Stop() {
			select {
			case <-w.timer.C:
			default:
			}
		}
		w.timer.Reset(w.stream.config.flushInterval)
	}
	w.timerC = w.timer.C
}

func (w *appendWorker) stopTimer() {
	if w.timer != nil && !w.timer.Stop() {
		select {
		case <-w.timer.C:
		default:
		}
	}
	w.timerC = nil
}

func (w *appendWorker) finalizeCurrent() {
	if len(w.current) == 0 {
		return
	}
	w.stopTimer()
	payload := make([]byte, 0, w.currentBytes)
	reservedBytes := 0
	for index := range w.current {
		if index > 0 {
			payload = append(payload, '\n')
		}
		payload = append(payload, w.current[index].payload...)
		reservedBytes += w.current[index].reservedBytes
	}
	w.ready = append(w.ready, appendBatch{
		payload:       payload,
		rows:          len(w.current),
		reservedBytes: reservedBytes,
	})
	w.current = nil
	w.currentBytes = 0
}

func (w *appendWorker) dispatchReady() {
	for !w.stopping && w.inFlight < w.stream.config.maxConcurrentBatches && len(w.ready) > 0 {
		batch := w.ready[0]
		w.ready[0] = appendBatch{}
		w.ready = w.ready[1:]
		w.inFlight++
		w.stream.shared.mu.Lock()
		w.stream.shared.inFlightBatches++
		w.stream.shared.mu.Unlock()
		go w.stream.sendBatch(batch)
	}
}

func (s *AppendStream) sendBatch(batch appendBatch) {
	var result AppendRowsResult
	var err error
	var retries uint64
	backoff := defaultAppendInitialBackoff
	for attempt := 0; ; attempt++ {
		ctx := context.Background()
		cancel := func() {}
		if s.config.attemptTimeout > 0 {
			ctx, cancel = context.WithTimeout(ctx, s.config.attemptTimeout)
		}
		result, err = s.client.appendNDJSON(
			ctx,
			s.database,
			s.schema,
			s.table,
			batch.payload,
		)
		cancel()
		if err == nil {
			if result.AppendState != AppendStateCommitted || result.NumRowsInserted != int64(batch.rows) {
				err = unknownAppendStreamError(fmt.Sprintf(
					"append response did not confirm all %d rows committed",
					batch.rows,
				), nil)
			}
		}
		if err == nil || attempt >= defaultAppendMaxRetries || !isRetryableRejectedAppend(err) {
			break
		}
		var retryAfter time.Duration
		var scopeErr *Error
		if errors.As(err, &scopeErr) {
			retryAfter = scopeErr.RetryAfter
		}
		delay := appendRetryDelay(backoff, retryAfter)
		time.Sleep(delay)
		retries++
		if backoff < defaultAppendMaxBackoff {
			backoff *= 2
			if backoff > defaultAppendMaxBackoff {
				backoff = defaultAppendMaxBackoff
			}
		}
	}
	s.results <- appendBatchResult{batch: batch, result: result, err: err, retries: retries}
}

func appendRetryDelay(backoff time.Duration, retryAfter time.Duration) time.Duration {
	delay := backoff
	if retryAfter > delay {
		delay = retryAfter
	}
	if delay > defaultAppendMaxBackoff {
		delay = defaultAppendMaxBackoff
	}
	return delay
}

func isRetryableRejectedAppend(err error) bool {
	var scopeErr *Error
	return errors.As(err, &scopeErr) &&
		scopeErr.Retryable &&
		scopeErr.AppendDetails != nil &&
		scopeErr.AppendDetails.AppendState == AppendStateRejected
}

func unknownAppendStreamError(message string, cause error) error {
	return &Error{
		Kind:      ErrorKindAppendRowsFailed,
		Message:   message,
		Retryable: false,
		AppendDetails: &AppendErrorDetails{
			AppendState: AppendStateUnknown,
			RowErrors:   []AppendRowError{},
		},
		cause: cause,
	}
}

func (w *appendWorker) handleResult(result appendBatchResult) {
	w.inFlight--
	w.stream.budget.release(result.batch.reservedBytes)
	rows := uint64(result.batch.rows) // #nosec G115 -- batch rows are capped at maxAppendRows.
	w.stream.shared.mu.Lock()
	w.stream.shared.inFlightBatches--
	w.stream.shared.pendingRows -= rows
	w.stream.shared.pendingBytes -= result.batch.reservedBytes
	w.stream.shared.counters.retries += result.retries
	if result.err == nil {
		w.stream.shared.counters.committedRows += rows
		w.stream.shared.counters.committedBatches++
		w.stream.shared.mu.Unlock()
		return
	}
	state := appendErrorState(result.err)
	if state == AppendStateRejected {
		w.stream.shared.counters.failedRows += rows
		w.stream.shared.counters.failedBatches++
	} else {
		state = AppendStateUnknown
		w.stream.shared.counters.unknownRows += rows
		w.stream.shared.counters.unknownBatches++
	}
	w.stream.shared.lastFailure = makeAppendLastFailure(result.err, state)
	w.stream.shared.mu.Unlock()

	if w.stream.config.failurePolicy == AppendFailureStop {
		w.startStopping(result.err)
	}
}

func appendErrorState(err error) AppendState {
	var scopeErr *Error
	if errors.As(err, &scopeErr) && scopeErr.AppendDetails != nil {
		return scopeErr.AppendDetails.AppendState
	}
	return AppendStateUnknown
}

func makeAppendLastFailure(err error, state AppendState) *AppendLastFailure {
	failure := &AppendLastFailure{
		At:          time.Now(),
		Message:     err.Error(),
		AppendState: state,
	}
	var scopeErr *Error
	if !errors.As(err, &scopeErr) {
		return failure
	}
	failure.HTTPStatus = scopeErr.HTTPStatus
	failure.RequestID = scopeErr.RequestID
	failure.RetryAfter = scopeErr.RetryAfter
	failure.Retryable = scopeErr.Retryable
	if scopeErr.AppendDetails != nil {
		failure.RowErrors = slices.Clone(scopeErr.AppendDetails.RowErrors)
		failure.RowErrorsTruncated = scopeErr.AppendDetails.RowErrorsTruncated
	}
	return failure
}

func copyAppendLastFailure(failure *AppendLastFailure) *AppendLastFailure {
	if failure == nil {
		return nil
	}
	cloned := *failure
	cloned.RowErrors = slices.Clone(failure.RowErrors)
	return &cloned
}

func (w *appendWorker) startStopping(err error) {
	if !w.stopping {
		w.stopping = true
		w.fatal = err
		w.stopTimer()
		w.stream.admissionMu.Lock()
		w.stream.shared.mu.Lock()
		w.stream.shared.state = AppendStreamFailed
		w.stream.shared.fatal = err
		w.stream.shared.mu.Unlock()
		w.stream.closeAdmission()
		w.stream.admissionMu.Unlock()
		for _, record := range w.current {
			w.failUnsentRecord(record)
		}
		w.current = nil
		w.currentBytes = 0
		for _, batch := range w.ready {
			w.failUnsentBatch(batch)
		}
		w.ready = nil
		if w.barrier != nil {
			w.failureBarrier = w.barrier
			w.barrier = nil
		}
		return
	}
	if appendErrorState(err) == AppendStateUnknown && appendErrorState(w.fatal) != AppendStateUnknown {
		w.fatal = err
		w.stream.shared.mu.Lock()
		w.stream.shared.fatal = err
		w.stream.shared.mu.Unlock()
	}
}

func (w *appendWorker) failUnsentRecord(record appendRecord) {
	w.stream.budget.release(record.reservedBytes)
	w.stream.shared.mu.Lock()
	w.stream.shared.pendingRows--
	w.stream.shared.pendingBytes -= record.reservedBytes
	w.stream.shared.counters.failedRows++
	w.stream.shared.mu.Unlock()
}

func (w *appendWorker) failUnsentBatch(batch appendBatch) {
	w.stream.budget.release(batch.reservedBytes)
	rows := uint64(batch.rows) // #nosec G115 -- batch rows are capped at maxAppendRows.
	w.stream.shared.mu.Lock()
	w.stream.shared.pendingRows -= rows
	w.stream.shared.pendingBytes -= batch.reservedBytes
	w.stream.shared.counters.failedRows += rows
	w.stream.shared.mu.Unlock()
}

func (w *appendWorker) drainAfterFailure() {
	for {
		select {
		case command := <-w.stream.commands:
			w.stream.releaseCommandSlot()
			w.handleCommand(command)
			if w.failureBarrier != nil {
				return
			}
		default:
			return
		}
	}
}

func (w *appendWorker) finishFailure() {
	report := w.makeReport(time.Now())
	w.stream.finishTerminal(report, w.fatal, AppendStreamFailed)
}

func (w *appendWorker) finishFailureBarrier() bool {
	barrier := w.failureBarrier
	w.failureBarrier = nil
	counters := w.snapshotCounters()
	counters.acceptedRows = barrier.acceptedRows
	counters.droppedRows = barrier.droppedRows
	report := w.makeReportFrom(barrier.startedAt, counters)
	w.baseline = counters
	w.stream.shared.mu.Lock()
	reportCopy := report
	w.stream.shared.lastReport = &reportCopy
	w.stream.shared.mu.Unlock()
	barrier.ack <- appendBarrierResult{report: report, err: w.fatal}
	if barrier.shutdown {
		w.stream.finishTerminal(report, w.fatal, AppendStreamFailed)
		return true
	}
	return false
}

func (w *appendWorker) finishBarrier() bool {
	barrier := w.barrier
	w.barrier = nil
	counters := w.snapshotCounters()
	counters.acceptedRows = barrier.acceptedRows
	counters.droppedRows = barrier.droppedRows
	report := w.makeReportFrom(barrier.startedAt, counters)
	w.baseline = counters
	w.stream.shared.mu.Lock()
	reportCopy := report
	w.stream.shared.lastReport = &reportCopy
	w.stream.shared.mu.Unlock()
	barrier.ack <- appendBarrierResult{report: report}
	if barrier.shutdown {
		w.stream.finishTerminal(report, nil, AppendStreamClosed)
		return true
	}
	return false
}

func (w *appendWorker) snapshotCounters() appendCounters {
	w.stream.shared.mu.Lock()
	defer w.stream.shared.mu.Unlock()
	return w.stream.shared.counters
}

func (w *appendWorker) makeReport(startedAt time.Time) AppendDeliveryReport {
	return w.makeReportFrom(startedAt, w.snapshotCounters())
}

func (w *appendWorker) makeReportFrom(startedAt time.Time, counters appendCounters) AppendDeliveryReport {
	report := AppendDeliveryReport{
		AcceptedRows:     counters.acceptedRows - w.baseline.acceptedRows,
		CommittedRows:    counters.committedRows - w.baseline.committedRows,
		FailedRows:       counters.failedRows - w.baseline.failedRows,
		UnknownRows:      counters.unknownRows - w.baseline.unknownRows,
		DroppedRows:      counters.droppedRows - w.baseline.droppedRows,
		CommittedBatches: counters.committedBatches - w.baseline.committedBatches,
		FailedBatches:    counters.failedBatches - w.baseline.failedBatches,
		UnknownBatches:   counters.unknownBatches - w.baseline.unknownBatches,
		Retries:          counters.retries - w.baseline.retries,
		Duration:         time.Since(startedAt),
	}
	lost := report.FailedRows + report.UnknownRows + report.DroppedRows
	switch {
	case lost == 0:
		report.Outcome = AppendDeliveryOK
	case report.CommittedRows > 0:
		report.Outcome = AppendDeliveryPartial
	case report.UnknownRows > 0:
		report.Outcome = AppendDeliveryUnknown
	default:
		report.Outcome = AppendDeliveryFailed
	}
	return report
}

func copyAppendReport(report AppendDeliveryReport) *AppendDeliveryReport {
	cloned := report
	return &cloned
}

type appendByteBudget struct {
	mu       sync.Mutex
	capacity int
	used     int
	closed   bool
	changed  chan struct{}
}

func newAppendByteBudget(capacity int) *appendByteBudget {
	return &appendByteBudget{capacity: capacity, changed: make(chan struct{})}
}

func (b *appendByteBudget) acquire(ctx context.Context, bytes int) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		b.mu.Lock()
		if b.closed {
			b.mu.Unlock()
			return ErrAppendStreamClosed
		}
		if b.used <= b.capacity-bytes {
			b.used += bytes
			b.mu.Unlock()
			return nil
		}
		changed := b.changed
		b.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (b *appendByteBudget) tryAcquire(bytes int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || b.used > b.capacity-bytes {
		return false
	}
	b.used += bytes
	return true
}

func (b *appendByteBudget) release(bytes int) {
	b.mu.Lock()
	b.used -= bytes
	if b.used < 0 {
		b.used = 0
	}
	b.signalLocked()
	b.mu.Unlock()
}

func (b *appendByteBudget) close() {
	b.mu.Lock()
	if !b.closed {
		b.closed = true
		b.signalLocked()
	}
	b.mu.Unlock()
}

func (b *appendByteBudget) signalLocked() {
	close(b.changed)
	b.changed = make(chan struct{})
}
