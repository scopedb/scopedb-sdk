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
	"fmt"
	"sync"
	"time"
)

const (
	defaultIngestTargetBatchBytes = 16 * 1024 * 1024
	defaultIngestMaxBatchRows     = 200_000
	defaultIngestFlushInterval    = time.Second
	defaultIngestMaxBufferedBytes = 64 * 1024 * 1024
	defaultIngestAttemptTimeout   = 30 * time.Second
	ingestCommandCapacity         = 1024
	maxIngestBodyBytes            = 16 * 1024 * 1024
	maxIngestRows                 = 200_000
)

// IngestStreamOptions configures transform-oriented JSON ingest batching.
// Zero values use the documented defaults.
type IngestStreamOptions struct {
	// TargetBatchBytes is the target NDJSON body size. A single row may exceed
	// this target up to the 16 MiB request limit.
	TargetBatchBytes int
	// MaxBatchRows is the maximum number of rows in one ingest request.
	MaxBatchRows int
	// FlushInterval is the maximum delay from the first buffered row until its
	// batch is dispatched.
	FlushInterval time.Duration
	// MaxBufferedBytes bounds serialized rows that have been admitted but have
	// not finished their remote request.
	MaxBufferedBytes int
	// AttemptTimeout bounds each transform-ingest request. It defaults to 30
	// seconds. A timeout can leave the remote commit outcome unknown.
	AttemptTimeout time.Duration
}

// IngestResult reports rows confirmed inserted by requests covered by a
// delivery barrier. When returned with an error, it counts only earlier
// confirmed batches; the failing batch can have an unknown commit outcome.
type IngestResult struct {
	NumRowsInserted int64
}

// IngestStream serializes JSON objects into bounded, sequential
// transform-ingest requests. It is safe for concurrent producers.
type IngestStream struct {
	inner *ingestStreamInner
}

type ingestStreamConfig struct {
	client           *Client
	statement        string
	targetBatchBytes int
	maxBatchRows     int
	flushInterval    time.Duration
	maxBufferedBytes int
	attemptTimeout   time.Duration
}

type ingestStreamInner struct {
	config   ingestStreamConfig
	commands chan ingestCommand
	budget   *ingestByteBudget

	// admission is a context-aware mutex. It linearizes record and barrier
	// commands without preventing a waiting caller from honoring cancellation.
	admission chan struct{}
	accepting bool

	shutdownOnce sync.Once
	failureOnce  sync.Once
	finalOnce    sync.Once

	failureDone chan struct{}
	finalDone   chan struct{}

	stateMu     sync.Mutex
	fatal       error
	finalResult IngestResult
	finalError  error
}

type ingestCommandKind uint8

const (
	ingestCommandRecord ingestCommandKind = iota
	ingestCommandFlush
	ingestCommandShutdown
)

type ingestCommand struct {
	kind   ingestCommandKind
	record *ingestRecord
	ack    chan ingestOutcome
}

type ingestRecord struct {
	payload     []byte
	reservation *ingestByteReservation
}

type ingestOutcome struct {
	result IngestResult
	err    error
}

var (
	errIngestBudgetClosed = errors.New("ingest stream byte budget is closed")
	errIngestRecordTooBig = errors.New("ingest stream record exceeds its byte budget")
)

// IngestStream creates a bounded transform-ingest stream and starts its worker.
func (c *Client) IngestStream(
	statement string,
	options IngestStreamOptions,
) (*IngestStream, error) {
	config, err := normalizeIngestStreamOptions(c, statement, options)
	if err != nil {
		return nil, err
	}

	inner := &ingestStreamInner{
		config:      config,
		commands:    make(chan ingestCommand, ingestCommandCapacity),
		budget:      newIngestByteBudget(config.maxBufferedBytes),
		admission:   make(chan struct{}, 1),
		accepting:   true,
		failureDone: make(chan struct{}),
		finalDone:   make(chan struct{}),
	}
	inner.admission <- struct{}{}
	go inner.run()
	return &IngestStream{inner: inner}, nil
}

// Send serializes and admits one JSON object, waiting for bounded local
// capacity. Success confirms local admission only; use Flush or Shutdown as a
// remote commit barrier.
func (s *IngestStream) Send(ctx context.Context, row any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	payload, err := serializeIngestRow(row)
	if err != nil {
		return err
	}

	reservation, err := s.inner.budget.acquire(ctx, len(payload)+1)
	if err != nil {
		return s.inner.mapAdmissionError(err, len(payload)+1)
	}
	record := &ingestRecord{payload: payload, reservation: reservation}

	if err := s.inner.acquireAdmission(ctx); err != nil {
		reservation.release()
		return err
	}
	if !s.inner.accepting {
		s.inner.releaseAdmission()
		reservation.release()
		return s.inner.closedOrFatalError()
	}

	select {
	case s.inner.commands <- ingestCommand{kind: ingestCommandRecord, record: record}:
		s.inner.releaseAdmission()
		return nil
	case <-ctx.Done():
		s.inner.releaseAdmission()
		reservation.release()
		return ctx.Err()
	case <-s.inner.failureDone:
		s.inner.releaseAdmission()
		reservation.release()
		return s.inner.closedOrFatalError()
	}
}

// Flush dispatches every row admitted before this barrier and waits for their
// remote outcome. Cancelling ctx stops only this caller's wait after the
// barrier has been enqueued; the worker continues settling the accepted prefix.
// A nonzero result returned with an error excludes the failing batch and is not
// a safe offset for replay.
func (s *IngestStream) Flush(ctx context.Context) (IngestResult, error) {
	ack := make(chan ingestOutcome, 1)
	if err := s.inner.acquireAdmission(ctx); err != nil {
		if ctx.Err() == nil && s.inner.fatalError() != nil {
			return s.inner.waitFinal(ctx)
		}
		return IngestResult{}, err
	}
	if !s.inner.accepting {
		s.inner.releaseAdmission()
		if s.inner.fatalError() != nil {
			return s.inner.waitFinal(ctx)
		}
		return IngestResult{}, s.inner.closedOrFatalError()
	}

	select {
	case s.inner.commands <- ingestCommand{kind: ingestCommandFlush, ack: ack}:
		s.inner.releaseAdmission()
	case <-ctx.Done():
		s.inner.releaseAdmission()
		return IngestResult{}, ctx.Err()
	case <-s.inner.failureDone:
		s.inner.releaseAdmission()
		return s.inner.waitFinal(ctx)
	}

	return s.inner.waitOutcome(ctx, ack)
}

// Shutdown closes admission, settles every accepted row, and returns the final
// barrier result. It is idempotent. Cancelling ctx stops only the caller's
// wait; shutdown continues in the background and a later call can await it.
// A nonzero result returned with an error excludes the failing batch and is not
// a safe offset for replay.
func (s *IngestStream) Shutdown(ctx context.Context) (IngestResult, error) {
	s.inner.shutdownOnce.Do(func() {
		go s.inner.initiateShutdown()
	})
	return s.inner.waitFinal(ctx)
}

func normalizeIngestStreamOptions(
	client *Client,
	statement string,
	options IngestStreamOptions,
) (ingestStreamConfig, error) {
	if statement == "" {
		return ingestStreamConfig{}, newError(
			ErrorKindConfigInvalid,
			"ingest stream statement must not be empty",
			nil,
		)
	}
	if options.TargetBatchBytes == 0 {
		options.TargetBatchBytes = defaultIngestTargetBatchBytes
	}
	if options.MaxBatchRows == 0 {
		options.MaxBatchRows = defaultIngestMaxBatchRows
	}
	if options.FlushInterval == 0 {
		options.FlushInterval = defaultIngestFlushInterval
	}
	if options.MaxBufferedBytes == 0 {
		options.MaxBufferedBytes = defaultIngestMaxBufferedBytes
	}
	if options.AttemptTimeout == 0 {
		options.AttemptTimeout = defaultIngestAttemptTimeout
	}

	switch {
	case options.TargetBatchBytes < 1 || options.TargetBatchBytes > maxIngestBodyBytes:
		return ingestStreamConfig{}, newError(
			ErrorKindConfigInvalid,
			fmt.Sprintf("ingest TargetBatchBytes must be from 1 to %d", maxIngestBodyBytes),
			nil,
		)
	case options.MaxBatchRows < 1 || options.MaxBatchRows > maxIngestRows:
		return ingestStreamConfig{}, newError(
			ErrorKindConfigInvalid,
			fmt.Sprintf("ingest MaxBatchRows must be from 1 to %d", maxIngestRows),
			nil,
		)
	case options.FlushInterval < 1:
		return ingestStreamConfig{}, newError(
			ErrorKindConfigInvalid,
			"ingest FlushInterval must be positive",
			nil,
		)
	case options.MaxBufferedBytes < 1:
		return ingestStreamConfig{}, newError(
			ErrorKindConfigInvalid,
			"ingest MaxBufferedBytes must be positive",
			nil,
		)
	case options.AttemptTimeout < 0:
		return ingestStreamConfig{}, newError(
			ErrorKindConfigInvalid,
			"ingest AttemptTimeout must be positive",
			nil,
		)
	}

	return ingestStreamConfig{
		client:           client,
		statement:        statement,
		targetBatchBytes: options.TargetBatchBytes,
		maxBatchRows:     options.MaxBatchRows,
		flushInterval:    options.FlushInterval,
		maxBufferedBytes: options.MaxBufferedBytes,
		attemptTimeout:   options.AttemptTimeout,
	}, nil
}

func serializeIngestRow(row any) ([]byte, error) {
	payload, err := json.Marshal(row)
	if err != nil {
		return nil, newError(
			ErrorKindConfigInvalid,
			"failed to serialize ingest stream row",
			err,
		)
	}
	if len(payload) == 0 || payload[0] != '{' {
		return nil, newError(
			ErrorKindConfigInvalid,
			"ingest stream rows must serialize to JSON objects",
			nil,
		)
	}
	if len(payload) > maxIngestBodyBytes {
		return nil, newError(
			ErrorKindConfigInvalid,
			fmt.Sprintf("ingest stream row exceeds the %d-byte request limit", maxIngestBodyBytes),
			nil,
		)
	}
	return payload, nil
}

func (s *ingestStreamInner) acquireAdmission(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-s.admission:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.failureDone:
		return s.closedOrFatalError()
	}
}

func (s *ingestStreamInner) releaseAdmission() {
	s.admission <- struct{}{}
}

func (s *ingestStreamInner) initiateShutdown() {
	select {
	case <-s.admission:
	case <-s.failureDone:
		return
	}
	if !s.accepting {
		s.releaseAdmission()
		return
	}
	s.accepting = false
	s.budget.close()
	select {
	case s.commands <- ingestCommand{kind: ingestCommandShutdown}:
	case <-s.failureDone:
	}
	s.releaseAdmission()
}

func (s *ingestStreamInner) waitOutcome(
	ctx context.Context,
	ack <-chan ingestOutcome,
) (IngestResult, error) {
	select {
	case outcome := <-ack:
		return outcome.result, outcome.err
	default:
	}
	select {
	case outcome := <-ack:
		return outcome.result, outcome.err
	case <-ctx.Done():
		return IngestResult{}, ctx.Err()
	}
}

func (s *ingestStreamInner) waitFinal(ctx context.Context) (IngestResult, error) {
	select {
	case <-s.finalDone:
		return s.finalOutcome()
	default:
	}
	select {
	case <-s.finalDone:
		return s.finalOutcome()
	case <-ctx.Done():
		return IngestResult{}, ctx.Err()
	}
}

func (s *ingestStreamInner) finalOutcome() (IngestResult, error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.finalResult, s.finalError
}

func (s *ingestStreamInner) fatalError() error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.fatal
}

func (s *ingestStreamInner) closedOrFatalError() error {
	if err := s.fatalError(); err != nil {
		return err
	}
	return newError(ErrorKindUnexpected, "ingest stream is closed", nil)
}

func (s *ingestStreamInner) mapAdmissionError(err error, requested int) error {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return err
	case errors.Is(err, errIngestBudgetClosed):
		return s.closedOrFatalError()
	case errors.Is(err, errIngestRecordTooBig):
		return newError(
			ErrorKindConfigInvalid,
			fmt.Sprintf(
				"ingest stream row requires %d buffered bytes, exceeds MaxBufferedBytes=%d",
				requested,
				s.config.maxBufferedBytes,
			),
			err,
		)
	default:
		return err
	}
}

func (s *ingestStreamInner) run() {
	var batch []*ingestRecord
	batchBytes := 0
	insertedSinceBarrier := int64(0)
	var timer *time.Timer
	var timerC <-chan time.Time

	stopTimer := func() {
		if timer == nil {
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer = nil
		timerC = nil
	}
	startTimer := func() {
		if timer != nil {
			return
		}
		timer = time.NewTimer(s.config.flushInterval)
		timerC = timer.C
	}
	flushBatch := func() (IngestResult, error) {
		if len(batch) == 0 {
			return IngestResult{}, nil
		}

		var body bytes.Buffer
		body.Grow(batchBytes)
		for i, record := range batch {
			if i > 0 {
				body.WriteByte('\n')
			}
			body.Write(record.payload)
		}

		attemptCtx, cancelAttempt := context.WithTimeout(
			context.Background(),
			s.config.attemptTimeout,
		)
		response, err := s.config.client.ingest(attemptCtx, &ingestRequest{
			Data: ingestData{
				Format: writeFormatJSON,
				Rows:   body.String(),
			},
			Statement: s.config.statement,
		})
		cancelAttempt()
		if err == nil && response.NumRowsInserted < 0 {
			err = newError(
				ErrorKindUnexpected,
				"ingest response reported a negative inserted row count",
				nil,
			)
		}
		if err != nil {
			// Publish the terminal failure before releasing capacity. Otherwise a
			// sender blocked on the failed batch's reservation can acquire that
			// capacity and report successful admission after the failure is known.
			s.beginFailure(err)
		}
		for _, record := range batch {
			record.reservation.release()
		}
		batch = nil
		batchBytes = 0
		stopTimer()
		if err != nil {
			return IngestResult{}, err
		}
		return IngestResult{NumRowsInserted: int64(response.NumRowsInserted)}, nil
	}

	for {
		select {
		case command := <-s.commands:
			switch command.kind {
			case ingestCommandRecord:
				record := command.record
				nextBytes := len(record.payload)
				if len(batch) > 0 {
					nextBytes++
				}
				if len(batch) > 0 &&
					(len(batch) >= s.config.maxBatchRows ||
						batchBytes+nextBytes > s.config.targetBatchBytes) {
					result, err := flushBatch()
					if err != nil {
						record.reservation.release()
						s.fail(err, IngestResult{NumRowsInserted: insertedSinceBarrier})
						return
					}
					insertedSinceBarrier += result.NumRowsInserted
				}

				if len(batch) == 0 {
					startTimer()
				} else {
					batchBytes++
				}
				batch = append(batch, record)
				batchBytes += len(record.payload)
				if len(batch) >= s.config.maxBatchRows ||
					batchBytes >= s.config.targetBatchBytes {
					result, err := flushBatch()
					if err != nil {
						s.fail(err, IngestResult{NumRowsInserted: insertedSinceBarrier})
						return
					}
					insertedSinceBarrier += result.NumRowsInserted
				}
			case ingestCommandFlush:
				result, err := flushBatch()
				insertedSinceBarrier += result.NumRowsInserted
				result.NumRowsInserted = insertedSinceBarrier
				if err == nil {
					insertedSinceBarrier = 0
				}
				command.ack <- ingestOutcome{result: result, err: err}
				if err != nil {
					s.fail(err, result)
					return
				}
			case ingestCommandShutdown:
				result, err := flushBatch()
				if err != nil {
					s.fail(err, IngestResult{NumRowsInserted: insertedSinceBarrier})
					return
				}
				insertedSinceBarrier += result.NumRowsInserted
				s.finish(IngestResult{NumRowsInserted: insertedSinceBarrier}, nil)
				return
			}
		case <-timerC:
			result, err := flushBatch()
			if err != nil {
				s.fail(err, IngestResult{NumRowsInserted: insertedSinceBarrier})
				return
			}
			insertedSinceBarrier += result.NumRowsInserted
		}
	}
}

func (s *ingestStreamInner) fail(err error, result IngestResult) {
	s.beginFailure(err)

	// Stop further admissions after any sender that was already linearizing has
	// observed failure and released the gate.
	<-s.admission
	s.accepting = false
	s.releaseAdmission()

	for {
		select {
		case command := <-s.commands:
			switch command.kind {
			case ingestCommandRecord:
				command.record.reservation.release()
			case ingestCommandFlush:
				command.ack <- ingestOutcome{result: result, err: err}
			case ingestCommandShutdown:
				// Shutdown callers wait on finalDone below.
			}
		default:
			s.finish(result, err)
			return
		}
	}
}

func (s *ingestStreamInner) beginFailure(err error) {
	s.failureOnce.Do(func() {
		s.stateMu.Lock()
		s.fatal = err
		s.stateMu.Unlock()
		close(s.failureDone)
		s.budget.close()
	})
}

func (s *ingestStreamInner) finish(result IngestResult, err error) {
	s.finalOnce.Do(func() {
		s.stateMu.Lock()
		s.finalResult = result
		s.finalError = err
		s.stateMu.Unlock()
		close(s.finalDone)
	})
}

type ingestByteBudget struct {
	mu       sync.Mutex
	capacity int
	used     int
	closed   bool
	changed  chan struct{}
}

func newIngestByteBudget(capacity int) *ingestByteBudget {
	return &ingestByteBudget{
		capacity: capacity,
		changed:  make(chan struct{}),
	}
}

func (b *ingestByteBudget) acquire(
	ctx context.Context,
	requested int,
) (*ingestByteReservation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if requested > b.capacity {
		return nil, errIngestRecordTooBig
	}
	for {
		b.mu.Lock()
		if b.closed {
			b.mu.Unlock()
			return nil, errIngestBudgetClosed
		}
		if requested <= b.capacity-b.used {
			b.used += requested
			b.mu.Unlock()
			return &ingestByteReservation{budget: b, bytes: requested}, nil
		}
		changed := b.changed
		b.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		}
	}
}

func (b *ingestByteBudget) release(bytes int) {
	b.mu.Lock()
	if bytes > b.used {
		b.used = 0
	} else {
		b.used -= bytes
	}
	b.notifyLocked()
	b.mu.Unlock()
}

func (b *ingestByteBudget) close() {
	b.mu.Lock()
	if !b.closed {
		b.closed = true
		b.notifyLocked()
	}
	b.mu.Unlock()
}

func (b *ingestByteBudget) notifyLocked() {
	close(b.changed)
	b.changed = make(chan struct{})
}

type ingestByteReservation struct {
	budget *ingestByteBudget
	bytes  int
	once   sync.Once
}

func (r *ingestByteReservation) release() {
	r.once.Do(func() {
		r.budget.release(r.bytes)
	})
}
