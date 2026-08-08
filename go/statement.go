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
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Statement configures a ScopeQL statement before submission.
type Statement struct {
	c *Client

	stmt string

	// ID of the statement.
	//
	// If provided, the ID must be a UUID, and ScopeDB will use the provided ID;
	// otherwise, ScopeDB will generate a random UUID for the statement submitted.
	ID *uuid.UUID
	// ExecTimeout is the maximum time allowed for statement execution.
	//
	// If the total execution time exceeds this value, the statement is failed
	// as timed out.
	//
	// Values use duration strings such as "1h".
	ExecTimeout string
	// MaxParallelism limits the statement's execution parallelism. Zero uses
	// the server default.
	MaxParallelism int
}

// Statement creates a new statement with the given ScopeQL statement.
func (c *Client) Statement(stmt string) *Statement {
	return &Statement{c: c, stmt: stmt}
}

// Query executes a ScopeQL statement and waits for all result rows.
func (c *Client) Query(ctx context.Context, scopeql string) (*ResultSet, error) {
	return c.Statement(scopeql).Execute(ctx)
}

// Submit submits the statement to ScopeDB for execution.
func (s *Statement) Submit(ctx context.Context) (*StatementHandle, error) {
	if s.MaxParallelism < 0 {
		return nil, newError(
			ErrorKindConfigInvalid,
			"statement MaxParallelism must not be negative",
			nil,
		)
	}
	resp, err := s.c.submitStatement(ctx, &statementRequest{
		StatementID:    s.ID,
		Statement:      s.stmt,
		ExecTimeout:    s.ExecTimeout,
		MaxParallelism: s.MaxParallelism,
		Format:         resultFormatJSON,
	})
	if err != nil {
		return nil, err
	}

	return &StatementHandle{
		c:    s.c,
		resp: resp,
		id:   resp.ID,
	}, nil
}

// Execute submits the statement to ScopeDB for execution and waits for its completion.
func (s *Statement) Execute(ctx context.Context) (*ResultSet, error) {
	handle, err := s.Submit(ctx)
	if err != nil {
		return nil, err
	}
	return handle.Wait(ctx)
}

// StatementHandle is a handle to a statement that has been submitted to ScopeDB.
type StatementHandle struct {
	c            *Client
	resp         *statementResponse
	cancelResult *StatementCancelResult
	id           uuid.UUID
}

// StatementHandle creates a new StatementHandle with the given ID.
func (c *Client) StatementHandle(id uuid.UUID) *StatementHandle {
	return &StatementHandle{
		c:  c,
		id: id,
	}
}

// ID returns the statement ID represented by this handle.
func (h *StatementHandle) ID() uuid.UUID {
	return h.id
}

// LastStatus returns the latest locally cached status without making a request.
func (h *StatementHandle) LastStatus() *StatementStatus {
	if h.resp == nil {
		return nil
	}
	status := h.resp.Status
	return &status
}

// Progress returns the last seen progress of the statement.
func (h *StatementHandle) Progress() *StatementProgress {
	if h.resp == nil {
		return nil
	}
	progress := h.resp.Progress
	return &progress
}

// ResultSet returns the result set of the statement if available.
func (h *StatementHandle) ResultSet() *ResultSet {
	if h.resp == nil {
		return nil
	}
	if h.resp.ResultSet == nil {
		return nil
	}
	return h.resp.ResultSet.toResultSet()
}

// Status fetches the latest status at most once. A cached terminal status is
// returned without making another request.
func (h *StatementHandle) Status(ctx context.Context) (StatementStatus, error) {
	if h.resp != nil && h.resp.Status.Terminated() {
		return h.resp.Status, nil
	}

	resp, err := h.c.fetchStatementResult(ctx, h.id)
	if err != nil {
		return "", err
	}

	h.resp = resp
	return h.resp.Status, nil
}

// Wait polls until the statement is finished, failed, or cancelled.
//
// When the statement is finished, the result set is returned. Otherwise, an error is returned.
func (h *StatementHandle) Wait(ctx context.Context) (*ResultSet, error) {
	delay := 5 * time.Millisecond
	maxDelay := time.Second
	for {
		status, err := h.Status(ctx)
		if err != nil {
			return nil, err
		}

		switch status {
		case StatementStatusFinished:
			result := h.ResultSet()
			if result == nil {
				return nil, &Error{
					Kind:    ErrorKindUnexpected,
					Message: "finished statement response has no result set",
				}
			}
			return result, nil
		case StatementStatusFailed, StatementStatusCancelled:
			message := fmt.Sprintf("statement is %s", status)
			if h.resp != nil && h.resp.Message != nil {
				message = *h.resp.Message
			}
			return nil, &Error{Kind: ErrorKindStatementFailed, Message: message}
		case StatementStatusPending, StatementStatusRunning:
			// Keep polling below.
		default:
			return nil, &Error{
				Kind:    ErrorKindUnexpected,
				Message: fmt.Sprintf("unknown statement status %q", status),
			}
		}

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
		if delay < maxDelay {
			delay = min(delay*2, maxDelay)
		}
	}
}

// StatementCancelResult reports the server's complete cancellation outcome.
type StatementCancelResult struct {
	StatementID uuid.UUID       `json:"statement_id"`
	CreatedAt   time.Time       `json:"created_at"`
	Status      StatementStatus `json:"status"`
	Message     string          `json:"message"`
}

// Cancel cancels the statement if it is running or pending.
func (h *StatementHandle) Cancel(ctx context.Context) (StatementCancelResult, error) {
	if h.cancelResult != nil {
		return *h.cancelResult, nil
	}
	if h.resp != nil && h.resp.Status.Terminated() {
		result := h.cancelResultFromResponse()
		h.cancelResult = &result
		return result, nil
	}

	resp, err := h.c.cancelStatement(ctx, h.id)
	if err != nil {
		return StatementCancelResult{}, err
	}

	result := *resp
	if result.Status.Terminated() {
		h.cancelResult = &result
	}
	if result.Status != StatementStatusFinished {
		message := result.Message
		h.resp = &statementResponse{
			ID:      result.StatementID,
			Created: result.CreatedAt,
			Status:  result.Status,
			Message: &message,
		}
	}
	return result, nil
}

func (h *StatementHandle) cancelResultFromResponse() StatementCancelResult {
	message := fmt.Sprintf("statement is %s", h.resp.Status)
	if h.resp.Message != nil && *h.resp.Message != "" {
		message = *h.resp.Message
	}
	return StatementCancelResult{
		StatementID: h.id,
		CreatedAt:   h.resp.Created,
		Status:      h.resp.Status,
		Message:     message,
	}
}

// StatementStatus is a string that represents the status of a statement.
type StatementStatus string

const (
	// StatementStatusPending indicates the query is not started yet.
	StatementStatusPending StatementStatus = "pending"
	// StatementStatusRunning indicates the query is not finished yet.
	StatementStatusRunning StatementStatus = "running"
	// StatementStatusFinished indicates the query is finished.
	StatementStatusFinished StatementStatus = "finished"
	// StatementStatusFailed indicates the query is failed.
	StatementStatusFailed StatementStatus = "failed"
	// StatementStatusCancelled indicates the query is cancelled.
	StatementStatusCancelled StatementStatus = "cancelled"
)

// Finished returns true if the statement is finished.
func (s StatementStatus) Finished() bool {
	switch s {
	case StatementStatusFinished:
		return true
	case StatementStatusRunning, StatementStatusPending, StatementStatusFailed, StatementStatusCancelled:
		return false
	default:
		return false
	}
}

// Terminated returns true if the statement is finished, failed, or cancelled.
func (s StatementStatus) Terminated() bool {
	switch s {
	case StatementStatusFinished, StatementStatusFailed, StatementStatusCancelled:
		return true
	case StatementStatusRunning, StatementStatusPending:
		return false
	default:
		return false
	}
}

// StatementProgress is a struct that represents the progress of a statement.
type StatementProgress struct {
	// TotalPercentage denotes the total progress in percentage: [0.0, 100.0].
	TotalPercentage float64 `json:"total_percentage"`
	// NanosFromSubmitted denotes the duration in nanoseconds since the statement is submitted.
	NanosFromSubmitted int64 `json:"nanos_from_submitted"`
	// NanosFromStarted denotes the duration in nanoseconds since the statement is started.
	NanosFromStarted int64 `json:"nanos_from_started"`
	// TotalStages denotes the total number of stages to execute.
	TotalStages int64 `json:"total_stages"`
	// TotalPartitions denotes the estimated total number of partitions to scan.
	TotalPartitions int64 `json:"total_partitions"`
	// TotalRows denotes the estimated total number of rows to scan.
	TotalRows int64 `json:"total_rows"`
	// TotalCompressedBytes denotes the estimated total number of compressed bytes to scan.
	TotalCompressedBytes int64 `json:"total_compressed_bytes"`
	// TotalUncompressedBytes denotes the estimated total number of uncompressed bytes to scan.
	TotalUncompressedBytes int64 `json:"total_uncompressed_bytes"`
	// TotalStages denotes the total number of stages executed.
	ScannedStages int64 `json:"scanned_stages"`
	// ScannedPartitions denotes the number of partitions scanned.
	ScannedPartitions int64 `json:"scanned_partitions"`
	// ScannedRows denotes the number of rows scanned.
	ScannedRows int64 `json:"scanned_rows"`
	// ScannedCompressedBytes denotes the number of compressed bytes scanned.
	ScannedCompressedBytes int64 `json:"scanned_compressed_bytes"`
	// ScannedUncompressedBytes denotes the number of uncompressed bytes scanned.
	ScannedUncompressedBytes int64 `json:"scanned_uncompressed_bytes"`
}
