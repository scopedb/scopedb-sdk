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
	"time"

	"github.com/google/uuid"
)

// ResultFormat defines the format of the ResultSet.
type ResultFormat string

const (
	// ResultFormatArrow parses the result set as BASE64 encoded Arrow IPC format.
	ResultFormatArrow ResultFormat = "arrow"
	// ResultFormatJSON parses the result set as JSON lines.
	ResultFormatJSON ResultFormat = "json"
)

// Statement is a struct that represents a statement to be executed on ScopeDB.
type Statement struct {
	c *Client

	stmt string

	// ID of the statement.
	//
	// If provided, the ID must be a UUID, and ScopeDB will use the provided ID;
	// otherwise, ScopeDB will generate a random UUID for the statement submitted.
	ID *uuid.UUID
	// ExecTimeout is the maximum time to for statement execution.
	//
	// If the total execution time exceeds this value, the statement is failed
	// as timed out.
	//
	// Possible values like "1h".
	ExecTimeout string
	// ResultFormat is the format of the result set.
	ResultFormat ResultFormat
}

// Statement creates a new statement with the given ScopeQL statement.
func (c *Client) Statement(stmt string) *Statement {
	return &Statement{
		c:            c,
		stmt:         stmt,
		ResultFormat: ResultFormatJSON,
	}
}

// Submit submits the statement to ScopeDB for execution.
func (s *Statement) Submit(ctx context.Context) (*StatementHandle, error) {
	resp, err := s.c.submitStatement(ctx, &statementRequest{
		StatementID: s.ID,
		Statement:   s.stmt,
		ExecTimeout: s.ExecTimeout,
		Format:      s.ResultFormat,
	})
	if err != nil {
		return nil, err
	}

	return &StatementHandle{
		c:      s.c,
		resp:   resp,
		id:     resp.ID,
		Format: s.ResultFormat,
	}, nil
}

// Execute submits the statement to ScopeDB for execution and waits for its completion.
func (s *Statement) Execute(ctx context.Context) (*ResultSet, error) {
	handle, err := s.Submit(ctx)
	if err != nil {
		return nil, err
	}
	return handle.Fetch(ctx)
}

// StatementHandle is a handle to a statement that has been submitted to ScopeDB.
type StatementHandle struct {
	c    *Client
	resp *statementResponse

	id uuid.UUID

	// Format is the expected format of the ResultSet.
	Format ResultFormat
}

// StatementHandle creates a new StatementHandle with the given ID.
func (c *Client) StatementHandle(id uuid.UUID) *StatementHandle {
	return &StatementHandle{
		c:      c,
		resp:   nil,
		id:     id,
		Format: ResultFormatJSON,
	}
}

// Status returns the last seen status of the statement.
func (h *StatementHandle) Status() *StatementStatus {
	if h.resp == nil {
		return nil
	}
	return &h.resp.Status
}

// Progress returns the last seen progress of the statement.
func (h *StatementHandle) Progress() *StatementProgress {
	if h.resp == nil {
		return nil
	}
	return &h.resp.Progress
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

// FetchOnce fetches the result set of the statement once.
//
// If the last seen status is terminated, no fetch is performed.
func (h *StatementHandle) FetchOnce(ctx context.Context) error {
	if h.resp != nil && h.resp.Status.Terminated() {
		return nil
	}

	resp, err := h.c.fetchStatementResult(ctx, h.id, h.Format)
	if resp != nil {
		h.resp = resp
	}
	return err
}

// Fetch fetches the result set of the statement until it is finished, failed or cancelled.
//
// When the statement is finished, the result set is returned. Otherwise, an error is returned.
func (h *StatementHandle) Fetch(ctx context.Context) (*ResultSet, error) {
	tick := 5 * time.Millisecond
	maxTick := 1 * time.Second

	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	for {
		if h.resp != nil && h.resp.Status.Finished() {
			return h.resp.ResultSet.toResultSet(), nil
		}

		if tick < maxTick {
			tick = min(tick*2, maxTick)
			ticker.Reset(tick)
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			err := h.FetchOnce(ctx)
			if err != nil {
				return nil, err
			}
		}
	}
}

// Cancel cancels the statement if it is running or pending.
func (h *StatementHandle) Cancel(ctx context.Context) (*StatementStatus, error) {
	if h.resp != nil {
		switch h.resp.Status {
		case StatementStatusRunning, StatementStatusPending:
			// possible to cancel the statement
		case StatementStatusFinished, StatementStatusFailed, StatementStatusCancelled:
			return &h.resp.Status, nil
		}
	}

	resp, err := h.c.cancelStatement(ctx, h.id)
	if resp != nil {
		h.resp.Status = *resp
	}
	return resp, err
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
	// NanosToFinish denotes the estimated duration in nanoseconds to finish the statement.
	NanosToFinish int64 `json:"nanos_to_finish"`
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
