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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
)

const ingestCommitOutcomeUnknownMessage = "ingest commit outcome is unknown; the request may have committed, so replay is not known to be safe"

// ErrorKind identifies the operation-level category of a ScopeDB error.
type ErrorKind string

const (
	// ErrorKindUnexpected is an error that has no more specific classification.
	ErrorKindUnexpected ErrorKind = "Unexpected"
	// ErrorKindConfigInvalid indicates invalid client configuration or arguments.
	ErrorKindConfigInvalid ErrorKind = "ConfigInvalid"
	// ErrorKindStatementFailed indicates a failed or cancelled statement.
	ErrorKindStatementFailed ErrorKind = "StatementFailed"
	// ErrorKindAppendRowsFailed indicates a rejected append or an unknown commit outcome.
	ErrorKindAppendRowsFailed ErrorKind = "AppendRowsFailed"
)

// Error represents a ScopeDB client or API error.
type Error struct {
	Kind          ErrorKind
	Message       string
	HTTPStatus    int
	RequestID     string
	RetryAfter    time.Duration
	Retryable     bool
	AppendDetails *AppendErrorDetails
	// StatementDetails contains the structured server failure for a failed
	// statement, when available.
	StatementDetails *StatementErrorDetails

	cause error
}

// Error returns the server message unchanged when the error came from ScopeDB.
func (e *Error) Error() string {
	return e.Message
}

// Unwrap returns the transport, decoding, or configuration error that caused this error.
func (e *Error) Unwrap() error {
	return e.cause
}

func newError(kind ErrorKind, message string, cause error) *Error {
	return &Error{Kind: kind, Message: message, cause: cause}
}

type apiErrorPayload struct {
	Message            string           `json:"message"`
	RequestID          string           `json:"request_id"`
	Retryable          *bool            `json:"retryable"`
	AppendState        AppendState      `json:"append_state"`
	RowErrors          []AppendRowError `json:"row_errors"`
	RowErrorsTruncated bool             `json:"row_errors_truncated"`
	Nested             *apiErrorPayload `json:"error"`
}

func responseError(resp *http.Response, body []byte, kind ErrorKind) *Error {
	message := string(body)
	if message == "" {
		message = resp.Status
		if message == "" {
			message = fmt.Sprintf("HTTP %d", resp.StatusCode)
		}
	}

	var payload apiErrorPayload
	parsed := json.Unmarshal(body, &payload) == nil
	if parsed && payload.Message == "" && payload.Nested != nil {
		nested := *payload.Nested
		if nested.RequestID == "" {
			nested.RequestID = payload.RequestID
		}
		if nested.Retryable == nil {
			nested.Retryable = payload.Retryable
		}
		payload = nested
	}
	if parsed && payload.Message != "" {
		message = payload.Message
	}

	err := &Error{
		Kind:       kind,
		Message:    message,
		HTTPStatus: resp.StatusCode,
		RequestID:  resp.Header.Get("X-Request-ID"),
		RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()),
		Retryable: resp.StatusCode == http.StatusRequestTimeout ||
			resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500,
	}
	if parsed {
		if payload.RequestID != "" {
			err.RequestID = payload.RequestID
		}
		if payload.Retryable != nil {
			err.Retryable = *payload.Retryable
		}
		if payload.AppendState == AppendStateRejected || payload.AppendState == AppendStateUnknown {
			rowErrors := payload.RowErrors
			if rowErrors == nil {
				rowErrors = []AppendRowError{}
			}
			err.AppendDetails = &AppendErrorDetails{
				AppendState:        payload.AppendState,
				RowErrors:          rowErrors,
				RowErrorsTruncated: payload.RowErrorsTruncated,
			}
		}
	}
	return err
}

func responseMetadataError(
	resp *http.Response,
	kind ErrorKind,
	message string,
	cause error,
) *Error {
	return &Error{
		Kind:       kind,
		Message:    message,
		HTTPStatus: resp.StatusCode,
		RequestID:  resp.Header.Get("X-Request-ID"),
		RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()),
		Retryable: resp.StatusCode == http.StatusRequestTimeout ||
			resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500,
		cause: cause,
	}
}

func parseRetryAfter(value string, now time.Time) time.Duration {
	if value == "" {
		return 0
	}
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if retryAt, err := http.ParseTime(value); err == nil {
		if delay := retryAt.Sub(now); delay > 0 {
			return delay
		}
	}
	return 0
}

func checkStatementResponse(resp *http.Response) (*statementResponse, error) {
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, responseMetadataError(
			resp,
			ErrorKindUnexpected,
			"failed to read statement response",
			err,
		)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, responseError(resp, data, ErrorKindUnexpected)
	}

	var stmtResp statementResponse
	if err := json.Unmarshal(data, &stmtResp); err != nil {
		return nil, responseMetadataError(
			resp,
			ErrorKindUnexpected,
			"failed to decode statement response",
			err,
		)
	}
	if stmtResp.Status == "" {
		return nil, responseMetadataError(
			resp,
			ErrorKindUnexpected,
			"statement response is missing status",
			nil,
		)
	}
	if err := validateStatementResponse(&stmtResp); err != nil {
		return nil, responseMetadataError(
			resp,
			ErrorKindUnexpected,
			"invalid statement response: "+err.Error(),
			err,
		)
	}
	return &stmtResp, nil
}

func validateStatementResponse(resp *statementResponse) error {
	if resp.ID == uuid.Nil {
		return fmt.Errorf("statement_id is missing")
	}
	if resp.Created.IsZero() {
		return fmt.Errorf("created_at is missing")
	}
	switch resp.Status {
	case StatementStatusPending,
		StatementStatusRunning,
		StatementStatusCancelled:
		return nil
	case StatementStatusFailed:
		if resp.Error == nil {
			// Older servers returned only the top-level message.
			return nil
		}
		return validateStatementError(resp.Error)
	case StatementStatusFinished:
		return validateFinishedResult(resp.ResultSet)
	default:
		return fmt.Errorf("unknown status %q", resp.Status)
	}
}

func validateStatementError(statementError *StatementErrorDetails) error {
	if statementError.Code == "" {
		return fmt.Errorf("failed statement error has no code")
	}
	if statementError.Message == "" {
		return fmt.Errorf("failed statement error has no message")
	}

	switch statementError.Code {
	case StatementErrorCodeRowLimitExceeded:
		return validateStatementLimitDetails(
			statementError.Details,
			"total_rows",
			"max_total_rows",
		)
	case StatementErrorCodeScanLimitExceeded:
		return validateStatementLimitDetails(
			statementError.Details,
			"scanned_uncompressed_bytes",
			"max_scanned_uncompressed_bytes",
		)
	case StatementErrorCodePrepareError,
		StatementErrorCodeExecuteError,
		StatementErrorCodePendingTimeout,
		StatementErrorCodeExecutionTimeout,
		StatementErrorCodeHeartbeatLost:
		return nil
	default:
		return nil
	}
}

func validateStatementLimitDetails(details json.RawMessage, fields ...string) error {
	if len(details) == 0 {
		return fmt.Errorf("failed statement error has no details")
	}

	var object map[string]json.RawMessage
	if err := json.Unmarshal(details, &object); err != nil || object == nil {
		return fmt.Errorf("failed statement error details must be an object")
	}
	for _, field := range fields {
		raw, ok := object[field]
		if !ok {
			return fmt.Errorf("failed statement error details has no %s", field)
		}
		var value *uint64
		if err := json.Unmarshal(raw, &value); err != nil || value == nil {
			return fmt.Errorf("failed statement error detail %s must be an unsigned integer", field)
		}
	}
	return nil
}

func validateFinishedResult(result *resultSet) error {
	if result == nil {
		return fmt.Errorf("finished statement has no result_set")
	}
	if result.Metadata == nil {
		return fmt.Errorf("finished statement result_set has no metadata")
	}
	if result.Format != resultFormatJSON {
		return fmt.Errorf("unsupported result format %q", result.Format)
	}
	if result.Metadata.Fields == nil {
		return fmt.Errorf("finished statement result_set metadata has no fields")
	}
	for i, field := range result.Metadata.Fields {
		if field == nil {
			return fmt.Errorf("finished statement result_set field %d is null", i)
		}
		if !field.DataType.valid() {
			return fmt.Errorf(
				"finished statement result_set field %d has invalid data_type %q",
				i,
				field.DataType,
			)
		}
	}

	return nil
}

func checkStatementCancelResponse(resp *http.Response) (*statementCancelResponse, error) {
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, responseMetadataError(
			resp,
			ErrorKindUnexpected,
			"failed to read statement cancel response",
			err,
		)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, responseError(resp, data, ErrorKindUnexpected)
	}

	var stmtResp statementCancelResponse
	if err := json.Unmarshal(data, &stmtResp); err != nil {
		return nil, responseMetadataError(
			resp,
			ErrorKindUnexpected,
			"failed to decode statement cancel response",
			err,
		)
	}
	if stmtResp.Status == "" {
		return nil, responseMetadataError(
			resp,
			ErrorKindUnexpected,
			"statement cancel response is missing status",
			nil,
		)
	}
	if stmtResp.StatementID == uuid.Nil {
		return nil, responseMetadataError(
			resp,
			ErrorKindUnexpected,
			"statement cancel response is missing statement_id",
			nil,
		)
	}
	if stmtResp.CreatedAt.IsZero() {
		return nil, responseMetadataError(
			resp,
			ErrorKindUnexpected,
			"statement cancel response is missing created_at",
			nil,
		)
	}
	switch stmtResp.Status {
	case StatementStatusPending,
		StatementStatusRunning,
		StatementStatusFinished,
		StatementStatusFailed,
		StatementStatusCancelled:
	default:
		return nil, responseMetadataError(
			resp,
			ErrorKindUnexpected,
			fmt.Sprintf("statement cancel response has unknown status %q", stmtResp.Status),
			nil,
		)
	}
	return &stmtResp, nil
}

func checkIngestResponse(resp *http.Response) (*ingestResponse, error) {
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil, unknownIngestCommitOutcomeError(resp, err)
		}
		readErr := responseMetadataError(
			resp,
			ErrorKindUnexpected,
			"failed to read ingest response",
			err,
		)
		readErr.Retryable = false
		return nil, readErr
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		httpErr := responseError(resp, data, ErrorKindUnexpected)
		httpErr.Retryable = false
		return nil, httpErr
	}

	var wireResponse *struct {
		NumRowsInserted *int `json:"num_rows_inserted"`
	}
	if err := json.Unmarshal(data, &wireResponse); err != nil {
		return nil, unknownIngestCommitOutcomeError(resp, err)
	}
	if wireResponse == nil {
		return nil, unknownIngestCommitOutcomeError(
			resp,
			fmt.Errorf("ingest response body is null"),
		)
	}
	if wireResponse.NumRowsInserted == nil {
		return nil, unknownIngestCommitOutcomeError(
			resp,
			fmt.Errorf("ingest response is missing num_rows_inserted"),
		)
	}
	if *wireResponse.NumRowsInserted < 0 {
		return nil, unknownIngestCommitOutcomeError(
			resp,
			fmt.Errorf("ingest response reported negative num_rows_inserted"),
		)
	}
	return &ingestResponse{NumRowsInserted: *wireResponse.NumRowsInserted}, nil
}

func unknownIngestCommitOutcomeError(resp *http.Response, cause error) *Error {
	if resp == nil {
		return newError(
			ErrorKindUnexpected,
			ingestCommitOutcomeUnknownMessage,
			cause,
		)
	}
	err := responseMetadataError(
		resp,
		ErrorKindUnexpected,
		ingestCommitOutcomeUnknownMessage,
		cause,
	)
	err.Retryable = false
	return err
}

// sneakyBodyClose closes the body and ignores the error.
func sneakyBodyClose(body io.ReadCloser) {
	if body != nil {
		_ = body.Close()
	}
}
