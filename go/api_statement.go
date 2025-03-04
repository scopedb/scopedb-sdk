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
	"io"
	"net/url"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/google/uuid"
)

// statementAPI defines interfaces under /v1/statements.
type statementAPI interface {
	// submitStatement submits a statement to the ScopeDB server and returns the statement ID.
	submitStatement(ctx context.Context, req *StatementRequest) (*StatementResponse, error)
	// fetchStatementResult fetches the result of a statement by its ID.
	fetchStatementResult(ctx context.Context, params *FetchStatementParams) (*StatementResponse, error)
	// cancelStatement cancels a statement by its ID.
	cancelStatement(ctx context.Context, statementId string) (*StatementStatus, error)
}

var _ statementAPI = (*Connection)(nil)

type StatementRequest struct {
	// StatementId is the ID of the statement.
	//
	// If provided, the ID must be a UUID, and the server uses the provided ID;
	// otherwise, the server will generate a new UUID for the statement submitted.
	StatementId *uuid.UUID `json:"statement_id,omitempty"`
	// Statement is the ScopeQL statement to execute.
	Statement string `json:"statement"`
	// WaitTimeout is the maximum time to wait for the statement to finish.
	// Possible values like "60s".
	WaitTimeout string `json:"wait_timeout,omitempty"`
	// ExecTimeout is the maximum time to for statement execution. If the total
	// execution time exceeds this value, the statement is failed timed out.
	// Possible values like "1h".
	ExecTimeout string `json:"exec_timeout,omitempty"`
	// Format is the format of the result set.
	Format ResultFormat `json:"format"`
}

type FetchStatementParams struct {
	// StatementId is the ID of the statement to fetch.
	StatementId string
	// Format is the format of the result set.
	Format ResultFormat
}

type ResultFormat string

const (
	// ArrowJSONFormat parses the result set as Arrow format with variant rendered as JSON (BASE64 encoded).
	ArrowJSONFormat ResultFormat = "arrow-json"
)

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

type StatementResponse struct {
	StatementId string            `json:"statement_id"`
	Progress    StatementProgress `json:"progress"`
	Status      StatementStatus   `json:"status"`
	ResultSet   *ResultSet        `json:"result_set"`
}

type StatementProgress struct {
	// TotalPercentage denotes the total progress in percentage: [0.0, 100.0].
	TotalPercentage float64 `json:"total_percentage"`

	// NanosFromSubmitted denotes the duration in nanoseconds since the statement is submitted.
	NanosFromSubmitted int64 `json:"nanos_from_submitted"`

	// NanosFromStarted denotes the duration in nanoseconds since the statement is started.
	NanosFromStarted int64 `json:"nanos_from_started"`

	// NanosToFinish denotes the estimated duration in nanoseconds to finish the statement.
	NanosToFinish int64 `json:"nanos_to_finish"`
}

type ResultSet struct {
	Metadata *ResultSetMetadata `json:"metadata"`
	Rows     string             `json:"rows"`
}

type ResultSetMetadata struct {
	Fields  []*ResultSetField `json:"fields"`
	Format  ResultFormat      `json:"format"`
	NumRows int64             `json:"num_rows"`
}

type ResultSetField struct {
	Name     string `json:"name"`
	DataType string `json:"data_Type"`
}

type ArrowResultSet struct {
	StatementId string
	Metadata    *ResultSetMetadata
	Records     *[]arrow.Record
}

func (rs *StatementResponse) ToArrowResultSet() (*ArrowResultSet, error) {
	if rs.ResultSet == nil {
		return nil, errors.New("result set is not available")
	}

	records, err := decodeRecordBatches([]byte(rs.ResultSet.Rows))
	if err != nil {
		return nil, err
	}
	return &ArrowResultSet{
		StatementId: rs.StatementId,
		Metadata:    rs.ResultSet.Metadata,
		Records:     &records,
	}, nil
}

type ResultSetFetcher struct {
	conn        *Connection
	fetchParams *FetchStatementParams
}

func NewResultSetFetcher(conn *Connection, params *FetchStatementParams) *ResultSetFetcher {
	return &ResultSetFetcher{
		conn:        conn,
		fetchParams: params,
	}
}

const (
	defaultFetchInterval = 1 * time.Second
)

// FetchResultSet fetches the result set of the configured fetch params.
//
// It polls the server until the query is finished, with fix delay of 1 second.
func (f *ResultSetFetcher) FetchResultSet(ctx context.Context) (*StatementResponse, error) {
	ticker := time.NewTicker(defaultFetchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			resp, err := f.FetchResultSetOnce(ctx)
			if err != nil {
				return nil, err
			}
			if resp.Status == StatementStatusFinished {
				return resp, nil
			}
		}
	}
}

// FetchResultSetOnce fetches the result set of the configured fetch params once.
//
// Either the query is finished or not, it returns the StatementResponse. This is useful
// when you would like to combine the fetch function with custom retry policy.
func (f *ResultSetFetcher) FetchResultSetOnce(ctx context.Context) (*StatementResponse, error) {
	return f.conn.fetchStatementResult(ctx, f.fetchParams)
}

func (conn *Connection) submitStatement(ctx context.Context, request *StatementRequest) (*StatementResponse, error) {
	req, err := url.Parse(conn.config.Endpoint + "/v1/statements")
	if err != nil {
		return nil, err
	}

	body, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	resp, err := conn.http.Post(ctx, req, body)
	if err != nil {
		return nil, err
	}
	defer sneakyBodyClose(resp.Body)
	if err := checkStatusCodeOK(resp); err != nil {
		return nil, err
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var respData StatementResponse
	err = json.Unmarshal(data, &respData)
	return &respData, err
}

type statementCancelResponse struct {
	Status StatementStatus `json:"status"`
}

func (conn *Connection) cancelStatement(ctx context.Context, statementId string) (*StatementStatus, error) {
	req, err := url.Parse(conn.config.Endpoint + "/v1/statements/" + statementId + "/cancel")
	if err != nil {
		return nil, err
	}

	resp, err := conn.http.Post(ctx, req, []byte{})
	if err != nil {
		return nil, err
	}
	defer sneakyBodyClose(resp.Body)
	if err := checkStatusCodeOK(resp); err != nil {
		return nil, err
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var respData statementCancelResponse
	err = json.Unmarshal(data, &respData)
	return &respData.Status, err
}

func (conn *Connection) fetchStatementResult(ctx context.Context, params *FetchStatementParams) (*StatementResponse, error) {
	req, err := url.Parse(conn.config.Endpoint + "/v1/statements/" + params.StatementId)
	if err != nil {
		return nil, err
	}
	q := req.Query()
	q.Add("format", string(params.Format))
	req.RawQuery = q.Encode()

	resp, err := conn.http.Get(ctx, req)
	if err != nil {
		return nil, err
	}
	defer sneakyBodyClose(resp.Body)
	if err := checkStatusCodeOK(resp); err != nil {
		return nil, err
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var respData StatementResponse
	err = json.Unmarshal(data, &respData)
	return &respData, err
}
