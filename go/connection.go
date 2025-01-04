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

	"github.com/apache/arrow/go/v17/arrow"
)

type Connection struct {
	config *Config
	http   HTTPClient
}

// Open creates a new connection.
func Open(config *Config) *Connection {
	return &Connection{
		config: config,
		http:   NewHTTPClient(),
	}
}

// Close closes the database connection.
//
// You don't typically need to call this as the garbage collector will release
// the resources when the connection is no longer referenced. However, it can be
// useful to call this if you want to release the resources immediately.
func (conn *Connection) Close() {
	conn.http.Close()
}

// Execute submits a statement to the server, waits for it to finish, and ignores the result.
func (conn *Connection) Execute(ctx context.Context, req *StatementRequest) error {
	resp, err := conn.submitStatement(ctx, req)
	if err != nil {
		return err
	}
	if resp.Status == StatementStatusFinished {
		return nil
	}
	f := NewResultSetFetcher(conn, &FetchStatementParams{
		StatementId: resp.StatementId,
		Format:      req.Format,
	})
	_, err = f.FetchResultSet(ctx)
	return err
}

// SubmitStatement submits a statement to the server and returns immediately.
//
// The Status of the returned StatementResponse may be StatementStatusFinished if the query is finished immediately.
// If so, the result set is in ArrowJSONFormat. Otherwise, you can fetch the result set by NewResultSetFetcher
// and calling 'FetchResultSet'.
func (conn *Connection) SubmitStatement(ctx context.Context, statement string) (*StatementResponse, error) {
	return conn.submitStatement(ctx, &StatementRequest{
		Statement: statement,
		Format:    ArrowJSONFormat,
	})
}

// CancelStatement cancels a statement by its ID.
func (conn *Connection) CancelStatement(ctx context.Context, statementId string) error {
	return conn.cancelStatement(ctx, statementId)
}

// QueryAsArrowBatch submits a query to the server and returns the result set as Arrow's record batches.
func (conn *Connection) QueryAsArrowBatch(ctx context.Context, req *StatementRequest) (*ArrowResultSet, error) {
	if err := checkResultFormat(req.Format, ArrowJSONFormat); err != nil {
		return nil, err
	}

	resp, err := conn.submitStatement(ctx, req)
	if err != nil {
		return nil, err
	}
	if resp.Status == StatementStatusFinished {
		return resp.ToArrowResultSet()
	}
	return conn.FetchResultSetAsArrowBatch(ctx, &FetchStatementParams{
		StatementId: resp.StatementId,
		Format:      req.Format,
	})
}

// FetchResultSetAsArrowBatch fetches the result set of the specified statement as Arrow's record batches.
func (conn *Connection) FetchResultSetAsArrowBatch(ctx context.Context, params *FetchStatementParams) (*ArrowResultSet, error) {
	if err := checkResultFormat(params.Format, ArrowJSONFormat); err != nil {
		return nil, err
	}

	f := NewResultSetFetcher(conn, params)
	resp, err := f.FetchResultSet(ctx)
	if err != nil {
		return nil, err
	}
	return resp.ToArrowResultSet()
}

// IngestArrowBatch ingests the specified Arrow record batches into ScopeDB.
func (conn *Connection) IngestArrowBatch(ctx context.Context, batches []arrow.Record, statement string) (*IngestResponse, error) {
	rows, err := encodeRecordBatches(batches)
	if err != nil {
		return nil, err
	}

	return conn.ingest(ctx, &ingestRequest{
		Data: &ingestData{
			Format: ingestFormatArrow,
			Rows:   string(rows),
		},
		Statement: statement,
	})
}
