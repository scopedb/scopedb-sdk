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

// CreateIngestChannel creates a new ingest channel and returns the channel ID.
func (conn *Connection) CreateIngestChannel(
	ctx context.Context,
	database string,
	schema string,
	table string,
	merge *MergeOption,
) (string, error) {
	req := &CreateIngestChannelRequest{
		Database: database,
		Schema:   schema,
		Table:    table,
		Merge:    merge,
	}
	return conn.createIngestChannel(ctx, req)
}

// IngestData ingests data into the specified channel.
func (conn *Connection) IngestData(ctx context.Context, channel string, batches []arrow.Record) error {
	rows, err := encodeRecordBatches(batches)
	if err != nil {
		return err
	}
	req := &IngestDataRequest{
		Data: &IngestData{
			Rows: string(rows),
		},
	}
	return conn.ingestData(ctx, channel, req)
}

// CommitIngestChannel commits the specified ingest channel.
func (conn *Connection) CommitIngestChannel(ctx context.Context, channel string) error {
	return conn.commitIngestChannel(ctx, channel)
}

// Execute submits a statement to the server, waits for it to finish, and ignores the result.
func (conn *Connection) Execute(ctx context.Context, req *StatementRequest) error {
	resp, err := conn.submitStatement(ctx, req)
	if err != nil {
		return err
	}
	if resp.Status == QueryStatusFinished {
		return nil
	}
	f := NewResultSetFetcher(conn, &FetchStatementParams{
		StatementId: resp.StatementId,
		Format:      req.Format,
	})
	_, err = f.FetchResultSet(ctx)
	return err
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
	if resp.Status == QueryStatusFinished {
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
