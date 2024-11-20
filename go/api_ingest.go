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
	"io"
	"net/url"

	"github.com/apache/arrow/go/v17/arrow"
)

// ingestAPI defines interfaces under /v1/ingest.
type ingestAPI interface {
	// createIngestChannel creates a new ingest channel and returns the channel ID.
	createIngestChannel(ctx context.Context) (string, error)
	// ingestData ingests data into the specified channel.
	ingestData(ctx context.Context, channel string, req *ingestDataRequest) error
	// commitIngest commits the specified ingest channel.
	commitIngest(ctx context.Context, channel string, req *commitIngestRequest) error
	// abortIngest aborts the specified ingest channel.
	abortIngest(ctx context.Context, channel string) error
}

var _ ingestAPI = (*Connection)(nil)

type createIngestChannelResponse struct {
	Id string `json:"ingest_id"`
}

type ingestDataRequest struct {
	Data *ingestData `json:"data"`
}

type ingestData struct {
	// Rows is a base64 encoded string, contains arrow record batches
	Rows string `json:"rows"`
}

type commitIngestRequest struct {
	// Statement is a ScopeQL statement to process and insert data.
	//
	// The statement must end with an insert clause or a merge clause, and all the intermediate
	// clauses must be read-only transformations.
	Statement string `json:"statement"`
}

// Ingester stages and ingests data into ScopeDB.
type Ingester struct {
	conn    *Connection
	channel string
}

// NewIngester creates a new ingester for ingesting data.
func NewIngester(ctx context.Context, conn *Connection) (*Ingester, error) {
	channel, err := conn.createIngestChannel(ctx)
	if err != nil {
		return nil, err
	}
	return &Ingester{conn: conn, channel: channel}, nil
}

// IngestData ingests data into the specified channel.
func (i *Ingester) IngestData(ctx context.Context, batches []arrow.Record) error {
	rows, err := encodeRecordBatches(batches)
	if err != nil {
		return err
	}
	return i.conn.ingestData(ctx, i.channel, &ingestDataRequest{
		Data: &ingestData{
			Rows: string(rows),
		},
	})
}

// Commit commits this ingester. Once committed, the ingester is invalid and cannot ingest more data.
func (i *Ingester) Commit(ctx context.Context, statement string) error {
	return i.conn.commitIngest(ctx, i.channel, &commitIngestRequest{
		Statement: statement,
	})
}

// Abort aborts this ingester. Once aborted, the ingester is invalid and the staging data is discarded.
func (i *Ingester) Abort(ctx context.Context) error {
	return i.conn.abortIngest(ctx, i.channel)
}

func (conn *Connection) createIngestChannel(ctx context.Context) (string, error) {
	req, err := url.Parse(conn.config.Endpoint + "/v1/ingest")
	if err != nil {
		return "", err
	}

	resp, err := conn.http.Post(ctx, req, nil)
	if err != nil {
		return "", err
	}
	defer sneakyBodyClose(resp.Body)
	if err := checkStatusCodeOK(resp); err != nil {
		return "", err
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	var respData createIngestChannelResponse
	err = json.Unmarshal(data, &respData)
	return respData.Id, err
}

func (conn *Connection) ingestData(ctx context.Context, channel string, request *ingestDataRequest) error {
	req, err := url.Parse(conn.config.Endpoint + "/v1/ingest/" + channel)
	if err != nil {
		return err
	}

	body, err := json.Marshal(request)
	if err != nil {
		return err
	}

	resp, err := conn.http.Post(ctx, req, body)
	if err != nil {
		return err
	}
	defer sneakyBodyClose(resp.Body)
	if err := checkStatusCodeOK(resp); err != nil {
		return err
	}
	return nil
}

func (conn *Connection) commitIngest(ctx context.Context, channel string, request *commitIngestRequest) error {
	req, err := url.Parse(conn.config.Endpoint + "/v1/ingest/" + channel + "/commit")
	if err != nil {
		return err
	}

	body, err := json.Marshal(request)
	if err != nil {
		return err
	}

	resp, err := conn.http.Post(ctx, req, body)
	if err != nil {
		return err
	}
	defer sneakyBodyClose(resp.Body)
	return checkStatusCodeOK(resp)
}

func (conn *Connection) abortIngest(ctx context.Context, channel string) error {
	req, err := url.Parse(conn.config.Endpoint + "/v1/ingest/" + channel + "/abort")
	if err != nil {
		return err
	}

	resp, err := conn.http.Post(ctx, req, nil)
	if err != nil {
		return err
	}
	defer sneakyBodyClose(resp.Body)
	return checkStatusCodeOK(resp)
}
