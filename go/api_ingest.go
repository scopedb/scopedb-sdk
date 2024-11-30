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
)

// ingestAPI defines interfaces under /v1/ingest.
type ingestAPI interface {
	// ingest ingests data with the specified statement.
	ingest(ctx context.Context, req *ingestRequest) (*IngestResponse, error)
}

var _ ingestAPI = (*Connection)(nil)

type ingestRequest struct {
	Data      *ingestData `json:"data"`
	Statement string      `json:"statement"`
}

type ingestData struct {
	// Rows is a base64 encoded string, contains arrow record batches
	Rows string `json:"rows"`
}

type IngestResponse struct {
	NumRowsInserted int `json:"num_rows_inserted"`
	NumRowsUpdated  int `json:"num_rows_updated"`
	NumRowsDeleted  int `json:"num_rows_deleted"`
}

func (conn *Connection) ingest(ctx context.Context, request *ingestRequest) (*IngestResponse, error) {
	// TODO(tisonkun): use /v1/ingest once server changes released.
	req, err := url.Parse(conn.config.Endpoint + "/v1/ingest_v2")
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
	var respData IngestResponse
	err = json.Unmarshal(data, &respData)
	return &respData, err
}
