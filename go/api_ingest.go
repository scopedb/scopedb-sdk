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
	// createIngestChannel creates a new ingest channel and returns the channel ID.
	createIngestChannel(ctx context.Context, req *CreateIngestChannelRequest) (string, error)
	// ingestData ingests data into the specified channel.
	ingestData(ctx context.Context, channel string, req *IngestDataRequest) error
	// commitIngest commits the specified ingest channel.
	commitIngest(ctx context.Context, channel string) error
	// abortIngest aborts the specified ingest channel.
	abortIngest(ctx context.Context, channel string) error
}

var _ ingestAPI = (*Connection)(nil)

type CreateIngestChannelRequest struct {
	Database string       `json:"database"`
	Schema   string       `json:"schema"`
	Table    string       `json:"table"`
	Merge    *MergeOption `json:"merge"`
}

type MergeOption struct {
	SourceTableAlias       string        `json:"source_table_alias"`
	SourceTableColumnNames []string      `json:"source_table_column_names"`
	MatchCondition         string        `json:"match_condition"`
	When                   []MergeAction `json:"when"`
}

type MergeAction struct {
	Matched bool    `json:"matched"`
	And     *string `json:"and"`
	Then    string  `json:"then"`
}

type CreateIngestChannelResponse struct {
	Id string `json:"ingest_id"`
}

type IngestDataRequest struct {
	Data *IngestData `json:"data"`
}

type IngestData struct {
	// Rows is a base64 encoded string, contains arrow record batches
	Rows string `json:"rows"`
}

func (conn *Connection) createIngestChannel(ctx context.Context, request *CreateIngestChannelRequest) (string, error) {
	req, err := url.Parse(conn.config.Endpoint + "/v1/ingest")
	if err != nil {
		return "", err
	}

	body, err := json.Marshal(request)
	if err != nil {
		return "", err
	}

	resp, err := conn.http.Post(ctx, req, body)
	defer sneakyBodyClose(resp.Body)
	if err != nil {
		return "", err
	}
	if err := checkStatusCodeOK(resp.StatusCode); err != nil {
		return "", err
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	var respData CreateIngestChannelResponse
	err = json.Unmarshal(data, &respData)
	return respData.Id, err
}

func (conn *Connection) ingestData(ctx context.Context, channel string, request *IngestDataRequest) error {
	req, err := url.Parse(conn.config.Endpoint + "/v1/ingest/" + channel)
	if err != nil {
		return err
	}

	body, err := json.Marshal(request)
	if err != nil {
		return err
	}

	resp, err := conn.http.Post(ctx, req, body)
	defer sneakyBodyClose(resp.Body)
	if err != nil {
		return err
	}
	if err := checkStatusCodeOK(resp.StatusCode); err != nil {
		return err
	}
	return nil
}

func (conn *Connection) commitIngest(ctx context.Context, channel string) error {
	req, err := url.Parse(conn.config.Endpoint + "/v1/ingest/" + channel + "/commit")
	if err != nil {
		return err
	}

	resp, err := conn.http.Post(ctx, req, nil)
	defer sneakyBodyClose(resp.Body)
	if err != nil {
		return err
	}
	return checkStatusCodeOK(resp.StatusCode)
}

func (conn *Connection) abortIngest(ctx context.Context, channel string) error {
	req, err := url.Parse(conn.config.Endpoint + "/v1/ingest/" + channel + "/abort")
	if err != nil {
		return err
	}

	resp, err := conn.http.Post(ctx, req, nil)
	defer sneakyBodyClose(resp.Body)
	if err != nil {
		return err
	}
	return checkStatusCodeOK(resp.StatusCode)
}
