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
	"compress/gzip"
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/google/uuid"
)

// Client is the major entrance to construct structs for interacting with ScopeDB.
type Client struct {
	config *Config
	http   *httpClient
}

// NewClient creates a new ScopeDB client with the given configuration.
func NewClient(config *Config) *Client {
	return &Client{
		config: config,
		http: &httpClient{
			client: http.DefaultClient,
		},
	}
}

// Close closes the ScopeDB client and release all associated resources.
//
// You don't typically need to call this as the garbage collector will release
// the resources when the connection is no longer referenced. However, it can be
// useful to call this if you want to release the resources immediately.
func (c *Client) Close() {
	c.http.Close()
}

// httpClient is a wrapper around the standard http.Client to decorate GET/POST requests.
type httpClient struct {
	client *http.Client
}

// doGet sends a GET request to the ScopeDB server.
func (c *httpClient) doGet(ctx context.Context, u *url.URL) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.client.Do(req)
	return resp, err
}

// doPost sends a POST request to the ScopeDB server.
func (c *httpClient) doPost(ctx context.Context, u *url.URL, body []byte) (*http.Response, error) {
	uncompressedContentLength := len(body)

	var b bytes.Buffer
	g := gzip.NewWriter(&b)
	if _, err := g.Write(body); err != nil {
		return nil, err
	}
	if err := g.Close(); err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), &b)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")
	req.Header.Set("X-ScopeDB-Uncompressed-Content-Length", strconv.Itoa(uncompressedContentLength))
	resp, err := c.client.Do(req)
	return resp, err
}

// Close closes the HTTP client.
//
// You don't typically need to call this as the garbage collector will release
// the resources when the client is no longer referenced. However, it can be
// useful to call this if you want to release the resources immediately.
func (c *httpClient) Close() {
	c.client.CloseIdleConnections()
}

type statementRequest struct {
	StatementID *uuid.UUID   `json:"statement_id,omitempty"`
	Statement   string       `json:"statement"`
	ExecTimeout string       `json:"exec_timeout,omitempty"`
	Format      ResultFormat `json:"format"`
}

type statementResponse struct {
	ID       uuid.UUID         `json:"statement_id"`
	Progress StatementProgress `json:"progress"`
	Status   StatementStatus   `json:"status"`
	Created  time.Time         `json:"created_at"`

	// Message is set when the statement was failed or canceled.
	Message *string `json:"message"`

	// ResultSet is set when the statement was successfully finished.
	ResultSet *resultSet `json:"result_set"`
}

type resultSet struct {
	Metadata *resultSetMetadata `json:"metadata"`
	Format   ResultFormat       `json:"format"`
	Rows     json.RawMessage    `json:"rows"`
}

type resultSetMetadata struct {
	Fields  []*resultSetField `json:"fields"`
	NumRows uint64            `json:"num_rows"`
}

type resultSetField struct {
	Name     string `json:"name"`
	DataType string `json:"data_Type"`
}

func (rs *resultSet) toResultSet() *ResultSet {
	schema := make(Schema, len(rs.Metadata.Fields))
	for i, field := range rs.Metadata.Fields {
		schema[i] = &FieldSchema{
			Name: field.Name,
			Type: DataType(field.DataType),
		}
	}

	return &ResultSet{
		TotalRows: rs.Metadata.NumRows,
		Schema:    schema,
		Format:    rs.Format,
		rows:      rs.Rows,
	}
}

func (c *Client) submitStatement(ctx context.Context, request *statementRequest) (*statementResponse, error) {
	req, err := url.Parse(c.config.Endpoint + "/v1/statements")
	if err != nil {
		return nil, err
	}

	body, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	resp, err := c.http.doPost(ctx, req, body)
	if err != nil {
		return nil, err
	}
	defer sneakyBodyClose(resp.Body)
	return checkStatementResponse(resp)
}

func (c *Client) fetchStatementResult(ctx context.Context, id uuid.UUID, format ResultFormat) (*statementResponse, error) {
	req, err := url.Parse(c.config.Endpoint + "/v1/statements/" + id.String())
	if err != nil {
		return nil, err
	}

	q := req.Query()
	q.Add("format", string(format))
	req.RawQuery = q.Encode()

	resp, err := c.http.doGet(ctx, req)
	if err != nil {
		return nil, err
	}
	defer sneakyBodyClose(resp.Body)
	return checkStatementResponse(resp)
}

type statementCancelResponse struct {
	Status  StatementStatus `json:"status"`
	Message string          `json:"message"`
}

func (c *Client) cancelStatement(ctx context.Context, statementID uuid.UUID) (*statementCancelResponse, error) {
	req, err := url.Parse(c.config.Endpoint + "/v1/statements/" + statementID.String() + "/cancel")
	if err != nil {
		return nil, err
	}

	resp, err := c.http.doPost(ctx, req, []byte{})
	if err != nil {
		return nil, err
	}
	defer sneakyBodyClose(resp.Body)
	return checkStatementCancelResponse(resp)
}

type writeFormat string

const (
	// writeFormatArrow is to ingest rows as base64 encoded arrow batches.
	writeFormatArrow writeFormat = "arrow"
	// writeFormatJSON is to ingest rows as JSON lines.
	writeFormatJSON writeFormat = "json"
)

type ingestRequest struct {
	Data      *ingestData `json:"data"`
	Statement string      `json:"statement"`
}

type ingestData struct {
	// Format is the format of the data to ingest.
	Format writeFormat `json:"format"`
	// Rows is the payload of the data to ingest.
	Rows string `json:"rows"`
}

type ingestResponse struct {
	NumRowsInserted int `json:"num_rows_inserted"`
}

func (c *Client) ingest(ctx context.Context, request *ingestRequest) (*ingestResponse, error) {
	req, err := url.Parse(c.config.Endpoint + "/v1/ingest")
	if err != nil {
		return nil, err
	}

	body, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	resp, err := c.http.doPost(ctx, req, body)
	if err != nil {
		return nil, err
	}
	defer sneakyBodyClose(resp.Body)
	return checkIngestResponse(resp)
}
