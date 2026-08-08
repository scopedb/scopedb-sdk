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
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"
)

// Client provides access to ScopeDB APIs.
type Client struct {
	endpoint *url.URL
	http     *httpClient
}

// NewClient creates a new ScopeDB client with the given configuration.
func NewClient(config Config) (*Client, error) {
	endpoint, err := normalizeEndpoint(config.Endpoint)
	if err != nil {
		return nil, newError(
			ErrorKindConfigInvalid,
			fmt.Sprintf("invalid ScopeDB endpoint: %v", err),
			err,
		)
	}

	compression := requestCompression(config)
	if compression != CompressionZstd && compression != CompressionGzip {
		return nil, newError(
			ErrorKindConfigInvalid,
			fmt.Sprintf("unsupported compression: %q", compression),
			nil,
		)
	}

	standardClient := config.HTTPClient
	owned := false
	if standardClient == nil {
		transport := independentDefaultTransport()
		standardClient = &http.Client{Transport: transport}
		owned = true
	}

	return &Client{
		endpoint: endpoint,
		http: &httpClient{
			client:        standardClient,
			authorization: bearerAuthorization(config),
			compression:   compression,
			owned:         owned,
		},
	}, nil
}

func independentDefaultTransport() *http.Transport {
	if transport, ok := http.DefaultTransport.(*http.Transport); ok && transport != nil {
		return transport.Clone()
	}

	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
	}
}

// Close releases idle connections owned by the client. It does not close a
// caller-provided HTTP client.
func (c *Client) Close() {
	c.http.Close()
}

// httpClient is a wrapper around the standard http.Client to decorate GET/POST requests.
type httpClient struct {
	client        *http.Client
	authorization string
	compression   Compression
	owned         bool
}

// doGet sends a GET request to the ScopeDB server.
func (c *httpClient) doGet(ctx context.Context, u *url.URL) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	return c.do(req)
}

// doPost sends a POST request to the ScopeDB server.
func (c *httpClient) doPost(ctx context.Context, u *url.URL, body []byte) (*http.Response, error) {
	uncompressedContentLength := len(body)

	compressedBody, compression, err := compressRequestBody(body, c.compression)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), &compressedBody)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", string(compression))
	req.Header.Set("X-ScopeDB-Uncompressed-Content-Length", strconv.Itoa(uncompressedContentLength))
	return c.do(req)
}

func (c *httpClient) do(req *http.Request) (*http.Response, error) {
	c.applyAuthorization(req)
	return c.client.Do(req)
}

func (c *httpClient) applyAuthorization(req *http.Request) {
	if c.authorization == "" {
		return
	}
	req.Header.Set("Authorization", c.authorization)
}

// Close releases idle connections when this wrapper owns its HTTP client.
func (c *httpClient) Close() {
	if c.owned {
		c.client.CloseIdleConnections()
	}
}

func bearerAuthorization(config Config) string {
	if config.APIKey == "" {
		return ""
	}
	return "Bearer " + config.APIKey
}

func requestCompression(config Config) Compression {
	if config.Compression == "" {
		return CompressionZstd
	}
	return config.Compression
}

func normalizeEndpoint(raw string) (*url.URL, error) {
	endpoint, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	if endpoint.Scheme != "http" && endpoint.Scheme != "https" {
		return nil, fmt.Errorf("endpoint scheme must be http or https")
	}
	if endpoint.Host == "" {
		return nil, fmt.Errorf("endpoint host is required")
	}
	if endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return nil, fmt.Errorf("endpoint must not contain a query or fragment")
	}
	return endpoint, nil
}

func (c *Client) resourceURL(segments ...string) (*url.URL, error) {
	rawPath := strings.TrimRight(c.endpoint.EscapedPath(), "/") + "/v1"
	for _, segment := range segments {
		rawPath += "/" + url.PathEscape(segment)
	}
	path, err := url.PathUnescape(rawPath)
	if err != nil {
		return nil, newError(ErrorKindConfigInvalid, "invalid ScopeDB endpoint path", err)
	}

	u := *c.endpoint
	u.Path = path
	u.RawPath = rawPath
	return &u, nil
}

func compressRequestBody(body []byte, compression Compression) (bytes.Buffer, Compression, error) {
	var b bytes.Buffer

	switch compression {
	case CompressionZstd:
		zw, err := zstd.NewWriter(&b)
		if err != nil {
			return bytes.Buffer{}, "", err
		}
		if _, err := zw.Write(body); err != nil {
			return bytes.Buffer{}, "", errors.Join(err, zw.Close())
		}
		if err := zw.Close(); err != nil {
			return bytes.Buffer{}, "", err
		}
	case CompressionGzip:
		gw := gzip.NewWriter(&b)
		if _, err := gw.Write(body); err != nil {
			return bytes.Buffer{}, "", err
		}
		if err := gw.Close(); err != nil {
			return bytes.Buffer{}, "", err
		}
	default:
		return bytes.Buffer{}, "", fmt.Errorf("unsupported compression: %q", compression)
	}

	return b, compression, nil
}

type resultFormat string

const resultFormatJSON resultFormat = "json"

type statementRequest struct {
	StatementID *uuid.UUID   `json:"statement_id,omitempty"`
	Statement   string       `json:"statement"`
	ExecTimeout string       `json:"exec_timeout,omitempty"`
	Format      resultFormat `json:"format"`
}

type statementResponse struct {
	ID       uuid.UUID         `json:"statement_id"`
	Progress StatementProgress `json:"progress"`
	Status   StatementStatus   `json:"status"`
	Created  time.Time         `json:"created_at"`

	// Message is set when the statement was failed or canceled.
	Message *string `json:"message"`
	// Error is set when the statement failed with structured details.
	Error *StatementErrorDetails `json:"error"`

	// ResultSet is set when the statement was successfully finished.
	ResultSet *resultSet `json:"result_set"`
}

type resultSet struct {
	Metadata *resultSetMetadata `json:"metadata"`
	Format   resultFormat       `json:"format"`
	Rows     json.RawMessage    `json:"rows"`
}

type resultSetMetadata struct {
	Fields  []*resultSetField `json:"fields"`
	NumRows uint64            `json:"num_rows"`
}

type resultSetField struct {
	Name     string   `json:"name"`
	DataType DataType `json:"data_type"`
}

func (rs *resultSet) toResultSet() *ResultSet {
	if rs == nil || rs.Metadata == nil {
		return nil
	}
	schema := make(Schema, len(rs.Metadata.Fields))
	for i, field := range rs.Metadata.Fields {
		if field == nil {
			return nil
		}
		schema[i] = &FieldSchema{
			Name: field.Name,
			Type: field.DataType,
		}
	}

	return &ResultSet{
		TotalRows: rs.Metadata.NumRows,
		Schema:    schema,
		rows:      rs.Rows,
	}
}

func (c *Client) submitStatement(ctx context.Context, request *statementRequest) (*statementResponse, error) {
	req, err := c.resourceURL("statements")
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

func (c *Client) fetchStatementResult(ctx context.Context, id uuid.UUID) (*statementResponse, error) {
	req, err := c.resourceURL("statements", id.String())
	if err != nil {
		return nil, err
	}

	q := req.Query()
	q.Add("format", string(resultFormatJSON))
	req.RawQuery = q.Encode()

	resp, err := c.http.doGet(ctx, req)
	if err != nil {
		return nil, err
	}
	defer sneakyBodyClose(resp.Body)
	return checkStatementResponse(resp)
}

type statementCancelResponse = StatementCancelResult

func (c *Client) cancelStatement(ctx context.Context, statementID uuid.UUID) (*statementCancelResponse, error) {
	req, err := c.resourceURL("statements", statementID.String(), "cancel")
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

// writeFormatJSON is to ingest rows as JSON lines.
const writeFormatJSON writeFormat = "json"

type ingestRequest struct {
	Data      ingestData `json:"data"`
	Statement string     `json:"statement"`
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
	req, err := c.resourceURL("ingest")
	if err != nil {
		return nil, err
	}

	body, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	resp, err := c.http.doPost(ctx, req, body)
	if err != nil {
		return nil, unknownIngestCommitOutcomeError(nil, err)
	}
	defer sneakyBodyClose(resp.Body)
	return checkIngestResponse(resp)
}
