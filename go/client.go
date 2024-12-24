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
	"context"
	"net/http"
	"net/url"
)

// HTTPClient is the interface for HTTP client.
type HTTPClient interface {
	// Get sends a GET request to the ScopeDB server.
	Get(context.Context, *url.URL) (*http.Response, error)
	// Post sends a POST request to the ScopeDB server.
	Post(context.Context, *url.URL, []byte) (*http.Response, error)
	// Close closes the HTTP client.
	//
	// You don't typically need to call this as the garbage collector will release
	// the resources when the client is no longer referenced. However, it can be
	// useful to call this if you want to release the resources immediately.
	Close()
}

type httpClient struct {
	client *http.Client
}

var _ HTTPClient = (*httpClient)(nil)

// NewHTTPClient creates a new internal HTTP client.
func NewHTTPClient() HTTPClient {
	return &httpClient{
		client: http.DefaultClient,
	}
}

func (c *httpClient) Get(ctx context.Context, u *url.URL) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.client.Do(req)
	return resp, err
}

func (c *httpClient) Post(ctx context.Context, u *url.URL, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	return resp, err
}

func (c *httpClient) Close() {
	c.client.CloseIdleConnections()
}
