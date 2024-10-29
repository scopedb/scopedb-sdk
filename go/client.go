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
}

type httpClient struct {
	client *http.Client
}

// NewHTTPClient creates a new internal HTTP client.
func NewHTTPClient() HTTPClient {
	return &httpClient{
		client: http.DefaultClient,
	}
}

// Ensure httpClient implements HTTPClient.
var _ HTTPClient = (*httpClient)(nil)

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
