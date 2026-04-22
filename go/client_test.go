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
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

func TestHTTPClientDoPostUsesZstdByDefault(t *testing.T) {
	t.Parallel()

	expected := []byte(`{"statement":"select 1"}`)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Helper()
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, string(CompressionZstd), r.Header.Get("Content-Encoding"))
		require.Equal(t, strconv.Itoa(len(expected)), r.Header.Get("X-ScopeDB-Uncompressed-Content-Length"))

		actual, err := decodeCompressedRequestBody(r)
		require.NoError(t, err)
		require.Equal(t, expected, actual)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := NewClient(&Config{Endpoint: server.URL})
	reqURL, err := url.Parse(server.URL)
	require.NoError(t, err)

	resp, err := client.http.doPost(context.Background(), reqURL, expected)
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
}

func TestHTTPClientDoPostSupportsGzip(t *testing.T) {
	t.Parallel()

	expected := []byte(`{"statement":"select 1"}`)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Helper()
		require.Equal(t, string(CompressionGzip), r.Header.Get("Content-Encoding"))

		actual, err := decodeCompressedRequestBody(r)
		require.NoError(t, err)
		require.Equal(t, expected, actual)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := NewClient(&Config{
		Endpoint:    server.URL,
		Compression: CompressionGzip,
	})
	reqURL, err := url.Parse(server.URL)
	require.NoError(t, err)

	resp, err := client.http.doPost(context.Background(), reqURL, expected)
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
}

func TestHTTPClientDoPostRejectsUnsupportedCompression(t *testing.T) {
	t.Parallel()

	client := NewClient(&Config{
		Endpoint:    "http://example.com",
		Compression: Compression("brotli"),
	})
	reqURL, err := url.Parse("http://example.com")
	require.NoError(t, err)

	_, err = client.http.doPost(context.Background(), reqURL, []byte(`{}`))
	require.ErrorContains(t, err, `unsupported compression: "brotli"`)
}

func decodeCompressedRequestBody(r *http.Request) ([]byte, error) {
	compressedBody, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	switch Compression(r.Header.Get("Content-Encoding")) {
	case CompressionZstd:
		zr, err := zstd.NewReader(bytes.NewReader(compressedBody))
		if err != nil {
			return nil, err
		}
		defer zr.Close()
		return io.ReadAll(zr)
	case CompressionGzip:
		gr, err := gzip.NewReader(bytes.NewReader(compressedBody))
		if err != nil {
			return nil, err
		}
		defer gr.Close()
		return io.ReadAll(gr)
	default:
		return nil, io.ErrUnexpectedEOF
	}
}
