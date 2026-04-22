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

// Compression defines the wire compression algorithm used for POST requests.
type Compression string

const (
	// CompressionZstd uses Zstandard compression.
	CompressionZstd Compression = "zstd"
	// CompressionGzip uses gzip compression.
	CompressionGzip Compression = "gzip"
)

// Config defines the configuration for the client.
type Config struct {
	// Endpoint is the URL of the ScopeDB service.
	Endpoint string `json:"endpoint"`
	// APIKey is the API key used for authentication.
	//
	// When provided, the client sends it as the Authorization header using the
	// Bearer scheme.
	APIKey string `json:"api_key"`
	// Compression controls how POST request bodies are compressed.
	//
	// The default is CompressionZstd. Set this to CompressionGzip to talk to
	// older deployments that do not support zstd yet.
	Compression Compression `json:"compression"`
}
