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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// Error represents an error response from the ScopeDB server.
type Error struct {
	Message string `json:"message"`
}

func (e *Error) Error() string {
	return e.Message
}

func checkStatementResponse(resp *http.Response) (*statementResponse, error) {
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var stmtResp statementResponse
	_ = json.Unmarshal(data, &stmtResp)
	if stmtResp.Status != "" {
		return &stmtResp, nil
	}

	var errResp Error
	if err := json.Unmarshal(data, &errResp); err != nil {
		msg := string(data)
		return nil, fmt.Errorf("%d: %s", resp.StatusCode, msg)
	}
	return nil, &errResp
}

func checkStatementCancelResponse(resp *http.Response) (*statementCancelResponse, error) {
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var stmtResp statementCancelResponse
	if err := json.Unmarshal(data, &stmtResp); err == nil {
		return &stmtResp, nil
	}

	var errResp Error
	if err := json.Unmarshal(data, &errResp); err != nil {
		msg := string(data)
		return nil, fmt.Errorf("%d: %s", resp.StatusCode, msg)
	}
	return nil, &errResp
}

func checkIngestResponse(resp *http.Response) (*ingestResponse, error) {
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var stmtResp ingestResponse
	if err := json.Unmarshal(data, &stmtResp); err == nil {
		return &stmtResp, nil
	}

	var errResp Error
	if err := json.Unmarshal(data, &errResp); err != nil {
		msg := string(data)
		return nil, fmt.Errorf("%d: %s", resp.StatusCode, msg)
	}
	return nil, &errResp
}

// sneakyBodyClose closes the body and ignores the error.
// This is useful to close the HTTP response body when we don't care about the error.
func sneakyBodyClose(body io.ReadCloser) {
	if body != nil {
		_ = body.Close()
	}
}
