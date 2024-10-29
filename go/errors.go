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
	"fmt"
	"io"
)

func checkStatusCodeOK(actual int) error {
	return checkStatusCode(actual, 200)
}

func checkStatusCode(actual int, expected int) error {
	if actual != expected {
		return fmt.Errorf("unexpected status code: %d", actual)
	}
	return nil
}

func checkResultFormat(actual ResultFormat, expected ResultFormat) error {
	if actual != expected {
		return fmt.Errorf("unexpected result format: %s", actual)
	}
	return nil
}

// sneakyBodyClose closes the body and ignores the error.
// This is useful to close the HTTP response body when we don't care about the error.
func sneakyBodyClose(body io.ReadCloser) {
	if body != nil {
		_ = body.Close()
	}
}
