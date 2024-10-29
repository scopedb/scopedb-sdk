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
