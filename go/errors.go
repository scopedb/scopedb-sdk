package scopedb

import (
	"errors"
	"fmt"
)

func checkStatusCodeOK(actual int) error {
	return checkStatusCode(actual, 200)
}

func checkStatusCode(actual int, expected int) error {
	if actual != expected {
		return errors.New(fmt.Sprintf("unexpected status code: %d", actual))
	}
	return nil
}

func checkResultFormat(actual ResultFormat, expected ResultFormat) error {
	if actual != expected {
		return errors.New(fmt.Sprintf("unexpected result format: %s", actual))
	}
	return nil
}
