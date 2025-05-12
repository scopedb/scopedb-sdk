package scopedb

import (
	"bytes"
	"encoding/base64"
	"errors"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/ipc"
)

// encodeArrowBatches encodes the given record batches into a base64 encoded byte slice.
func encodeArrowBatches(schema *arrow.Schema, batches []arrow.Record) (payload []byte, err error) {
	if len(batches) == 0 {
		return nil, errors.New("cannot encode empty batches")
	}

	var buf bytes.Buffer
	defer func() {
		if err == nil {
			payload = buf.Bytes()
		}
	}()

	encoder := base64.NewEncoder(base64.StdEncoding, &buf)
	defer func() {
		err = errors.Join(err, encoder.Close())
	}()

	writer := ipc.NewWriter(encoder, ipc.WithSchema(schema))
	defer func() {
		err = errors.Join(err, writer.Close())
	}()

	for _, batch := range batches {
		if err := writer.Write(batch); err != nil {
			return nil, err
		}
	}
	return
}

// decodeArrowBatches decodes the given base64 encoded byte slice into record batches.
func decodeArrowBatches(data []byte) ([]arrow.Record, error) {
	decoder := base64.NewDecoder(base64.StdEncoding, bytes.NewReader(data))
	reader, err := ipc.NewReader(decoder, ipc.WithDelayReadSchema(true))
	if err != nil {
		return nil, err
	}

	batches := make([]arrow.Record, 0)
	for reader.Next() {
		batch := reader.Record()
		batch.Retain()
		batches = append(batches, batch)
	}
	return batches, nil
}
