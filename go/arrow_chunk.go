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
	"encoding/base64"
	"errors"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/ipc"
)

// encodeRecordBatches encodes the given record batches into a base64 encoded byte slice.
func encodeRecordBatches(batches []arrow.Record) (payload []byte, err error) {
	if len(batches) == 0 {
		return nil, errors.New("cannot ingest empty batches")
	}

	schema := batches[0].Schema()

	var buf bytes.Buffer
	encoder := base64.NewEncoder(base64.StdEncoding, &buf)
	writer := ipc.NewWriter(encoder, ipc.WithSchema(schema))

	for _, batch := range batches {
		if err := writer.Write(batch); err != nil {
			return nil, err
		}
	}

	if err := writer.Close(); err != nil {
		return nil, err
	}
	if err := encoder.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// decodeRecordBatches decodes the given base64 encoded byte slice into record batches.
func decodeRecordBatches(data []byte) ([]arrow.Record, error) {
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
