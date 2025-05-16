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
	"encoding/json"
	"errors"
	"math"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
)

const (
	defaultBatchSize     = 16 * 1024 * 1024 // default to 16 MiB
	defaultBatchInterval = time.Second      // default to 1 second
)

// ArrowBatchCable is a cable for sending Arrow batches to ScopeDB.
//
// You can create an ArrowBatchCable using the Client's ArrowBatchCable method,
// and start it using the Start method.
//
// Then, you can send Arrow batches using the Send method. Once the staged batches
// reach the specified BatchSize or BatchInterval, they will be sent to ScopeDB.
type ArrowBatchCable struct {
	c *Client

	schema      *arrow.Schema
	transforms  string
	currentSize uint64
	sendBatches []*arrowSendRecord
	sendBatchCh chan *arrowSendRecord

	// BatchSize is the maximum size in bytes of the batches to be sent.
	BatchSize uint64
	// BatchInterval is the maximum time to wait before sending the batches.
	BatchInterval time.Duration
}

type arrowSendRecord struct {
	record arrow.Record
	err    chan error
}

// ArrowBatchCable creates a new ArrowBatchCable with the specified schema and transforms.
//
// The cable must be started before sending batches, and all the batches sent must have the same schema
// as the one provided here.
//
// The transforms are ScopeQL statements that assume the data sent as the source table. The schema
// of the source table is the one provided here. The transforms must end with an INSERT statement.
// For example:
//
//	INSERT INTO my_table (col1, col2)
func (c *Client) ArrowBatchCable(schema *arrow.Schema, transforms string) *ArrowBatchCable {
	cable := &ArrowBatchCable{
		c:             c,
		schema:        schema,
		transforms:    transforms,
		currentSize:   0,
		sendBatches:   make([]*arrowSendRecord, 0),
		sendBatchCh:   make(chan *arrowSendRecord),
		BatchSize:     defaultBatchSize,
		BatchInterval: defaultBatchInterval,
	}

	return cable
}

// Start starts the ArrowBatchCable background task.
//
// It will receive batches that users Send, package them based on the BatchSize and BatchInterval,
// and send them to ScopeDB.
func (c *ArrowBatchCable) Start(ctx context.Context) {
	ticker := time.Tick(c.BatchInterval)
	batchSize := c.BatchSize

	go func() {
		stop, tick := false, false
		for {
			if tick || c.currentSize > batchSize {
				sendBatches := c.sendBatches
				go func() {
					batches := make([]arrow.Record, 0, len(sendBatches))
					for _, sendBatch := range sendBatches {
						batches = append(batches, sendBatch.record)
					}

					defer func() {
						for _, sendBatch := range sendBatches {
							sendBatch.record.Release()
						}
					}()

					rows, err := encodeArrowBatches(c.schema, batches)
					if err != nil {
						for _, sendBatch := range sendBatches {
							sendBatch.err <- err
							close(sendBatch.err)
						}
						return
					}

					if _, err = c.c.ingest(ctx, &ingestRequest{
						Data: &ingestData{
							Format: writeFormatArrow,
							Rows:   string(rows),
						},
						Statement: c.transforms,
					}); err != nil {
						for _, sendBatch := range sendBatches {
							sendBatch.err <- err
							close(sendBatch.err)
						}
						return
					}

					for _, sendBatch := range sendBatches {
						close(sendBatch.err)
					}
				}()

				tick = false
				c.currentSize = 0
				c.sendBatches = make([]*arrowSendRecord, 0)
			}

			if stop {
				break
			}

			select {
			case <-ticker:
				if len(c.sendBatches) > 0 {
					tick = true
				}
			case sendBatch, more := <-c.sendBatchCh:
				if !more {
					stop = true
					continue
				}

				if !sendBatch.record.Schema().Equal(c.schema) {
					sendBatch.err <- errors.New("schema mismatch")
					close(sendBatch.err)
					continue
				}

				for _, col := range sendBatch.record.Columns() {
					size := col.Data().SizeInBytes()
					if size > math.MaxUint64-c.currentSize {
						c.currentSize = math.MaxUint64
						break
					}
					c.currentSize += size
				}

				c.sendBatches = append(c.sendBatches, sendBatch)
			}
		}
	}()
}

// Send sends an Arrow RecordBatch to the cable. The record must have the same schema
// as the one provided when creating the cable.
//
// The ownership of the record is transferred to the cable, and the record will be released
// after it is sent. The caller must not use/release the record after sending it.
//
// Returns a channel that will be closed when the batch is sent to ScopeDB, or an error occurs.
func (c *ArrowBatchCable) Send(record arrow.Record) <-chan error {
	sendBatch := &arrowSendRecord{
		record: record,
		err:    make(chan error, 1),
	}
	if sendBatch.record == nil {
		sendBatch.err <- errors.New("nil batch")
		close(sendBatch.err)
		return sendBatch.err
	}
	c.sendBatchCh <- sendBatch
	return sendBatch.err
}

// Close closes the ArrowBatchCable and stops sending batches.
func (c *ArrowBatchCable) Close() {
	close(c.sendBatchCh)
}

// VariantBatchCable is a cable for sending any records as variant data to ScopeDB.
//
// You can create an VariantBatchCable using the Client's VariantBatchCable method,
// and start it using the Start method.
//
// Then, you can send any records using the Send method. Once the staged batches
// reach the specified BatchSize or BatchInterval, they will be sent to ScopeDB.
//
// The records sent must be JSON-serializable.
type VariantBatchCable struct {
	c *Client

	transforms  string
	currentSize uint64
	sendBatches []*variantSendRecord
	sendBatchCh chan *variantSendRecord

	BatchSize     uint64
	BatchInterval time.Duration
}

type variantSendRecord struct {
	payload string
	err     chan error
}

// VariantBatchCable creates a new VariantBatchCable with the specified transforms.
//
// The cable must be started before sending batches. All the records sent must be JSON-serializable.
//
// The transforms are ScopeQL statements that assume the data sent as the source table. The schema
// of the source table is a one-column variant table. The transforms must end with an INSERT statement.
// For example:
//
//	SELECT $0["col1"], $0["col2"]
//	INSERT INTO my_table (col1, col2)
func (c *Client) VariantBatchCable(transforms string) *VariantBatchCable {
	cable := &VariantBatchCable{
		c:             c,
		transforms:    transforms,
		currentSize:   0,
		sendBatches:   make([]*variantSendRecord, 0),
		sendBatchCh:   make(chan *variantSendRecord),
		BatchSize:     defaultBatchSize,
		BatchInterval: defaultBatchInterval,
	}

	return cable
}

// Start starts the VariantBatchCable background task.
//
// It will receive batches that users Send, package them based on the BatchSize and BatchInterval,
// and send them to ScopeDB.
func (c *VariantBatchCable) Start(ctx context.Context) {
	ticker := time.Tick(c.BatchInterval)
	batchSize := c.BatchSize

	go func() {
		stop, tick := false, false
		for {
			if tick || c.currentSize > batchSize {
				sendBatches := c.sendBatches
				go func() {
					rows := ""
					for _, sendBatch := range sendBatches {
						if rows != "" {
							rows += "\n"
						}
						rows += sendBatch.payload
					}

					if _, err := c.c.ingest(ctx, &ingestRequest{
						Data: &ingestData{
							Format: writeFormatJSON,
							Rows:   rows,
						},
						Statement: c.transforms,
					}); err != nil {
						for _, sendBatch := range sendBatches {
							sendBatch.err <- err
							close(sendBatch.err)
						}
						return
					}

					for _, sendBatch := range sendBatches {
						close(sendBatch.err)
					}
				}()

				tick = false
				c.currentSize = 0
				c.sendBatches = make([]*variantSendRecord, 0)
			}

			if stop {
				break
			}

			select {
			case <-ticker:
				if len(c.sendBatches) > 0 {
					tick = true
				}
			case sendBatch, more := <-c.sendBatchCh:
				if !more {
					stop = true
					continue
				}

				size := uint64(len(sendBatch.payload))
				if size > math.MaxUint64-c.currentSize {
					c.currentSize = math.MaxUint64
				} else {
					c.currentSize += size
				}
				c.sendBatches = append(c.sendBatches, sendBatch)
			}
		}
	}()
}

// Send sends a JSON-serializable record to the cable.
//
// Returns a channel that will be closed when the record is sent to ScopeDB, or an error occurs.
func (c *VariantBatchCable) Send(record any) <-chan error {
	errCh := make(chan error, 1)

	bs, err := json.Marshal(record)
	if err != nil {
		errCh <- err
		close(errCh)
		return errCh
	}

	var buf bytes.Buffer
	if err := json.Compact(&buf, bs); err != nil {
		errCh <- err
		close(errCh)
		return errCh
	}

	sendBatch := &variantSendRecord{
		payload: buf.String(),
		err:     errCh,
	}
	c.sendBatchCh <- sendBatch
	return sendBatch.err
}

// Close closes the VariantBatchCable and stops sending batches.
func (c *VariantBatchCable) Close() {
	close(c.sendBatchCh)
}
