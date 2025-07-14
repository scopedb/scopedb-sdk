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
	"math"
	"time"
)

const (
	defaultBatchSize     = 16 * 1024 * 1024 // default to 16 MiB
	defaultBatchInterval = time.Second      // default to 1 second
)

// DataCable is a cable for sending any records as raw data to ScopeDB.
//
// You can create an DataCable using the Client's DataCable method,
// and start it using the Start method.
//
// Then, you can send any records using the Send method. Once the staged batches
// reach the specified BatchSize or BatchInterval, they will be sent to ScopeDB.
//
// The records sent should be JSON-serializable.
type DataCable struct {
	c *Client

	transforms  string
	currentSize uint64
	sendBatches []*dataSendRecord
	sendBatchCh chan *dataSendRecord

	// AutoCommit indicates whether the cable should automatically commit the batches
	AutoCommit bool
	// BatchSize is the maximum size in bytes of the batches to be sent.
	BatchSize uint64
	// BatchInterval is the maximum time to wait before sending the batches.
	BatchInterval time.Duration
}

type dataSendRecord struct {
	payload string
	err     chan error
}

// DataCable creates a new DataCable with the specified transforms.
//
// The cable must be started before sending batches. All the records sent should be JSON-serializable.
//
// The transforms are ScopeQL statements that assume the data sent as the source table. The schema
// of the source table is a one-column (of the "any" type) table. The transforms must end with an
// INSERT statement. For example:
//
//	SELECT $0["col1"]::int, $0["col2"]::string, $0
//	INSERT INTO my_table (col1, col2, v)
func (c *Client) DataCable(transforms string) *DataCable {
	cable := &DataCable{
		c:             c,
		transforms:    transforms,
		currentSize:   0,
		sendBatches:   nil,
		sendBatchCh:   make(chan *dataSendRecord),
		AutoCommit:    false,
		BatchSize:     defaultBatchSize,
		BatchInterval: defaultBatchInterval,
	}

	return cable
}

// Start starts the DataCable background task.
//
// It will receive batches that users Send, package them based on the BatchSize and BatchInterval,
// and send them to ScopeDB.
func (c *DataCable) Start(ctx context.Context) {
	ticker := time.Tick(c.BatchInterval)

	batchSize := c.BatchSize
	ingestType := writeTypeBuffered
	if c.AutoCommit {
		ingestType = writeTypeCommitted
	}

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
						Data: ingestData{
							Format: writeFormatJSON,
							Rows:   rows,
						},
						Type:      ingestType,
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
				c.sendBatches = nil
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

// Send sends a record to the cable. The record should be JSON-serializable.
//
// Returns a channel that will be closed when the record is sent to ScopeDB, or an error occurs.
func (c *DataCable) Send(record any) <-chan error {
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

	sendBatch := &dataSendRecord{
		payload: buf.String(),
		err:     errCh,
	}
	c.sendBatchCh <- sendBatch
	return sendBatch.err
}

// Close closes the DataCable and stops sending batches.
func (c *DataCable) Close() {
	close(c.sendBatchCh)
}
