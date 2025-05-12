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

type ArrowBatchCable struct {
	c *Client

	schema      *arrow.Schema
	transforms  string
	currentSize uint64
	sendBatches []*arrowSendRecord
	sendBatchCh chan *arrowSendRecord

	BatchSize     uint64
	BatchInterval time.Duration
}

type arrowSendRecord struct {
	record arrow.Record
	err    chan error
}

func (c *Client) ArrowBatchCable(schema *arrow.Schema, transforms string) *ArrowBatchCable {
	cable := &ArrowBatchCable{
		c:             c,
		schema:        schema,
		transforms:    transforms,
		currentSize:   0,
		sendBatches:   make([]*arrowSendRecord, 0),
		sendBatchCh:   make(chan *arrowSendRecord),
		BatchSize:     1024 * 1024, // default to 1MiB
		BatchInterval: time.Second, // default to 1 second
	}

	return cable
}

func (c *ArrowBatchCable) Start(ctx context.Context) {
	go func() {
		ticker := time.Tick(c.BatchInterval)

		stop, tick := false, false
		for {
			if tick || c.currentSize > c.BatchSize {
				sendBatches := c.sendBatches
				go func() {
					batches := make([]arrow.Record, 0, len(sendBatches))
					for _, sendBatch := range sendBatches {
						batches = append(batches, sendBatch.record)
					}

					rows, err := encodeArrowBatches(c.schema, batches)
					if err != nil {
						for _, sendBatch := range sendBatches {
							sendBatch.err <- err
							close(sendBatch.err)
						}
						return
					}

					_, err = c.c.ingest(ctx, &ingestRequest{
						Data: &ingestData{
							Format: writeFormatArrow,
							Rows:   string(rows),
						},
						Statement: c.transforms,
					})
					if err != nil {
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
				c.sendBatches = c.sendBatches[:0]
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

func (c *ArrowBatchCable) Close() {
	close(c.sendBatchCh)
}

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

func (c *Client) VariantBatchCable(transforms string) *VariantBatchCable {
	cable := &VariantBatchCable{
		c:             c,
		transforms:    transforms,
		currentSize:   0,
		sendBatches:   make([]*variantSendRecord, 0),
		sendBatchCh:   make(chan *variantSendRecord),
		BatchSize:     1024 * 1024, // default to 1MiB
		BatchInterval: time.Second, // default to 1 second
	}

	return cable
}

func (c *VariantBatchCable) Start(ctx context.Context) {
	go func() {
		ticker := time.Tick(c.BatchInterval)

		stop, tick := false, false
		for {
			if tick || c.currentSize > c.BatchSize {
				sendBatches := c.sendBatches
				go func() {
					rows := ""
					for _, sendBatch := range sendBatches {
						if rows != "" {
							rows += "\n"
						}
						rows += sendBatch.payload
					}
					_, err := c.c.ingest(ctx, &ingestRequest{
						Data: &ingestData{
							Format: writeFormatJSON,
							Rows:   rows,
						},
						Statement: c.transforms,
					})
					if err != nil {
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
				c.sendBatches = c.sendBatches[:0]
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

func (c *VariantBatchCable) Close() {
	close(c.sendBatchCh)
}
