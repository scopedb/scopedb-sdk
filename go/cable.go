package scopedb

import (
	"context"
	"errors"
	"github.com/apache/arrow/go/v17/arrow"
	"math"
	"time"
)

type ArrowBatchCable struct {
	c *Client

	schema      *arrow.Schema
	transforms  string
	currentSize uint64
	sendBatches []*arrowSendBatch
	sendBatchCh chan *arrowSendBatch

	BatchSize     uint64
	BatchInterval time.Duration
}

type arrowSendBatch struct {
	batch arrow.Record

	err  chan error
	done chan struct{}
}

func (c *Client) ArrowBatchCable(schema *arrow.Schema, transforms string) *ArrowBatchCable {
	cable := &ArrowBatchCable{
		c:             c,
		schema:        schema,
		transforms:    transforms,
		currentSize:   0,
		sendBatches:   make([]*arrowSendBatch, 0),
		sendBatchCh:   make(chan *arrowSendBatch),
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
						batches = append(batches, sendBatch.batch)
					}

					rows, err := encodeArrowBatches(c.schema, batches)
					if err != nil {
						for _, sendBatch := range sendBatches {
							sendBatch.err <- err
							close(sendBatch.done)
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
							close(sendBatch.done)
						}
						return
					}

					for _, sendBatch := range sendBatches {
						close(sendBatch.err)
						close(sendBatch.done)
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

				if sendBatch.batch == nil {
					continue
				}

				if !sendBatch.batch.Schema().Equal(c.schema) {
					sendBatch.err <- errors.New("schema mismatch")
					continue
				}

				for _, col := range sendBatch.batch.Columns() {
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

func (c *ArrowBatchCable) Send(batch arrow.Record) (<-chan struct{}, <-chan error) {
	sendBatch := &arrowSendBatch{
		batch: batch,
		err:   make(chan error, 1),
		done:  make(chan struct{}, 1),
	}
	c.sendBatchCh <- sendBatch
	return sendBatch.done, sendBatch.err
}

func (c *ArrowBatchCable) Close() {
	close(c.sendBatchCh)
}
