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

// Package main demonstrates strict bounded asynchronous table appends.
package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	scopedb "github.com/scopedb/scopedb-sdk/go"
	"github.com/scopedb/scopedb-sdk/go/examples/internal/exampleutil"
)

// Event is one typed row written to the example destination table.
type Event struct {
	ID         int               `json:"id"`
	EventID    string            `json:"event_id"`
	OccurredAt time.Time         `json:"occurred_at"`
	Name       string            `json:"name"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	client, err := exampleutil.NewClient()
	if err != nil {
		return err
	}
	defer client.Close()

	table, err := exampleutil.WriteTable(client)
	if err != nil {
		return err
	}
	stream, err := table.AppendStream(scopedb.AppendStreamOptions{
		FailurePolicy:        scopedb.AppendFailureStop,
		TargetBatchBytes:     4 * 1024 * 1024,
		MaxBatchRows:         10_000,
		FlushInterval:        time.Second,
		MaxBufferedBytes:     64 * 1024 * 1024,
		MaxConcurrentBatches: 4,
		AttemptTimeout:       30 * time.Second,
	})
	if err != nil {
		return err
	}

	now := time.Now().UTC()
	if err := sendConcurrently(ctx, stream, now); err != nil {
		_, _ = stream.Shutdown(ctx)
		return err
	}

	// Flush is a remote commit barrier for the accepted prefix in strict mode.
	report, err := stream.Flush(ctx)
	if err != nil {
		_, _ = stream.Shutdown(ctx)
		return err
	}
	fmt.Printf("flush committed %d of %d accepted rows\n", report.CommittedRows, report.AcceptedRows)

	// Flush keeps the stream open, so later rows belong to the next barrier.
	if err := stream.Send(ctx, Event{
		ID:         101,
		EventID:    fmt.Sprintf("go-stream-%d-101", now.UnixNano()),
		OccurredAt: now,
		Name:       "example.stream",
		Attributes: map[string]string{"source": "go"},
	}); err != nil {
		_, _ = stream.Shutdown(ctx)
		return err
	}

	// Shutdown closes admission and settles any rows accepted after the flush.
	finalReport, err := stream.Shutdown(ctx)
	if err != nil {
		return err
	}
	if finalReport.Outcome != scopedb.AppendDeliveryOK {
		return fmt.Errorf("shutdown did not settle cleanly: %+v", finalReport)
	}
	fmt.Printf("shutdown committed %d additional rows\n", finalReport.CommittedRows)
	return nil
}

func sendConcurrently(ctx context.Context, stream *scopedb.AppendStream, now time.Time) error {
	const producerCount = 4

	var producers sync.WaitGroup
	errors := make(chan error, producerCount)
	for producer := range producerCount {
		producers.Add(1)
		go func() {
			defer producers.Done()
			for id := producer + 1; id <= 100; id += producerCount {
				row := Event{
					ID:         id,
					EventID:    fmt.Sprintf("go-stream-%d-%d", now.UnixNano(), id),
					OccurredAt: now,
					Name:       "example.stream",
					Attributes: map[string]string{"source": "go"},
				}
				// Send is safe for concurrent producers and waits for bounded
				// local capacity. It does not wait for a remote commit.
				if err := stream.Send(ctx, row); err != nil {
					errors <- err
					return
				}
			}
		}()
	}
	producers.Wait()
	close(errors)
	for err := range errors {
		return err
	}
	return nil
}
