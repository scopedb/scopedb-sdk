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

// Package main demonstrates observable best-effort telemetry delivery.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	scopedb "github.com/scopedb/scopedb-sdk/go"
	"github.com/scopedb/scopedb-sdk/go/examples/internal/exampleutil"
)

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
		FailurePolicy:        scopedb.AppendFailureContinue,
		FlushInterval:        time.Second,
		MaxBufferedBytes:     16 * 1024 * 1024,
		MaxConcurrentBatches: 4,
		AttemptTimeout:       5 * time.Second,
	})
	if err != nil {
		return err
	}

	startedAt := time.Now().UTC()
	for id := 0; id < 1_000; id++ {
		row := map[string]any{
			"id":          id,
			"event_id":    fmt.Sprintf("go-telemetry-%d-%d", startedAt.UnixNano(), id),
			"occurred_at": startedAt.Format(time.RFC3339Nano),
			"name":        "request.completed",
			"attributes":  map[string]any{"status": 200},
		}
		// TrySend does not wait for capacity. A nil error means local admission,
		// not remote commit. Send this diagnostic to a different sink.
		if err := stream.TrySend(row); err != nil {
			log.Printf("telemetry row dropped locally: %v", err)
		}
	}

	// Continue mode settles later batches after a failure. The report makes
	// loss and ambiguous outcomes visible; it is not a durable replay queue.
	report, shutdownErr := stream.Shutdown(ctx)
	stats := stream.Stats()
	fmt.Printf(
		"accepted=%d committed=%d failed=%d unknown=%d dropped=%d retries=%d\n",
		stats.AcceptedRows,
		stats.CommittedRows,
		stats.FailedRows,
		stats.UnknownRows,
		stats.DroppedRows,
		stats.Retries,
	)
	if shutdownErr != nil {
		return shutdownErr
	}
	if report.Outcome != scopedb.AppendDeliveryOK {
		log.Printf("telemetry loss or ambiguity: %+v", report)
		// Unknown rows may already be committed. Reconcile them; never replay
		// the same payload blindly.
	}
	return nil
}
