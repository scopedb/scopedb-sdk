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

// Package main demonstrates bounded streaming writes for a bulk source.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	scopedb "github.com/scopedb/scopedb-sdk/go"
	"github.com/scopedb/scopedb-sdk/go/examples/internal/exampleutil"
)

const totalRows = 25_000

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
		MaxBatchRows:         5_000,
		FlushInterval:        time.Second,
		MaxBufferedBytes:     32 * 1024 * 1024,
		MaxConcurrentBatches: 4,
		AttemptTimeout:       30 * time.Second,
	})
	if err != nil {
		return err
	}

	startedAt := time.Now().UTC()
	// Replace this generated loop with a sequential file or queue reader. Do
	// not start one goroutine per row; Send already provides bounded admission.
	for id := 0; id < totalRows; id++ {
		row := map[string]any{
			"id":          id,
			"event_id":    fmt.Sprintf("go-bulk-%d-%d", startedAt.UnixNano(), id),
			"occurred_at": startedAt.Format(time.RFC3339Nano),
			"name":        "example.bulk",
			"attributes":  map[string]any{"source": "go"},
		}
		if err := stream.Send(ctx, row); err != nil {
			_, _ = stream.Shutdown(ctx)
			return err
		}
	}

	report, err := stream.Shutdown(ctx)
	if err != nil {
		return err
	}
	fmt.Printf(
		"accepted=%d committed=%d retries=%d duration=%s\n",
		report.AcceptedRows,
		report.CommittedRows,
		report.Retries,
		report.Duration,
	)
	return nil
}
