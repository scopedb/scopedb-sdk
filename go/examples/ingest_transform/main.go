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

// Package main demonstrates the advanced IngestStream path for source JSON
// that needs a server-side ScopeQL transformation before it matches a table.
// Most applications should use Table.AppendStream.
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
	statement := fmt.Sprintf(`
		SELECT
			$0["id"]::int,
			$0["event_id"]::string,
			$0["ts"]::timestamp,
			$0["name"]::string,
			$0["attributes"]::object
		INSERT INTO %s (id, event_id, occurred_at, name, attributes)
	`, table.Identifier())
	stream, err := client.IngestStream(statement, scopedb.IngestStreamOptions{
		TargetBatchBytes: 4 * 1024 * 1024,
		MaxBatchRows:     10_000,
		FlushInterval:    time.Second,
		MaxBufferedBytes: 32 * 1024 * 1024,
	})
	if err != nil {
		return err
	}

	now := time.Now().UTC()
	for id := 1; id <= 2; id++ {
		row := map[string]any{
			"id":         id,
			"event_id":   fmt.Sprintf("go-transform-%d-%d", now.UnixNano(), id),
			"ts":         now.Format(time.RFC3339Nano),
			"name":       "example.transform",
			"attributes": map[string]any{"source": "go"},
		}
		// Send confirms local admission only; Shutdown is the commit barrier.
		if err := stream.Send(ctx, row); err != nil {
			_, _ = stream.Shutdown(ctx)
			return err
		}
	}

	result, err := stream.Shutdown(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("inserted %d transformed rows into %s\n", result.NumRowsInserted, table.Identifier())
	return nil
}
