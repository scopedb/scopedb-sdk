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

// Package main demonstrates one caller-encoded raw NDJSON table append.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

	now := time.Now().UTC()
	rows := []any{
		map[string]any{
			"id":          1,
			"event_id":    fmt.Sprintf("go-direct-%d-1", now.UnixNano()),
			"occurred_at": now.Format(time.RFC3339Nano),
			"name":        "example.direct",
			"attributes":  map[string]any{"source": "go"},
		},
		map[string]any{
			"id":          2,
			"event_id":    fmt.Sprintf("go-direct-%d-2", now.UnixNano()),
			"occurred_at": now.Format(time.RFC3339Nano),
			"name":        "example.direct",
			"attributes":  map[string]any{"source": "go"},
		},
	}

	var ndjson bytes.Buffer
	encoder := json.NewEncoder(&ndjson)
	for _, row := range rows {
		if err := encoder.Encode(row); err != nil {
			return err
		}
	}

	result, err := table.AppendNDJSON(ctx, ndjson.Bytes())
	if err != nil {
		var scopeErr *scopedb.Error
		if errors.As(err, &scopeErr) && scopeErr.AppendDetails != nil &&
			scopeErr.AppendDetails.AppendState == scopedb.AppendStateUnknown {
			log.Print("append may have committed; reconcile before replaying this payload")
		}
		return err
	}
	fmt.Printf("committed %d rows to %s\n", result.NumRowsInserted, table.Identifier())
	return nil
}
