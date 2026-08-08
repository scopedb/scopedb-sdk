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

// Package main demonstrates synchronous and asynchronous statement queries.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/google/uuid"
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

	result, err := client.Query(ctx, "SELECT 1 AS ready")
	if err != nil {
		return err
	}
	row, found, err := result.First()
	if err != nil {
		return err
	}
	if found {
		fmt.Printf("query result: %#v\n", row)
	}

	statement := client.Statement("SELECT 2 AS value")
	statementID := uuid.New() // Optional: omit ID to let ScopeDB generate one.
	statement.ID = &statementID
	statement.ExecTimeout = "30s"
	handle, err := statement.Submit(ctx)
	if err != nil {
		return err
	}
	fmt.Println("statement ID:", handle.ID())
	if cached := handle.LastStatus(); cached != nil {
		fmt.Println("cached status:", *cached)
	}

	latest, err := handle.Status(ctx)
	if err != nil {
		return err
	}
	fmt.Println("latest status:", latest)

	result, err = handle.Wait(ctx)
	if err != nil {
		return err
	}
	rows, err := result.ToObjects()
	if err != nil {
		return err
	}
	fmt.Printf("wait result: %#v\n", rows)
	return nil
}
