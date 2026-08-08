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

// Package main demonstrates lazy REST catalog discovery.
package main

import (
	"context"
	"fmt"
	"log"

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

	fmt.Println("databases:")
	for database, err := range client.IterateDatabases(ctx, scopedb.CatalogListOptions{
		PageSize: 100,
	}) {
		if err != nil {
			return err
		}
		fmt.Println(" -", database.Name)
	}

	fmt.Printf("tables in %s.%s:\n", exampleutil.Database(), exampleutil.Schema())
	for summary, err := range client.IterateTables(
		ctx,
		exampleutil.Database(),
		exampleutil.Schema(),
		scopedb.CatalogListOptions{PageSize: 100},
	) {
		if err != nil {
			return err
		}
		fmt.Println(" -", summary.Name)

		table := client.Table(summary.Name)
		table.Database = summary.Database
		table.Schema = summary.Schema
		description, err := table.Describe(ctx)
		if err != nil {
			return err
		}
		fmt.Printf("   first table has %d columns\n", len(description.Columns))
		break
	}

	return nil
}
