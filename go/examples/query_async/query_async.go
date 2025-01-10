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

package main

import (
	"context"
	"fmt"
	scopedb "github.com/scopedb/scopedb-sdk/go"
)

func main() {
	conn := scopedb.Open(&scopedb.Config{
		Endpoint: "http://localhost:6543",
	})

	ctx := context.Background()

	// Submit the statement to ScopeDB
	response, err := conn.SubmitStatement(ctx, &scopedb.StatementRequest{
		Statement: "from system.tables",
		Format:    scopedb.ArrowJSONFormat,
	})
	if err != nil {
		panic(err)
	}

	// Build ResultSetFetcher
	respCh := make(chan *scopedb.StatementResponse, 1)
	f := scopedb.NewResultSetFetcher(conn, &scopedb.FetchStatementParams{
		StatementId: response.StatementId,
		Format:      scopedb.ArrowJSONFormat,
	})

	go func() {
		for {
			resp, err := f.FetchResultSetOnce(ctx)
			if err != nil {
				panic(err)
			}

			if resp.Status == scopedb.StatementStatusFinished {
				respCh <- resp
			}
		}
	}()

	// Wait for the result
	resp := <-respCh
	resultSet, err := resp.ToArrowResultSet()
	if err != nil {
		panic(err)
	}
	fmt.Printf("%v\n", resultSet.StatementId)
	fmt.Printf("%v\n", resultSet.Metadata)
	fmt.Printf("%v\n", resultSet.Records)
}
