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
	scopedb "github.com/scopedb/scopedb-sdk/go/v0"
)

func main() {
	conn := scopedb.Open(&scopedb.Config{
		Endpoint: "http://localhost:6543",
	})

	// Query data from ScopeDB
	resultSet, err := conn.QueryAsArrowBatch(context.Background(), &scopedb.StatementRequest{
		Statement:   "read information_schema.tables",
		WaitTimeout: "60s",
		Format:      scopedb.ArrowJSONFormat,
	})
	if err != nil {
		panic(err)
	}

	fmt.Printf("%v\n", resultSet.StatementId)
	fmt.Printf("%v\n", resultSet.Metadata)
	fmt.Printf("%v\n", resultSet.Records)
}
