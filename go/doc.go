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

/*
Package scopedb provides a lightweight and easy-to-use client for interacting with a ScopeDB cluster.

# Connection String

Use the Open() function to create a database handle with connection parameters:

	conn, err := scopedb.Open(&scopedb.Config{
		Endpoint: "http://<scopedb-host>:<scopedb-port:-6543>",
	})

# Ingest Data

Use the Ingest() method to ingest data with a statement:

	records := makeArrowRecords()
	err := conn.IngestArrowBatch(ctx, records, "INSERT INTO target_table")

Alternatively, use [MERGE INTO] to upsert data:

	err := ingester.Commit(ctx, `
	MERGE INTO target_table ON $0 = target_table.a
	WHEN MATCHED THEN UPDATE ALL
	WHEN NOT MATCHED THEN INSERT ALL
	`)

# Query Data

Currently, the only supported result format is ArrowJSONFormat, which encodes the result table
in Arrow format with variant rendered as JSON, and then encodes the result bytes with BASE64.

1. QueryAsArrowBatch

Use the QueryAsArrowBatch() method as an one-for-all API to submit a statement and fetch the result
as decoded Arrow batches:

	rs, err := conn.QueryAsArrowBatch(ctx,  &scopedb.StatementRequest{
		Statement: "FROM table WHERE ts > '2022-01-01'::timestamp",
		Format:    scopedb.ArrowJSONFormat,
	})

2. SubmitQuery and ResultSetFetcher

Use the SubmitQuery() method to submits a query to the server and returns immediately.

	resp, err := conn.SubmitQuery(ctx, "FROM table")

The Status of the returned StatementResponse may be QueryStatusFinished if the query is finished immediately.
If so, the result set is in ArrowJSONFormat. Otherwise, you can fetch the result set by NewResultSetFetcher
and calling its FetchResultSet method.

	f := NewResultSetFetcher(conn, &FetchStatementParams{
	    StatementId: resp.StatementId,
	    Format:      scopedb.ArrowJSONFormat,
	})

	// FetchResultSet blocking loops until the query is finished.
	// By default, it waits for up to 60 seconds.
	rs, err := f.FetchResultSet(ctx)
	if err != nil {
	    return nil, err
	}
	return resp.ToArrowResultSet()

# Execute Statement

Use the Execute() method to submit a statement to the server, wait for it to finish, and ignore the result:

	err := conn.Execute(ctx, &scopedb.StatementRequest{
		Statement: "CREATE TABLE t (n INT, v VARIANT)",
		// Placeholder only, the result is ignored.
		Format:    scopedb.ArrowJSONFormat,
	})

[MERGE INTO]: https://www.scopedb.io/reference/stmt-dml#merge
*/
package scopedb
