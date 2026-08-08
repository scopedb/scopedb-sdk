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
Package scopedb provides a Go client for ScopeQL statements, REST catalog
discovery, and streaming writes.

Create and close one Client for the application's connection pool:

	client, err := scopedb.NewClient(scopedb.Config{
		Endpoint: os.Getenv("SCOPEDB_ENDPOINT"),
		APIKey:   os.Getenv("SCOPEDB_API_KEY"),
	})
	if err != nil {
		return err
	}
	defer client.Close()

Query waits for a statement result, which can be converted to keyed objects:

	result, err := client.Query(ctx, "SELECT 1 AS ready")
	if err != nil {
		return err
	}
	rows, err := result.ToObjects()
	if err != nil {
		return err
	}
	fmt.Println(rows)

Use Statement.Submit when an application needs the statement ID, a local status
snapshot, an explicit remote status request, or a separate wait. ExecTimeout is
the only optional statement execution setting. Failed statements expose
structured server error details on Error.StatementDetails. Catalog iterators
lazily traverse REST pages. Table.Describe returns table metadata, Table.Append
sends one caller-owned NDJSON request, and Table.AppendStream adds bounded
asynchronous batching that is safe for concurrent producers. Client.IngestStream
is the secondary path when input JSON needs a ScopeQL transformation before
insertion.

Stream Send methods confirm local admission only. Flush and Shutdown wait for
the accepted prefix. AppendStream reports rejected and unknown outcomes;
an IngestStream error can also follow a remote commit, so callers must
reconcile before replaying the same records. A nonzero ingest result returned
with an error covers only earlier confirmed batches, not a safe replay offset.

ScopeQL is documented separately in the [quickstart], [query guide], and
[language reference].

[quickstart]: https://docs.scopedb.io/guides/quickstart
[query guide]: https://docs.scopedb.io/guides/query-events
[language reference]: https://docs.scopedb.io/reference/
*/
package scopedb
