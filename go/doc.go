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
Package scopedb provides a lightweight and easy-to-use client for interacting with a ScopeDB service.

# Client

Use NewClient to create a client struct. This is the major entrance to construct structs for interacting with ScopeDB:

	client := scopedb.NewClient(&scopedb.Config{
		Endpoint: "http://<scopedb-host>:<scopedb-port:-6543>",
	})

# Write Data via Cables

Use VariantBatchCable or ArrowBatchCable to write data to ScopeDB:

	cable := c.VariantBatchCable(fmt.Sprintf(`
		SELECT $0["ts"], $0["v"]
		INSERT INTO %s (ts, v)
	`, tbl.Identifier()))
	cable.Start(ctx)
	defer cable.Close()

	resCh := cable.Send(struct {
		TS int64 `json:"ts"`
		V  any   `json:"v"`
	}{
		TS: -1024,
		V:  "scopedb",
	})

# Query Data

Create a Statement and submit or execute it to get a result set:

	s := c.Statement(fmt.Sprintf(`FROM %s ORDER BY ts`, tbl.Identifier()))
	result, err := s.Execute(ctx)
	if err != nil {
		return err
	}
	values := result.ToValues()
*/
package scopedb
