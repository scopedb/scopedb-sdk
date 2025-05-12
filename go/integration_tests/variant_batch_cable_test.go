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

package integration_tests

import (
	"context"
	"fmt"
	"testing"

	scopedb "github.com/scopedb/scopedb-sdk/go"
	"github.com/stretchr/testify/require"
)

func TestVariantBatchCable(t *testing.T) {
	ctx := context.Background()
	client := scopedb.NewClient(&scopedb.Config{
		Endpoint: "http://localhost:6543",
	})
	defer client.Close()

	_, err := client.Statement("DROP TABLE IF EXISTS variants").Execute(ctx)
	require.NoError(t, err)
	_, err = client.Statement("CREATE TABLE variants (i INT, v VARIANT)").Execute(ctx)
	require.NoError(t, err)

	cable := client.VariantBatchCable(`
	SELECT $0["i"], $0["v"]
	INSERT INTO variants (i, v)
	`)
	// cable.BatchSize = 0 // immediately flush
	cable.Start(ctx)
	defer cable.Close()

	require.NoError(t, <-cable.Send(struct {
		I int64 `json:"i"`
		V any   `json:"v"`
	}{
		I: 101,
		V: "scopedb",
	}))

	require.NoError(t, <-cable.Send(struct {
		I int64 `json:"i"`
		V any   `json:"v"`
	}{
		I: 102,
		V: 42.1,
	}))

	s := client.Statement("FROM variants")
	s.ResultFormat = scopedb.ResultFormatArrow
	result, err := s.Execute(ctx)
	require.NoError(t, err)

	records, err := result.ToArrowBatch()
	require.NoError(t, err)

	fmt.Printf("%v\n", records)
}
