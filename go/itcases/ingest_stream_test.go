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

package itcases

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gkampitakis/go-snaps/snaps"
	scopedb "github.com/scopedb/scopedb-sdk/go"
	"github.com/stretchr/testify/require"
)

func TestIngestStream(t *testing.T) {
	c := NewClient(t)
	defer c.Close()

	ctx := context.Background()
	tbl := c.Table(RandomName(t))
	_, err := c.Statement(fmt.Sprintf(`
		CREATE TABLE %s (
			ts timestamp,
			name string,
			var object,
		)
	`, tbl.Identifier())).Execute(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, tbl.Drop(ctx))
	}()

	stream, err := c.IngestStream(fmt.Sprintf(`
		SELECT
			$0["ts"]::timestamp as ts,
			$0["name"]::string as name,
			$0,
		WHERE LENGTH(name) > 0
		INSERT INTO %s (ts, name, var)
	`, tbl.Identifier()), scopedb.IngestStreamOptions{
		MaxBatchRows:  1,
		FlushInterval: time.Hour,
	})
	require.NoError(t, err)

	type TestData struct {
		TS        int64  `json:"ts"`
		Name      string `json:"name"`
		Arbitrary any    `json:"arbitrary"`
	}

	require.NoError(t, stream.Send(ctx, TestData{
		TS:        335503360000000,
		Name:      "tison",
		Arbitrary: 27,
	}))
	require.NoError(t, stream.Send(ctx, TestData{
		TS:        315360000000000,
		Name:      "scopedb",
		Arbitrary: "Schema On The Fly",
	}))
	result, err := stream.Shutdown(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(2), result.NumRowsInserted)

	s := c.Statement(fmt.Sprintf(`FROM %s ORDER BY ts`, tbl.Identifier()))
	queryResult, err := s.Execute(ctx)
	require.NoError(t, err)

	records, err := queryResult.ToValues()
	require.NoError(t, err)

	snaps.MatchSnapshot(t, queryResult.Schema)
	snaps.MatchSnapshot(t, records)
}
