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

	"github.com/gkampitakis/go-snaps/snaps"
	"github.com/stretchr/testify/require"
)

func TestTableSchema(t *testing.T) {
	c := NewClient(t)
	defer c.Close()

	ctx := context.Background()
	tbl := c.Table(RandomName(t))
	_, err := c.Statement(fmt.Sprintf(`
		CREATE TABLE %s (
			i INT,
			u UINT,
			f FLOAT,
			s STRING,
			b BOOLEAN,
			ts TIMESTAMP,
			var any,
		)
	`, tbl.Identifier())).Execute(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, tbl.Drop(ctx))
	}()

	schema, err := tbl.TableSchema(ctx)
	require.NoError(t, err)
	snaps.MatchSnapshot(t, schema)
}
